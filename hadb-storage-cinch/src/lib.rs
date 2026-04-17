//! `hadb-storage-cinch`: Cinch-protocol HTTP `StorageBackend`.
//!
//! Speaks the `/v1/sync/{prefix}/` routes that Cinch's Grabby (and engine
//! in embedded mode) exposes:
//!
//! ```text
//! GET    /v1/sync/{prefix}/{key}                 -> body
//! HEAD   /v1/sync/{prefix}/{key}                 -> headers only
//! PUT    /v1/sync/{prefix}/{key}   [Fence-Token] -> write
//! DELETE /v1/sync/{prefix}/{key}   [Fence-Token] -> delete
//! GET    /v1/sync/{prefix}?prefix=…&after=…      -> { "keys": [...] }
//! POST   /v1/sync/{prefix}/_delete [Fence-Token] -> batch delete
//! ```
//!
//! Authentication is via a Bearer token that scopes writes to a specific
//! database. Writes include a `Fence-Token` header whose value is the
//! current lease revision; a former leader's stale token is rejected by
//! the server before the write commits.
//!
//! # Fence
//!
//! If constructed with an `Arc<dyn FenceSource>` via
//! [`CinchHttpStorage::with_fence`], every write calls `fence.require()`
//! first and refuses (with `NoActiveLease`) to issue a write when no
//! lease is held. If no fence is configured, writes proceed without a
//! `Fence-Token` header: useful for single-writer deployments and tests.
//!
//! # CAS
//!
//! `put_if_absent` / `put_if_match` send standard HTTP `If-None-Match` /
//! `If-Match` headers. A 412 response is surfaced as
//! `CasResult { success: false }` and does not retry (CAS conflicts are
//! signals, not transients). 5xx / 429 / transport errors retry with
//! exponential backoff, matching the other write paths.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use reqwest::{RequestBuilder, StatusCode};
use serde::Deserialize;

use hadb_lease::FenceSource;
use hadb_storage::{CasResult, StorageBackend};

/// Default retry count for transient HTTP failures.
pub const DEFAULT_MAX_RETRIES: u32 = 3;

/// Default request timeout.
const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Characters that must be percent-encoded inside a URL path segment.
/// `/` is intentionally allowed so hierarchical keys (`p/d/0_v1`) pass
/// through unchanged; everything that could break path parsing (`?`, `#`,
/// `%`, space, etc.) gets encoded.
const PATH_UNSAFE: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'[')
    .add(b']')
    .add(b'\\')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

pub struct CinchHttpStorage {
    client: reqwest::Client,
    endpoint: String,
    token: String,
    /// URL path segment after `/v1/sync/`, e.g. `"pages"` for turbolite page
    /// storage, `"wal"` for walrust WAL frames.
    prefix: String,
    fence: Option<Arc<dyn FenceSource>>,
    max_retries: u32,
    fetch_count: Arc<AtomicU64>,
    fetch_bytes: Arc<AtomicU64>,
    put_count: Arc<AtomicU64>,
    put_bytes: Arc<AtomicU64>,
}

impl CinchHttpStorage {
    /// Construct a new client. `prefix` is the URL path segment after
    /// `/v1/sync/` (e.g. `"pages"`, `"wal"`).
    pub fn new(
        endpoint: impl Into<String>,
        token: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .build()
            .expect("reqwest client build should not fail with default config");
        Self::with_client(client, endpoint, token, prefix)
    }

    /// Construct with a caller-provided HTTP client. Useful for sharing
    /// connection pools, timeouts, or proxies across stores.
    pub fn with_client(
        client: reqwest::Client,
        endpoint: impl Into<String>,
        token: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Self {
        Self {
            client,
            endpoint: endpoint.into().trim_end_matches('/').to_string(),
            token: token.into(),
            prefix: prefix.into(),
            fence: None,
            max_retries: DEFAULT_MAX_RETRIES,
            fetch_count: Arc::new(AtomicU64::new(0)),
            fetch_bytes: Arc::new(AtomicU64::new(0)),
            put_count: Arc::new(AtomicU64::new(0)),
            put_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Attach a fence source. Writes will require a current lease revision
    /// and send `Fence-Token` headers. Without this, writes are unfenced.
    pub fn with_fence(mut self, fence: Arc<dyn FenceSource>) -> Self {
        self.fence = Some(fence);
        self
    }

    pub fn with_max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn bytes_fetched(&self) -> u64 {
        self.fetch_bytes.load(Ordering::Relaxed)
    }

    pub fn fetch_count(&self) -> u64 {
        self.fetch_count.load(Ordering::Relaxed)
    }

    pub fn bytes_put(&self) -> u64 {
        self.put_bytes.load(Ordering::Relaxed)
    }

    pub fn put_count(&self) -> u64 {
        self.put_count.load(Ordering::Relaxed)
    }

    fn object_url(&self, key: &str) -> String {
        let encoded_prefix = utf8_percent_encode(&self.prefix, PATH_UNSAFE);
        let encoded_key = utf8_percent_encode(key, PATH_UNSAFE);
        format!(
            "{}/v1/sync/{}/{}",
            self.endpoint, encoded_prefix, encoded_key
        )
    }

    fn list_url(&self) -> String {
        let encoded_prefix = utf8_percent_encode(&self.prefix, PATH_UNSAFE);
        format!("{}/v1/sync/{}", self.endpoint, encoded_prefix)
    }

    fn batch_delete_url(&self) -> String {
        let encoded_prefix = utf8_percent_encode(&self.prefix, PATH_UNSAFE);
        format!("{}/v1/sync/{}/_delete", self.endpoint, encoded_prefix)
    }

    /// Add `Fence-Token` header to a write. Returns an error if a fence is
    /// configured but no lease is currently held.
    fn apply_fence(&self, req: RequestBuilder) -> Result<RequestBuilder> {
        match &self.fence {
            Some(fence) => {
                let rev = fence
                    .require()
                    .map_err(|_| anyhow!("no active lease; refusing write"))?;
                Ok(req.header("Fence-Token", rev.to_string()))
            }
            None => Ok(req),
        }
    }

    async fn backoff(&self, attempt: u32) {
        let ms = 100u64.saturating_mul(1u64 << attempt.min(8));
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    fn is_transient_status(status: StatusCode) -> bool {
        status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS
    }
}

#[async_trait]
impl StorageBackend for CinchHttpStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let url = self.object_url(key);
        let mut attempt = 0u32;
        loop {
            let resp = self
                .client
                .get(&url)
                .bearer_auth(&self.token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status() == StatusCode::OK => {
                    let bytes = r.bytes().await?.to_vec();
                    self.fetch_count.fetch_add(1, Ordering::Relaxed);
                    self.fetch_bytes
                        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok(Some(bytes));
                }
                Ok(r) if r.status() == StatusCode::NOT_FOUND => return Ok(None),
                Ok(r) if Self::is_transient_status(r.status()) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!(
                            "GET {url} returned {} after {attempt} attempts",
                            r.status()
                        ));
                    }
                    self.backoff(attempt).await;
                }
                Ok(r) => {
                    let s = r.status();
                    let body = r.text().await.unwrap_or_default();
                    return Err(anyhow!("GET {url} returned {s}: {body}"));
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!(
                            "GET {url} transport failed after {attempt} attempts: {e}"
                        ));
                    }
                    self.backoff(attempt).await;
                }
            }
        }
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        let url = self.object_url(key);
        let len = data.len() as u64;
        let mut attempt = 0u32;
        loop {
            let req = self
                .client
                .put(&url)
                .bearer_auth(&self.token)
                .body(data.to_vec());
            let req = self.apply_fence(req)?;
            match req.send().await {
                Ok(r) if r.status().is_success() => {
                    self.put_count.fetch_add(1, Ordering::Relaxed);
                    self.put_bytes.fetch_add(len, Ordering::Relaxed);
                    return Ok(());
                }
                Ok(r) if Self::is_transient_status(r.status()) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!(
                            "PUT {url} returned {} after {attempt} attempts",
                            r.status()
                        ));
                    }
                    self.backoff(attempt).await;
                }
                Ok(r) => {
                    let s = r.status();
                    let body = r.text().await.unwrap_or_default();
                    return Err(anyhow!("PUT {url} returned {s}: {body}"));
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!(
                            "PUT {url} transport failed after {attempt} attempts: {e}"
                        ));
                    }
                    self.backoff(attempt).await;
                }
            }
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let url = self.object_url(key);
        let mut attempt = 0u32;
        loop {
            let req = self.client.delete(&url).bearer_auth(&self.token);
            let req = self.apply_fence(req)?;
            match req.send().await {
                Ok(r)
                    if r.status() == StatusCode::NO_CONTENT
                        || r.status() == StatusCode::OK
                        || r.status() == StatusCode::NOT_FOUND =>
                {
                    return Ok(());
                }
                Ok(r) if Self::is_transient_status(r.status()) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!(
                            "DELETE {url} returned {} after {attempt} attempts",
                            r.status()
                        ));
                    }
                    self.backoff(attempt).await;
                }
                Ok(r) => {
                    let s = r.status();
                    let body = r.text().await.unwrap_or_default();
                    return Err(anyhow!("DELETE {url} returned {s}: {body}"));
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!(
                            "DELETE {url} transport failed after {attempt} attempts: {e}"
                        ));
                    }
                    self.backoff(attempt).await;
                }
            }
        }
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        let url = self.list_url();
        let mut keys = Vec::new();
        let mut cursor: Option<String> = after.map(str::to_string);
        loop {
            let mut query: Vec<(&str, String)> = Vec::new();
            query.push(("prefix", prefix.to_string()));
            if let Some(ref c) = cursor {
                if !c.is_empty() {
                    query.push(("after", c.clone()));
                }
            }
            let resp = self
                .client
                .get(&url)
                .bearer_auth(&self.token)
                .query(&query)
                .send()
                .await
                .with_context(|| format!("LIST {url} transport failed"))?;
            if !resp.status().is_success() {
                let s = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(anyhow!("LIST {url} returned {s}: {body}"));
            }
            let body: ListResponse = resp
                .json()
                .await
                .with_context(|| format!("LIST {url} JSON parse"))?;

            // Check pagination BEFORE considering emptiness: an
            // empty-but-truncated page is a protocol violation, not an
            // early-terminate signal.
            if body.is_truncated && body.keys.is_empty() {
                return Err(anyhow!(
                    "LIST {url} returned is_truncated=true with empty keys; \
                     server protocol violation, refusing to continue"
                ));
            }

            if body.keys.is_empty() {
                break;
            }

            let last = body
                .keys
                .last()
                .cloned()
                .expect("keys non-empty just checked above");
            keys.extend(body.keys);

            if !body.is_truncated {
                break;
            }
            cursor = Some(last);
        }
        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let url = self.object_url(key);
        let mut attempt = 0u32;
        loop {
            let resp = self
                .client
                .head(&url)
                .bearer_auth(&self.token)
                .send()
                .await;
            match resp {
                Ok(r) if r.status() == StatusCode::OK => return Ok(true),
                Ok(r) if r.status() == StatusCode::NOT_FOUND => return Ok(false),
                Ok(r) if Self::is_transient_status(r.status()) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!(
                            "HEAD {url} returned {} after {attempt} attempts",
                            r.status()
                        ));
                    }
                    self.backoff(attempt).await;
                }
                Ok(r) => {
                    let s = r.status();
                    let body = r.text().await.unwrap_or_default();
                    return Err(anyhow!("HEAD {url} returned {s}: {body}"));
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!(
                            "HEAD {url} transport failed after {attempt} attempts: {e}"
                        ));
                    }
                    self.backoff(attempt).await;
                }
            }
        }
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        let url = self.object_url(key);
        let len = data.len() as u64;
        let mut attempt = 0u32;
        loop {
            let req = self
                .client
                .put(&url)
                .bearer_auth(&self.token)
                .header("If-None-Match", "*")
                .body(data.to_vec());
            let req = self.apply_fence(req)?;
            match req.send().await {
                Ok(r) if r.status().is_success() => {
                    self.put_count.fetch_add(1, Ordering::Relaxed);
                    self.put_bytes.fetch_add(len, Ordering::Relaxed);
                    let etag = extract_etag(&r);
                    return Ok(CasResult {
                        success: true,
                        etag,
                    });
                }
                Ok(r) if r.status() == StatusCode::PRECONDITION_FAILED
                    || r.status() == StatusCode::CONFLICT =>
                {
                    // CAS signal, not transient; do not retry.
                    return Ok(CasResult {
                        success: false,
                        etag: None,
                    });
                }
                Ok(r) if Self::is_transient_status(r.status()) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!(
                            "PUT-if-absent {url} returned {} after {attempt} attempts",
                            r.status()
                        ));
                    }
                    self.backoff(attempt).await;
                }
                Ok(r) => {
                    let s = r.status();
                    let body = r.text().await.unwrap_or_default();
                    return Err(anyhow!("PUT-if-absent {url} returned {s}: {body}"));
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!(
                            "PUT-if-absent {url} transport failed after {attempt} attempts: {e}"
                        ));
                    }
                    self.backoff(attempt).await;
                }
            }
        }
    }

    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult> {
        let url = self.object_url(key);
        let len = data.len() as u64;
        let mut attempt = 0u32;
        loop {
            let req = self
                .client
                .put(&url)
                .bearer_auth(&self.token)
                .header("If-Match", etag)
                .body(data.to_vec());
            let req = self.apply_fence(req)?;
            match req.send().await {
                Ok(r) if r.status().is_success() => {
                    self.put_count.fetch_add(1, Ordering::Relaxed);
                    self.put_bytes.fetch_add(len, Ordering::Relaxed);
                    let new_etag = extract_etag(&r);
                    return Ok(CasResult {
                        success: true,
                        etag: new_etag,
                    });
                }
                Ok(r) if r.status() == StatusCode::PRECONDITION_FAILED
                    || r.status() == StatusCode::CONFLICT =>
                {
                    return Ok(CasResult {
                        success: false,
                        etag: None,
                    });
                }
                Ok(r) if Self::is_transient_status(r.status()) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!(
                            "PUT-if-match {url} returned {} after {attempt} attempts",
                            r.status()
                        ));
                    }
                    self.backoff(attempt).await;
                }
                Ok(r) => {
                    let s = r.status();
                    let body = r.text().await.unwrap_or_default();
                    return Err(anyhow!("PUT-if-match {url} returned {s}: {body}"));
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!(
                            "PUT-if-match {url} transport failed after {attempt} attempts: {e}"
                        ));
                    }
                    self.backoff(attempt).await;
                }
            }
        }
    }

    async fn range_get(&self, key: &str, start: u64, len: u32) -> Result<Option<Vec<u8>>> {
        if len == 0 {
            return Ok(Some(Vec::new()));
        }
        let url = self.object_url(key);
        // Saturating arithmetic avoids overflow when start is near u64::MAX.
        let end = start.saturating_add((len as u64).saturating_sub(1));
        let range = format!("bytes={start}-{end}");
        let mut attempt = 0u32;
        loop {
            let resp = self
                .client
                .get(&url)
                .bearer_auth(&self.token)
                .header("Range", &range)
                .send()
                .await;
            match resp {
                Ok(r) if r.status() == StatusCode::OK || r.status() == StatusCode::PARTIAL_CONTENT => {
                    let bytes = r.bytes().await?.to_vec();
                    self.fetch_count.fetch_add(1, Ordering::Relaxed);
                    self.fetch_bytes
                        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok(Some(bytes));
                }
                Ok(r) if r.status() == StatusCode::NOT_FOUND => return Ok(None),
                Ok(r) if Self::is_transient_status(r.status()) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!(
                            "range-GET {url} ({range}) returned {} after {attempt} attempts",
                            r.status()
                        ));
                    }
                    self.backoff(attempt).await;
                }
                Ok(r) => {
                    let s = r.status();
                    let body = r.text().await.unwrap_or_default();
                    return Err(anyhow!("range-GET {url} ({range}) returned {s}: {body}"));
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!(
                            "range-GET {url} ({range}) transport failed after {attempt} attempts: {e}"
                        ));
                    }
                    self.backoff(attempt).await;
                }
            }
        }
    }

    async fn delete_many(&self, keys: &[String]) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }
        let url = self.batch_delete_url();
        let mut total = 0usize;
        for chunk in keys.chunks(1000) {
            let req = self
                .client
                .post(&url)
                .bearer_auth(&self.token)
                .json(&serde_json::json!({ "keys": chunk }));
            let req = self.apply_fence(req)?;
            let resp = req
                .send()
                .await
                .with_context(|| format!("batch-DELETE {url} transport failed"))?;
            if !resp.status().is_success() {
                let s = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(anyhow!("batch-DELETE {url} returned {s}: {body}"));
            }
            let body: BatchDeleteResponse = resp
                .json()
                .await
                .with_context(|| format!("batch-DELETE {url} JSON parse"))?;
            // `deleted` may legitimately be smaller than `chunk.len()` when
            // some keys were already absent (delete is idempotent). Surface
            // explicit per-key failures from the server if present.
            if !body.failed.is_empty() {
                return Err(anyhow!(
                    "batch-DELETE {url}: {} of {} keys failed; first failure: {}",
                    body.failed.len(),
                    chunk.len(),
                    body.failed
                        .first()
                        .map(|f| f.as_str())
                        .unwrap_or("<none>"),
                ));
            }
            total += body.deleted;
        }
        Ok(total)
    }

    fn backend_name(&self) -> &str {
        "cinch"
    }
}

fn extract_etag(resp: &reqwest::Response) -> Option<String> {
    resp.headers()
        .get(reqwest::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

#[derive(Debug, Deserialize)]
struct ListResponse {
    keys: Vec<String>,
    #[serde(default)]
    is_truncated: bool,
}

#[derive(Debug, Deserialize)]
struct BatchDeleteResponse {
    deleted: usize,
    #[serde(default)]
    failed: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use hadb_lease::NoActiveLease;

    struct StaticFence(Option<u64>);
    impl FenceSource for StaticFence {
        fn current(&self) -> Option<u64> {
            self.0
        }
    }

    #[test]
    fn new_defaults_are_sensible() {
        let s = CinchHttpStorage::new("https://example.com/", "tok", "pages");
        assert_eq!(s.endpoint(), "https://example.com");
        assert_eq!(s.prefix(), "pages");
        assert!(s.fence.is_none());
        assert_eq!(s.max_retries, DEFAULT_MAX_RETRIES);
        assert_eq!(s.backend_name(), "cinch");
    }

    #[test]
    fn with_fence_attaches_fence_handle() {
        let s = CinchHttpStorage::new("https://example.com", "tok", "pages")
            .with_fence(Arc::new(StaticFence(Some(1))));
        assert!(s.fence.is_some());
    }

    #[test]
    fn with_max_retries_overrides_default() {
        let s = CinchHttpStorage::new("https://example.com", "tok", "pages").with_max_retries(9);
        assert_eq!(s.max_retries, 9);
    }

    #[test]
    fn object_url_composes_correctly() {
        let s = CinchHttpStorage::new("https://example.com/", "tok", "pages");
        assert_eq!(
            s.object_url("p/d/0_v1"),
            "https://example.com/v1/sync/pages/p/d/0_v1"
        );
        assert_eq!(s.list_url(), "https://example.com/v1/sync/pages");
        assert_eq!(
            s.batch_delete_url(),
            "https://example.com/v1/sync/pages/_delete"
        );
    }

    #[test]
    fn object_url_percent_encodes_unsafe_chars() {
        let s = CinchHttpStorage::new("https://example.com", "tok", "pages");
        // `?`, `#`, ` `, `%` must be encoded; `/` must pass through.
        assert_eq!(
            s.object_url("foo?bar"),
            "https://example.com/v1/sync/pages/foo%3Fbar"
        );
        assert_eq!(
            s.object_url("a b/c#d"),
            "https://example.com/v1/sync/pages/a%20b/c%23d"
        );
        assert_eq!(
            s.object_url("100%"),
            "https://example.com/v1/sync/pages/100%25"
        );
    }

    #[test]
    fn apply_fence_without_fence_is_noop() {
        let s = CinchHttpStorage::new("https://example.com", "tok", "pages");
        let req = s.client.put("https://example.com/x");
        assert!(s.apply_fence(req).is_ok());
    }

    #[test]
    fn apply_fence_with_unset_fence_errors() {
        let s = CinchHttpStorage::new("https://example.com", "tok", "pages")
            .with_fence(Arc::new(StaticFence(None)));
        let req = s.client.put("https://example.com/x");
        let err = s.apply_fence(req).unwrap_err();
        assert!(err.to_string().contains("no active lease"));
    }

    #[test]
    fn apply_fence_with_set_fence_succeeds() {
        let s = CinchHttpStorage::new("https://example.com", "tok", "pages")
            .with_fence(Arc::new(StaticFence(Some(42))));
        let req = s.client.put("https://example.com/x");
        assert!(s.apply_fence(req).is_ok());
    }

    #[test]
    fn metric_counters_start_at_zero() {
        let s = CinchHttpStorage::new("https://example.com", "tok", "pages");
        assert_eq!(s.bytes_fetched(), 0);
        assert_eq!(s.bytes_put(), 0);
        assert_eq!(s.fetch_count(), 0);
        assert_eq!(s.put_count(), 0);
    }

    #[test]
    fn no_active_lease_error_round_trip() {
        // Sanity check that the re-exported error type still implements Error.
        let e: Result<u64, NoActiveLease> = Err(NoActiveLease);
        assert!(e.is_err());
    }

    // Send/Sync + object safety.
    #[allow(dead_code)]
    fn _usable_as_arc_dyn(_: Arc<dyn StorageBackend>) {}

    #[allow(dead_code)]
    fn _send_sync<T: StorageBackend>() {}

    #[allow(dead_code)]
    fn _check_traits() {
        _send_sync::<CinchHttpStorage>();
    }
}
