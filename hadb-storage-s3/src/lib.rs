//! `hadb-storage-s3`: S3-compatible `StorageBackend` implementation.
//!
//! Works with AWS S3, Tigris, MinIO, Cloudflare R2, Wasabi, or any other
//! service that speaks the S3 API.
//!
//! # CAS
//!
//! `put_if_absent` uses `If-None-Match: *`; `put_if_match` uses `If-Match: <etag>`.
//! A 412 Precondition Failed is reported as `CasResult { success: false, etag: None }`.
//! Note: some S3-compatible backends (Tigris under concurrency) may accept both
//! conflicting writes. Callers that need strict CAS should verify with a read.
//!
//! # Retries
//!
//! GET / PUT / DELETE / LIST retry transient errors with exponential backoff
//! (default 3 attempts, 100ms → 200ms → 400ms). Conditional writes do NOT
//! retry on 412 (that's a CAS signal, not a transient failure).
//!
//! # Metrics
//!
//! The struct tracks bytes transferred and op counts on `AtomicU64`s.
//! Accessor methods (`bytes_fetched`, `fetch_count`, etc.) let consumers
//! observe without the storage impl having to know about any metrics trait.
//!
//! # Prefix
//!
//! `with_prefix("some/path")` prepends `"some/path/"` to every key before
//! hitting S3. Keys handed back by `list` have the prefix stripped so
//! callers see the same space they wrote into.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_s3::{primitives::ByteStream, Client};

use hadb_storage::{CasResult, StorageBackend};

mod error;
use error::{is_not_found, is_precondition_failed};

/// Default maximum retry attempts for transient errors.
pub const DEFAULT_MAX_RETRIES: u32 = 3;

/// S3-compatible storage backend.
///
/// Construct with an `aws_sdk_s3::Client` or convenience constructor
/// `from_env`. Cheap to clone: the client is an `Arc` internally and
/// the metrics counters are `Arc<AtomicU64>`.
pub struct S3Storage {
    client: Client,
    bucket: String,
    prefix: String,
    max_retries: u32,
    fetch_count: Arc<AtomicU64>,
    fetch_bytes: Arc<AtomicU64>,
    put_count: Arc<AtomicU64>,
    put_bytes: Arc<AtomicU64>,
}

impl S3Storage {
    /// Construct with an existing S3 client and bucket name.
    pub fn new(client: Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            prefix: String::new(),
            max_retries: DEFAULT_MAX_RETRIES,
            fetch_count: Arc::new(AtomicU64::new(0)),
            fetch_bytes: Arc::new(AtomicU64::new(0)),
            put_count: Arc::new(AtomicU64::new(0)),
            put_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Construct from environment (AWS_* env vars) with an optional custom
    /// endpoint URL (Tigris, MinIO, etc.).
    pub async fn from_env(bucket: impl Into<String>, endpoint: Option<&str>) -> Result<Self> {
        let base = aws_config::from_env().load().await;
        let mut s3_config = aws_sdk_s3::config::Builder::from(&base);
        if let Some(endpoint) = endpoint {
            s3_config = s3_config.endpoint_url(endpoint).force_path_style(true);
        }
        let client = Client::from_conf(s3_config.build());
        Ok(Self::new(client, bucket))
    }

    /// Set a key prefix. Applied to every key before hitting S3; transparent
    /// on reads (callers see and list the un-prefixed keys).
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        let mut p = prefix.into();
        if !p.is_empty() && !p.ends_with('/') {
            p.push('/');
        }
        self.prefix = p;
        self
    }

    /// Override the default retry count.
    pub fn with_max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    /// Access to the underlying S3 client (for consumers that need raw
    /// operations not exposed by the `StorageBackend` trait, e.g. multipart
    /// uploads or checksum metadata).
    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Total bytes returned by successful GETs since construction.
    pub fn bytes_fetched(&self) -> u64 {
        self.fetch_bytes.load(Ordering::Relaxed)
    }

    /// Number of successful GETs (includes range GETs) since construction.
    pub fn fetch_count(&self) -> u64 {
        self.fetch_count.load(Ordering::Relaxed)
    }

    /// Total bytes sent by successful PUTs since construction.
    pub fn bytes_put(&self) -> u64 {
        self.put_bytes.load(Ordering::Relaxed)
    }

    /// Number of successful PUTs since construction.
    pub fn put_count(&self) -> u64 {
        self.put_count.load(Ordering::Relaxed)
    }

    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}{}", self.prefix, key)
        }
    }

    /// Strip the configured prefix from a server-returned key.
    fn unprefix<'a>(&self, key: &'a str) -> &'a str {
        if self.prefix.is_empty() {
            key
        } else {
            key.strip_prefix(&self.prefix).unwrap_or(key)
        }
    }

    async fn backoff(&self, attempt: u32) {
        let base_ms = 100u64;
        let ms = base_ms.saturating_mul(1u64 << attempt.min(8));
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
}

#[async_trait]
impl StorageBackend for S3Storage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let full = self.full_key(key);
        let mut attempt = 0u32;
        loop {
            match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&full)
                .send()
                .await
            {
                Ok(resp) => {
                    let bytes = resp.body.collect().await?.into_bytes().to_vec();
                    self.fetch_count.fetch_add(1, Ordering::Relaxed);
                    self.fetch_bytes
                        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok(Some(bytes));
                }
                Err(e) if is_not_found(&e) => return Ok(None),
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!("S3 GET {full} failed after {attempt} attempts: {e}"));
                    }
                    tracing::debug!("S3 GET {full} attempt {attempt} failed: {e}; retrying");
                    self.backoff(attempt).await;
                }
            }
        }
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        let full = self.full_key(key);
        let len = data.len() as u64;
        let mut attempt = 0u32;
        loop {
            let body = ByteStream::from(data.to_vec());
            match self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(&full)
                .body(body)
                .send()
                .await
            {
                Ok(_) => {
                    self.put_count.fetch_add(1, Ordering::Relaxed);
                    self.put_bytes.fetch_add(len, Ordering::Relaxed);
                    return Ok(());
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!("S3 PUT {full} failed after {attempt} attempts: {e}"));
                    }
                    tracing::debug!("S3 PUT {full} attempt {attempt} failed: {e}; retrying");
                    self.backoff(attempt).await;
                }
            }
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let full = self.full_key(key);
        let mut attempt = 0u32;
        loop {
            match self
                .client
                .delete_object()
                .bucket(&self.bucket)
                .key(&full)
                .send()
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) if is_not_found(&e) => return Ok(()),
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!(
                            "S3 DELETE {full} failed after {attempt} attempts: {e}"
                        ));
                    }
                    tracing::debug!("S3 DELETE {full} attempt {attempt} failed: {e}; retrying");
                    self.backoff(attempt).await;
                }
            }
        }
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        let full_prefix = self.full_key(prefix);
        let full_after = after.map(|a| self.full_key(a));
        let mut keys = Vec::new();
        let mut continuation: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&full_prefix);
            if let Some(ref a) = full_after {
                req = req.start_after(a);
            }
            if let Some(token) = &continuation {
                req = req.continuation_token(token);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| anyhow!("S3 LIST {full_prefix} failed: {e}"))?;
            if let Some(contents) = resp.contents {
                for obj in contents {
                    if let Some(k) = obj.key {
                        keys.push(self.unprefix(&k).to_string());
                    }
                }
            }
            if resp.is_truncated.unwrap_or(false) {
                continuation = resp.next_continuation_token;
                if continuation.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let full = self.full_key(key);
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&full)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) if is_not_found(&e) => Ok(false),
            Err(e) => Err(anyhow!("S3 HEAD {full} failed: {e}")),
        }
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        let full = self.full_key(key);
        let len = data.len() as u64;
        let mut attempt = 0u32;
        let mut tried_fallback = false;
        loop {
            let body = ByteStream::from(data.to_vec());
            match self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(&full)
                .body(body)
                .if_none_match("*")
                .send()
                .await
            {
                Ok(resp) => {
                    self.put_count.fetch_add(1, Ordering::Relaxed);
                    self.put_bytes.fetch_add(len, Ordering::Relaxed);
                    return Ok(CasResult {
                        success: true,
                        etag: resp.e_tag().map(|s| s.to_string()),
                    });
                }
                Err(e) if is_precondition_failed(&e) => {
                    return Ok(CasResult {
                        success: false,
                        etag: None,
                    })
                }
                Err(e) if !tried_fallback => {
                    tried_fallback = true;
                    tracing::debug!(
                        "S3 PUT-if-absent {full} failed with {e}; trying HEAD+PUT fallback for S3-compatible stores"
                    );
                    match self
                        .client
                        .head_object()
                        .bucket(&self.bucket)
                        .key(&full)
                        .send()
                        .await
                    {
                        Ok(_) => {
                            return Ok(CasResult {
                                success: false,
                                etag: None,
                            });
                        }
                        Err(head_err) if is_not_found(&head_err) => {
                            let body = ByteStream::from(data.to_vec());
                            match self
                                .client
                                .put_object()
                                .bucket(&self.bucket)
                                .key(&full)
                                .body(body)
                                .send()
                                .await
                            {
                                Ok(resp) => {
                                    self.put_count.fetch_add(1, Ordering::Relaxed);
                                    self.put_bytes.fetch_add(len, Ordering::Relaxed);
                                    return Ok(CasResult {
                                        success: true,
                                        etag: resp.e_tag().map(|s| s.to_string()),
                                    });
                                }
                                Err(put_err) => {
                                    return Err(anyhow!(
                                        "S3 PUT-if-absent {full} fallback PUT failed: {put_err}"
                                    ));
                                }
                            }
                        }
                        Err(head_err) => {
                            return Err(anyhow!(
                                "S3 PUT-if-absent {full} fallback HEAD failed: {head_err}"
                            ));
                        }
                    }
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!(
                            "S3 PUT-if-absent {full} failed after {attempt} attempts: {e}"
                        ));
                    }
                    tracing::debug!(
                        "S3 PUT-if-absent {full} attempt {attempt} failed: {e}; retrying"
                    );
                    self.backoff(attempt).await;
                }
            }
        }
    }

    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult> {
        let full = self.full_key(key);
        let len = data.len() as u64;
        let mut attempt = 0u32;
        loop {
            let body = ByteStream::from(data.to_vec());
            match self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(&full)
                .body(body)
                .if_match(etag)
                .send()
                .await
            {
                Ok(resp) => {
                    self.put_count.fetch_add(1, Ordering::Relaxed);
                    self.put_bytes.fetch_add(len, Ordering::Relaxed);
                    return Ok(CasResult {
                        success: true,
                        etag: resp.e_tag().map(|s| s.to_string()),
                    });
                }
                Err(e) if is_precondition_failed(&e) => {
                    return Ok(CasResult {
                        success: false,
                        etag: None,
                    })
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!(
                            "S3 PUT-if-match {full} failed after {attempt} attempts: {e}"
                        ));
                    }
                    tracing::debug!(
                        "S3 PUT-if-match {full} attempt {attempt} failed: {e}; retrying"
                    );
                    self.backoff(attempt).await;
                }
            }
        }
    }

    async fn range_get(&self, key: &str, start: u64, len: u32) -> Result<Option<Vec<u8>>> {
        if len == 0 {
            return Ok(Some(Vec::new()));
        }
        let full = self.full_key(key);
        // Saturating arithmetic avoids overflow when start is near u64::MAX.
        let end = start.saturating_add((len as u64).saturating_sub(1));
        let range = format!("bytes={start}-{end}");
        let mut attempt = 0u32;
        loop {
            match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&full)
                .range(&range)
                .send()
                .await
            {
                Ok(resp) => {
                    let bytes = resp.body.collect().await?.into_bytes().to_vec();
                    self.fetch_count.fetch_add(1, Ordering::Relaxed);
                    self.fetch_bytes
                        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok(Some(bytes));
                }
                Err(e) if is_not_found(&e) => return Ok(None),
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow!(
                            "S3 range-GET {full} ({range}) failed after {attempt} attempts: {e}"
                        ));
                    }
                    tracing::debug!(
                        "S3 range-GET {full} ({range}) attempt {attempt} failed: {e}; retrying"
                    );
                    self.backoff(attempt).await;
                }
            }
        }
    }

    async fn delete_many(&self, keys: &[String]) -> Result<usize> {
        use aws_sdk_s3::types::{Delete, ObjectIdentifier};

        if keys.is_empty() {
            return Ok(0);
        }
        let mut total = 0usize;
        // S3 DeleteObjects caps at 1000 per call.
        for chunk in keys.chunks(1000) {
            let objects: Vec<ObjectIdentifier> = chunk
                .iter()
                .filter_map(|k| ObjectIdentifier::builder().key(self.full_key(k)).build().ok())
                .collect();
            if objects.is_empty() {
                continue;
            }
            let delete = Delete::builder()
                .set_objects(Some(objects))
                .quiet(true)
                .build()
                .map_err(|e| anyhow!("Delete builder failed: {e}"))?;
            let result = self
                .client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(delete)
                .send()
                .await
                .map_err(|e| anyhow!("S3 DELETE-objects failed: {e}"))?;

            // Per-key errors are explicit server signals ("we tried, we
            // failed"), distinct from "key was absent" (which S3 treats as
            // success). Surface them as a hard error: compaction/GC
            // callers need to see orphaned objects immediately, not
            // discover them later from a buried warning.
            let errs = result.errors();
            if !errs.is_empty() {
                let first = errs
                    .first()
                    .map(|e| {
                        format!(
                            "{}: {}",
                            e.key().unwrap_or("<unknown>"),
                            e.message().unwrap_or("<no message>")
                        )
                    })
                    .unwrap_or_else(|| "<no detail>".to_string());
                return Err(anyhow!(
                    "S3 DELETE-objects: {} of {} keys failed; first: {first}",
                    errs.len(),
                    chunk.len(),
                ));
            }
            total += chunk.len();
        }
        Ok(total)
    }

    fn backend_name(&self) -> &str {
        "s3"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    // Construct without contacting S3 (validates plumbing).
    fn dummy_client() -> Client {
        let conf = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "test", None, None, "test",
            ))
            .build();
        Client::from_conf(conf)
    }

    #[test]
    fn new_defaults_are_sensible() {
        let s = S3Storage::new(dummy_client(), "b");
        assert_eq!(s.bucket(), "b");
        assert_eq!(s.prefix(), "");
        assert_eq!(s.max_retries, DEFAULT_MAX_RETRIES);
        assert_eq!(s.backend_name(), "s3");
    }

    #[test]
    fn with_prefix_normalizes_trailing_slash() {
        let a = S3Storage::new(dummy_client(), "b").with_prefix("foo");
        assert_eq!(a.prefix(), "foo/");
        let b = S3Storage::new(dummy_client(), "b").with_prefix("foo/");
        assert_eq!(b.prefix(), "foo/");
        let c = S3Storage::new(dummy_client(), "b").with_prefix("");
        assert_eq!(c.prefix(), "");
        let d = S3Storage::new(dummy_client(), "b").with_prefix("a/b/c");
        assert_eq!(d.prefix(), "a/b/c/");
    }

    #[test]
    fn full_key_applies_prefix() {
        let s = S3Storage::new(dummy_client(), "b").with_prefix("scope");
        assert_eq!(s.full_key("leases/x"), "scope/leases/x");
        let bare = S3Storage::new(dummy_client(), "b");
        assert_eq!(bare.full_key("x"), "x");
    }

    #[test]
    fn unprefix_strips_configured_prefix() {
        let s = S3Storage::new(dummy_client(), "b").with_prefix("scope");
        assert_eq!(s.unprefix("scope/leases/x"), "leases/x");
        assert_eq!(s.unprefix("unrelated/x"), "unrelated/x");
    }

    #[test]
    fn with_max_retries_overrides_default() {
        let s = S3Storage::new(dummy_client(), "b").with_max_retries(7);
        assert_eq!(s.max_retries, 7);
    }

    #[test]
    fn metric_counters_start_at_zero() {
        let s = S3Storage::new(dummy_client(), "b");
        assert_eq!(s.bytes_fetched(), 0);
        assert_eq!(s.bytes_put(), 0);
        assert_eq!(s.fetch_count(), 0);
        assert_eq!(s.put_count(), 0);
    }

    // Object-safety + Send+Sync check.
    #[allow(dead_code)]
    fn _usable_as_arc_dyn(_: Arc<dyn StorageBackend>) {}

    #[allow(dead_code)]
    fn _send_sync<T: StorageBackend>() {}

    #[allow(dead_code)]
    fn _check_s3storage_traits() {
        _send_sync::<S3Storage>();
    }
}
