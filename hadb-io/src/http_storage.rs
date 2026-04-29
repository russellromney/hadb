//! HTTP object storage backend for the hadb ecosystem.
//!
//! Implements `ObjectStore` over HTTP with Bearer token auth, designed
//! for the Grabby `/v1/sync/` API. Replaces S3 SDK with simple HTTP calls.
//!
//! ```ignore
//! use hadb_io::HttpObjectStore;
//!
//! let fence = Arc::new(AtomicU64::new(0));
//! let store = HttpObjectStore::new("https://storage.iad.example.com", "my-token", "wal", fence);
//! store.upload_bytes("segment-001.bin", data).await?;
//! ```

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use std::path::Path;
use tracing::warn;

use crate::storage::ObjectStore;

/// Object storage backed by HTTP endpoints with Bearer token auth.
///
/// Maps ObjectStore operations to REST calls:
///   upload  -> PUT  /v1/sync/{prefix}/{key}
///   download -> GET  /v1/sync/{prefix}/{key}
///   list    -> GET  /v1/sync/{prefix}?prefix={p}&after={a}
///   exists  -> HEAD /v1/sync/{prefix}/{key}
///   delete  -> DELETE /v1/sync/{prefix}/{key}
///   batch delete -> POST /v1/sync/{prefix}/_delete
pub struct HttpObjectStore {
    client: reqwest::Client,
    endpoint: String,
    token: String,
    /// URL path prefix (e.g., "wal", "pages"). Appended after /v1/sync/.
    prefix: String,
    /// Current fence token (NATS KV revision). Updated by the lease holder
    /// on each heartbeat. Sent as `Fence-Token` header on all writes.
    /// 0 means no fence (writes will be rejected by Grabby if a lease exists).
    fence_token: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl HttpObjectStore {
    /// Create a new HTTP object store.
    ///
    /// - `endpoint`: Base URL (e.g., "https://storage.iad.example.com")
    /// - `token`: Bearer token for authentication
    /// - `prefix`: Storage prefix for URL routing (e.g., "wal" or "pages")
    /// - `fence_token`: Shared atomic fence token, updated by the lease renewal loop
    pub fn new(
        endpoint: &str,
        token: &str,
        prefix: &str,
        fence_token: std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("failed to build HTTP client");
        Self {
            client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            token: token.to_string(),
            prefix: prefix.to_string(),
            fence_token,
        }
    }

    fn url(&self, key: &str) -> String {
        format!("{}/v1/sync/{}/{}", self.endpoint, self.prefix, key)
    }

    fn list_url(&self) -> String {
        format!("{}/v1/sync/{}", self.endpoint, self.prefix)
    }

    fn batch_delete_url(&self) -> String {
        format!("{}/v1/sync/{}/_delete", self.endpoint, self.prefix)
    }

    /// Get the current fence token value. 0 means no fence set yet.
    fn current_fence(&self) -> u64 {
        self.fence_token.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Add Fence-Token header to a write request if a fence is active.
    fn with_fence(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let fence = self.current_fence();
        if fence > 0 {
            req.header("Fence-Token", fence.to_string())
        } else {
            req
        }
    }

    /// Send a request with retries (3 attempts, exponential backoff).
    async fn send_with_retry(
        &self,
        build_request: impl Fn() -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response> {
        let mut retries = 0u32;
        loop {
            match build_request().send().await {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    retries += 1;
                    if retries >= 3 {
                        return Err(anyhow!("HTTP request failed after 3 retries: {}", e));
                    }
                    warn!("HTTP request failed (retry {}/3): {}", retries, e);
                    tokio::time::sleep(std::time::Duration::from_millis(100 * (1 << retries)))
                        .await;
                }
            }
        }
    }
}

#[async_trait]
impl ObjectStore for HttpObjectStore {
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()> {
        let url = self.url(key);
        let resp = self
            .send_with_retry(|| {
                self.with_fence(
                    self.client
                        .put(&url)
                        .bearer_auth(&self.token)
                        .body(data.clone()),
                )
            })
            .await
            .with_context(|| format!("HTTP upload failed: {}", key))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp
                .text()
                .await
                .unwrap_or_else(|_| "<error reading body>".to_string());
            return Err(anyhow!("HTTP upload {} returned {}: {}", key, status, text));
        }
        Ok(())
    }

    async fn upload_bytes_with_checksum(
        &self,
        key: &str,
        data: Vec<u8>,
        checksum: &str,
    ) -> Result<()> {
        let url = self.url(key);
        let checksum = checksum.to_string();
        let resp = self
            .send_with_retry(|| {
                self.with_fence(
                    self.client
                        .put(&url)
                        .bearer_auth(&self.token)
                        .header("x-checksum-sha256", &checksum)
                        .body(data.clone()),
                )
            })
            .await
            .with_context(|| format!("HTTP upload failed: {}", key))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp
                .text()
                .await
                .unwrap_or_else(|_| "<error reading body>".to_string());
            return Err(anyhow!("HTTP upload {} returned {}: {}", key, status, text));
        }
        Ok(())
    }

    async fn upload_file(&self, key: &str, path: &Path) -> Result<()> {
        let data = tokio::fs::read(path)
            .await
            .with_context(|| format!("Failed to read file: {}", path.display()))?;
        self.upload_bytes(key, data).await
    }

    async fn upload_file_with_checksum(
        &self,
        key: &str,
        path: &Path,
        checksum: &str,
    ) -> Result<()> {
        let data = tokio::fs::read(path)
            .await
            .with_context(|| format!("Failed to read file: {}", path.display()))?;
        self.upload_bytes_with_checksum(key, data, checksum).await
    }

    async fn download_bytes(&self, key: &str) -> Result<Vec<u8>> {
        let url = self.url(key);
        let resp = self
            .send_with_retry(|| self.client.get(&url).bearer_auth(&self.token))
            .await
            .with_context(|| format!("HTTP download failed: {}", key))?;

        match resp.status() {
            reqwest::StatusCode::OK => {
                let bytes = resp
                    .bytes()
                    .await
                    .with_context(|| format!("Failed to read body: {}", key))?;
                Ok(bytes.to_vec())
            }
            reqwest::StatusCode::NOT_FOUND => Err(anyhow!("Object not found: {}", key)),
            status => {
                let text = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "<error reading body>".to_string());
                Err(anyhow!(
                    "HTTP download {} returned {}: {}",
                    key,
                    status,
                    text
                ))
            }
        }
    }

    async fn download_file(&self, key: &str, path: &Path) -> Result<()> {
        let data = self.download_bytes(key).await?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(path, &data)
            .await
            .with_context(|| format!("Failed to write file: {}", path.display()))
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        self.list_objects_after(prefix, "").await
    }

    async fn list_objects_after(&self, prefix: &str, start_after: &str) -> Result<Vec<String>> {
        let mut all_keys = Vec::new();
        let mut after = start_after.to_string();

        loop {
            let mut query = vec![("prefix", prefix.to_string())];
            if !after.is_empty() {
                query.push(("after", after.clone()));
            }

            let list_url = self.list_url();
            let query_clone = query.clone();
            let resp = self
                .send_with_retry(|| {
                    self.client
                        .get(&list_url)
                        .bearer_auth(&self.token)
                        .query(&query_clone)
                })
                .await
                .with_context(|| format!("HTTP list failed: prefix={}", prefix))?;

            if !resp.status().is_success() {
                let status = resp.status();
                let text = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "<error reading body>".to_string());
                return Err(anyhow!(
                    "HTTP list prefix={} returned {}: {}",
                    prefix,
                    status,
                    text
                ));
            }

            let body: serde_json::Value =
                resp.json().await.context("Failed to parse list response")?;
            let keys = body["keys"]
                .as_array()
                .ok_or_else(|| anyhow!("List response missing 'keys' array"))?;

            for v in keys {
                let key = v
                    .as_str()
                    .ok_or_else(|| anyhow!("List response contains non-string key"))?;
                all_keys.push(key.to_string());
            }

            let is_truncated = body["is_truncated"].as_bool().unwrap_or(false);
            if !is_truncated || keys.is_empty() {
                break;
            }

            // Next page starts after the last key
            after = all_keys.last().expect("keys non-empty").clone();
        }

        Ok(all_keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let url = self.url(key);
        let resp = self
            .send_with_retry(|| self.client.head(&url).bearer_auth(&self.token))
            .await
            .with_context(|| format!("HTTP exists check failed: {}", key))?;

        Ok(resp.status() == reqwest::StatusCode::OK)
    }

    async fn get_checksum(&self, key: &str) -> Result<Option<String>> {
        let url = self.url(key);
        let resp = self
            .send_with_retry(|| self.client.head(&url).bearer_auth(&self.token))
            .await
            .with_context(|| format!("HTTP head failed: {}", key))?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(anyhow!("HTTP head {} returned {}", key, resp.status()));
        }

        Ok(resp
            .headers()
            .get("x-checksum-sha256")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()))
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        let url = self.url(key);
        let resp = self
            .send_with_retry(|| self.with_fence(self.client.delete(&url).bearer_auth(&self.token)))
            .await
            .with_context(|| format!("HTTP delete failed: {}", key))?;

        match resp.status() {
            reqwest::StatusCode::NO_CONTENT
            | reqwest::StatusCode::OK
            | reqwest::StatusCode::NOT_FOUND => Ok(()),
            status => {
                let text = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "<error reading body>".to_string());
                Err(anyhow!("HTTP delete {} returned {}: {}", key, status, text))
            }
        }
    }

    async fn delete_objects(&self, keys: &[String]) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }

        let mut total_deleted = 0usize;

        // Chunk at 1000 to match S3 batch delete limits
        for chunk in keys.chunks(1000) {
            let resp = self
                .with_fence(
                    self.client
                        .post(&self.batch_delete_url())
                        .bearer_auth(&self.token)
                        .json(&serde_json::json!({ "keys": chunk })),
                )
                .send()
                .await
                .context("HTTP batch delete failed")?;

            if !resp.status().is_success() {
                let status = resp.status();
                let text = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "<error reading body>".to_string());
                return Err(anyhow!("HTTP batch delete returned {}: {}", status, text));
            }

            let body: serde_json::Value = resp
                .json()
                .await
                .context("Failed to parse batch delete response")?;
            let deleted = body["deleted"]
                .as_u64()
                .ok_or_else(|| anyhow!("Batch delete response missing 'deleted' field"))?;
            total_deleted += deleted as usize;
        }

        Ok(total_deleted)
    }

    fn bucket_name(&self) -> &str {
        &self.prefix
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    fn no_fence() -> std::sync::Arc<AtomicU64> {
        std::sync::Arc::new(AtomicU64::new(0))
    }

    use axum::{
        body::Body,
        extract::{Path, Query, State},
        http::{HeaderMap, StatusCode},
        response::IntoResponse,
        routing::{delete, get, head, post, put},
        Json, Router,
    };
    use serde::Deserialize;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Clone)]
    struct MockState {
        objects: Arc<Mutex<HashMap<String, (Vec<u8>, Option<String>)>>>,
    }

    impl MockState {
        fn new() -> Self {
            Self {
                objects: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    async fn mock_put(
        State(state): State<MockState>,
        Path(key): Path<String>,
        headers: HeaderMap,
        body: axum::body::Bytes,
    ) -> impl IntoResponse {
        let checksum = headers
            .get("x-checksum-sha256")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        state
            .objects
            .lock()
            .await
            .insert(key, (body.to_vec(), checksum));
        StatusCode::OK
    }

    async fn mock_get(
        State(state): State<MockState>,
        Path(key): Path<String>,
    ) -> impl IntoResponse {
        match state.objects.lock().await.get(&key) {
            Some((data, _)) => (StatusCode::OK, data.clone()).into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    async fn mock_head(
        State(state): State<MockState>,
        Path(key): Path<String>,
    ) -> impl IntoResponse {
        match state.objects.lock().await.get(&key) {
            Some((_, checksum)) => {
                let mut headers = HeaderMap::new();
                if let Some(cs) = checksum {
                    headers.insert("x-checksum-sha256", cs.parse().expect("header"));
                }
                (StatusCode::OK, headers).into_response()
            }
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    async fn mock_delete(
        State(state): State<MockState>,
        Path(key): Path<String>,
    ) -> impl IntoResponse {
        state.objects.lock().await.remove(&key);
        StatusCode::NO_CONTENT
    }

    #[derive(Deserialize)]
    struct ListParams {
        prefix: Option<String>,
        after: Option<String>,
    }

    async fn mock_list(
        State(state): State<MockState>,
        Query(params): Query<ListParams>,
    ) -> impl IntoResponse {
        let objects = state.objects.lock().await;
        let prefix = params.prefix.unwrap_or_default();
        let after = params.after.unwrap_or_default();
        let keys: Vec<String> = objects
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .filter(|k| after.is_empty() || k.as_str() > after.as_str())
            .cloned()
            .collect();
        Json(serde_json::json!({ "keys": keys }))
    }

    #[derive(Deserialize)]
    struct BatchDeleteBody {
        keys: Vec<String>,
    }

    async fn mock_batch_delete(
        State(state): State<MockState>,
        Json(body): Json<BatchDeleteBody>,
    ) -> impl IntoResponse {
        let mut objects = state.objects.lock().await;
        let mut deleted = 0usize;
        for key in &body.keys {
            if objects.remove(key).is_some() {
                deleted += 1;
            }
        }
        Json(serde_json::json!({ "deleted": deleted }))
    }

    fn mock_app(state: MockState) -> Router {
        Router::new()
            .route("/v1/sync/wal/{*key}", put(mock_put))
            .route("/v1/sync/wal/{*key}", get(mock_get))
            .route("/v1/sync/wal/{*key}", head(mock_head))
            .route("/v1/sync/wal/{*key}", delete(mock_delete))
            .route("/v1/sync/wal", get(mock_list))
            .route("/v1/sync/wal/_delete", post(mock_batch_delete))
            .with_state(state)
    }

    async fn start_mock() -> (String, tokio::task::JoinHandle<()>) {
        let state = MockState::new();
        let app = mock_app(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let url = format!("http://{}", addr);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        (url, handle)
    }

    #[tokio::test]
    async fn upload_and_download_bytes() {
        let (url, _h) = start_mock().await;
        let store = HttpObjectStore::new(&url, "test-token", "wal", no_fence());

        store
            .upload_bytes("seg-001.bin", b"hello world".to_vec())
            .await
            .expect("upload");

        let data = store.download_bytes("seg-001.bin").await.expect("download");
        assert_eq!(data, b"hello world");
    }

    #[tokio::test]
    async fn upload_with_checksum_and_retrieve() {
        let (url, _h) = start_mock().await;
        let store = HttpObjectStore::new(&url, "test-token", "wal", no_fence());

        store
            .upload_bytes_with_checksum("seg-002.bin", b"data".to_vec(), "abc123")
            .await
            .expect("upload");

        let checksum = store.get_checksum("seg-002.bin").await.expect("checksum");
        assert_eq!(checksum, Some("abc123".to_string()));
    }

    #[tokio::test]
    async fn download_not_found() {
        let (url, _h) = start_mock().await;
        let store = HttpObjectStore::new(&url, "test-token", "wal", no_fence());

        let result = store.download_bytes("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn exists_check() {
        let (url, _h) = start_mock().await;
        let store = HttpObjectStore::new(&url, "test-token", "wal", no_fence());

        assert!(!store.exists("nope").await.expect("exists"));

        store
            .upload_bytes("exists-test", b"data".to_vec())
            .await
            .expect("upload");
        assert!(store.exists("exists-test").await.expect("exists"));
    }

    #[tokio::test]
    async fn delete_and_batch_delete() {
        let (url, _h) = start_mock().await;
        let store = HttpObjectStore::new(&url, "test-token", "wal", no_fence());

        store
            .upload_bytes("a", b"1".to_vec())
            .await
            .expect("upload");
        store
            .upload_bytes("b", b"2".to_vec())
            .await
            .expect("upload");
        store
            .upload_bytes("c", b"3".to_vec())
            .await
            .expect("upload");

        // Single delete
        store.delete_object("a").await.expect("delete");
        assert!(!store.exists("a").await.expect("exists"));

        // Batch delete
        let deleted = store
            .delete_objects(&["b".to_string(), "c".to_string()])
            .await
            .expect("batch delete");
        assert_eq!(deleted, 2);
        assert!(!store.exists("b").await.expect("exists"));
        assert!(!store.exists("c").await.expect("exists"));
    }

    #[tokio::test]
    async fn delete_idempotent() {
        let (url, _h) = start_mock().await;
        let store = HttpObjectStore::new(&url, "test-token", "wal", no_fence());
        store
            .delete_object("never-existed")
            .await
            .expect("delete nonexistent should succeed");
    }

    #[tokio::test]
    async fn list_objects_with_prefix() {
        let (url, _h) = start_mock().await;
        let store = HttpObjectStore::new(&url, "test-token", "wal", no_fence());

        store
            .upload_bytes("seg/001", b"1".to_vec())
            .await
            .expect("upload");
        store
            .upload_bytes("seg/002", b"2".to_vec())
            .await
            .expect("upload");
        store
            .upload_bytes("other/001", b"3".to_vec())
            .await
            .expect("upload");

        let keys = store.list_objects("seg/").await.expect("list");
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"seg/001".to_string()));
        assert!(keys.contains(&"seg/002".to_string()));
    }

    #[tokio::test]
    async fn list_objects_after() {
        let (url, _h) = start_mock().await;
        let store = HttpObjectStore::new(&url, "test-token", "wal", no_fence());

        store
            .upload_bytes("seg/001", b"1".to_vec())
            .await
            .expect("upload");
        store
            .upload_bytes("seg/002", b"2".to_vec())
            .await
            .expect("upload");
        store
            .upload_bytes("seg/003", b"3".to_vec())
            .await
            .expect("upload");

        let keys = store
            .list_objects_after("seg/", "seg/001")
            .await
            .expect("list");
        assert!(!keys.contains(&"seg/001".to_string()));
        assert!(keys.contains(&"seg/002".to_string()));
        assert!(keys.contains(&"seg/003".to_string()));
    }

    #[tokio::test]
    async fn upload_and_download_file() {
        let (url, _h) = start_mock().await;
        let store = HttpObjectStore::new(&url, "test-token", "wal", no_fence());

        let dir = tempfile::tempdir().expect("tmpdir");
        let src = dir.path().join("source.bin");
        let dst = dir.path().join("dest.bin");

        tokio::fs::write(&src, b"file content")
            .await
            .expect("write");

        store
            .upload_file("file-test", &src)
            .await
            .expect("upload file");
        store
            .download_file("file-test", &dst)
            .await
            .expect("download file");

        let content = tokio::fs::read(&dst).await.expect("read");
        assert_eq!(content, b"file content");
    }
}
