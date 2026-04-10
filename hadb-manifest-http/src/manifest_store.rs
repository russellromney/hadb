//! HttpManifestStore: ManifestStore implementation over HTTP.
//!
//! HTTP API contract:
//!   GET  /v1/manifest?key=...  -> 200 { manifest: HaManifest } or 404
//!   POST /v1/manifest?key=...  -> 201 { etag: "..." } or 409 (conflict)
//!                                 Body: { manifest: HaManifest, expected_version: u64|null }
//!   HEAD /v1/manifest?key=...  -> 200 with X-Manifest-Version, X-Writer-Id, X-Lease-Epoch headers
//!                                 or 404
//!
//! The manifest key is passed as a query param. The Bearer token scopes
//! access to the database.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use hadb::{CasResult, HaManifest, ManifestMeta, ManifestStore};
use serde::{Deserialize, Serialize};

/// HTTP response for manifest GET.
#[derive(Debug, Deserialize)]
struct ManifestGetResponse {
    manifest: HaManifest,
}

/// HTTP request body for manifest POST.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ManifestPutRequest {
    manifest: HaManifest,
    expected_version: Option<u64>,
}

/// HTTP response for manifest POST.
#[derive(Debug, Deserialize)]
struct ManifestPutResponse {
    etag: String,
}

/// ManifestStore backed by an HTTP endpoint.
pub struct HttpManifestStore {
    client: reqwest::Client,
    endpoint: String,
    token: String,
}

impl HttpManifestStore {
    /// Create a new HTTP manifest store.
    ///
    /// - `endpoint`: Base URL of the manifest proxy (e.g., "https://manifest-proxy.example.com")
    /// - `token`: Bearer token for authentication
    pub fn new(endpoint: &str, token: &str) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("failed to build HTTP client");
        Self {
            client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            token: token.to_string(),
        }
    }

    /// Create with a custom reqwest client.
    pub fn with_client(client: reqwest::Client, endpoint: &str, token: &str) -> Self {
        Self {
            client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            token: token.to_string(),
        }
    }

    fn manifest_url(&self, key: &str) -> String {
        format!(
            "{}/v1/manifest?key={}",
            self.endpoint,
            urlencoding::encode(key)
        )
    }
}

mod urlencoding {
    pub fn encode(input: &str) -> String {
        let mut result = String::with_capacity(input.len());
        for byte in input.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    result.push(byte as char);
                }
                _ => {
                    result.push('%');
                    result.push_str(&format!("{:02X}", byte));
                }
            }
        }
        result
    }
}

#[async_trait]
impl ManifestStore for HttpManifestStore {
    async fn get(&self, key: &str) -> Result<Option<HaManifest>> {
        let resp = self
            .client
            .get(&self.manifest_url(key))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| anyhow!("HTTP manifest get failed: {}", e))?;

        match resp.status() {
            reqwest::StatusCode::OK => {
                let body: ManifestGetResponse = resp
                    .json()
                    .await
                    .map_err(|e| anyhow!("failed to parse manifest response: {}", e))?;
                Ok(Some(body.manifest))
            }
            reqwest::StatusCode::NOT_FOUND => Ok(None),
            status => {
                let text = resp.text().await.unwrap_or_default();
                Err(anyhow!("HTTP manifest get returned {}: {}", status, text))
            }
        }
    }

    async fn put(
        &self,
        key: &str,
        manifest: &HaManifest,
        expected_version: Option<u64>,
    ) -> Result<CasResult> {
        let body = ManifestPutRequest {
            manifest: manifest.clone(),
            expected_version,
        };

        let resp = self
            .client
            .post(&self.manifest_url(key))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| anyhow!("HTTP manifest put failed: {}", e))?;

        match resp.status() {
            reqwest::StatusCode::CREATED | reqwest::StatusCode::OK => {
                let body: ManifestPutResponse = resp
                    .json()
                    .await
                    .map_err(|e| anyhow!("failed to parse manifest put response: {}", e))?;
                Ok(CasResult {
                    success: true,
                    etag: Some(body.etag),
                })
            }
            reqwest::StatusCode::CONFLICT | reqwest::StatusCode::PRECONDITION_FAILED => {
                Ok(CasResult {
                    success: false,
                    etag: None,
                })
            }
            status => {
                let text = resp.text().await.unwrap_or_default();
                Err(anyhow!("HTTP manifest put returned {}: {}", status, text))
            }
        }
    }

    async fn meta(&self, key: &str) -> Result<Option<ManifestMeta>> {
        let resp = self
            .client
            .head(&self.manifest_url(key))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| anyhow!("HTTP manifest meta failed: {}", e))?;

        match resp.status() {
            reqwest::StatusCode::OK => {
                let headers = resp.headers();

                let version: u64 = headers
                    .get("X-Manifest-Version")
                    .ok_or_else(|| anyhow!("missing X-Manifest-Version header"))?
                    .to_str()
                    .map_err(|e| anyhow!("invalid X-Manifest-Version: {}", e))?
                    .parse()
                    .map_err(|e| anyhow!("non-numeric X-Manifest-Version: {}", e))?;

                let writer_id = headers
                    .get("X-Writer-Id")
                    .ok_or_else(|| anyhow!("missing X-Writer-Id header"))?
                    .to_str()
                    .map_err(|e| anyhow!("invalid X-Writer-Id: {}", e))?
                    .to_string();

                let lease_epoch: u64 = headers
                    .get("X-Lease-Epoch")
                    .ok_or_else(|| anyhow!("missing X-Lease-Epoch header"))?
                    .to_str()
                    .map_err(|e| anyhow!("invalid X-Lease-Epoch: {}", e))?
                    .parse()
                    .map_err(|e| anyhow!("non-numeric X-Lease-Epoch: {}", e))?;

                Ok(Some(ManifestMeta {
                    version,
                    writer_id,
                    lease_epoch,
                }))
            }
            reqwest::StatusCode::NOT_FOUND => Ok(None),
            status => {
                let text = resp.text().await.unwrap_or_default();
                Err(anyhow!("HTTP manifest meta returned {}: {}", status, text))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::{Query, State},
        http::{HeaderMap, StatusCode},
        response::IntoResponse,
        routing::{get, head, post},
        Json, Router,
    };
    use hadb::StorageManifest;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    // ========================================================================
    // Mock HTTP server implementing the manifest API
    // ========================================================================

    #[derive(Clone)]
    struct MockState {
        store: Arc<Mutex<HashMap<String, HaManifest>>>,
    }

    impl MockState {
        fn new() -> Self {
            Self {
                store: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    #[derive(Deserialize)]
    struct KeyParam {
        key: String,
    }

    async fn mock_get(
        State(state): State<MockState>,
        Query(params): Query<KeyParam>,
    ) -> impl IntoResponse {
        let store = state.store.lock().await;
        match store.get(&params.key) {
            Some(manifest) => {
                let body = serde_json::json!({ "manifest": manifest });
                (StatusCode::OK, Json(body)).into_response()
            }
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    async fn mock_put(
        State(state): State<MockState>,
        Query(params): Query<KeyParam>,
        Json(body): Json<ManifestPutRequest>,
    ) -> impl IntoResponse {
        let mut store = state.store.lock().await;

        match body.expected_version {
            None => {
                if store.contains_key(&params.key) {
                    return StatusCode::CONFLICT.into_response();
                }
                let mut m = body.manifest;
                m.version = 1;
                let etag = m.version.to_string();
                store.insert(params.key, m);
                (StatusCode::CREATED, Json(serde_json::json!({ "etag": etag }))).into_response()
            }
            Some(expected) => {
                let current = store.get(&params.key);
                match current {
                    Some(existing) if existing.version == expected => {
                        let mut m = body.manifest;
                        m.version = expected + 1;
                        let etag = m.version.to_string();
                        store.insert(params.key, m);
                        (StatusCode::OK, Json(serde_json::json!({ "etag": etag }))).into_response()
                    }
                    _ => StatusCode::CONFLICT.into_response(),
                }
            }
        }
    }

    async fn mock_head(
        State(state): State<MockState>,
        Query(params): Query<KeyParam>,
    ) -> impl IntoResponse {
        let store = state.store.lock().await;
        match store.get(&params.key) {
            Some(manifest) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    "X-Manifest-Version",
                    manifest.version.to_string().parse().expect("header"),
                );
                headers.insert(
                    "X-Writer-Id",
                    manifest.writer_id.parse().expect("header"),
                );
                headers.insert(
                    "X-Lease-Epoch",
                    manifest.lease_epoch.to_string().parse().expect("header"),
                );
                (StatusCode::OK, headers).into_response()
            }
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    fn mock_app(state: MockState) -> Router {
        Router::new()
            .route("/v1/manifest", get(mock_get))
            .route("/v1/manifest", post(mock_put))
            .route("/v1/manifest", head(mock_head))
            .with_state(state)
    }

    async fn start_mock_server() -> (String, tokio::task::JoinHandle<()>) {
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

    fn make_walrust_manifest(writer: &str, epoch: u64) -> HaManifest {
        HaManifest {
            version: 0,
            writer_id: writer.to_string(),
            lease_epoch: epoch,
            timestamp_ms: 1000,
            storage: StorageManifest::Walrust {
                txid: 1,
                changeset_prefix: "cs/".to_string(),
                latest_changeset_key: "cs/1".to_string(),
                snapshot_key: None,
                snapshot_txid: None,
            },
        }
    }

    // ========================================================================
    // Tests
    // ========================================================================

    #[tokio::test]
    async fn test_get_nonexistent() {
        let (url, _h) = start_mock_server().await;
        let store = HttpManifestStore::new(&url, "test-token");
        assert!(store.get("nope").await.expect("get").is_none());
    }

    #[tokio::test]
    async fn test_put_create_and_get() {
        let (url, _h) = start_mock_server().await;
        let store = HttpManifestStore::new(&url, "test-token");

        let m = make_walrust_manifest("node-1", 1);
        let res = store.put("db1", &m, None).await.expect("put");
        assert!(res.success);
        assert!(res.etag.is_some());

        let fetched = store.get("db1").await.expect("get").expect("should exist");
        assert_eq!(fetched.version, 1);
        assert_eq!(fetched.writer_id, "node-1");
        assert_eq!(fetched.lease_epoch, 1);
    }

    #[tokio::test]
    async fn test_put_create_conflict() {
        let (url, _h) = start_mock_server().await;
        let store = HttpManifestStore::new(&url, "test-token");

        let m = make_walrust_manifest("node-1", 1);
        store.put("db1", &m, None).await.expect("first put");

        let res = store.put("db1", &m, None).await.expect("second put");
        assert!(!res.success);
    }

    #[tokio::test]
    async fn test_put_update_correct_version() {
        let (url, _h) = start_mock_server().await;
        let store = HttpManifestStore::new(&url, "test-token");

        let m = make_walrust_manifest("node-1", 1);
        store.put("db1", &m, None).await.expect("create");

        let res = store
            .put("db1", &make_walrust_manifest("node-1", 2), Some(1))
            .await
            .expect("update");
        assert!(res.success);

        let fetched = store.get("db1").await.expect("get").expect("exists");
        assert_eq!(fetched.version, 2);
        assert_eq!(fetched.lease_epoch, 2);
    }

    #[tokio::test]
    async fn test_put_stale_version() {
        let (url, _h) = start_mock_server().await;
        let store = HttpManifestStore::new(&url, "test-token");

        let m = make_walrust_manifest("node-1", 1);
        store.put("db1", &m, None).await.expect("create");
        store
            .put("db1", &make_walrust_manifest("node-1", 1), Some(1))
            .await
            .expect("update v1->v2");

        let res = store
            .put("db1", &make_walrust_manifest("node-1", 1), Some(1))
            .await
            .expect("stale update");
        assert!(!res.success);
    }

    #[tokio::test]
    async fn test_meta_returns_correct_fields() {
        let (url, _h) = start_mock_server().await;
        let store = HttpManifestStore::new(&url, "test-token");

        let m = make_walrust_manifest("node-42", 7);
        store.put("db1", &m, None).await.expect("create");

        let meta = store.meta("db1").await.expect("meta").expect("exists");
        assert_eq!(meta.version, 1);
        assert_eq!(meta.writer_id, "node-42");
        assert_eq!(meta.lease_epoch, 7);
    }

    #[tokio::test]
    async fn test_meta_nonexistent() {
        let (url, _h) = start_mock_server().await;
        let store = HttpManifestStore::new(&url, "test-token");
        assert!(store.meta("nope").await.expect("meta").is_none());
    }

    #[tokio::test]
    async fn test_sequential_version_increments() {
        let (url, _h) = start_mock_server().await;
        let store = HttpManifestStore::new(&url, "test-token");

        let m = make_walrust_manifest("node-1", 1);
        store.put("db1", &m, None).await.expect("v1");
        assert_eq!(store.get("db1").await.unwrap().unwrap().version, 1);

        store.put("db1", &m, Some(1)).await.expect("v2");
        assert_eq!(store.get("db1").await.unwrap().unwrap().version, 2);

        store.put("db1", &m, Some(2)).await.expect("v3");
        assert_eq!(store.get("db1").await.unwrap().unwrap().version, 3);
    }

    #[tokio::test]
    async fn test_put_ignores_caller_version() {
        let (url, _h) = start_mock_server().await;
        let store = HttpManifestStore::new(&url, "test-token");

        let mut m = make_walrust_manifest("node-1", 1);
        m.version = 999;
        store.put("db1", &m, None).await.expect("create");

        let fetched = store.get("db1").await.unwrap().unwrap();
        assert_eq!(fetched.version, 1, "store must assign version, not use caller's");
    }

    #[tokio::test]
    async fn test_meta_updates_after_put() {
        let (url, _h) = start_mock_server().await;
        let store = HttpManifestStore::new(&url, "test-token");

        store
            .put("db1", &make_walrust_manifest("node-1", 1), None)
            .await
            .expect("create");
        store
            .put("db1", &make_walrust_manifest("node-2", 5), Some(1))
            .await
            .expect("update");

        let meta = store.meta("db1").await.unwrap().unwrap();
        assert_eq!(meta.version, 2);
        assert_eq!(meta.writer_id, "node-2");
        assert_eq!(meta.lease_epoch, 5);
    }
}
