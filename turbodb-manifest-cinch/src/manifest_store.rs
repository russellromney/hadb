//! CinchManifestStore: ManifestStore implementation over Cinch's HTTP sync API.
//!
//! Wire contract (msgpack):
//! ```text
//!   GET  /v1/sync/manifest?key=...
//!       → 200 Content-Type: application/msgpack  body = rmp_serde(Manifest)
//!       → 404 (no manifest)
//!
//!   PUT  /v1/sync/manifest?key=...
//!       Content-Type: application/msgpack  body = rmp_serde(Manifest)
//!       Optional header `If-Match: <expected_version>`
//!           (absent = create; numeric = CAS against the server-side
//!           manifest's version field)
//!       → 201/200 Content-Type: application/msgpack  body = rmp_serde({ etag })
//!       → 409 (CAS mismatch or already exists)
//!
//!   HEAD /v1/sync/manifest?key=...
//!       → 200 with `X-Manifest-Version` + `X-Writer-Id` headers
//!       → 404 (no manifest)
//! ```
//!
//! PUT matches the verb used by the rest of grabby's sync API
//! (`/v1/sync/wal/{segment}`, `/v1/sync/snapshot`) for idempotent
//! writes; the query-string key names the resource.
//!
//! The manifest key is a query param. The Bearer token scopes access to
//! the database.
//!
//! Phase Turbogenesis-b: switched from JSON to msgpack. The
//! pre-Turbogenesis-b client wrapped the manifest in
//! `{ manifest, expected_version }` and sent JSON — which also happened
//! to be incompatible with grabby's /v1/sync/manifest server (it used
//! an If-Match header but the client never set one). The whole path
//! was effectively vestigial. Now both sides speak the same msgpack
//! wire.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use hadb_lease::FenceSource;
use hadb_storage::CasResult;
use reqwest::header::{CONTENT_TYPE, IF_MATCH};
use serde::Deserialize;
use turbodb::{Manifest, ManifestMeta, ManifestStore};

const CONTENT_TYPE_MSGPACK: &str = "application/msgpack";

#[derive(Debug, Deserialize)]
struct ManifestPutResponse {
    etag: String,
}

/// ManifestStore backed by Cinch's `/v1/sync/manifest` HTTP endpoint.
pub struct CinchManifestStore {
    client: reqwest::Client,
    endpoint: String,
    token: String,
    fence: Option<Arc<dyn FenceSource>>,
    max_retries: u32,
}

impl CinchManifestStore {
    /// Create a new Cinch manifest store.
    ///
    /// - `endpoint`: Base URL (e.g. `https://grabby.example.com`).
    /// - `token`: Bearer token for authentication.
    pub fn new(endpoint: &str, token: &str) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("failed to build HTTP client");
        Self {
            client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            token: token.to_string(),
            fence: None,
            max_retries: 3,
        }
    }

    /// Create with a custom reqwest client.
    pub fn with_client(client: reqwest::Client, endpoint: &str, token: &str) -> Self {
        Self {
            client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            token: token.to_string(),
            fence: None,
            max_retries: 3,
        }
    }

    /// Attach a fence source. Manifest writes will require a current lease
    /// revision and send `Fence-Token` headers.
    pub fn with_fence(mut self, fence: Arc<dyn FenceSource>) -> Self {
        self.fence = Some(fence);
        self
    }

    fn manifest_url(&self, key: &str) -> String {
        format!(
            "{}/v1/sync/manifest?key={}",
            self.endpoint,
            urlencoding::encode(key)
        )
    }

    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if self.token.is_empty() {
            req
        } else {
            req.bearer_auth(&self.token)
        }
    }

    fn apply_fence(&self, req: reqwest::RequestBuilder) -> Result<reqwest::RequestBuilder> {
        match &self.fence {
            Some(fence) => {
                let rev = fence
                    .require()
                    .map_err(|_| anyhow!("no active lease; refusing manifest write"))?;
                Ok(req.header("Fence-Token", rev.to_string()))
            }
            None => Ok(req),
        }
    }

    async fn backoff(&self, attempt: u32) {
        let ms = 10u64.saturating_mul(1u64 << attempt.min(8));
        tokio::time::sleep(Duration::from_millis(ms)).await;
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
impl ManifestStore for CinchManifestStore {
    async fn get(&self, key: &str) -> Result<Option<Manifest>> {
        let resp = self
            .auth(self.client.get(&self.manifest_url(key)))
            .header(reqwest::header::ACCEPT, CONTENT_TYPE_MSGPACK)
            .send()
            .await
            .map_err(|e| anyhow!("HTTP manifest get failed: {}", e))?;

        match resp.status() {
            reqwest::StatusCode::OK => {
                let bytes = resp
                    .bytes()
                    .await
                    .map_err(|e| anyhow!("read manifest response bytes: {}", e))?;
                let manifest: Manifest = rmp_serde::from_slice(&bytes)
                    .map_err(|e| anyhow!("decode msgpack manifest: {}", e))?;
                Ok(Some(manifest))
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
        manifest: &Manifest,
        expected_version: Option<u64>,
    ) -> Result<CasResult> {
        let body =
            rmp_serde::to_vec(manifest).map_err(|e| anyhow!("encode msgpack manifest: {}", e))?;
        let mut attempt = 0u32;
        loop {
            let mut req = self
                .auth(self.client.put(&self.manifest_url(key)))
                .header(CONTENT_TYPE, CONTENT_TYPE_MSGPACK)
                .body(body.clone());

            if let Some(v) = expected_version {
                req = req.header(IF_MATCH, v.to_string());
            }

            let req = self.apply_fence(req)?;
            let resp = req
                .send()
                .await
                .map_err(|e| anyhow!("HTTP manifest put failed: {}", e))?;

            match resp.status() {
                reqwest::StatusCode::CREATED | reqwest::StatusCode::OK => {
                    let bytes = resp
                        .bytes()
                        .await
                        .map_err(|e| anyhow!("read put response bytes: {}", e))?;
                    let body: ManifestPutResponse = rmp_serde::from_slice(&bytes)
                        .map_err(|e| anyhow!("decode msgpack put response: {}", e))?;
                    return Ok(CasResult {
                        success: true,
                        etag: Some(body.etag),
                    });
                }
                reqwest::StatusCode::CONFLICT => {
                    return Ok(CasResult {
                        success: false,
                        etag: None,
                    });
                }
                reqwest::StatusCode::PRECONDITION_FAILED => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        let text = resp.text().await.unwrap_or_default();
                        return Err(anyhow!(
                            "HTTP manifest put returned {} after {} attempts: {}",
                            reqwest::StatusCode::PRECONDITION_FAILED,
                            attempt,
                            text
                        ));
                    }
                    self.backoff(attempt).await;
                }
                status => {
                    let text = resp.text().await.unwrap_or_default();
                    return Err(anyhow!("HTTP manifest put returned {}: {}", status, text));
                }
            }
        }
    }

    async fn meta(&self, key: &str) -> Result<Option<ManifestMeta>> {
        let resp = self
            .auth(self.client.head(&self.manifest_url(key)))
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

                Ok(Some(ManifestMeta { version, writer_id }))
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
    //! The mock server here intentionally mirrors the real grabby
    //! handlers' behavior (msgpack in, msgpack out, `If-Match` for
    //! CAS) so that this test is a meaningful wire-contract check —
    //! not a self-fulfilling "client talks to a client-shaped mock."

    use super::*;
    use axum::{
        extract::{Query, State},
        http::{HeaderMap, StatusCode},
        response::{IntoResponse, Response},
        routing::{get, head, put},
        Router,
    };
    use hadb_lease::FenceSource;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Clone)]
    struct MockState {
        store: Arc<Mutex<HashMap<String, Manifest>>>,
        required_fence: Option<u64>,
    }

    impl MockState {
        fn new() -> Self {
            Self {
                store: Arc::new(Mutex::new(HashMap::new())),
                required_fence: None,
            }
        }

        fn with_required_fence(required_fence: u64) -> Self {
            Self {
                store: Arc::new(Mutex::new(HashMap::new())),
                required_fence: Some(required_fence),
            }
        }
    }

    #[derive(Deserialize)]
    struct KeyParam {
        key: String,
    }

    fn msgpack_response(status: StatusCode, body: Vec<u8>) -> Response {
        let mut resp = (status, body).into_response();
        resp.headers_mut()
            .insert(CONTENT_TYPE, CONTENT_TYPE_MSGPACK.parse().unwrap());
        resp
    }

    async fn mock_get(State(state): State<MockState>, Query(params): Query<KeyParam>) -> Response {
        let store = state.store.lock().await;
        match store.get(&params.key) {
            Some(manifest) => {
                let body = rmp_serde::to_vec(manifest).expect("encode");
                msgpack_response(StatusCode::OK, body)
            }
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    async fn mock_update(
        State(state): State<MockState>,
        Query(params): Query<KeyParam>,
        headers: HeaderMap,
        body: axum::body::Bytes,
    ) -> Response {
        let incoming: Manifest = match rmp_serde::from_slice(&body) {
            Ok(m) => m,
            Err(e) => return (StatusCode::BAD_REQUEST, format!("decode: {e}")).into_response(),
        };
        let expected: Option<u64> = match headers.get(IF_MATCH) {
            Some(v) => match v.to_str().ok().and_then(|s| s.parse().ok()) {
                Some(n) => Some(n),
                None => return (StatusCode::BAD_REQUEST, "bad if-match").into_response(),
            },
            None => None,
        };
        if let Some(required_fence) = state.required_fence {
            let provided_fence = headers
                .get("Fence-Token")
                .or_else(|| headers.get("fence-token"))
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
            if provided_fence != Some(required_fence) {
                return StatusCode::PRECONDITION_FAILED.into_response();
            }
        }

        let mut store = state.store.lock().await;
        match (expected, store.get(&params.key).cloned()) {
            (None, Some(_)) => StatusCode::CONFLICT.into_response(),
            (None, None) => {
                let mut m = incoming;
                m.version = 1;
                store.insert(params.key, m);
                let body = rmp_serde::to_vec(&serde_json::json!({ "etag": "1" })).expect("encode");
                msgpack_response(StatusCode::CREATED, body)
            }
            (Some(v), Some(existing)) if existing.version == v => {
                let new_version = v + 1;
                let mut m = incoming;
                m.version = new_version;
                store.insert(params.key, m);
                let body =
                    rmp_serde::to_vec(&serde_json::json!({ "etag": new_version.to_string() }))
                        .expect("encode");
                msgpack_response(StatusCode::OK, body)
            }
            _ => StatusCode::CONFLICT.into_response(),
        }
    }

    async fn mock_head(State(state): State<MockState>, Query(params): Query<KeyParam>) -> Response {
        let store = state.store.lock().await;
        match store.get(&params.key) {
            Some(manifest) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    "X-Manifest-Version",
                    manifest.version.to_string().parse().expect("header"),
                );
                headers.insert("X-Writer-Id", manifest.writer_id.parse().expect("header"));
                (StatusCode::OK, headers).into_response()
            }
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    fn mock_app(state: MockState) -> Router {
        Router::new()
            .route("/v1/sync/manifest", get(mock_get))
            .route("/v1/sync/manifest", put(mock_update))
            .route("/v1/sync/manifest", head(mock_head))
            .with_state(state)
    }

    async fn start_mock_server() -> (String, tokio::task::JoinHandle<()>) {
        start_mock_server_with_state(MockState::new()).await
    }

    async fn start_mock_server_with_state(
        state: MockState,
    ) -> (String, tokio::task::JoinHandle<()>) {
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

    fn make_manifest(writer: &str) -> Manifest {
        Manifest {
            version: 0,
            writer_id: writer.to_string(),
            timestamp_ms: 1000,
            payload: b"test-payload".to_vec(),
        }
    }

    struct StaticFence(Option<u64>);

    impl FenceSource for StaticFence {
        fn current(&self) -> Option<u64> {
            self.0
        }
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let (url, _h) = start_mock_server().await;
        let store = CinchManifestStore::new(&url, "test-token");
        assert!(store.get("nope").await.expect("get").is_none());
    }

    #[tokio::test]
    async fn test_put_create_and_get() {
        let (url, _h) = start_mock_server().await;
        let store = CinchManifestStore::new(&url, "test-token");

        let m = make_manifest("node-1");
        let res = store.put("db1", &m, None).await.expect("put");
        assert!(res.success);
        assert_eq!(res.etag.as_deref(), Some("1"));

        let fetched = store.get("db1").await.expect("get").expect("should exist");
        assert_eq!(fetched.version, 1);
        assert_eq!(fetched.writer_id, "node-1");
        assert_eq!(fetched.payload, b"test-payload");
    }

    #[tokio::test]
    async fn test_put_create_conflict() {
        let (url, _h) = start_mock_server().await;
        let store = CinchManifestStore::new(&url, "test-token");

        let m = make_manifest("node-1");
        store.put("db1", &m, None).await.expect("first put");

        let res = store.put("db1", &m, None).await.expect("second put");
        assert!(!res.success);
    }

    #[tokio::test]
    async fn test_put_update_correct_version() {
        let (url, _h) = start_mock_server().await;
        let store = CinchManifestStore::new(&url, "test-token");

        let m = make_manifest("node-1");
        store.put("db1", &m, None).await.expect("create");

        let res = store
            .put("db1", &make_manifest("node-1"), Some(1))
            .await
            .expect("update");
        assert!(res.success);
        assert_eq!(res.etag.as_deref(), Some("2"));

        let fetched = store.get("db1").await.expect("get").expect("exists");
        assert_eq!(fetched.version, 2);
    }

    #[tokio::test]
    async fn test_put_stale_version() {
        let (url, _h) = start_mock_server().await;
        let store = CinchManifestStore::new(&url, "test-token");

        let m = make_manifest("node-1");
        store.put("db1", &m, None).await.expect("create");
        store
            .put("db1", &make_manifest("node-1"), Some(1))
            .await
            .expect("update v1->v2");

        let res = store
            .put("db1", &make_manifest("node-1"), Some(1))
            .await
            .expect("stale update");
        assert!(!res.success);
    }

    #[tokio::test]
    async fn test_meta_returns_correct_fields() {
        let (url, _h) = start_mock_server().await;
        let store = CinchManifestStore::new(&url, "test-token");

        let m = make_manifest("node-42");
        store.put("db1", &m, None).await.expect("create");

        let meta = store.meta("db1").await.expect("meta").expect("exists");
        assert_eq!(meta.version, 1);
        assert_eq!(meta.writer_id, "node-42");
    }

    #[tokio::test]
    async fn test_meta_nonexistent() {
        let (url, _h) = start_mock_server().await;
        let store = CinchManifestStore::new(&url, "test-token");
        assert!(store.meta("nope").await.expect("meta").is_none());
    }

    #[tokio::test]
    async fn test_sequential_version_increments() {
        let (url, _h) = start_mock_server().await;
        let store = CinchManifestStore::new(&url, "test-token");

        let m = make_manifest("node-1");
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
        let store = CinchManifestStore::new(&url, "test-token");

        let mut m = make_manifest("node-1");
        m.version = 999;
        store.put("db1", &m, None).await.expect("create");

        let fetched = store.get("db1").await.unwrap().unwrap();
        assert_eq!(
            fetched.version, 1,
            "store must assign version, not use caller's"
        );
    }

    #[tokio::test]
    async fn test_meta_updates_after_put() {
        let (url, _h) = start_mock_server().await;
        let store = CinchManifestStore::new(&url, "test-token");

        store
            .put("db1", &make_manifest("node-1"), None)
            .await
            .expect("create");
        store
            .put("db1", &make_manifest("node-2"), Some(1))
            .await
            .expect("update");

        let meta = store.meta("db1").await.unwrap().unwrap();
        assert_eq!(meta.version, 2);
        assert_eq!(meta.writer_id, "node-2");
    }

    #[tokio::test]
    async fn test_content_type_is_msgpack_not_json() {
        // Regression guard against a future accidental revert to JSON.
        // The mock server only decodes msgpack; if the client sent JSON
        // it would 400 on the decode step.
        let (url, _h) = start_mock_server().await;
        let store = CinchManifestStore::new(&url, "test-token");

        let payload: Vec<u8> = (0u32..512).map(|i| (i & 0xff) as u8).collect();
        let m = Manifest {
            version: 0,
            writer_id: "node-1".into(),
            timestamp_ms: 0,
            payload: payload.clone(),
        };
        let res = store.put("db1", &m, None).await.expect("put");
        assert!(res.success);

        let fetched = store.get("db1").await.expect("get").expect("exists");
        assert_eq!(fetched.payload, payload, "binary payload must round-trip");
    }

    #[tokio::test]
    async fn test_put_with_required_fence_succeeds() {
        let (url, _h) = start_mock_server_with_state(MockState::with_required_fence(7)).await;
        let store =
            CinchManifestStore::new(&url, "test-token").with_fence(Arc::new(StaticFence(Some(7))));

        let res = store
            .put("db1", &make_manifest("node-1"), None)
            .await
            .expect("put");
        assert!(res.success);
        assert_eq!(res.etag.as_deref(), Some("1"));
    }

    #[tokio::test]
    async fn test_put_without_required_fence_fails() {
        let (url, _h) = start_mock_server_with_state(MockState::with_required_fence(7)).await;
        let store = CinchManifestStore::new(&url, "test-token");

        let err = store
            .put("db1", &make_manifest("node-1"), None)
            .await
            .expect_err("missing fence must fail");
        let msg = err.to_string();
        assert!(msg.contains("HTTP manifest put returned"));
        assert!(msg.contains("412"));
        assert!(msg.contains("after 3 attempts"));
    }
}
