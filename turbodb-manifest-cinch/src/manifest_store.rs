//! CinchManifestStore: ManifestStore implementation over Cinch's HTTP sync API.
//!
//! Wire contract (msgpack):
//! ```text
//!   GET  /v1/sync/manifest?key=...
//!       → 200 Content-Type: application/msgpack  body = rmp_serde(Manifest)
//!       → 404 (no manifest)
//!
//!   POST /v1/sync/manifest?key=...
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

use anyhow::{anyhow, Result};
use async_trait::async_trait;
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
            "{}/v1/sync/manifest?key={}",
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
impl ManifestStore for CinchManifestStore {
    async fn get(&self, key: &str) -> Result<Option<Manifest>> {
        let resp = self
            .client
            .get(&self.manifest_url(key))
            .bearer_auth(&self.token)
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
        let body = rmp_serde::to_vec(manifest)
            .map_err(|e| anyhow!("encode msgpack manifest: {}", e))?;

        let mut req = self
            .client
            .post(&self.manifest_url(key))
            .bearer_auth(&self.token)
            .header(CONTENT_TYPE, CONTENT_TYPE_MSGPACK)
            .body(body);

        if let Some(v) = expected_version {
            req = req.header(IF_MATCH, v.to_string());
        }

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

                Ok(Some(ManifestMeta {
                    version,
                    writer_id,
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
    //! The mock server here intentionally mirrors the real grabby
    //! handlers' behavior (msgpack in, msgpack out, `If-Match` for
    //! CAS) so that this test is a meaningful wire-contract check —
    //! not a self-fulfilling "client talks to a client-shaped mock."

    use super::*;
    use axum::{
        extract::{Query, State},
        http::{HeaderMap, StatusCode},
        response::{IntoResponse, Response},
        routing::{get, head, post},
        Router,
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Clone)]
    struct MockState {
        store: Arc<Mutex<HashMap<String, Manifest>>>,
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

    fn msgpack_response(status: StatusCode, body: Vec<u8>) -> Response {
        let mut resp = (status, body).into_response();
        resp.headers_mut()
            .insert(CONTENT_TYPE, CONTENT_TYPE_MSGPACK.parse().unwrap());
        resp
    }

    async fn mock_get(
        State(state): State<MockState>,
        Query(params): Query<KeyParam>,
    ) -> Response {
        let store = state.store.lock().await;
        match store.get(&params.key) {
            Some(manifest) => {
                let body = rmp_serde::to_vec(manifest).expect("encode");
                msgpack_response(StatusCode::OK, body)
            }
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    async fn mock_put(
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
                let body = rmp_serde::to_vec(&serde_json::json!({ "etag": new_version.to_string() }))
                    .expect("encode");
                msgpack_response(StatusCode::OK, body)
            }
            _ => StatusCode::CONFLICT.into_response(),
        }
    }

    async fn mock_head(
        State(state): State<MockState>,
        Query(params): Query<KeyParam>,
    ) -> Response {
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
                (StatusCode::OK, headers).into_response()
            }
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    fn mock_app(state: MockState) -> Router {
        Router::new()
            .route("/v1/sync/manifest", get(mock_get))
            .route("/v1/sync/manifest", post(mock_put))
            .route("/v1/sync/manifest", head(mock_head))
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

    fn make_manifest(writer: &str) -> Manifest {
        Manifest {
            version: 0,
            writer_id: writer.to_string(),
            timestamp_ms: 1000,
            payload: b"test-payload".to_vec(),
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
        assert_eq!(fetched.version, 1, "store must assign version, not use caller's");
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
}
