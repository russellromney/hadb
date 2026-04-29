//! CinchLeaseStore: LeaseStore implementation over the Cinch HTTP lease
//! protocol.
//!
//! Translates CAS lease operations to HTTP requests against Grabby's (or
//! engine's embedded) /v1/lease routes.
//!
//! HTTP API contract:
//!   POST   /v1/lease?key=...  -> 201 { fence }              (acquire, 409 if taken)
//!   PUT    /v1/lease?key=...  -> 200 { fence }              (heartbeat, If-Match required)
//!   GET    /v1/lease?key=...  -> 200 { fence, holder } or 404
//!   DELETE /v1/lease?key=...  -> 204                          (release)
//!
//! The `fence` field (NATS KV revision) is used as the opaque etag for
//! CAS operations. The `holder` field is base64-encoded lease data.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use hadb_lease::{CasResult, LeaseStore};
use serde::Deserialize;

/// HTTP response for lease read (GET).
#[derive(Debug, Deserialize)]
struct LeaseReadResponse {
    /// NATS KV revision, used as opaque etag.
    fence: u64,
    /// Base64-encoded lease holder data.
    holder: String,
}

/// HTTP response for lease write (POST acquire, PUT heartbeat).
#[derive(Debug, Deserialize)]
struct LeaseWriteResponse {
    fence: u64,
}

/// LeaseStore backed by an HTTP endpoint.
///
/// Two modes:
///
/// - **Public (token-scoped):** [`CinchLeaseStore::new`]. Authenticates
///   with a per-database Bearer token; the server scopes leases by
///   whatever that token identifies, and `key` identifies the lease
///   within that scope.
/// - **Internal (no auth, scope on the wire):**
///   [`CinchLeaseStore::new_internal`]. For system DBs whose token would
///   create a bootstrap cycle (e.g. `_system/token-db` cannot mint its
///   own auth). No Bearer header is sent; `database_id` rides as a
///   URL-encoded query parameter alongside `key`. The transport must be
///   network-isolated; auth is by topology.
pub struct CinchLeaseStore {
    client: reqwest::Client,
    endpoint: String,
    token: String,
    /// Returned by `key_for(_)` — the on-server lease name within whatever
    /// scope the token (or `internal_database_id`) identifies. Defaults
    /// to `"writer"` since each token covers exactly one writable
    /// database.
    lease_key: String,
    /// `Some(database_id)` for internal mode — sent as a query parameter
    /// because the internal listener does not consult Bearer tokens.
    /// `None` for public mode.
    internal_database_id: Option<String>,
}

impl CinchLeaseStore {
    /// Create a new HTTP lease store in public (token-scoped) mode.
    /// `token` is sent as a Bearer token on every request; the server
    /// scopes leases by whatever that token identifies. `key_for(_)`
    /// returns `"writer"` by default — override with [`with_lease_key`]
    /// if you need a different name.
    pub fn new(endpoint: &str, token: &str) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("failed to build HTTP client");
        Self {
            client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            token: token.to_string(),
            lease_key: "writer".to_string(),
            internal_database_id: None,
        }
    }

    /// Create a lease store for grabby's internal NoAuth listener.
    ///
    /// `database_id` is carried on every request as a URL-encoded query
    /// parameter (`?database_id=...`); no Bearer token is sent. Used for
    /// system DBs (`_system/token-db`, `_system/placement-db`,
    /// `_system/cp-config-db`) where a token-scoped lease would require
    /// auth material the system DB itself stores. The server is expected
    /// to be reachable only over a trusted network (Fly 6PN / private
    /// subnet / `.internal` DNS); confidentiality and integrity rest on
    /// transport isolation, not on this client.
    pub fn new_internal(internal_endpoint: &str, database_id: &str) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("failed to build HTTP client");
        Self {
            client,
            endpoint: internal_endpoint.trim_end_matches('/').to_string(),
            token: String::new(),
            lease_key: "writer".to_string(),
            internal_database_id: Some(database_id.to_string()),
        }
    }

    /// Create with a custom reqwest client (for testing or custom TLS config).
    pub fn with_client(client: reqwest::Client, endpoint: &str, token: &str) -> Self {
        Self {
            client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            token: token.to_string(),
            lease_key: "writer".to_string(),
            internal_database_id: None,
        }
    }

    /// Override the lease name `key_for(_)` returns. The Coordinator passes
    /// the database name as `scope`, but the Cinch HTTP server already
    /// scopes by token (public mode) or by query-param `database_id`
    /// (internal mode), so the scope is intentionally ignored here.
    pub fn with_lease_key(mut self, key: &str) -> Self {
        self.lease_key = key.to_string();
        self
    }

    fn lease_url(&self, key: &str) -> String {
        let mut url = format!(
            "{}/v1/lease?key={}",
            self.endpoint,
            urlencoding::encode(key)
        );
        if let Some(database_id) = &self.internal_database_id {
            url.push_str("&database_id=");
            url.push_str(&urlencoding::encode(database_id));
        }
        url
    }

    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if self.token.is_empty() {
            req
        } else {
            req.bearer_auth(&self.token)
        }
    }
}

/// URL-encode a string for use in query parameters.
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
impl LeaseStore for CinchLeaseStore {
    /// Server scopes by token; the per-database `scope` is ignored.
    fn key_for(&self, _scope: &str) -> String {
        self.lease_key.clone()
    }

    async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        let resp = self
            .auth(self.client.get(&self.lease_url(key)))
            .send()
            .await
            .map_err(|e| anyhow!("HTTP lease read failed: {}", e))?;

        match resp.status() {
            reqwest::StatusCode::OK => {
                let body: LeaseReadResponse = resp
                    .json()
                    .await
                    .map_err(|e| anyhow!("failed to parse lease read response: {}", e))?;
                use base64::Engine;
                let data = base64::engine::general_purpose::STANDARD
                    .decode(&body.holder)
                    .map_err(|e| anyhow!("failed to decode lease holder data: {}", e))?;
                // Use fence (revision) as the opaque etag
                Ok(Some((data, body.fence.to_string())))
            }
            reqwest::StatusCode::NOT_FOUND => Ok(None),
            status => {
                let text = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "<error reading body>".to_string());
                Err(anyhow!("HTTP lease read returned {}: {}", status, text))
            }
        }
    }

    async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult> {
        use base64::Engine;
        let body = serde_json::json!({
            "data": base64::engine::general_purpose::STANDARD.encode(&data),
        });

        let resp = self
            .auth(self.client.post(&self.lease_url(key)))
            .json(&body)
            .send()
            .await
            .map_err(|e| anyhow!("HTTP lease acquire failed: {}", e))?;

        match resp.status() {
            reqwest::StatusCode::CREATED => {
                let body: LeaseWriteResponse = resp
                    .json()
                    .await
                    .map_err(|e| anyhow!("failed to parse lease acquire response: {}", e))?;
                Ok(CasResult {
                    success: true,
                    etag: Some(body.fence.to_string()),
                })
            }
            reqwest::StatusCode::CONFLICT => Ok(CasResult {
                success: false,
                etag: None,
            }),
            status => {
                let text = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "<error reading body>".to_string());
                Err(anyhow!("HTTP lease acquire returned {}: {}", status, text))
            }
        }
    }

    async fn write_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<CasResult> {
        use base64::Engine;
        let body = serde_json::json!({
            "data": base64::engine::general_purpose::STANDARD.encode(&data),
        });

        let resp = self
            .auth(self.client.put(&self.lease_url(key)))
            .header("If-Match", etag)
            .json(&body)
            .send()
            .await
            .map_err(|e| anyhow!("HTTP lease heartbeat failed: {}", e))?;

        match resp.status() {
            reqwest::StatusCode::OK => {
                let body: LeaseWriteResponse = resp
                    .json()
                    .await
                    .map_err(|e| anyhow!("failed to parse lease heartbeat response: {}", e))?;
                Ok(CasResult {
                    success: true,
                    etag: Some(body.fence.to_string()),
                })
            }
            reqwest::StatusCode::CONFLICT | reqwest::StatusCode::PRECONDITION_FAILED => {
                Ok(CasResult {
                    success: false,
                    etag: None,
                })
            }
            status => {
                let text = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "<error reading body>".to_string());
                Err(anyhow!(
                    "HTTP lease heartbeat returned {}: {}",
                    status,
                    text
                ))
            }
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let resp = self
            .auth(self.client.delete(&self.lease_url(key)))
            .send()
            .await
            .map_err(|e| anyhow!("HTTP lease release failed: {}", e))?;

        match resp.status() {
            reqwest::StatusCode::NO_CONTENT | reqwest::StatusCode::NOT_FOUND => Ok(()),
            status => {
                let text = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "<error reading body>".to_string());
                Err(anyhow!("HTTP lease release returned {}: {}", status, text))
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
        routing::{delete, get, post, put},
        Json, Router,
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    // ========================================================================
    // Mock HTTP server implementing the fenced lease API
    // ========================================================================

    #[derive(Clone)]
    struct MockState {
        store: Arc<Mutex<HashMap<String, (Vec<u8>, u64)>>>, // key -> (data, revision)
        revision: Arc<Mutex<u64>>,
    }

    impl MockState {
        fn new() -> Self {
            Self {
                store: Arc::new(Mutex::new(HashMap::new())),
                revision: Arc::new(Mutex::new(0)),
            }
        }

        async fn next_rev(&self) -> u64 {
            let mut rev = self.revision.lock().await;
            *rev += 1;
            *rev
        }
    }

    #[derive(Deserialize)]
    struct KeyParam {
        key: String,
        /// Internal-mode tenant scoping. `None` for public token-scoped
        /// requests; in those tests the token (or a fixed bucket) is the
        /// scope. When present, the mock composes a `database_id|key`
        /// store key so two distinct database_ids cannot collide on the
        /// same `key=writer`.
        database_id: Option<String>,
    }

    impl KeyParam {
        fn store_key(&self) -> String {
            match &self.database_id {
                Some(db) => format!("{}|{}", db, self.key),
                None => self.key.clone(),
            }
        }
    }

    #[derive(Deserialize)]
    struct LeaseBody {
        data: String,
    }

    async fn mock_read(
        State(state): State<MockState>,
        Query(params): Query<KeyParam>,
    ) -> impl IntoResponse {
        let store_key = params.store_key();
        let store = state.store.lock().await;
        match store.get(&store_key) {
            Some((data, rev)) => {
                use base64::Engine;
                let holder = base64::engine::general_purpose::STANDARD.encode(data);
                (
                    StatusCode::OK,
                    Json(serde_json::json!({ "fence": rev, "holder": holder })),
                )
                    .into_response()
            }
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    async fn mock_acquire(
        State(state): State<MockState>,
        Query(params): Query<KeyParam>,
        Json(body): Json<LeaseBody>,
    ) -> impl IntoResponse {
        use base64::Engine;
        let data = base64::engine::general_purpose::STANDARD
            .decode(&body.data)
            .expect("invalid base64");

        let store_key = params.store_key();
        let mut store = state.store.lock().await;
        if store.contains_key(&store_key) {
            return (StatusCode::CONFLICT, "Lease already held").into_response();
        }

        let rev = state.next_rev().await;
        store.insert(store_key, (data, rev));
        (
            StatusCode::CREATED,
            Json(serde_json::json!({ "fence": rev })),
        )
            .into_response()
    }

    async fn mock_heartbeat(
        State(state): State<MockState>,
        Query(params): Query<KeyParam>,
        headers: HeaderMap,
        Json(body): Json<LeaseBody>,
    ) -> impl IntoResponse {
        use base64::Engine;
        let data = base64::engine::general_purpose::STANDARD
            .decode(&body.data)
            .expect("invalid base64");

        let expected_fence: u64 = match headers.get("If-Match") {
            Some(v) => v.to_str().unwrap().parse().unwrap(),
            None => return StatusCode::BAD_REQUEST.into_response(),
        };

        let store_key = params.store_key();
        let mut store = state.store.lock().await;
        match store.get(&store_key) {
            Some((_, current_rev)) if *current_rev == expected_fence => {
                let new_rev = state.next_rev().await;
                store.insert(store_key, (data, new_rev));
                (
                    StatusCode::OK,
                    Json(serde_json::json!({ "fence": new_rev })),
                )
                    .into_response()
            }
            _ => StatusCode::CONFLICT.into_response(),
        }
    }

    async fn mock_release(
        State(state): State<MockState>,
        Query(params): Query<KeyParam>,
    ) -> impl IntoResponse {
        let store_key = params.store_key();
        state.store.lock().await.remove(&store_key);
        StatusCode::NO_CONTENT
    }

    fn mock_app(state: MockState) -> Router {
        Router::new()
            .route("/v1/lease", get(mock_read))
            .route("/v1/lease", post(mock_acquire))
            .route("/v1/lease", put(mock_heartbeat))
            .route("/v1/lease", delete(mock_release))
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

    // ========================================================================
    // Tests
    // ========================================================================

    #[tokio::test]
    async fn test_read_nonexistent() {
        let (url, _h) = start_mock_server().await;
        let store = CinchLeaseStore::new(&url, "test-token");
        let result = store
            .read("no-such-key")
            .await
            .expect("read should succeed");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_acquire_and_read() {
        let (url, _h) = start_mock_server().await;
        let store = CinchLeaseStore::new(&url, "test-token");

        let cas = store
            .write_if_not_exists("lease1", b"node-a".to_vec())
            .await
            .expect("acquire");
        assert!(cas.success);
        assert!(cas.etag.is_some());

        let read = store
            .read("lease1")
            .await
            .expect("read")
            .expect("should exist");
        assert_eq!(read.0, b"node-a");
        assert_eq!(read.1, cas.etag.unwrap());
    }

    #[tokio::test]
    async fn test_acquire_conflict() {
        let (url, _h) = start_mock_server().await;
        let store = CinchLeaseStore::new(&url, "test-token");

        let first = store
            .write_if_not_exists("lease1", b"node-a".to_vec())
            .await
            .expect("first acquire");
        assert!(first.success);

        let second = store
            .write_if_not_exists("lease1", b"node-b".to_vec())
            .await
            .expect("second acquire");
        assert!(!second.success);
        assert!(second.etag.is_none());
    }

    #[tokio::test]
    async fn test_heartbeat_with_correct_fence() {
        let (url, _h) = start_mock_server().await;
        let store = CinchLeaseStore::new(&url, "test-token");

        let acquire = store
            .write_if_not_exists("lease1", b"v1".to_vec())
            .await
            .expect("acquire");
        let fence = acquire.etag.unwrap();

        let heartbeat = store
            .write_if_match("lease1", b"v2".to_vec(), &fence)
            .await
            .expect("heartbeat");
        assert!(heartbeat.success);
        assert!(heartbeat.etag.is_some());

        let (data, new_fence) = store.read("lease1").await.expect("read").expect("exists");
        assert_eq!(data, b"v2");
        assert_eq!(new_fence, heartbeat.etag.unwrap());
    }

    #[tokio::test]
    async fn test_heartbeat_stale_fence() {
        let (url, _h) = start_mock_server().await;
        let store = CinchLeaseStore::new(&url, "test-token");

        let acquire = store
            .write_if_not_exists("lease1", b"v1".to_vec())
            .await
            .expect("acquire");
        let first_fence = acquire.etag.unwrap();

        store
            .write_if_match("lease1", b"v2".to_vec(), &first_fence)
            .await
            .expect("advance");

        let stale = store
            .write_if_match("lease1", b"v3".to_vec(), &first_fence)
            .await
            .expect("stale heartbeat");
        assert!(!stale.success);
    }

    #[tokio::test]
    async fn test_release_and_read() {
        let (url, _h) = start_mock_server().await;
        let store = CinchLeaseStore::new(&url, "test-token");

        store
            .write_if_not_exists("lease1", b"node-a".to_vec())
            .await
            .expect("acquire");

        store.delete("lease1").await.expect("release");

        let read = store.read("lease1").await.expect("read");
        assert!(read.is_none());
    }

    #[tokio::test]
    async fn test_release_idempotent() {
        let (url, _h) = start_mock_server().await;
        let store = CinchLeaseStore::new(&url, "test-token");
        store
            .delete("never-existed")
            .await
            .expect("release nonexistent should succeed");
    }

    #[tokio::test]
    async fn test_release_then_reacquire() {
        let (url, _h) = start_mock_server().await;
        let store = CinchLeaseStore::new(&url, "test-token");

        store
            .write_if_not_exists("lease1", b"v1".to_vec())
            .await
            .expect("acquire");
        store.delete("lease1").await.expect("release");

        let result = store
            .write_if_not_exists("lease1", b"v2".to_vec())
            .await
            .expect("re-acquire");
        assert!(result.success);

        let (data, _) = store.read("lease1").await.expect("read").expect("exists");
        assert_eq!(data, b"v2");
    }

    #[tokio::test]
    async fn test_special_characters_in_key() {
        let (url, _h) = start_mock_server().await;
        let store = CinchLeaseStore::new(&url, "test-token");

        let key = "embedded/db-123/_lease.json";
        let cas = store
            .write_if_not_exists(key, b"data".to_vec())
            .await
            .expect("acquire");
        assert!(cas.success);

        let (data, _) = store.read(key).await.expect("read").expect("exists");
        assert_eq!(data, b"data");
    }

    #[tokio::test]
    async fn test_fence_is_numeric_string() {
        let (url, _h) = start_mock_server().await;
        let store = CinchLeaseStore::new(&url, "test-token");

        let cas = store
            .write_if_not_exists("lease1", b"data".to_vec())
            .await
            .expect("acquire");
        let fence = cas.etag.unwrap();

        // Fence should be a parseable u64 (NATS revision)
        let _: u64 = fence.parse().expect("fence should be a numeric string");
    }

    // ── Internal-mode tests (Hoist prereq) ──

    #[test]
    fn test_internal_url_includes_database_id() {
        let store = CinchLeaseStore::new_internal(
            "http://grabby.internal:8010",
            "_system/placement-db",
        );
        let url = store.lease_url("writer");
        assert!(url.contains("key=writer"), "url missing key: {}", url);
        assert!(
            url.contains("database_id=_system%2Fplacement-db"),
            "url missing url-encoded database_id: {}",
            url
        );
    }

    #[test]
    fn test_public_url_omits_database_id() {
        let store = CinchLeaseStore::new("http://grabby.example.com", "tok");
        let url = store.lease_url("writer");
        assert!(url.contains("key=writer"));
        assert!(
            !url.contains("database_id="),
            "public mode must not send database_id: {}",
            url
        );
    }

    #[test]
    fn test_internal_mode_skips_bearer() {
        // Empty token → auth() does not set Bearer header (existing
        // behavior pinned for the internal-mode contract).
        let store = CinchLeaseStore::new_internal(
            "http://grabby.internal:8010",
            "_system/token-db",
        );
        assert!(store.token.is_empty());
        assert!(store.internal_database_id.is_some());
    }

    #[tokio::test]
    async fn test_internal_acquire_and_read() {
        let (url, _h) = start_mock_server().await;
        let store =
            CinchLeaseStore::new_internal(&url, "_system/placement-db");

        let cas = store
            .write_if_not_exists("writer", b"placement-instance-A".to_vec())
            .await
            .expect("acquire");
        assert!(cas.success);

        let read = store
            .read("writer")
            .await
            .expect("read")
            .expect("should exist");
        assert_eq!(read.0, b"placement-instance-A");
    }

    #[tokio::test]
    async fn test_internal_database_ids_do_not_collide() {
        // The system DBs all use key="writer". Without database_id
        // scoping on the wire, three separate `_system/...` writers
        // would fight over one record. This test pins that the wire
        // shape carries database_id so two distinct system DBs can
        // each hold their own writer lease independently.
        let (url, _h) = start_mock_server().await;
        let placement = CinchLeaseStore::new_internal(&url, "_system/placement-db");
        let token_db = CinchLeaseStore::new_internal(&url, "_system/token-db");

        let p = placement
            .write_if_not_exists("writer", b"placement-A".to_vec())
            .await
            .expect("placement acquire");
        assert!(p.success, "placement acquire should succeed");

        // token-db's acquire on the same key="writer" must NOT see a
        // conflict because the mock scopes by (database_id, key).
        let t = token_db
            .write_if_not_exists("writer", b"token-A".to_vec())
            .await
            .expect("token-db acquire");
        assert!(
            t.success,
            "token-db acquire should not collide with placement-db"
        );

        let p_read = placement.read("writer").await.expect("p read").expect("p exists");
        let t_read = token_db.read("writer").await.expect("t read").expect("t exists");
        assert_eq!(p_read.0, b"placement-A");
        assert_eq!(t_read.0, b"token-A");
    }

    #[tokio::test]
    async fn test_internal_heartbeat_with_correct_fence() {
        let (url, _h) = start_mock_server().await;
        let store = CinchLeaseStore::new_internal(&url, "_system/cp-config-db");

        let acquire = store
            .write_if_not_exists("writer", b"v1".to_vec())
            .await
            .expect("acquire");
        let fence = acquire.etag.expect("fence");

        let heartbeat = store
            .write_if_match("writer", b"v2".to_vec(), &fence)
            .await
            .expect("heartbeat");
        assert!(heartbeat.success);

        let (data, _) = store.read("writer").await.expect("read").expect("exists");
        assert_eq!(data, b"v2");
    }

    #[tokio::test]
    async fn test_internal_release_and_reacquire() {
        let (url, _h) = start_mock_server().await;
        let store = CinchLeaseStore::new_internal(&url, "_system/token-db");

        store
            .write_if_not_exists("writer", b"v1".to_vec())
            .await
            .expect("acquire");
        store.delete("writer").await.expect("release");

        let read = store.read("writer").await.expect("read");
        assert!(read.is_none(), "lease should be gone after release");

        let reacquire = store
            .write_if_not_exists("writer", b"v2".to_vec())
            .await
            .expect("re-acquire");
        assert!(reacquire.success);
    }

    #[tokio::test]
    async fn test_internal_two_contenders_one_wins() {
        // Pinned by the Hoist plan: two writers race the initial claim
        // for the same system DB; exactly one wins.
        let (url, _h) = start_mock_server().await;
        let writer_a =
            CinchLeaseStore::new_internal(&url, "_system/placement-db");
        let writer_b =
            CinchLeaseStore::new_internal(&url, "_system/placement-db");

        let a = writer_a
            .write_if_not_exists("writer", b"A".to_vec())
            .await
            .expect("a acquire");
        let b = writer_b
            .write_if_not_exists("writer", b"B".to_vec())
            .await
            .expect("b acquire");

        assert!(a.success != b.success, "exactly one must win");
    }
}
