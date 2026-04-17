//! Integration tests for `CinchHttpStorage` against a mock axum server
//! that speaks the `/v1/sync/{prefix}/` protocol.
//!
//! Covers the full `StorageBackend` contract, Bearer auth, and Fence-Token
//! fencing semantics (including refusing writes with no lease and
//! rejecting stale fences server-side).

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get, head, post, put},
    Json, Router,
};
use serde::Deserialize;
use tokio::sync::Mutex;

use hadb_storage::{Fence, StorageBackend};
use hadb_storage_cinch::CinchHttpStorage;

// ── Mock server ────────────────────────────────────────────────────────────

#[derive(Clone, Default)]
struct MockState {
    objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    /// Current valid fence. If `Some(rev)`, writes must send `Fence-Token: rev`.
    /// If `None`, the server ignores Fence-Token.
    required_fence: Arc<Mutex<Option<u64>>>,
    /// Auth token the server expects.
    token: String,
}

impl MockState {
    fn new(token: &str) -> Self {
        Self {
            objects: Arc::new(Mutex::new(HashMap::new())),
            required_fence: Arc::new(Mutex::new(None)),
            token: token.to_string(),
        }
    }
}

fn check_auth(headers: &HeaderMap, expected: &str) -> bool {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|v| v == format!("Bearer {expected}"))
        .unwrap_or(false)
}

async fn check_fence(state: &MockState, headers: &HeaderMap) -> Result<(), StatusCode> {
    let required = *state.required_fence.lock().await;
    match required {
        None => Ok(()),
        Some(required) => {
            let supplied = headers
                .get("fence-token")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
            match supplied {
                Some(v) if v == required => Ok(()),
                _ => Err(StatusCode::PRECONDITION_FAILED),
            }
        }
    }
}

async fn mock_put(
    State(state): State<MockState>,
    Path(key): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<StatusCode, StatusCode> {
    if !check_auth(&headers, &state.token) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    check_fence(&state, &headers).await?;

    // Conditional PUT: If-None-Match: * or If-Match: <etag>
    let if_none_match = headers.get("if-none-match").and_then(|v| v.to_str().ok());
    let if_match = headers.get("if-match").and_then(|v| v.to_str().ok());
    {
        let mut guard = state.objects.lock().await;
        if if_none_match == Some("*") && guard.contains_key(&key) {
            return Err(StatusCode::PRECONDITION_FAILED);
        }
        if let Some(expected) = if_match {
            let current = guard.get(&key).map(|b| etag_for(b));
            if current.as_deref() != Some(expected) {
                return Err(StatusCode::PRECONDITION_FAILED);
            }
        }
        guard.insert(key, body.to_vec());
    }
    Ok(StatusCode::OK)
}

fn etag_for(bytes: &[u8]) -> String {
    // Simple content-based etag for tests; real server uses its own scheme.
    format!("\"{}\"", bytes.len())
}

async fn mock_get(
    State(state): State<MockState>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    if !check_auth(&headers, &state.token) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let objects = state.objects.lock().await;
    let Some(data) = objects.get(&key).cloned() else {
        return Err(StatusCode::NOT_FOUND);
    };

    if let Some(range) = headers.get("range").and_then(|v| v.to_str().ok()) {
        // Parse "bytes=start-end"
        if let Some(rest) = range.strip_prefix("bytes=") {
            if let Some((s, e)) = rest.split_once('-') {
                let start: usize = s.parse().unwrap_or(0);
                let end: usize = e.parse().unwrap_or(data.len());
                let slice = &data[start.min(data.len())..=end.min(data.len().saturating_sub(1))];
                let body = axum::body::Body::from(slice.to_vec());
                return Ok((StatusCode::PARTIAL_CONTENT, body).into_response());
            }
        }
    }

    Ok((StatusCode::OK, data).into_response())
}

async fn mock_head(
    State(state): State<MockState>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    if !check_auth(&headers, &state.token) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    if state.objects.lock().await.contains_key(&key) {
        Ok(StatusCode::OK)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn mock_delete(
    State(state): State<MockState>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    if !check_auth(&headers, &state.token) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    check_fence(&state, &headers).await?;
    state.objects.lock().await.remove(&key);
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct ListParams {
    prefix: Option<String>,
    after: Option<String>,
}

async fn mock_list(
    State(state): State<MockState>,
    Query(params): Query<ListParams>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    if !check_auth(&headers, &state.token) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let objects = state.objects.lock().await;
    let prefix = params.prefix.unwrap_or_default();
    let after = params.after.unwrap_or_default();
    let mut keys: Vec<String> = objects
        .keys()
        .filter(|k| k.starts_with(&prefix))
        .filter(|k| after.is_empty() || k.as_str() > after.as_str())
        .cloned()
        .collect();
    keys.sort();
    Ok(Json(serde_json::json!({
        "keys": keys,
        "is_truncated": false,
    })))
}

#[derive(Deserialize)]
struct BatchDeleteBody {
    keys: Vec<String>,
}

async fn mock_batch_delete(
    State(state): State<MockState>,
    headers: HeaderMap,
    Json(body): Json<BatchDeleteBody>,
) -> Result<impl IntoResponse, StatusCode> {
    if !check_auth(&headers, &state.token) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    check_fence(&state, &headers).await?;
    let mut objects = state.objects.lock().await;
    let mut deleted = 0usize;
    for k in &body.keys {
        if objects.remove(k).is_some() {
            deleted += 1;
        }
    }
    Ok(Json(serde_json::json!({ "deleted": deleted })))
}

fn mock_app(state: MockState) -> Router {
    Router::new()
        .route("/v1/sync/pages/_delete", post(mock_batch_delete))
        .route("/v1/sync/pages/{*key}", put(mock_put))
        .route("/v1/sync/pages/{*key}", get(mock_get))
        .route("/v1/sync/pages/{*key}", head(mock_head))
        .route("/v1/sync/pages/{*key}", delete(mock_delete))
        .route("/v1/sync/pages", get(mock_list))
        .with_state(state)
}

async fn start_mock(state: MockState) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}");
    let handle = tokio::spawn(async move {
        axum::serve(listener, mock_app(state)).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    (url, handle)
}

fn store(url: &str, token: &str) -> CinchHttpStorage {
    CinchHttpStorage::new(url, token, "pages")
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn put_get_roundtrips() {
    let state = MockState::new("tok");
    let (url, _h) = start_mock(state).await;
    let s = store(&url, "tok");

    s.put("k", b"hello").await.unwrap();
    let got = s.get("k").await.unwrap().unwrap();
    assert_eq!(got, b"hello");
}

#[tokio::test]
async fn get_missing_returns_none() {
    let state = MockState::new("tok");
    let (url, _h) = start_mock(state).await;
    let s = store(&url, "tok");
    assert!(s.get("nope").await.unwrap().is_none());
}

#[tokio::test]
async fn exists_reflects_state() {
    let state = MockState::new("tok");
    let (url, _h) = start_mock(state).await;
    let s = store(&url, "tok");

    assert!(!s.exists("k").await.unwrap());
    s.put("k", b"").await.unwrap();
    assert!(s.exists("k").await.unwrap());
}

#[tokio::test]
async fn delete_is_idempotent() {
    let state = MockState::new("tok");
    let (url, _h) = start_mock(state).await;
    let s = store(&url, "tok");

    s.put("k", b"v").await.unwrap();
    s.delete("k").await.unwrap();
    s.delete("k").await.unwrap();
    assert!(!s.exists("k").await.unwrap());
}

#[tokio::test]
async fn list_prefix_and_after() {
    let state = MockState::new("tok");
    let (url, _h) = start_mock(state).await;
    let s = store(&url, "tok");

    for k in ["a/1", "a/2", "a/3", "b/1"] {
        s.put(k, b"").await.unwrap();
    }
    let all_a = s.list("a/", None).await.unwrap();
    assert_eq!(all_a, vec!["a/1", "a/2", "a/3"]);
    let after = s.list("a/", Some("a/1")).await.unwrap();
    assert_eq!(after, vec!["a/2", "a/3"]);
}

#[tokio::test]
async fn range_get_returns_requested_slice() {
    let state = MockState::new("tok");
    let (url, _h) = start_mock(state).await;
    let s = store(&url, "tok");

    s.put("k", b"abcdefghij").await.unwrap();
    let slice = s.range_get("k", 2, 4).await.unwrap().unwrap();
    assert_eq!(slice, b"cdef");
}

#[tokio::test]
async fn unauthorized_token_fails() {
    let state = MockState::new("good-token");
    let (url, _h) = start_mock(state).await;
    let s = store(&url, "bad-token");
    let err = s.get("k").await.unwrap_err();
    assert!(err.to_string().contains("401"));
}

#[tokio::test]
async fn put_if_absent_first_wins() {
    let state = MockState::new("tok");
    let (url, _h) = start_mock(state).await;
    let s = store(&url, "tok");

    let a = s.put_if_absent("k", b"first").await.unwrap();
    assert!(a.success);
    let b = s.put_if_absent("k", b"second").await.unwrap();
    assert!(!b.success);
    assert_eq!(s.get("k").await.unwrap().unwrap(), b"first");
}

#[tokio::test]
async fn put_if_match_advances_and_rejects_stale() {
    let state = MockState::new("tok");
    let (url, _h) = start_mock(state).await;
    let s = store(&url, "tok");

    // Seed an object so we know its etag (server returns "<len>" for the body).
    s.put("k", b"hi").await.unwrap();
    // Mock server's etag is the len in quotes; put_if_match sends that via If-Match.
    let good = r#""2""#;
    let ok = s.put_if_match("k", b"hello", good).await.unwrap();
    assert!(ok.success);

    // Now the content is 5 bytes; stale etag "2" must be rejected.
    let stale = s.put_if_match("k", b"xxx", good).await.unwrap();
    assert!(!stale.success);
}

#[tokio::test]
async fn batch_delete_removes_keys() {
    let state = MockState::new("tok");
    let (url, _h) = start_mock(state).await;
    let s = store(&url, "tok");

    for k in ["a", "b", "c"] {
        s.put(k, b"").await.unwrap();
    }
    let deleted = s
        .delete_many(&["a".into(), "b".into(), "c".into()])
        .await
        .unwrap();
    assert_eq!(deleted, 3);
    assert!(!s.exists("a").await.unwrap());
    assert!(!s.exists("b").await.unwrap());
    assert!(!s.exists("c").await.unwrap());
}

#[tokio::test]
async fn metrics_count_operations() {
    let state = MockState::new("tok");
    let (url, _h) = start_mock(state).await;
    let s = store(&url, "tok");

    s.put("k", b"abcd").await.unwrap();
    s.get("k").await.unwrap();
    assert_eq!(s.put_count(), 1);
    assert_eq!(s.fetch_count(), 1);
    assert_eq!(s.bytes_put(), 4);
    assert_eq!(s.bytes_fetched(), 4);
}

// ── Fence-Token tests ──────────────────────────────────────────────────────

#[tokio::test]
async fn write_without_fence_when_server_requires_fails() {
    let state = MockState::new("tok");
    *state.required_fence.lock().await = Some(7);
    let (url, _h) = start_mock(state).await;

    // No Fence attached to the store — writes don't send a Fence-Token header;
    // the server rejects with 412.
    let s = store(&url, "tok");
    let err = s.put("k", b"v").await.unwrap_err();
    assert!(err.to_string().contains("412"));
}

#[tokio::test]
async fn write_with_correct_fence_succeeds() {
    let state = MockState::new("tok");
    *state.required_fence.lock().await = Some(7);
    let (url, _h) = start_mock(state).await;

    let (fence, writer) = Fence::new();
    writer.set(7);
    let s = store(&url, "tok").with_fence(fence);

    s.put("k", b"v").await.unwrap();
    assert_eq!(s.get("k").await.unwrap().unwrap(), b"v");
}

#[tokio::test]
async fn write_with_stale_fence_server_rejects_412() {
    let state = MockState::new("tok");
    *state.required_fence.lock().await = Some(10);
    let (url, _h) = start_mock(state).await;

    let (fence, writer) = Fence::new();
    writer.set(7); // Stale — server expects 10.
    let s = store(&url, "tok").with_fence(fence);

    let err = s.put("k", b"v").await.unwrap_err();
    assert!(err.to_string().contains("412"));
}

#[tokio::test]
async fn write_refuses_when_fence_unset() {
    let state = MockState::new("tok");
    let (url, _h) = start_mock(state).await;

    // Fence attached but no lease claimed yet (writer never called `set`).
    let (fence, _writer) = Fence::new();
    let s = store(&url, "tok").with_fence(fence);

    let err = s.put("k", b"v").await.unwrap_err();
    assert!(err.to_string().contains("no active lease"));
    // Server side: the request should never have been sent at all.
}

#[tokio::test]
async fn write_refuses_after_fence_cleared() {
    let state = MockState::new("tok");
    let (url, _h) = start_mock(state).await;

    let (fence, writer) = Fence::new();
    writer.set(5);
    let s = store(&url, "tok").with_fence(fence);
    s.put("before", b"").await.unwrap();

    writer.clear();
    let err = s.put("after", b"").await.unwrap_err();
    assert!(err.to_string().contains("no active lease"));
}

#[tokio::test]
async fn reads_do_not_require_fence() {
    let state = MockState::new("tok");
    *state.required_fence.lock().await = Some(7);
    let (url, _h) = start_mock(state).await;

    // Seed a value through a fenced-write path.
    {
        let (fence, writer) = Fence::new();
        writer.set(7);
        let writer_store = store(&url, "tok").with_fence(fence);
        writer_store.put("k", b"v").await.unwrap();
    }

    // Reader has NO fence; server doesn't check fence on GET.
    let (fence, _w) = Fence::new();
    let reader = store(&url, "tok").with_fence(fence);
    assert_eq!(reader.get("k").await.unwrap().unwrap(), b"v");
    assert!(reader.exists("k").await.unwrap());
    let keys = reader.list("", None).await.unwrap();
    assert!(keys.iter().any(|k| k == "k"));
}

#[tokio::test]
async fn delete_requires_fence_when_configured() {
    let state = MockState::new("tok");
    *state.required_fence.lock().await = Some(7);
    let (url, _h) = start_mock(state).await;

    let (fence, writer) = Fence::new();
    writer.set(7);
    let s = store(&url, "tok").with_fence(fence);
    s.put("k", b"v").await.unwrap();
    s.delete("k").await.unwrap();
    assert!(s.get("k").await.unwrap().is_none());
}

#[tokio::test]
async fn batch_delete_uses_fence() {
    let state = MockState::new("tok");
    *state.required_fence.lock().await = Some(11);
    let (url, _h) = start_mock(state).await;

    let (fence, writer) = Fence::new();
    writer.set(11);
    let s = store(&url, "tok").with_fence(fence);

    s.put("a", b"").await.unwrap();
    s.put("b", b"").await.unwrap();
    let deleted = s
        .delete_many(&["a".into(), "b".into()])
        .await
        .unwrap();
    assert_eq!(deleted, 2);
}
