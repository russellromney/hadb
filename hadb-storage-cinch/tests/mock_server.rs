//! Integration tests for `CinchHttpStorage` against a mock axum server
//! that speaks the `/v1/sync/{prefix}/` protocol.
//!
//! Covers the full `StorageBackend` contract, Bearer auth, and Fence-Token
//! fencing semantics (including refusing writes with no lease and
//! rejecting stale fences server-side).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
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

use hadb_lease::FenceSource;
use hadb_lease_cinch::AtomicFence;
use hadb_storage::StorageBackend;
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
    /// If > 0, the next N requests that touch these counters return 503.
    /// Used to exercise retry/backoff paths. Decremented once per triggered
    /// 503 response.
    flaky_write_failures: Arc<AtomicU32>,
    /// If > 0, the next N list requests return `{keys: [], is_truncated: true}`
    /// to simulate a protocol-violating server. Decremented per triggered 503.
    empty_truncated_count: Arc<AtomicU32>,
    /// If set, the next batch-delete response reports `N` keys in `failed`.
    batch_delete_failed_count: Arc<AtomicU32>,
}

impl MockState {
    fn new(token: &str) -> Self {
        Self {
            objects: Arc::new(Mutex::new(HashMap::new())),
            required_fence: Arc::new(Mutex::new(None)),
            token: token.to_string(),
            flaky_write_failures: Arc::new(AtomicU32::new(0)),
            empty_truncated_count: Arc::new(AtomicU32::new(0)),
            batch_delete_failed_count: Arc::new(AtomicU32::new(0)),
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

/// Returns `true` iff this request should be rejected with 503 (to exercise
/// client-side retry). Decrements the counter on success.
fn trigger_flaky(state: &MockState) -> bool {
    let prev = state.flaky_write_failures.load(Ordering::SeqCst);
    if prev == 0 {
        return false;
    }
    state
        .flaky_write_failures
        .fetch_sub(1, Ordering::SeqCst);
    true
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
    if trigger_flaky(&state) {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
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

    // Protocol-violation simulator: return empty-but-truncated for the
    // configured number of responses, then behave normally.
    if state.empty_truncated_count.load(Ordering::SeqCst) > 0 {
        state
            .empty_truncated_count
            .fetch_sub(1, Ordering::SeqCst);
        return Ok(Json(serde_json::json!({
            "keys": Vec::<String>::new(),
            "is_truncated": true,
        })));
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
    let fail_n = state.batch_delete_failed_count.load(Ordering::SeqCst) as usize;
    let mut deleted = 0usize;
    let mut failed: Vec<String> = Vec::new();
    for (i, k) in body.keys.iter().enumerate() {
        if i < fail_n {
            failed.push(k.clone());
            continue;
        }
        if objects.remove(k).is_some() {
            deleted += 1;
        }
    }
    // Reset the simulator after one batch.
    state.batch_delete_failed_count.store(0, Ordering::SeqCst);
    Ok(Json(serde_json::json!({
        "deleted": deleted,
        "failed": failed,
    })))
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

    s.put("k", b"hi").await.unwrap();
    let good = r#""2""#;
    let ok = s.put_if_match("k", b"hello", good).await.unwrap();
    assert!(ok.success);

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

    let s = store(&url, "tok");
    let err = s.put("k", b"v").await.unwrap_err();
    assert!(err.to_string().contains("412"));
}

#[tokio::test]
async fn write_with_correct_fence_succeeds() {
    let state = MockState::new("tok");
    *state.required_fence.lock().await = Some(7);
    let (url, _h) = start_mock(state).await;

    let (fence, writer) = AtomicFence::new();
    writer.set(7);
    let s = store(&url, "tok").with_fence(Arc::new(fence) as Arc<dyn FenceSource>);

    s.put("k", b"v").await.unwrap();
    assert_eq!(s.get("k").await.unwrap().unwrap(), b"v");
}

#[tokio::test]
async fn write_with_stale_fence_server_rejects_412() {
    let state = MockState::new("tok");
    *state.required_fence.lock().await = Some(10);
    let (url, _h) = start_mock(state).await;

    let (fence, writer) = AtomicFence::new();
    writer.set(7); // Stale: server expects 10.
    let s = store(&url, "tok").with_fence(Arc::new(fence) as Arc<dyn FenceSource>);

    let err = s.put("k", b"v").await.unwrap_err();
    assert!(err.to_string().contains("412"));
}

#[tokio::test]
async fn write_retries_on_412_when_fence_advances() {
    // Regression: Phase Shoal v2's renewal loop advances the server's
    // NATS-KV revision a few ms before our local AtomicFence picks up
    // the new value. A turbolite page write in that window sends the
    // stale fence, server returns 412, and pre-fix turbolite surfaced
    // the error as SQLite "disk I/O error" during migrate — breaking
    // provisioning on strict e2e runs.
    //
    // This test simulates the race: server requires fence=10, writer
    // starts at fence=7 (stale), and a background task bumps the
    // writer to fence=10 after a short delay. The put should succeed
    // on retry, not fail.
    let state = MockState::new("tok");
    *state.required_fence.lock().await = Some(10);
    let (url, _h) = start_mock(state).await;

    let (fence, writer) = AtomicFence::new();
    writer.set(7);
    let s = store(&url, "tok").with_fence(Arc::new(fence) as Arc<dyn FenceSource>);

    // Bump the fence to the server's required value after the first
    // request has had time to fail. The put retry-loop's backoff
    // (10ms * 2^attempt) will cover this. AtomicFenceWriter is not
    // Clone by design (single-owner semantic), so we move it into
    // the task.
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        writer.set(10);
    });

    // Should succeed after retrying with the updated fence.
    s.put("race", b"v").await.expect("put should retry past 412");
    assert_eq!(s.get("race").await.unwrap().unwrap(), b"v");
}

#[tokio::test]
async fn write_refuses_when_fence_unset() {
    let state = MockState::new("tok");
    let (url, _h) = start_mock(state).await;

    let (fence, _writer) = AtomicFence::new();
    let s = store(&url, "tok").with_fence(Arc::new(fence) as Arc<dyn FenceSource>);

    let err = s.put("k", b"v").await.unwrap_err();
    assert!(err.to_string().contains("no active lease"));
}

#[tokio::test]
async fn write_refuses_after_fence_cleared() {
    let state = MockState::new("tok");
    let (url, _h) = start_mock(state).await;

    let (fence, writer) = AtomicFence::new();
    writer.set(5);
    let s = store(&url, "tok").with_fence(Arc::new(fence) as Arc<dyn FenceSource>);
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

    {
        let (fence, writer) = AtomicFence::new();
        writer.set(7);
        let writer_store =
            store(&url, "tok").with_fence(Arc::new(fence) as Arc<dyn FenceSource>);
        writer_store.put("k", b"v").await.unwrap();
    }

    let (fence, _w) = AtomicFence::new();
    let reader = store(&url, "tok").with_fence(Arc::new(fence) as Arc<dyn FenceSource>);
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

    let (fence, writer) = AtomicFence::new();
    writer.set(7);
    let s = store(&url, "tok").with_fence(Arc::new(fence) as Arc<dyn FenceSource>);
    s.put("k", b"v").await.unwrap();
    s.delete("k").await.unwrap();
    assert!(s.get("k").await.unwrap().is_none());
}

#[tokio::test]
async fn batch_delete_uses_fence() {
    let state = MockState::new("tok");
    *state.required_fence.lock().await = Some(11);
    let (url, _h) = start_mock(state).await;

    let (fence, writer) = AtomicFence::new();
    writer.set(11);
    let s = store(&url, "tok").with_fence(Arc::new(fence) as Arc<dyn FenceSource>);

    s.put("a", b"").await.unwrap();
    s.put("b", b"").await.unwrap();
    let deleted = s
        .delete_many(&["a".into(), "b".into()])
        .await
        .unwrap();
    assert_eq!(deleted, 2);
}

// ── Regression tests: review-fix findings ──────────────────────────────────

/// C1/K10: `list` must refuse to silently truncate when the server returns
/// `{keys: [], is_truncated: true}`. Before the fix, pagination ended early.
#[tokio::test]
async fn list_rejects_empty_but_truncated_page() {
    let state = MockState::new("tok");
    state.empty_truncated_count.store(1, Ordering::SeqCst);
    let (url, _h) = start_mock(state.clone()).await;
    let s = store(&url, "tok");

    // Seed something so pagination would otherwise have content to return.
    state
        .objects
        .lock()
        .await
        .insert("k".to_string(), b"v".to_vec());

    let err = s.list("", None).await.unwrap_err();
    assert!(
        err.to_string().contains("is_truncated"),
        "expected protocol-violation error, got: {err}"
    );
}

/// C2/C3: conditional PUT must retry 5xx (transient) but not retry 412 (CAS).
#[tokio::test]
async fn put_if_absent_retries_transient_503() {
    let state = MockState::new("tok");
    state.flaky_write_failures.store(2, Ordering::SeqCst);
    let (url, _h) = start_mock(state).await;
    let s = store(&url, "tok");

    // First two attempts 503, third succeeds (default max_retries = 3).
    let r = s.put_if_absent("k", b"v").await.unwrap();
    assert!(r.success);
    assert_eq!(s.get("k").await.unwrap().unwrap(), b"v");
}

#[tokio::test]
async fn put_if_match_retries_transient_503() {
    let state = MockState::new("tok");
    let (url, _h) = start_mock(state.clone()).await;
    let s = store(&url, "tok");

    s.put("k", b"hi").await.unwrap();
    let good = r#""2""#;

    // Now arm 2 flaky failures.
    state.flaky_write_failures.store(2, Ordering::SeqCst);
    let r = s.put_if_match("k", b"hello", good).await.unwrap();
    assert!(r.success);
}

/// C4: `delete_many` must surface explicit per-key server failures as Err,
/// not swallow them with a reduced count.
#[tokio::test]
async fn delete_many_fails_on_server_reported_failures() {
    let state = MockState::new("tok");
    state.batch_delete_failed_count.store(1, Ordering::SeqCst);
    let (url, _h) = start_mock(state).await;
    let s = store(&url, "tok");

    s.put("a", b"").await.unwrap();
    s.put("b", b"").await.unwrap();

    // Server will report 1 key failed; client must return Err.
    let err = s
        .delete_many(&["a".into(), "b".into()])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("failed"),
        "expected per-key-failure error, got: {err}"
    );
}

/// C5: unsafe URL chars in keys must be percent-encoded so the server sees
/// the correct path. Before the fix, a `?` in the key truncated the path.
#[tokio::test]
async fn url_unsafe_keys_roundtrip() {
    let state = MockState::new("tok");
    let (url, _h) = start_mock(state).await;
    let s = store(&url, "tok");

    // Keys with ?, #, space, %, and unicode must all roundtrip correctly.
    let keys = [
        "foo?bar",
        "a b/c",
        "100%",
        "tag#1",
        "ünïcödé/x",
    ];
    for k in &keys {
        s.put(k, k.as_bytes()).await.unwrap();
    }
    for k in &keys {
        let got = s.get(k).await.unwrap().expect("should be present");
        assert_eq!(got, k.as_bytes(), "roundtrip failed for key {k:?}");
    }

    // list() reveals them all
    let listed = s.list("", None).await.unwrap();
    for k in &keys {
        assert!(
            listed.iter().any(|l| l == *k),
            "listed keys missing {k:?}: {listed:?}"
        );
    }

    // delete() works on unsafe keys
    for k in &keys {
        s.delete(k).await.unwrap();
        assert!(s.get(k).await.unwrap().is_none());
    }
}
