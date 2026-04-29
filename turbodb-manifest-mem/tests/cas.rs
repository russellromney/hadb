//! CAS-semantic tests for `MemManifestStore`. Ported from
//! `hadb::manifest` tests during Phase Turbogenesis; reduced to
//! envelope-level checks in Phase Turbogenesis-b (payload shape is
//! no longer turbodb's concern).

use turbodb::{Manifest, ManifestStore};
use turbodb_manifest_mem::MemManifestStore;

fn make_manifest(writer: &str) -> Manifest {
    Manifest {
        version: 0,
        writer_id: writer.to_string(),
        timestamp_ms: 1000,
        payload: b"test-payload".to_vec(),
    }
}

// ============================================================================
// Happy path
// ============================================================================

#[tokio::test]
async fn put_none_on_empty_store_succeeds() {
    let store = MemManifestStore::new();
    let m = make_manifest("node-1");
    let res = store.put("db1", &m, None).await.unwrap();
    assert!(res.success);
    assert_eq!(res.etag, Some("1".to_string()));
}

#[tokio::test]
async fn get_returns_what_was_put() {
    let store = MemManifestStore::new();
    let m = make_manifest("node-1");
    store.put("db1", &m, None).await.unwrap();

    let fetched = store.get("db1").await.unwrap().expect("should exist");
    assert_eq!(fetched.version, 1);
    assert_eq!(fetched.writer_id, "node-1");
    assert_eq!(fetched.timestamp_ms, 1000);
    assert_eq!(fetched.payload, b"test-payload");
}

#[tokio::test]
async fn put_with_correct_expected_version_succeeds() {
    let store = MemManifestStore::new();
    let m = make_manifest("node-1");
    store.put("db1", &m, None).await.unwrap();

    let m2 = make_manifest("node-1");
    let res = store.put("db1", &m2, Some(1)).await.unwrap();
    assert!(res.success);
    assert_eq!(res.etag, Some("2".to_string()));

    let fetched = store.get("db1").await.unwrap().expect("should exist");
    assert_eq!(fetched.version, 2);
}

#[tokio::test]
async fn meta_returns_correct_fields() {
    let store = MemManifestStore::new();
    let m = make_manifest("node-1");
    store.put("db1", &m, None).await.unwrap();

    let meta = store.meta("db1").await.unwrap().expect("should exist");
    assert_eq!(meta.version, 1);
    assert_eq!(meta.writer_id, "node-1");
}

// ============================================================================
// Failure modes
// ============================================================================

#[tokio::test]
async fn put_none_on_existing_key_fails() {
    let store = MemManifestStore::new();
    let m = make_manifest("node-1");
    store.put("db1", &m, None).await.unwrap();

    let res = store.put("db1", &m, None).await.unwrap();
    assert!(!res.success);
    assert!(res.etag.is_none());
}

#[tokio::test]
async fn put_with_stale_expected_version_fails() {
    let store = MemManifestStore::new();
    let m = make_manifest("node-1");
    store.put("db1", &m, None).await.unwrap();

    store
        .put("db1", &make_manifest("node-1"), Some(1))
        .await
        .unwrap();

    let res = store
        .put("db1", &make_manifest("node-1"), Some(1))
        .await
        .unwrap();
    assert!(!res.success);
}

#[tokio::test]
async fn put_with_expected_version_on_nonexistent_fails() {
    let store = MemManifestStore::new();
    let m = make_manifest("node-1");
    let res = store.put("db1", &m, Some(1)).await.unwrap();
    assert!(!res.success);
}

#[tokio::test]
async fn get_nonexistent_returns_none() {
    let store = MemManifestStore::new();
    assert!(store.get("nope").await.unwrap().is_none());
}

#[tokio::test]
async fn meta_nonexistent_returns_none() {
    let store = MemManifestStore::new();
    assert!(store.meta("nope").await.unwrap().is_none());
}

// ============================================================================
// Edge cases
// ============================================================================

#[tokio::test]
async fn sequential_puts_increment_version() {
    let store = MemManifestStore::new();
    let m = make_manifest("node-1");

    store.put("db1", &m, None).await.unwrap();
    assert_eq!(store.get("db1").await.unwrap().unwrap().version, 1);

    store.put("db1", &m, Some(1)).await.unwrap();
    assert_eq!(store.get("db1").await.unwrap().unwrap().version, 2);

    store.put("db1", &m, Some(2)).await.unwrap();
    assert_eq!(store.get("db1").await.unwrap().unwrap().version, 3);

    store.put("db1", &m, Some(3)).await.unwrap();
    assert_eq!(store.get("db1").await.unwrap().unwrap().version, 4);
}

#[tokio::test]
async fn concurrent_puts_same_expected_version_one_wins() {
    let store = MemManifestStore::new();
    let m = make_manifest("node-1");
    store.put("db1", &m, None).await.unwrap();

    let r1 = store
        .put("db1", &make_manifest("node-A"), Some(1))
        .await
        .unwrap();
    let r2 = store
        .put("db1", &make_manifest("node-B"), Some(1))
        .await
        .unwrap();

    assert!(r1.success);
    assert!(!r2.success);

    let fetched = store.get("db1").await.unwrap().unwrap();
    assert_eq!(fetched.writer_id, "node-A");
    assert_eq!(fetched.version, 2);
}

#[tokio::test]
async fn put_overwrites_payload_bytes() {
    let store = MemManifestStore::new();

    let mut m1 = make_manifest("node-1");
    m1.payload = b"first".to_vec();
    store.put("db1", &m1, None).await.unwrap();

    let mut m2 = make_manifest("node-1");
    m2.payload = b"second".to_vec();
    let res = store.put("db1", &m2, Some(1)).await.unwrap();
    assert!(res.success);

    let fetched = store.get("db1").await.unwrap().unwrap();
    assert_eq!(fetched.payload, b"second");
    assert_eq!(fetched.version, 2);
}

#[tokio::test]
async fn meta_updates_after_put() {
    let store = MemManifestStore::new();
    let m = make_manifest("node-1");
    store.put("db1", &m, None).await.unwrap();

    let meta1 = store.meta("db1").await.unwrap().unwrap();
    assert_eq!(meta1.version, 1);

    let m2 = make_manifest("node-2");
    store.put("db1", &m2, Some(1)).await.unwrap();

    let meta2 = store.meta("db1").await.unwrap().unwrap();
    assert_eq!(meta2.version, 2);
    assert_eq!(meta2.writer_id, "node-2");
}

#[tokio::test]
async fn cas_after_delete_allows_recreate() {
    let store = MemManifestStore::new();
    let m = make_manifest("node-1");

    let res = store.put("db1", &m, None).await.unwrap();
    assert!(res.success);
    assert_eq!(store.get("db1").await.unwrap().unwrap().version, 1);

    store.delete("db1").await;
    assert!(store.get("db1").await.unwrap().is_none());

    let res = store.put("db1", &m, None).await.unwrap();
    assert!(res.success);
    assert_eq!(store.get("db1").await.unwrap().unwrap().version, 1);

    let res = store.put("db1", &m, Some(999)).await.unwrap();
    assert!(!res.success);
}

#[tokio::test]
async fn multiple_keys_are_independent() {
    let store = MemManifestStore::new();

    store
        .put("db1", &make_manifest("node-1"), None)
        .await
        .unwrap();
    store
        .put("db2", &make_manifest("node-2"), None)
        .await
        .unwrap();

    let m1 = store.get("db1").await.unwrap().unwrap();
    let m2 = store.get("db2").await.unwrap().unwrap();

    assert_eq!(m1.writer_id, "node-1");
    assert_eq!(m2.writer_id, "node-2");
}

// ============================================================================
// Boundary values
// ============================================================================

#[tokio::test]
async fn empty_string_writer_id_and_key() {
    let store = MemManifestStore::new();
    let m = Manifest {
        version: 0,
        writer_id: "".to_string(),
        timestamp_ms: 0,
        payload: Vec::new(),
    };

    let res = store.put("", &m, None).await.unwrap();
    assert!(res.success);

    let fetched = store.get("").await.unwrap().unwrap();
    assert_eq!(fetched.writer_id, "");
    assert_eq!(fetched.timestamp_ms, 0);
    assert!(fetched.payload.is_empty());
}

// ============================================================================
// Contract enforcement
// ============================================================================

#[tokio::test]
async fn put_ignores_caller_version_on_create() {
    let store = MemManifestStore::new();
    let m = Manifest {
        version: 999,
        writer_id: "node-1".to_string(),
        timestamp_ms: 1000,
        payload: b"test-payload".to_vec(),
    };
    store.put("db1", &m, None).await.unwrap();
    let fetched = store.get("db1").await.unwrap().unwrap();
    assert_eq!(
        fetched.version, 1,
        "store must assign version 1, not caller's 999"
    );
}

#[tokio::test]
async fn put_ignores_caller_version_on_update() {
    let store = MemManifestStore::new();
    let m = make_manifest("node-1");
    store.put("db1", &m, None).await.unwrap();

    let m2 = Manifest {
        version: 500,
        writer_id: "node-1".to_string(),
        timestamp_ms: 2000,
        payload: b"test-payload-2".to_vec(),
    };
    store.put("db1", &m2, Some(1)).await.unwrap();
    let fetched = store.get("db1").await.unwrap().unwrap();
    assert_eq!(
        fetched.version, 2,
        "store must assign version 2, not caller's 500"
    );
}

#[tokio::test]
async fn put_expected_version_zero_on_empty_store_fails() {
    let store = MemManifestStore::new();
    let m = make_manifest("node-1");
    let res = store.put("db1", &m, Some(0)).await.unwrap();
    assert!(!res.success);
}
