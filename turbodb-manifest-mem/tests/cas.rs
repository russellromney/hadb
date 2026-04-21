//! CAS-semantic tests for `MemManifestStore`. Ported from
//! `hadb::manifest` tests during Phase Turbogenesis.

use std::collections::BTreeMap;
use turbodb::{BTreeManifestEntry, Backend, FrameEntry, Manifest, ManifestStore};
use turbodb_manifest_mem::MemManifestStore;

fn make_turbolite_manifest(writer: &str, epoch: u64) -> Manifest {
    Manifest {
        version: 0,
        writer_id: writer.to_string(),
        lease_epoch: epoch,
        timestamp_ms: 1000,
        storage: Backend::Turbolite {
            page_count: 100,
            page_size: 4096,
            pages_per_group: 256,
            sub_pages_per_frame: 16,
            strategy: "Positional".to_string(),
            page_group_keys: vec!["pg-0".to_string()],
            frame_tables: vec![vec![FrameEntry {
                offset: 0,
                len: 4096,
                page_count: 0,
            }]],
            group_pages: vec![vec![1, 2, 3]],
            btrees: BTreeMap::from([(
                1,
                BTreeManifestEntry {
                    name: "sqlite_master".to_string(),
                    obj_type: "table".to_string(),
                    group_ids: vec![0, 1],
                },
            )]),
            interior_chunk_keys: BTreeMap::from([(0, "ic-0".to_string())]),
            index_chunk_keys: BTreeMap::from([(0, "idx-0".to_string())]),
            subframe_overrides: vec![BTreeMap::new()],
            turbolite_version: 0,
            db_header: None,
        },
    }
}

fn make_walrust_manifest(writer: &str, epoch: u64) -> Manifest {
    Manifest {
        version: 0,
        writer_id: writer.to_string(),
        lease_epoch: epoch,
        timestamp_ms: 2000,
        storage: Backend::Walrust {
            txid: 42,
            changeset_prefix: "cs/".to_string(),
            latest_changeset_key: "cs/42".to_string(),
            snapshot_key: Some("snap/1".to_string()),
            snapshot_txid: Some(40),
        },
    }
}

// ============================================================================
// Happy path
// ============================================================================

#[tokio::test]
async fn put_none_on_empty_store_succeeds() {
    let store = MemManifestStore::new();
    let m = make_turbolite_manifest("node-1", 1);
    let res = store.put("db1", &m, None).await.unwrap();
    assert!(res.success);
    assert_eq!(res.etag, Some("1".to_string()));
}

#[tokio::test]
async fn get_returns_what_was_put_turbolite() {
    let store = MemManifestStore::new();
    let m = make_turbolite_manifest("node-1", 1);
    store.put("db1", &m, None).await.unwrap();

    let fetched = store.get("db1").await.unwrap().expect("should exist");
    assert_eq!(fetched.version, 1);
    assert_eq!(fetched.writer_id, "node-1");
    assert_eq!(fetched.lease_epoch, 1);
    assert_eq!(fetched.timestamp_ms, 1000);
    match &fetched.storage {
        Backend::Turbolite { page_count, page_size, .. } => {
            assert_eq!(*page_count, 100);
            assert_eq!(*page_size, 4096);
        }
        _ => panic!("expected Turbolite variant"),
    }
}

#[tokio::test]
async fn get_returns_what_was_put_walrust() {
    let store = MemManifestStore::new();
    let m = make_walrust_manifest("node-2", 5);
    store.put("db2", &m, None).await.unwrap();

    let fetched = store.get("db2").await.unwrap().expect("should exist");
    assert_eq!(fetched.writer_id, "node-2");
    match &fetched.storage {
        Backend::Walrust { txid, snapshot_key, .. } => {
            assert_eq!(*txid, 42);
            assert_eq!(snapshot_key.as_deref(), Some("snap/1"));
        }
        _ => panic!("expected Walrust variant"),
    }
}

#[tokio::test]
async fn put_with_correct_expected_version_succeeds() {
    let store = MemManifestStore::new();
    let m = make_turbolite_manifest("node-1", 1);
    store.put("db1", &m, None).await.unwrap();

    let m2 = make_turbolite_manifest("node-1", 1);
    let res = store.put("db1", &m2, Some(1)).await.unwrap();
    assert!(res.success);
    assert_eq!(res.etag, Some("2".to_string()));

    let fetched = store.get("db1").await.unwrap().expect("should exist");
    assert_eq!(fetched.version, 2);
}

#[tokio::test]
async fn meta_returns_correct_fields() {
    let store = MemManifestStore::new();
    let m = make_turbolite_manifest("node-1", 7);
    store.put("db1", &m, None).await.unwrap();

    let meta = store.meta("db1").await.unwrap().expect("should exist");
    assert_eq!(meta.version, 1);
    assert_eq!(meta.writer_id, "node-1");
    assert_eq!(meta.lease_epoch, 7);
}

// ============================================================================
// Failure modes
// ============================================================================

#[tokio::test]
async fn put_none_on_existing_key_fails() {
    let store = MemManifestStore::new();
    let m = make_turbolite_manifest("node-1", 1);
    store.put("db1", &m, None).await.unwrap();

    let res = store.put("db1", &m, None).await.unwrap();
    assert!(!res.success);
    assert!(res.etag.is_none());
}

#[tokio::test]
async fn put_with_stale_expected_version_fails() {
    let store = MemManifestStore::new();
    let m = make_turbolite_manifest("node-1", 1);
    store.put("db1", &m, None).await.unwrap();

    store
        .put("db1", &make_turbolite_manifest("node-1", 1), Some(1))
        .await
        .unwrap();

    let res = store
        .put("db1", &make_turbolite_manifest("node-1", 1), Some(1))
        .await
        .unwrap();
    assert!(!res.success);
}

#[tokio::test]
async fn put_with_expected_version_on_nonexistent_fails() {
    let store = MemManifestStore::new();
    let m = make_turbolite_manifest("node-1", 1);
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
    let m = make_turbolite_manifest("node-1", 1);

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
    let m = make_turbolite_manifest("node-1", 1);
    store.put("db1", &m, None).await.unwrap();

    let r1 = store
        .put("db1", &make_turbolite_manifest("node-A", 1), Some(1))
        .await
        .unwrap();
    let r2 = store
        .put("db1", &make_turbolite_manifest("node-B", 1), Some(1))
        .await
        .unwrap();

    assert!(r1.success);
    assert!(!r2.success);

    let fetched = store.get("db1").await.unwrap().unwrap();
    assert_eq!(fetched.writer_id, "node-A");
    assert_eq!(fetched.version, 2);
}

#[tokio::test]
async fn put_after_overwrite_with_different_storage_variant() {
    let store = MemManifestStore::new();

    let m1 = make_turbolite_manifest("node-1", 1);
    store.put("db1", &m1, None).await.unwrap();

    let m2 = make_walrust_manifest("node-1", 2);
    let res = store.put("db1", &m2, Some(1)).await.unwrap();
    assert!(res.success);

    let fetched = store.get("db1").await.unwrap().unwrap();
    assert!(matches!(fetched.storage, Backend::Walrust { .. }));
    assert_eq!(fetched.version, 2);
}

#[tokio::test]
async fn meta_updates_after_put() {
    let store = MemManifestStore::new();
    let m = make_turbolite_manifest("node-1", 1);
    store.put("db1", &m, None).await.unwrap();

    let meta1 = store.meta("db1").await.unwrap().unwrap();
    assert_eq!(meta1.version, 1);

    let m2 = make_turbolite_manifest("node-2", 3);
    store.put("db1", &m2, Some(1)).await.unwrap();

    let meta2 = store.meta("db1").await.unwrap().unwrap();
    assert_eq!(meta2.version, 2);
    assert_eq!(meta2.writer_id, "node-2");
    assert_eq!(meta2.lease_epoch, 3);
}

#[tokio::test]
async fn cas_after_delete_allows_recreate() {
    let store = MemManifestStore::new();
    let m = make_turbolite_manifest("node-1", 1);

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
        .put("db1", &make_turbolite_manifest("node-1", 1), None)
        .await
        .unwrap();
    store
        .put("db2", &make_walrust_manifest("node-2", 2), None)
        .await
        .unwrap();

    let m1 = store.get("db1").await.unwrap().unwrap();
    let m2 = store.get("db2").await.unwrap().unwrap();

    assert_eq!(m1.writer_id, "node-1");
    assert_eq!(m2.writer_id, "node-2");
    assert!(matches!(m1.storage, Backend::Turbolite { .. }));
    assert!(matches!(m2.storage, Backend::Walrust { .. }));
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
        lease_epoch: 0,
        timestamp_ms: 0,
        storage: Backend::Walrust {
            txid: 0,
            changeset_prefix: "".to_string(),
            latest_changeset_key: "".to_string(),
            snapshot_key: None,
            snapshot_txid: None,
        },
    };

    let res = store.put("", &m, None).await.unwrap();
    assert!(res.success);

    let fetched = store.get("").await.unwrap().unwrap();
    assert_eq!(fetched.writer_id, "");
    assert_eq!(fetched.lease_epoch, 0);
    assert_eq!(fetched.timestamp_ms, 0);
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
        lease_epoch: 1,
        timestamp_ms: 1000,
        storage: Backend::Walrust {
            txid: 1,
            changeset_prefix: "cs/".to_string(),
            latest_changeset_key: "cs/1".to_string(),
            snapshot_key: None,
            snapshot_txid: None,
        },
    };
    store.put("db1", &m, None).await.unwrap();
    let fetched = store.get("db1").await.unwrap().unwrap();
    assert_eq!(fetched.version, 1, "store must assign version 1, not caller's 999");
}

#[tokio::test]
async fn put_ignores_caller_version_on_update() {
    let store = MemManifestStore::new();
    let m = make_turbolite_manifest("node-1", 1);
    store.put("db1", &m, None).await.unwrap();

    let m2 = Manifest {
        version: 500,
        writer_id: "node-1".to_string(),
        lease_epoch: 1,
        timestamp_ms: 2000,
        storage: Backend::Walrust {
            txid: 2,
            changeset_prefix: "cs/".to_string(),
            latest_changeset_key: "cs/2".to_string(),
            snapshot_key: None,
            snapshot_txid: None,
        },
    };
    store.put("db1", &m2, Some(1)).await.unwrap();
    let fetched = store.get("db1").await.unwrap().unwrap();
    assert_eq!(fetched.version, 2, "store must assign version 2, not caller's 500");
}

#[tokio::test]
async fn put_expected_version_zero_on_empty_store_fails() {
    let store = MemManifestStore::new();
    let m = make_turbolite_manifest("node-1", 1);
    let res = store.put("db1", &m, Some(0)).await.unwrap();
    assert!(!res.success);
}
