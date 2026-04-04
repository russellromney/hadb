//! Manifest types and store trait for HA coordination.
//!
//! The manifest collapses coordination + storage state into one small object.
//! One fetch tells a node everything: who last wrote, what epoch, and what the
//! current storage state is.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::CasResult;

// ============================================================================
// Types
// ============================================================================

/// Full manifest for a database, combining coordination metadata with
/// storage-engine-specific state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HaManifest {
    /// Monotonically increasing version, bumped on every successful put.
    pub version: u64,
    /// Instance ID that last published this manifest.
    pub writer_id: String,
    /// Fencing token from the lease. Prevents stale writers from publishing.
    pub lease_epoch: u64,
    /// Unix timestamp in milliseconds when this manifest was published.
    pub timestamp_ms: u64,
    /// Storage-engine-specific state.
    pub storage: StorageManifest,
}

/// Lightweight metadata extracted from HaManifest. Returned by `meta()` to
/// allow cheap polling without fetching the full storage payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestMeta {
    pub version: u64,
    pub writer_id: String,
    pub lease_epoch: u64,
}

impl From<&HaManifest> for ManifestMeta {
    fn from(m: &HaManifest) -> Self {
        Self {
            version: m.version,
            writer_id: m.writer_id.clone(),
            lease_epoch: m.lease_epoch,
        }
    }
}

/// Storage-engine-specific manifest data.
///
/// Each storage backend (turbolite, walrust, duckblock, graphstream) adds a
/// variant here. The coordination layer (version, writer_id, lease_epoch) is
/// shared across all backends.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StorageManifest {
    Turbolite {
        page_count: u64,
        page_size: u32,
        pages_per_group: u32,
        sub_pages_per_frame: u32,
        strategy: String,
        page_group_keys: Vec<String>,
        frame_tables: Vec<Vec<FrameEntry>>,
        group_pages: Vec<Vec<u64>>,
        btrees: BTreeMap<u64, BTreeManifestEntry>,
        interior_chunk_keys: BTreeMap<u32, String>,
        index_chunk_keys: BTreeMap<u32, String>,
        subframe_overrides: Vec<BTreeMap<usize, SubframeOverride>>,
    },
    Walrust {
        txid: u64,
        changeset_prefix: String,
        latest_changeset_key: String,
        snapshot_key: Option<String>,
        snapshot_txid: Option<u64>,
    },
}

/// A single frame entry in a turbolite frame table.
/// Represents a byte range within an S3 object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FrameEntry {
    pub offset: u64,
    pub len: u32,
}

/// B-tree metadata with group associations for a turbolite page group.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BTreeManifestEntry {
    pub name: String,
    pub obj_type: String,
    pub group_ids: Vec<u64>,
}

/// Override for a specific subframe (future turbolite Phase Drift).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubframeOverride {
    pub key: String,
    pub entry: FrameEntry,
}

// ManifestStore trait is in traits.rs alongside LeaseStore.
pub use crate::traits::ManifestStore;

// ============================================================================
// InMemoryManifestStore (for testing)
// ============================================================================

use std::collections::HashMap;
use tokio::sync::Mutex;

/// In-memory ManifestStore for testing. CAS enforcement on put.
pub struct InMemoryManifestStore {
    data: Mutex<HashMap<String, HaManifest>>,
}

impl Default for InMemoryManifestStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryManifestStore {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }

    /// Remove a manifest by key. Test-only (the trait has no delete).
    pub async fn delete(&self, key: &str) {
        self.data.lock().await.remove(key);
    }
}

#[async_trait]
impl ManifestStore for InMemoryManifestStore {
    async fn get(&self, key: &str) -> Result<Option<HaManifest>> {
        Ok(self.data.lock().await.get(key).cloned())
    }

    async fn put(
        &self,
        key: &str,
        manifest: &HaManifest,
        expected_version: Option<u64>,
    ) -> Result<CasResult> {
        let mut store = self.data.lock().await;

        match expected_version {
            None => {
                // Must not exist
                if store.contains_key(key) {
                    return Ok(CasResult {
                        success: false,
                        etag: None,
                    });
                }
                let mut m = manifest.clone();
                m.version = 1;
                let etag = m.version.to_string();
                store.insert(key.to_string(), m);
                Ok(CasResult {
                    success: true,
                    etag: Some(etag),
                })
            }
            Some(expected) => {
                let current = store.get(key);
                match current {
                    Some(existing) if existing.version == expected => {
                        let mut m = manifest.clone();
                        m.version = expected + 1;
                        let etag = m.version.to_string();
                        store.insert(key.to_string(), m);
                        Ok(CasResult {
                            success: true,
                            etag: Some(etag),
                        })
                    }
                    _ => Ok(CasResult {
                        success: false,
                        etag: None,
                    }),
                }
            }
        }
    }

    async fn meta(&self, key: &str) -> Result<Option<ManifestMeta>> {
        Ok(self.data.lock().await.get(key).map(ManifestMeta::from))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_turbolite_manifest(writer: &str, epoch: u64) -> HaManifest {
        HaManifest {
            version: 0, // store assigns real version
            writer_id: writer.to_string(),
            lease_epoch: epoch,
            timestamp_ms: 1000,
            storage: StorageManifest::Turbolite {
                page_count: 100,
                page_size: 4096,
                pages_per_group: 256,
                sub_pages_per_frame: 16,
                strategy: "Positional".to_string(),
                page_group_keys: vec!["pg-0".to_string()],
                frame_tables: vec![vec![FrameEntry {
                    offset: 0,
                    len: 4096,
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
            },
        }
    }

    fn make_walrust_manifest(writer: &str, epoch: u64) -> HaManifest {
        HaManifest {
            version: 0,
            writer_id: writer.to_string(),
            lease_epoch: epoch,
            timestamp_ms: 2000,
            storage: StorageManifest::Walrust {
                txid: 42,
                changeset_prefix: "cs/".to_string(),
                latest_changeset_key: "cs/42".to_string(),
                snapshot_key: Some("snap/1".to_string()),
                snapshot_txid: Some(40),
            },
        }
    }

    // ========================================================================
    // Happy path
    // ========================================================================

    #[tokio::test]
    async fn put_none_on_empty_store_succeeds() {
        let store = InMemoryManifestStore::new();
        let m = make_turbolite_manifest("node-1", 1);
        let res = store.put("db1", &m, None).await.unwrap();
        assert!(res.success);
        assert_eq!(res.etag, Some("1".to_string()));
    }

    #[tokio::test]
    async fn get_returns_what_was_put_turbolite() {
        let store = InMemoryManifestStore::new();
        let m = make_turbolite_manifest("node-1", 1);
        store.put("db1", &m, None).await.unwrap();

        let fetched = store.get("db1").await.unwrap().expect("should exist");
        assert_eq!(fetched.version, 1);
        assert_eq!(fetched.writer_id, "node-1");
        assert_eq!(fetched.lease_epoch, 1);
        assert_eq!(fetched.timestamp_ms, 1000);
        match &fetched.storage {
            StorageManifest::Turbolite { page_count, page_size, .. } => {
                assert_eq!(*page_count, 100);
                assert_eq!(*page_size, 4096);
            }
            _ => panic!("expected Turbolite variant"),
        }
    }

    #[tokio::test]
    async fn get_returns_what_was_put_walrust() {
        let store = InMemoryManifestStore::new();
        let m = make_walrust_manifest("node-2", 5);
        store.put("db2", &m, None).await.unwrap();

        let fetched = store.get("db2").await.unwrap().expect("should exist");
        assert_eq!(fetched.writer_id, "node-2");
        match &fetched.storage {
            StorageManifest::Walrust { txid, snapshot_key, .. } => {
                assert_eq!(*txid, 42);
                assert_eq!(snapshot_key.as_deref(), Some("snap/1"));
            }
            _ => panic!("expected Walrust variant"),
        }
    }

    #[tokio::test]
    async fn put_with_correct_expected_version_succeeds() {
        let store = InMemoryManifestStore::new();
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
        let store = InMemoryManifestStore::new();
        let m = make_turbolite_manifest("node-1", 7);
        store.put("db1", &m, None).await.unwrap();

        let meta = store.meta("db1").await.unwrap().expect("should exist");
        assert_eq!(meta.version, 1);
        assert_eq!(meta.writer_id, "node-1");
        assert_eq!(meta.lease_epoch, 7);
    }

    // ========================================================================
    // Failure modes
    // ========================================================================

    #[tokio::test]
    async fn put_none_on_existing_key_fails() {
        let store = InMemoryManifestStore::new();
        let m = make_turbolite_manifest("node-1", 1);
        store.put("db1", &m, None).await.unwrap();

        let res = store.put("db1", &m, None).await.unwrap();
        assert!(!res.success);
        assert!(res.etag.is_none());
    }

    #[tokio::test]
    async fn put_with_stale_expected_version_fails() {
        let store = InMemoryManifestStore::new();
        let m = make_turbolite_manifest("node-1", 1);
        store.put("db1", &m, None).await.unwrap();

        // Advance to version 2
        store
            .put("db1", &make_turbolite_manifest("node-1", 1), Some(1))
            .await
            .unwrap();

        // Try with stale version 1
        let res = store
            .put("db1", &make_turbolite_manifest("node-1", 1), Some(1))
            .await
            .unwrap();
        assert!(!res.success);
    }

    #[tokio::test]
    async fn put_with_expected_version_on_nonexistent_fails() {
        let store = InMemoryManifestStore::new();
        let m = make_turbolite_manifest("node-1", 1);
        let res = store.put("db1", &m, Some(1)).await.unwrap();
        assert!(!res.success);
    }

    #[tokio::test]
    async fn get_nonexistent_returns_none() {
        let store = InMemoryManifestStore::new();
        assert!(store.get("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn meta_nonexistent_returns_none() {
        let store = InMemoryManifestStore::new();
        assert!(store.meta("nope").await.unwrap().is_none());
    }

    // ========================================================================
    // Serialization round-trips
    // ========================================================================

    #[tokio::test]
    async fn manifest_roundtrips_through_msgpack() {
        let m = make_turbolite_manifest("node-1", 3);
        let bytes = rmp_serde::to_vec(&m).expect("msgpack serialize");
        let decoded: HaManifest = rmp_serde::from_slice(&bytes).expect("msgpack deserialize");
        assert_eq!(m, decoded);
    }

    #[tokio::test]
    async fn manifest_roundtrips_through_json() {
        let m = make_walrust_manifest("node-2", 5);
        let json = serde_json::to_string(&m).expect("json serialize");
        let decoded: HaManifest = serde_json::from_str(&json).expect("json deserialize");
        assert_eq!(m, decoded);
    }

    #[tokio::test]
    async fn turbolite_variant_serde_roundtrip() {
        let storage = StorageManifest::Turbolite {
            page_count: 50,
            page_size: 8192,
            pages_per_group: 128,
            sub_pages_per_frame: 8,
            strategy: "BTreeAware".to_string(),
            page_group_keys: vec!["a".into(), "b".into()],
            frame_tables: vec![vec![
                FrameEntry { offset: 0, len: 4096 },
                FrameEntry { offset: 4096, len: 4096 },
            ]],
            group_pages: vec![vec![1, 2]],
            btrees: BTreeMap::from([(1, BTreeManifestEntry {
                name: "idx_foo".to_string(),
                obj_type: "index".to_string(),
                group_ids: vec![0],
            })]),
            interior_chunk_keys: BTreeMap::from([(0, "ic".to_string())]),
            index_chunk_keys: BTreeMap::new(),
            subframe_overrides: vec![],
        };
        let bytes = rmp_serde::to_vec(&storage).expect("serialize");
        let decoded: StorageManifest = rmp_serde::from_slice(&bytes).expect("deserialize");
        assert_eq!(storage, decoded);
    }

    #[tokio::test]
    async fn walrust_variant_serde_roundtrip() {
        let storage = StorageManifest::Walrust {
            txid: 100,
            changeset_prefix: "cs/".into(),
            latest_changeset_key: "cs/100".into(),
            snapshot_key: None,
            snapshot_txid: None,
        };
        let json = serde_json::to_string(&storage).expect("serialize");
        let decoded: StorageManifest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(storage, decoded);
    }

    // ========================================================================
    // Edge cases
    // ========================================================================

    #[tokio::test]
    async fn sequential_puts_increment_version() {
        let store = InMemoryManifestStore::new();
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
        let store = InMemoryManifestStore::new();
        let m = make_turbolite_manifest("node-1", 1);
        store.put("db1", &m, None).await.unwrap();

        // Both try to update from version 1
        let r1 = store
            .put("db1", &make_turbolite_manifest("node-A", 1), Some(1))
            .await
            .unwrap();
        let r2 = store
            .put("db1", &make_turbolite_manifest("node-B", 1), Some(1))
            .await
            .unwrap();

        // First succeeds, second fails (sequential under mutex, but tests CAS logic)
        assert!(r1.success);
        assert!(!r2.success);

        let fetched = store.get("db1").await.unwrap().unwrap();
        assert_eq!(fetched.writer_id, "node-A");
        assert_eq!(fetched.version, 2);
    }

    #[tokio::test]
    async fn put_after_overwrite_with_different_storage_variant() {
        let store = InMemoryManifestStore::new();

        // Start with Turbolite
        let m1 = make_turbolite_manifest("node-1", 1);
        store.put("db1", &m1, None).await.unwrap();

        // Overwrite with Walrust
        let m2 = make_walrust_manifest("node-1", 2);
        let res = store.put("db1", &m2, Some(1)).await.unwrap();
        assert!(res.success);

        let fetched = store.get("db1").await.unwrap().unwrap();
        assert!(matches!(fetched.storage, StorageManifest::Walrust { .. }));
        assert_eq!(fetched.version, 2);
    }

    #[tokio::test]
    async fn meta_updates_after_put() {
        let store = InMemoryManifestStore::new();
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
    async fn subframe_override_serde_roundtrip() {
        let ovr = SubframeOverride {
            key: "subframe-42".to_string(),
            entry: FrameEntry {
                offset: 8192,
                len: 4096,
            },
        };
        let bytes = rmp_serde::to_vec(&ovr).expect("serialize");
        let decoded: SubframeOverride = rmp_serde::from_slice(&bytes).expect("deserialize");
        assert_eq!(ovr, decoded);

        let json = serde_json::to_string(&ovr).expect("json serialize");
        let decoded2: SubframeOverride = serde_json::from_str(&json).expect("json deserialize");
        assert_eq!(ovr, decoded2);
    }

    #[tokio::test]
    async fn cas_after_delete_allows_recreate() {
        let store = InMemoryManifestStore::new();
        let m = make_turbolite_manifest("node-1", 1);

        // Create
        let res = store.put("db1", &m, None).await.unwrap();
        assert!(res.success);
        assert_eq!(store.get("db1").await.unwrap().unwrap().version, 1);

        // Delete
        store.delete("db1").await;
        assert!(store.get("db1").await.unwrap().is_none());

        // Re-create with None (first publish semantics)
        let res = store.put("db1", &m, None).await.unwrap();
        assert!(res.success);
        assert_eq!(store.get("db1").await.unwrap().unwrap().version, 1);

        // Stale version from before delete fails
        let res = store.put("db1", &m, Some(999)).await.unwrap();
        assert!(!res.success);
    }

    #[tokio::test]
    async fn multiple_keys_are_independent() {
        let store = InMemoryManifestStore::new();

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
        assert!(matches!(m1.storage, StorageManifest::Turbolite { .. }));
        assert!(matches!(m2.storage, StorageManifest::Walrust { .. }));
    }

    // ========================================================================
    // Boundary values
    // ========================================================================

    #[tokio::test]
    async fn empty_string_writer_id_and_key() {
        let store = InMemoryManifestStore::new();
        let m = HaManifest {
            version: 0,
            writer_id: "".to_string(),
            lease_epoch: 0,
            timestamp_ms: 0,
            storage: StorageManifest::Walrust {
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

    #[tokio::test]
    async fn turbolite_with_empty_vecs() {
        let m = HaManifest {
            version: 0,
            writer_id: "node-1".to_string(),
            lease_epoch: 1,
            timestamp_ms: 1000,
            storage: StorageManifest::Turbolite {
                page_count: 0,
                page_size: 0,
                pages_per_group: 0,
                sub_pages_per_frame: 0,
                strategy: "Positional".to_string(),
                page_group_keys: vec![],
                frame_tables: vec![],
                group_pages: vec![],
                btrees: BTreeMap::new(),
                interior_chunk_keys: BTreeMap::new(),
                index_chunk_keys: BTreeMap::new(),
                subframe_overrides: vec![],
            },
        };

        // Survives msgpack round-trip
        let bytes = rmp_serde::to_vec(&m).expect("serialize");
        let decoded: HaManifest = rmp_serde::from_slice(&bytes).expect("deserialize");
        assert_eq!(m, decoded);

        // Survives store round-trip
        let store = InMemoryManifestStore::new();
        store.put("db1", &m, None).await.unwrap();
        let fetched = store.get("db1").await.unwrap().unwrap();
        match &fetched.storage {
            StorageManifest::Turbolite {
                page_count,
                page_group_keys,
                frame_tables,
                btrees,
                ..
            } => {
                assert_eq!(*page_count, 0);
                assert!(page_group_keys.is_empty());
                assert!(frame_tables.is_empty());
                assert!(btrees.is_empty());
            }
            _ => panic!("expected Turbolite"),
        }
    }

    #[tokio::test]
    async fn walrust_with_all_none_optionals_through_store() {
        let store = InMemoryManifestStore::new();
        let m = HaManifest {
            version: 0,
            writer_id: "node-1".to_string(),
            lease_epoch: 1,
            timestamp_ms: 1000,
            storage: StorageManifest::Walrust {
                txid: 1,
                changeset_prefix: "cs/".to_string(),
                latest_changeset_key: "cs/1".to_string(),
                snapshot_key: None,
                snapshot_txid: None,
            },
        };
        store.put("db1", &m, None).await.unwrap();
        let fetched = store.get("db1").await.unwrap().unwrap();
        match &fetched.storage {
            StorageManifest::Walrust {
                snapshot_key,
                snapshot_txid,
                ..
            } => {
                assert!(snapshot_key.is_none());
                assert!(snapshot_txid.is_none());
            }
            _ => panic!("expected Walrust"),
        }
    }

    // ========================================================================
    // Contract enforcement
    // ========================================================================

    #[tokio::test]
    async fn put_ignores_caller_version_on_create() {
        let store = InMemoryManifestStore::new();
        let m = HaManifest {
            version: 999, // caller sets garbage version
            writer_id: "node-1".to_string(),
            lease_epoch: 1,
            timestamp_ms: 1000,
            storage: StorageManifest::Walrust {
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
        let store = InMemoryManifestStore::new();
        let m = make_turbolite_manifest("node-1", 1);
        store.put("db1", &m, None).await.unwrap();

        let m2 = HaManifest {
            version: 500, // caller sets garbage version
            writer_id: "node-1".to_string(),
            lease_epoch: 1,
            timestamp_ms: 2000,
            storage: StorageManifest::Walrust {
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
    async fn manifest_meta_from_conversion() {
        let m = make_turbolite_manifest("node-42", 99);
        let m = HaManifest {
            version: 7,
            ..m
        };
        let meta = ManifestMeta::from(&m);
        assert_eq!(meta.version, 7);
        assert_eq!(meta.writer_id, "node-42");
        assert_eq!(meta.lease_epoch, 99);
    }

    // ========================================================================
    // Full field verification
    // ========================================================================

    #[tokio::test]
    async fn turbolite_all_fields_survive_store_roundtrip() {
        let store = InMemoryManifestStore::new();
        let m = make_turbolite_manifest("node-1", 1);
        store.put("db1", &m, None).await.unwrap();
        let fetched = store.get("db1").await.unwrap().unwrap();

        match &fetched.storage {
            StorageManifest::Turbolite {
                page_count,
                page_size,
                pages_per_group,
                sub_pages_per_frame,
                strategy,
                page_group_keys,
                frame_tables,
                group_pages,
                btrees,
                interior_chunk_keys,
                index_chunk_keys,
                subframe_overrides,
            } => {
                assert_eq!(*page_count, 100);
                assert_eq!(*page_size, 4096);
                assert_eq!(*pages_per_group, 256);
                assert_eq!(*sub_pages_per_frame, 16);
                assert_eq!(strategy, "Positional");
                assert_eq!(page_group_keys, &vec!["pg-0".to_string()]);
                assert_eq!(
                    frame_tables,
                    &vec![vec![FrameEntry {
                        offset: 0,
                        len: 4096,
                    }]]
                );
                assert_eq!(group_pages, &vec![vec![1u64, 2, 3]]);
                assert_eq!(
                    btrees,
                    &BTreeMap::from([(1, BTreeManifestEntry {
                        name: "sqlite_master".to_string(),
                        obj_type: "table".to_string(),
                        group_ids: vec![0, 1],
                    })])
                );
                assert_eq!(interior_chunk_keys, &BTreeMap::from([(0, "ic-0".to_string())]));
                assert_eq!(index_chunk_keys, &BTreeMap::from([(0, "idx-0".to_string())]));
                assert_eq!(subframe_overrides, &vec![BTreeMap::new()]);
            }
            _ => panic!("expected Turbolite"),
        }
    }

    #[tokio::test]
    async fn walrust_all_fields_survive_store_roundtrip() {
        let store = InMemoryManifestStore::new();
        let m = make_walrust_manifest("node-2", 5);
        store.put("db1", &m, None).await.unwrap();
        let fetched = store.get("db1").await.unwrap().unwrap();

        match &fetched.storage {
            StorageManifest::Walrust {
                txid,
                changeset_prefix,
                latest_changeset_key,
                snapshot_key,
                snapshot_txid,
            } => {
                assert_eq!(*txid, 42);
                assert_eq!(changeset_prefix, "cs/");
                assert_eq!(latest_changeset_key, "cs/42");
                assert_eq!(snapshot_key.as_deref(), Some("snap/1"));
                assert_eq!(*snapshot_txid, Some(40));
            }
            _ => panic!("expected Walrust"),
        }
    }

    #[tokio::test]
    async fn put_expected_version_zero_on_empty_store_fails() {
        let store = InMemoryManifestStore::new();
        let m = make_turbolite_manifest("node-1", 1);
        // expected_version: Some(0) on empty store should fail (no manifest with version 0 exists)
        let res = store.put("db1", &m, Some(0)).await.unwrap();
        assert!(!res.success);
    }
}
