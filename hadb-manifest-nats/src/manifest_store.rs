//! NatsManifestStore: ManifestStore implementation using NATS JetStream KV.
//!
//! Full manifest stored as msgpack value in NATS KV.
//! NATS KV revisions provide natural CAS semantics.
//! `meta()` fetches the full value and extracts ManifestMeta (NATS KV
//! has no partial-read API, but reads are ~1-2ms so this is fine).

use anyhow::{anyhow, Result};
use async_nats::jetstream::kv;
use async_trait::async_trait;
use bytes::Bytes;
use hadb::{CasResult, HaManifest, ManifestMeta, ManifestStore};

/// ManifestStore backed by NATS JetStream Key-Value.
///
/// Uses KV `create` for first publish (expected_version: None) and
/// `update` with revision for subsequent publishes.
pub struct NatsManifestStore {
    store: kv::Store,
}

impl NatsManifestStore {
    /// Create from an existing JetStream KV store handle.
    pub fn new(store: kv::Store) -> Self {
        Self { store }
    }

    /// Connect to NATS and create/open a KV bucket for manifests.
    pub async fn connect(url: &str, bucket: &str) -> Result<Self> {
        let client = async_nats::connect(url)
            .await
            .map_err(|e| anyhow!("NATS connect failed: {}", e))?;
        let jetstream = async_nats::jetstream::new(client);
        let store = jetstream
            .create_key_value(kv::Config {
                bucket: bucket.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow!("NATS KV create/open failed: {}", e))?;
        Ok(Self { store })
    }

    /// Deserialize a NATS KV entry into HaManifest, returning None for
    /// deleted/purged entries.
    fn decode_entry(entry: Option<kv::Entry>) -> Result<Option<HaManifest>> {
        match entry {
            Some(e) => match e.operation {
                kv::Operation::Put => {
                    let manifest: HaManifest = rmp_serde::from_slice(&e.value)
                        .map_err(|err| anyhow!("failed to deserialize manifest: {}", err))?;
                    Ok(Some(manifest))
                }
                kv::Operation::Delete | kv::Operation::Purge => Ok(None),
            },
            None => Ok(None),
        }
    }
}

#[async_trait]
impl ManifestStore for NatsManifestStore {
    async fn get(&self, key: &str) -> Result<Option<HaManifest>> {
        let entry = self
            .store
            .entry(key)
            .await
            .map_err(|e| anyhow!("NATS KV entry failed: {}", e))?;
        Self::decode_entry(entry)
    }

    async fn put(
        &self,
        key: &str,
        manifest: &HaManifest,
        expected_version: Option<u64>,
    ) -> Result<CasResult> {
        let new_version = match expected_version {
            None => 1u64,
            Some(v) => v + 1,
        };

        let mut stored = manifest.clone();
        stored.version = new_version;

        let body = rmp_serde::to_vec(&stored)
            .map_err(|e| anyhow!("failed to serialize manifest: {}", e))?;

        match expected_version {
            None => {
                // First publish: key must not exist.
                match self.store.create(key, Bytes::from(body)).await {
                    Ok(revision) => Ok(CasResult {
                        success: true,
                        etag: Some(revision.to_string()),
                    }),
                    Err(e) => match e.kind() {
                        kv::CreateErrorKind::AlreadyExists => Ok(CasResult {
                            success: false,
                            etag: None,
                        }),
                        _ => Err(anyhow!("NATS KV create failed: {}", e)),
                    },
                }
            }
            Some(_) => {
                // Update: need current revision for CAS.
                // Read current entry to get revision and verify version.
                let entry = self
                    .store
                    .entry(key)
                    .await
                    .map_err(|e| anyhow!("NATS KV entry failed: {}", e))?;

                let (revision, current_version) = match &entry {
                    Some(e) => match e.operation {
                        kv::Operation::Put => {
                            let current: HaManifest = rmp_serde::from_slice(&e.value)
                                .map_err(|err| anyhow!("failed to deserialize manifest: {}", err))?;
                            (e.revision, current.version)
                        }
                        kv::Operation::Delete | kv::Operation::Purge => {
                            return Ok(CasResult {
                                success: false,
                                etag: None,
                            });
                        }
                    },
                    None => {
                        return Ok(CasResult {
                            success: false,
                            etag: None,
                        });
                    }
                };

                // Check version matches.
                if current_version != expected_version.expect("checked above") {
                    return Ok(CasResult {
                        success: false,
                        etag: None,
                    });
                }

                match self.store.update(key, Bytes::from(body), revision).await {
                    Ok(new_revision) => Ok(CasResult {
                        success: true,
                        etag: Some(new_revision.to_string()),
                    }),
                    Err(e) => match e.kind() {
                        kv::UpdateErrorKind::WrongLastRevision => Ok(CasResult {
                            success: false,
                            etag: None,
                        }),
                        _ => Err(anyhow!("NATS KV update failed: {}", e)),
                    },
                }
            }
        }
    }

    async fn meta(&self, key: &str) -> Result<Option<ManifestMeta>> {
        let entry = self
            .store
            .entry(key)
            .await
            .map_err(|e| anyhow!("NATS KV entry failed: {}", e))?;
        match Self::decode_entry(entry)? {
            Some(manifest) => Ok(Some(ManifestMeta::from(&manifest))),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hadb::{StorageManifest, FrameEntry, BTreeInfo};
    use std::collections::BTreeMap;

    fn make_manifest(writer: &str, epoch: u64) -> HaManifest {
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

    fn make_turbolite_manifest(writer: &str, epoch: u64) -> HaManifest {
        HaManifest {
            version: 0,
            writer_id: writer.to_string(),
            lease_epoch: epoch,
            timestamp_ms: 2000,
            storage: StorageManifest::Turbolite {
                page_count: 100,
                page_size: 4096,
                page_group_keys: vec!["pg-0".to_string()],
                frame_tables: vec![vec![FrameEntry { page_number: 1, frame_offset: 0 }]],
                group_pages: vec![vec![1, 2, 3]],
                btrees: BTreeMap::from([(1, BTreeInfo { root_page: 1, depth: 2 })]),
                interior_chunk_keys: vec!["ic-0".to_string()],
                index_chunk_keys: vec!["idx-0".to_string()],
            },
        }
    }

    struct TestFixture {
        store: NatsManifestStore,
        jetstream: async_nats::jetstream::Context,
        bucket_name: String,
    }

    impl TestFixture {
        async fn new() -> Option<Self> {
            let url = match std::env::var("NATS_URL") {
                Ok(u) => u,
                Err(_) => return None,
            };
            let bucket_name =
                format!("test-manifest-{}", uuid::Uuid::new_v4().to_string().replace('-', ""));
            let client = async_nats::connect(&url).await.expect("connect");
            let jetstream = async_nats::jetstream::new(client);
            let kv_store = jetstream
                .create_key_value(kv::Config {
                    bucket: bucket_name.clone(),
                    history: 1,
                    ..Default::default()
                })
                .await
                .expect("create kv");
            Some(Self {
                store: NatsManifestStore::new(kv_store),
                jetstream,
                bucket_name,
            })
        }

        async fn cleanup(self) {
            let _ = self.jetstream.delete_key_value(&self.bucket_name).await;
        }
    }

    impl std::ops::Deref for TestFixture {
        type Target = NatsManifestStore;
        fn deref(&self) -> &Self::Target {
            &self.store
        }
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let Some(f) = TestFixture::new().await else { return };
        assert!(f.get("nope").await.unwrap().is_none());
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_create_and_get() {
        let Some(f) = TestFixture::new().await else { return };

        let res = f.put("db1", &make_manifest("node-1", 1), None).await.unwrap();
        assert!(res.success);
        assert!(res.etag.is_some());

        let fetched = f.get("db1").await.unwrap().expect("should exist");
        assert_eq!(fetched.version, 1);
        assert_eq!(fetched.writer_id, "node-1");
        assert_eq!(fetched.lease_epoch, 1);
        assert!(matches!(fetched.storage, StorageManifest::Walrust { .. }));
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_create_turbolite_roundtrip() {
        let Some(f) = TestFixture::new().await else { return };

        f.put("db1", &make_turbolite_manifest("node-1", 1), None).await.unwrap();
        let fetched = f.get("db1").await.unwrap().expect("should exist");
        match &fetched.storage {
            StorageManifest::Turbolite { page_count, page_size, .. } => {
                assert_eq!(*page_count, 100);
                assert_eq!(*page_size, 4096);
            }
            _ => panic!("expected Turbolite"),
        }
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_update_with_correct_version() {
        let Some(f) = TestFixture::new().await else { return };

        f.put("db1", &make_manifest("node-1", 1), None).await.unwrap();
        let res = f.put("db1", &make_manifest("node-1", 2), Some(1)).await.unwrap();
        assert!(res.success);

        let fetched = f.get("db1").await.unwrap().unwrap();
        assert_eq!(fetched.version, 2);
        assert_eq!(fetched.lease_epoch, 2);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_meta_returns_correct_fields() {
        let Some(f) = TestFixture::new().await else { return };

        f.put("db1", &make_manifest("node-42", 7), None).await.unwrap();
        let meta = f.meta("db1").await.unwrap().expect("should exist");
        assert_eq!(meta.version, 1);
        assert_eq!(meta.writer_id, "node-42");
        assert_eq!(meta.lease_epoch, 7);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_meta_nonexistent() {
        let Some(f) = TestFixture::new().await else { return };
        assert!(f.meta("nope").await.unwrap().is_none());
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_create_conflict() {
        let Some(f) = TestFixture::new().await else { return };

        f.put("db1", &make_manifest("node-1", 1), None).await.unwrap();
        let res = f.put("db1", &make_manifest("node-2", 1), None).await.unwrap();
        assert!(!res.success);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_stale_version() {
        let Some(f) = TestFixture::new().await else { return };

        f.put("db1", &make_manifest("node-1", 1), None).await.unwrap();
        f.put("db1", &make_manifest("node-1", 1), Some(1)).await.unwrap();

        let res = f.put("db1", &make_manifest("node-1", 1), Some(1)).await.unwrap();
        assert!(!res.success);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_version_on_nonexistent() {
        let Some(f) = TestFixture::new().await else { return };
        let res = f.put("db1", &make_manifest("node-1", 1), Some(1)).await.unwrap();
        assert!(!res.success);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_sequential_version_increments() {
        let Some(f) = TestFixture::new().await else { return };

        f.put("db1", &make_manifest("node-1", 1), None).await.unwrap();
        assert_eq!(f.get("db1").await.unwrap().unwrap().version, 1);

        f.put("db1", &make_manifest("node-1", 1), Some(1)).await.unwrap();
        assert_eq!(f.get("db1").await.unwrap().unwrap().version, 2);

        f.put("db1", &make_manifest("node-1", 1), Some(2)).await.unwrap();
        assert_eq!(f.get("db1").await.unwrap().unwrap().version, 3);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_meta_updates_after_put() {
        let Some(f) = TestFixture::new().await else { return };

        f.put("db1", &make_manifest("node-1", 1), None).await.unwrap();
        f.put("db1", &make_manifest("node-2", 5), Some(1)).await.unwrap();

        let meta = f.meta("db1").await.unwrap().unwrap();
        assert_eq!(meta.version, 2);
        assert_eq!(meta.writer_id, "node-2");
        assert_eq!(meta.lease_epoch, 5);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_ignores_caller_version() {
        let Some(f) = TestFixture::new().await else { return };

        let mut m = make_manifest("node-1", 1);
        m.version = 999;
        f.put("db1", &m, None).await.unwrap();

        let fetched = f.get("db1").await.unwrap().unwrap();
        assert_eq!(fetched.version, 1);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_multiple_keys_independent() {
        let Some(f) = TestFixture::new().await else { return };

        f.put("db1", &make_manifest("node-1", 1), None).await.unwrap();
        f.put("db2", &make_turbolite_manifest("node-2", 2), None).await.unwrap();

        let m1 = f.get("db1").await.unwrap().unwrap();
        let m2 = f.get("db2").await.unwrap().unwrap();
        assert_eq!(m1.writer_id, "node-1");
        assert_eq!(m2.writer_id, "node-2");
        assert!(matches!(m1.storage, StorageManifest::Walrust { .. }));
        assert!(matches!(m2.storage, StorageManifest::Turbolite { .. }));
        f.cleanup().await;
    }
}
