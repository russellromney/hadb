//! EtcdManifestStore: ManifestStore implementation using etcd KV transactions.
//!
//! Full manifest stored as msgpack value in etcd.
//! Uses etcd transactions for CAS: CreateRevision==0 for first publish,
//! ModRevision compare for updates. Version is embedded in the manifest body;
//! etcd's mod_revision provides the CAS primitive.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use etcd_client::{Client, Compare, CompareOp, Txn, TxnOp};
use hadb::{CasResult, HaManifest, ManifestMeta, ManifestStore};

/// ManifestStore backed by etcd KV with transactional CAS.
///
/// Keys are prefixed to avoid collisions with other etcd users.
pub struct EtcdManifestStore {
    client: Client,
    prefix: String,
}

impl EtcdManifestStore {
    /// Create from an existing etcd client.
    pub fn new(client: Client, prefix: &str) -> Self {
        Self {
            client,
            prefix: prefix.to_string(),
        }
    }

    /// Connect to etcd and create a manifest store with key prefix.
    pub async fn connect(
        endpoints: impl AsRef<[&str]>,
        prefix: &str,
    ) -> Result<Self> {
        let client = Client::connect(endpoints.as_ref(), None)
            .await
            .map_err(|e| anyhow!("etcd connect failed: {}", e))?;
        Ok(Self::new(client, prefix))
    }

    fn prefixed_key(&self, key: &str) -> String {
        format!("{}{}", self.prefix, key)
    }
}

#[async_trait]
impl ManifestStore for EtcdManifestStore {
    async fn get(&self, key: &str) -> Result<Option<HaManifest>> {
        let mut client = self.client.clone();
        let resp = client
            .get(self.prefixed_key(key), None)
            .await
            .map_err(|e| anyhow!("etcd get failed: {}", e))?;

        match resp.kvs().first() {
            Some(kv) => {
                let manifest: HaManifest = rmp_serde::from_slice(kv.value())
                    .map_err(|e| anyhow!("failed to deserialize manifest: {}", e))?;
                Ok(Some(manifest))
            }
            None => Ok(None),
        }
    }

    async fn put(
        &self,
        key: &str,
        manifest: &HaManifest,
        expected_version: Option<u64>,
    ) -> Result<CasResult> {
        let pkey = self.prefixed_key(key);
        let mut client = self.client.clone();

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
                // First publish: key must not exist (create_revision == 0).
                let txn = Txn::new()
                    .when([Compare::create_revision(pkey.clone(), CompareOp::Equal, 0)])
                    .and_then([TxnOp::put(pkey.clone(), body, None)]);

                let resp = client
                    .txn(txn)
                    .await
                    .map_err(|e| anyhow!("etcd txn failed: {}", e))?;

                if resp.succeeded() {
                    let etag = resp.header().map(|h| h.revision().to_string());
                    Ok(CasResult {
                        success: true,
                        etag,
                    })
                } else {
                    Ok(CasResult {
                        success: false,
                        etag: None,
                    })
                }
            }
            Some(expected) => {
                // First read current value to verify manifest version matches.
                let resp = client
                    .get(pkey.clone(), None)
                    .await
                    .map_err(|e| anyhow!("etcd get failed: {}", e))?;

                let (mod_revision, current_version) = match resp.kvs().first() {
                    Some(kv) => {
                        let current: HaManifest = rmp_serde::from_slice(kv.value())
                            .map_err(|e| anyhow!("failed to deserialize manifest: {}", e))?;
                        (kv.mod_revision(), current.version)
                    }
                    None => {
                        return Ok(CasResult {
                            success: false,
                            etag: None,
                        });
                    }
                };

                if current_version != expected {
                    return Ok(CasResult {
                        success: false,
                        etag: None,
                    });
                }

                // CAS: update only if mod_revision matches.
                let txn = Txn::new()
                    .when([Compare::mod_revision(
                        pkey.clone(),
                        CompareOp::Equal,
                        mod_revision,
                    )])
                    .and_then([TxnOp::put(pkey.clone(), body, None)]);

                let resp = client
                    .txn(txn)
                    .await
                    .map_err(|e| anyhow!("etcd txn failed: {}", e))?;

                if resp.succeeded() {
                    let etag = resp.header().map(|h| h.revision().to_string());
                    Ok(CasResult {
                        success: true,
                        etag,
                    })
                } else {
                    Ok(CasResult {
                        success: false,
                        etag: None,
                    })
                }
            }
        }
    }

    async fn meta(&self, key: &str) -> Result<Option<ManifestMeta>> {
        match self.get(key).await? {
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
        store: EtcdManifestStore,
        prefix: String,
        client: Client,
    }

    impl TestFixture {
        async fn new() -> Option<Self> {
            let endpoints = match std::env::var("ETCD_ENDPOINTS") {
                Ok(e) => e,
                Err(_) => return None,
            };
            let endpoints: Vec<&str> = endpoints.split(',').collect();
            let prefix = format!(
                "test-manifest-{}/",
                uuid::Uuid::new_v4().to_string().replace('-', "")
            );
            let client = Client::connect(&endpoints, None)
                .await
                .expect("connect");
            let store = EtcdManifestStore::new(client.clone(), &prefix);
            Some(Self {
                store,
                prefix,
                client,
            })
        }

        async fn cleanup(mut self) {
            let _ = self
                .client
                .delete(
                    self.prefix.clone(),
                    Some(etcd_client::DeleteOptions::new().with_prefix()),
                )
                .await;
        }
    }

    impl std::ops::Deref for TestFixture {
        type Target = EtcdManifestStore;
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

        let fetched = f.get("db1").await.unwrap().expect("should exist");
        assert_eq!(fetched.version, 1);
        assert_eq!(fetched.writer_id, "node-1");
        assert_eq!(fetched.lease_epoch, 1);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_create_turbolite_roundtrip() {
        let Some(f) = TestFixture::new().await else { return };

        f.put("db1", &make_turbolite_manifest("node-1", 1), None).await.unwrap();
        let fetched = f.get("db1").await.unwrap().unwrap();
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
        assert_eq!(f.get("db1").await.unwrap().unwrap().version, 1);
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
        f.cleanup().await;
    }
}
