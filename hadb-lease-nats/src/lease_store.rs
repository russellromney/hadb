//! NatsLeaseStore: LeaseStore implementation using NATS JetStream KV.

use anyhow::{anyhow, Result};
use async_nats::jetstream::kv;
use async_trait::async_trait;
use bytes::Bytes;
use hadb::{CasResult, LeaseStore};

/// LeaseStore backed by NATS JetStream Key-Value.
///
/// Uses KV `create` for write-if-not-exists (CAS create) and
/// `update` with revision for write-if-match (CAS update).
/// Revisions are bucket-wide monotonically increasing u64 values,
/// used as the opaque "etag" in the LeaseStore trait.
pub struct NatsLeaseStore {
    store: kv::Store,
}

impl NatsLeaseStore {
    /// Create from an existing JetStream KV store handle.
    pub fn new(store: kv::Store) -> Self {
        Self { store }
    }

    /// Connect to NATS and create/open a KV bucket for leases.
    ///
    /// If the bucket already exists, it is opened. If it doesn't, it is created
    /// with history=1 (only latest value matters for leases).
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
}

#[async_trait]
impl LeaseStore for NatsLeaseStore {
    async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        let entry = self
            .store
            .entry(key)
            .await
            .map_err(|e| anyhow!("NATS KV entry failed: {}", e))?;

        match entry {
            Some(e) => match e.operation {
                kv::Operation::Put => {
                    Ok(Some((e.value.to_vec(), e.revision.to_string())))
                }
                // Deleted or purged entries are treated as absent.
                kv::Operation::Delete | kv::Operation::Purge => Ok(None),
            },
            None => Ok(None),
        }
    }

    async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult> {
        match self.store.create(key, Bytes::from(data)).await {
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

    async fn write_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<CasResult> {
        let revision: u64 = etag
            .parse()
            .map_err(|_| anyhow!("Invalid NATS revision etag: {}", etag))?;

        match self.store.update(key, Bytes::from(data), revision).await {
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

    async fn delete(&self, key: &str) -> Result<()> {
        // Purge removes all history for the key.
        // Must be idempotent per LeaseStore contract: swallow errors if
        // the key doesn't exist or was already purged.
        match self.store.purge(key).await {
            Ok(()) => Ok(()),
            Err(e) => match e.kind() {
                // WrongLastRevision can occur if the key never existed or
                // was already purged. Treat as successful no-op.
                kv::UpdateErrorKind::WrongLastRevision => Ok(()),
                _ => Err(anyhow!("NATS KV purge failed: {}", e)),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test fixture: connects to NATS, creates an isolated KV bucket,
    /// and deletes it on drop to avoid orphaned buckets.
    struct TestFixture {
        store: NatsLeaseStore,
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
                format!("test-{}", uuid::Uuid::new_v4().to_string().replace('-', ""));
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
                store: NatsLeaseStore::new(kv_store),
                jetstream,
                bucket_name,
            })
        }

        /// Delete the test bucket. Call at the end of each test.
        async fn cleanup(self) {
            let _ = self
                .jetstream
                .delete_key_value(&self.bucket_name)
                .await;
        }
    }

    impl std::ops::Deref for TestFixture {
        type Target = NatsLeaseStore;
        fn deref(&self) -> &Self::Target {
            &self.store
        }
    }

    #[tokio::test]
    async fn test_read_nonexistent() {
        let Some(f) = TestFixture::new().await else { return };
        let result = f.read("no-such-key").await.unwrap();
        assert!(result.is_none());
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_create_and_read() {
        let Some(f) = TestFixture::new().await else { return };

        let cas = f
            .write_if_not_exists("lease1", b"node-a".to_vec())
            .await
            .unwrap();
        assert!(cas.success);
        assert!(cas.etag.is_some());

        let read = f.read("lease1").await.unwrap();
        assert!(read.is_some());
        let (data, etag) = read.unwrap();
        assert_eq!(data, b"node-a");
        assert_eq!(etag, cas.etag.unwrap());
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_create_conflict() {
        let Some(f) = TestFixture::new().await else { return };

        let first = f
            .write_if_not_exists("lease1", b"node-a".to_vec())
            .await
            .unwrap();
        assert!(first.success);

        let second = f
            .write_if_not_exists("lease1", b"node-b".to_vec())
            .await
            .unwrap();
        assert!(!second.success);
        assert!(second.etag.is_none());
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_update_with_correct_revision() {
        let Some(f) = TestFixture::new().await else { return };

        let create = f
            .write_if_not_exists("lease1", b"v1".to_vec())
            .await
            .unwrap();
        let etag = create.etag.unwrap();

        let update = f
            .write_if_match("lease1", b"v2".to_vec(), &etag)
            .await
            .unwrap();
        assert!(update.success);
        assert!(update.etag.is_some());

        let (data, new_etag) = f.read("lease1").await.unwrap().unwrap();
        assert_eq!(data, b"v2");
        assert_eq!(new_etag, update.etag.unwrap());
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_update_cas_conflict() {
        let Some(f) = TestFixture::new().await else { return };

        f.write_if_not_exists("lease1", b"v1".to_vec())
            .await
            .unwrap();

        // Use a bogus revision that definitely doesn't match.
        let result = f
            .write_if_match("lease1", b"v2".to_vec(), "999999")
            .await
            .unwrap();
        assert!(!result.success);
        assert!(result.etag.is_none());
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_update_stale_revision() {
        let Some(f) = TestFixture::new().await else { return };

        let create = f
            .write_if_not_exists("lease1", b"v1".to_vec())
            .await
            .unwrap();
        let first_etag = create.etag.unwrap();

        // Update once to advance the revision.
        let update = f
            .write_if_match("lease1", b"v2".to_vec(), &first_etag)
            .await
            .unwrap();
        assert!(update.success);

        // Try to update with the stale first_etag.
        let stale = f
            .write_if_match("lease1", b"v3".to_vec(), &first_etag)
            .await
            .unwrap();
        assert!(!stale.success);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_delete_and_read() {
        let Some(f) = TestFixture::new().await else { return };

        f.write_if_not_exists("lease1", b"node-a".to_vec())
            .await
            .unwrap();

        f.delete("lease1").await.unwrap();

        let read = f.read("lease1").await.unwrap();
        assert!(read.is_none());
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_delete_idempotent() {
        let Some(f) = TestFixture::new().await else { return };
        // Deleting a key that was never created should not error.
        f.delete("never-existed").await.unwrap();
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_delete_then_create() {
        let Some(f) = TestFixture::new().await else { return };

        f.write_if_not_exists("lease1", b"v1".to_vec())
            .await
            .unwrap();
        f.delete("lease1").await.unwrap();

        // After purge, create should succeed again.
        let result = f
            .write_if_not_exists("lease1", b"v2".to_vec())
            .await
            .unwrap();
        assert!(result.success);

        let (data, _) = f.read("lease1").await.unwrap().unwrap();
        assert_eq!(data, b"v2");
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_concurrent_create_is_atomic() {
        let Some(f) = TestFixture::new().await else { return };

        // Spawn 10 concurrent tasks all trying to create the same key
        let mut handles = vec![];
        for i in 0..10u32 {
            let store_clone = NatsLeaseStore::new(f.store.store.clone());
            handles.push(tokio::spawn(async move {
                store_clone
                    .write_if_not_exists("race-key", format!("writer-{}", i).into_bytes())
                    .await
            }));
        }

        let mut successes = 0;
        let mut failures = 0;
        for h in handles {
            match h.await.unwrap() {
                Ok(cas) if cas.success => successes += 1,
                Ok(_) => failures += 1,
                Err(e) => panic!("unexpected error: {}", e),
            }
        }

        // Exactly one writer must succeed
        assert_eq!(successes, 1, "expected exactly 1 success, got {successes}");
        assert_eq!(failures, 9, "expected exactly 9 failures, got {failures}");

        // Read back: should see one of the writers' data
        let (data, _etag) = f.read("race-key").await.unwrap().unwrap();
        let value = String::from_utf8(data).unwrap();
        assert!(value.starts_with("writer-"), "unexpected value: {value}");

        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_etag_is_numeric_revision() {
        let Some(f) = TestFixture::new().await else { return };

        let create = f
            .write_if_not_exists("lease1", b"data".to_vec())
            .await
            .unwrap();
        let etag = create.etag.unwrap();

        // etag must be a valid u64 (NATS revision).
        let revision: u64 = etag.parse().expect("etag should be a numeric revision");
        assert!(revision > 0);
        f.cleanup().await;
    }
}
