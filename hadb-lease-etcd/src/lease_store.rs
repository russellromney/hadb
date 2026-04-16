//! EtcdLeaseStore: LeaseStore implementation using etcd KV transactions.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use etcd_client::{Client, Compare, CompareOp, Txn, TxnOp};
use hadb_lease::{CasResult, LeaseStore};

/// LeaseStore backed by etcd KV with transactional CAS.
///
/// Uses etcd transactions for compare-and-swap:
/// - `write_if_not_exists`: `If(CreateRevision == 0).Then(Put)`
/// - `write_if_match`: `If(ModRevision == etag).Then(Put)`
///
/// Keys are prefixed to avoid collisions with other etcd users.
/// Revisions (mod_revision) are used as opaque etags.
pub struct EtcdLeaseStore {
    client: Client,
    prefix: String,
}

impl EtcdLeaseStore {
    /// Create from an existing etcd client.
    pub fn new(client: Client, prefix: &str) -> Self {
        Self {
            client,
            prefix: prefix.to_string(),
        }
    }

    /// Connect to etcd and create a lease store with key prefix.
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
impl LeaseStore for EtcdLeaseStore {
    async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        let mut client = self.client.clone();
        let resp = client
            .get(self.prefixed_key(key), None)
            .await
            .map_err(|e| anyhow!("etcd get failed: {}", e))?;

        match resp.kvs().first() {
            Some(kv) => Ok(Some((
                kv.value().to_vec(),
                kv.mod_revision().to_string(),
            ))),
            None => Ok(None),
        }
    }

    async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult> {
        let pkey = self.prefixed_key(key);
        let mut client = self.client.clone();

        // Txn: if key doesn't exist (create_revision == 0), put it.
        let txn = Txn::new()
            .when([Compare::create_revision(pkey.clone(), CompareOp::Equal, 0)])
            .and_then([TxnOp::put(pkey.clone(), data, None)]);

        let resp = client
            .txn(txn)
            .await
            .map_err(|e| anyhow!("etcd txn failed: {}", e))?;

        if resp.succeeded() {
            // The transaction header revision is the mod_revision of the key we just wrote.
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

    async fn write_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<CasResult> {
        let pkey = self.prefixed_key(key);
        let revision: i64 = etag
            .parse()
            .map_err(|_| anyhow!("Invalid etcd revision etag: {}", etag))?;

        let mut client = self.client.clone();

        // Txn: if mod_revision matches, update.
        let txn = Txn::new()
            .when([Compare::mod_revision(pkey.clone(), CompareOp::Equal, revision)])
            .and_then([TxnOp::put(pkey.clone(), data, None)]);

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

    async fn delete(&self, key: &str) -> Result<()> {
        let mut client = self.client.clone();
        client
            .delete(self.prefixed_key(key), None)
            .await
            .map_err(|e| anyhow!("etcd delete failed: {}", e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test fixture: connects to etcd with a unique prefix per test.
    /// Returns None if ETCD_ENDPOINTS is not set.
    struct TestFixture {
        store: EtcdLeaseStore,
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
                "test-{}/",
                uuid::Uuid::new_v4().to_string().replace('-', "")
            );
            let client = Client::connect(&endpoints, None)
                .await
                .expect("connect");
            let store = EtcdLeaseStore::new(client.clone(), &prefix);
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
        type Target = EtcdLeaseStore;
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

        let update = f
            .write_if_match("lease1", b"v2".to_vec(), &first_etag)
            .await
            .unwrap();
        assert!(update.success);

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
    async fn test_etag_is_numeric_revision() {
        let Some(f) = TestFixture::new().await else { return };

        let create = f
            .write_if_not_exists("lease1", b"data".to_vec())
            .await
            .unwrap();
        let etag = create.etag.unwrap();

        let revision: i64 = etag.parse().expect("etag should be a numeric revision");
        assert!(revision > 0);
        f.cleanup().await;
    }
}
