//! `hadb-lease`: LeaseStore trait for HA leader election.
//!
//! Trait-only. Backend implementations live in sibling crates
//! (`hadb-lease-s3`, `hadb-lease-nats`, `hadb-lease-etcd`, `hadb-lease-cinch`,
//! `hadb-lease-mem`).
//!
//! # Why trait-only?
//!
//! Backend crates shouldn't pull in the full hadb coordinator just to get
//! the LeaseStore trait definition. Consumers that only need to call the
//! trait (haqlite, hadb) can depend on this tiny crate directly.
//!
//! # Relationship to `hadb-storage`
//!
//! `CasResult` is the shared CAS primitive; it lives in `hadb-storage` and is
//! re-exported here for ergonomic use.

use anyhow::Result;
use async_trait::async_trait;

pub use hadb_storage::CasResult;

/// Trait for CAS lease operations on a key-value store.
///
/// Used for leader election via conditional writes. Any storage system
/// with CAS support can implement this: S3 (conditional PUT), etcd (CAS),
/// Consul (check-and-set), Redis (SETNX), DynamoDB (conditional writes).
///
/// The coordinator uses this for:
/// - Leader claims lease via `write_if_not_exists`
/// - Leader renews lease via `write_if_match`
/// - Followers read lease to discover leader via `read`
/// - Leader releases lease via `delete` on graceful shutdown
#[async_trait]
pub trait LeaseStore: Send + Sync {
    /// Read a key, returning (data, etag). None if key doesn't exist.
    ///
    /// The etag is an opaque version token used for CAS operations.
    /// For S3: the ETag header. For etcd: the revision number.
    async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>>;

    /// Write only if key doesn't exist (create). CAS.
    ///
    /// Used by followers to claim an expired lease. Returns success=true
    /// if the write succeeded (lease was claimed), false if another node
    /// already claimed it (CAS conflict).
    async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult>;

    /// Write only if current etag matches (update). CAS.
    ///
    /// Used by leader to renew its lease. Returns success=true if the
    /// renewal succeeded (still leader), false if another node claimed
    /// the lease (CAS conflict - self-fencing).
    async fn write_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<CasResult>;

    /// Delete a key (best-effort for lease release).
    ///
    /// Used by leader on graceful shutdown to release the lease immediately
    /// instead of waiting for TTL expiration. Failures are logged but not fatal.
    async fn delete(&self, key: &str) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[allow(dead_code)]
    fn _object_safe(_: &dyn LeaseStore) {}

    #[allow(dead_code)]
    fn _send_sync<T: LeaseStore>() {}

    /// Stateful in-memory mock used to exercise the trait contract here at
    /// the abstraction layer. Real impls (`hadb-lease-s3`, `-http`, `-nats`,
    /// `-etcd`) have their own tests; this one just pins the trait semantics.
    struct StatefulLeaseStore {
        data: Arc<Mutex<HashMap<String, (Vec<u8>, String)>>>,
    }

    impl StatefulLeaseStore {
        fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    #[async_trait]
    impl LeaseStore for StatefulLeaseStore {
        async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
            Ok(self.data.lock().await.get(key).cloned())
        }

        async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult> {
            let mut store = self.data.lock().await;
            if store.contains_key(key) {
                Ok(CasResult {
                    success: false,
                    etag: None,
                })
            } else {
                let etag = format!("etag-{}", uuid::Uuid::new_v4());
                store.insert(key.to_string(), (data, etag.clone()));
                Ok(CasResult {
                    success: true,
                    etag: Some(etag),
                })
            }
        }

        async fn write_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<CasResult> {
            let mut store = self.data.lock().await;
            if let Some((_, current_etag)) = store.get(key) {
                if current_etag == etag {
                    let new_etag = format!("etag-{}", uuid::Uuid::new_v4());
                    store.insert(key.to_string(), (data, new_etag.clone()));
                    Ok(CasResult {
                        success: true,
                        etag: Some(new_etag),
                    })
                } else {
                    Ok(CasResult {
                        success: false,
                        etag: None,
                    })
                }
            } else {
                Ok(CasResult {
                    success: false,
                    etag: None,
                })
            }
        }

        async fn delete(&self, key: &str) -> Result<()> {
            self.data.lock().await.remove(key);
            Ok(())
        }
    }

    #[test]
    fn cas_result_equality() {
        let r1 = CasResult { success: true, etag: Some("v1".into()) };
        let r2 = CasResult { success: true, etag: Some("v1".into()) };
        let r3 = CasResult { success: false, etag: None };
        assert_eq!(r1, r2);
        assert_ne!(r1, r3);
    }

    #[test]
    fn cas_result_edge_cases() {
        let r1 = CasResult { success: false, etag: None };
        let r2 = CasResult { success: false, etag: None };
        assert_eq!(r1, r2);

        let r3 = CasResult { success: true, etag: Some(String::new()) };
        let r4 = CasResult { success: true, etag: Some(String::new()) };
        assert_eq!(r3, r4);

        let r5 = CasResult { success: true, etag: None };
        assert!(r5.success);
        assert!(r5.etag.is_none());
    }

    #[tokio::test]
    async fn lease_store_cas_create() {
        let store = StatefulLeaseStore::new();

        let result = store
            .write_if_not_exists("lease1", b"node1".to_vec())
            .await
            .unwrap();
        assert!(result.success);
        assert!(result.etag.is_some());

        let result = store
            .write_if_not_exists("lease1", b"node2".to_vec())
            .await
            .unwrap();
        assert!(!result.success);
        assert!(result.etag.is_none());
    }

    #[tokio::test]
    async fn lease_store_cas_update() {
        let store = StatefulLeaseStore::new();

        let result = store
            .write_if_not_exists("lease1", b"node1-v1".to_vec())
            .await
            .unwrap();
        assert!(result.success);
        let etag = result.etag.unwrap();

        let result = store
            .write_if_match("lease1", b"node1-v2".to_vec(), &etag)
            .await
            .unwrap();
        assert!(result.success);
        let new_etag = result.etag.unwrap();
        assert_ne!(etag, new_etag);

        let result = store
            .write_if_match("lease1", b"node1-v3".to_vec(), &etag)
            .await
            .unwrap();
        assert!(!result.success);
    }

    #[tokio::test]
    async fn lease_store_read() {
        let store = StatefulLeaseStore::new();

        let result = store.read("lease1").await.unwrap();
        assert!(result.is_none());

        let write_result = store
            .write_if_not_exists("lease1", b"node1".to_vec())
            .await
            .unwrap();
        let etag = write_result.etag.unwrap();

        let result = store.read("lease1").await.unwrap();
        assert!(result.is_some());
        let (data, read_etag) = result.unwrap();
        assert_eq!(data, b"node1");
        assert_eq!(read_etag, etag);
    }

    #[tokio::test]
    async fn lease_store_delete() {
        let store = StatefulLeaseStore::new();

        store
            .write_if_not_exists("lease1", b"node1".to_vec())
            .await
            .unwrap();
        assert!(store.read("lease1").await.unwrap().is_some());

        store.delete("lease1").await.unwrap();
        assert!(store.read("lease1").await.unwrap().is_none());

        assert!(store.delete("lease1").await.is_ok());
    }
}
