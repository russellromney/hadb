//! Core traits for database-agnostic HA coordination.
//!
//! These traits abstract away database-specific details (SQL vs graph),
//! storage backends (S3 vs etcd), and execution mechanisms.

use anyhow::Result;
use async_trait::async_trait;
use std::path::Path;

// ============================================================================
// Replicator: Sync layer abstraction
// ============================================================================

/// Trait for database replication/sync.
///
/// Abstracts away the replication mechanism (walrust LTX for SQLite,
/// graphstream .graphj for Kuzu, etc.). The coordinator calls these
/// methods based on role (leader vs follower).
#[async_trait]
pub trait Replicator: Send + Sync {
    /// Add a database to replication (leader starts syncing).
    ///
    /// Called when this node becomes leader. Should start background sync
    /// to storage backend (S3, etcd, etc.).
    async fn add(&self, name: &str, path: &Path) -> Result<()>;

    /// Pull updates from storage and apply locally (follower catch-up).
    ///
    /// Called periodically by followers to catch up with leader's writes.
    /// Should download incremental updates and apply them to local DB.
    async fn pull(&self, name: &str, path: &Path) -> Result<()>;

    /// Remove database from replication (stop syncing).
    ///
    /// Called when leaving the cluster or on shutdown. Should stop
    /// background sync and clean up resources.
    async fn remove(&self, name: &str) -> Result<()>;

    /// Force sync to storage (for graceful shutdown).
    ///
    /// Called during graceful leader handoff. Should flush all pending
    /// writes to storage before returning.
    async fn sync(&self, name: &str) -> Result<()>;
}

// ============================================================================
// Executor: Query execution abstraction
// ============================================================================

/// Trait for database query execution.
///
/// Abstracts away the database engine (rusqlite, Kuzu, Postgres, etc.).
/// The coordinator routes queries through this trait, forwarding writes
/// to the leader if this node is a follower.
#[async_trait]
pub trait Executor: Send + Sync {
    /// Parameter type (e.g., SqlValue for SQL, serde_json::Value for Cypher).
    type Params: Send + Sync;

    /// Result type (e.g., Vec<Row> for SQL, QueryResult for Cypher).
    type Result: Send + Sync;

    /// Execute a query (write or read).
    ///
    /// For leaders: execute locally.
    /// For followers: reads execute locally, writes are forwarded by coordinator.
    async fn execute(&self, query: &str, params: Self::Params) -> Result<Self::Result>;

    /// Check if query is a mutation (needs forwarding if follower).
    ///
    /// Used by coordinator to decide whether to forward to leader.
    /// Should return true for INSERT/UPDATE/DELETE/CREATE, false for SELECT/MATCH.
    fn is_mutation(&self, query: &str) -> bool;
}

// ============================================================================
// LeaseStore: Leader election abstraction
// ============================================================================

/// Result of a CAS (compare-and-swap) write operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CasResult {
    /// true if the write succeeded, false if precondition failed.
    pub success: bool,
    /// New etag if the write succeeded (opaque version token).
    pub etag: Option<String>,
}

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

// ============================================================================
// StorageBackend: Replication storage abstraction
// ============================================================================

/// Trait for replication data storage.
///
/// Abstracts away the storage layer for replication data (WAL frames,
/// journal entries, snapshots). Separate from LeaseStore (which is for
/// small CAS lease operations). This is for bulk data transfer.
///
/// Implementations: S3, etcd (for small DBs), local disk, etc.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Upload data to storage.
    ///
    /// Called by leader's replicator to upload WAL/journal data.
    /// Should be idempotent (overwrite if key exists).
    async fn upload(&self, key: &str, data: &[u8]) -> Result<()>;

    /// Download data from storage.
    ///
    /// Called by follower's replicator to download WAL/journal data.
    /// Returns error if key doesn't exist.
    async fn download(&self, key: &str) -> Result<Vec<u8>>;

    /// List objects by prefix.
    ///
    /// Called by follower to discover available WAL/journal segments.
    /// Returns list of keys matching the prefix, up to `max_keys` if specified.
    /// If `max_keys` is None, returns all matching keys (use with caution).
    async fn list(&self, prefix: &str, max_keys: Option<usize>) -> Result<Vec<String>>;

    /// Delete object.
    ///
    /// Called during garbage collection of old WAL/journal segments.
    /// Should be idempotent (no error if key doesn't exist).
    async fn delete(&self, key: &str) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Mock implementations for testing
    // ========================================================================

    struct MockReplicator;

    #[async_trait]
    impl Replicator for MockReplicator {
        async fn add(&self, _name: &str, _path: &Path) -> Result<()> {
            Ok(())
        }
        async fn pull(&self, _name: &str, _path: &Path) -> Result<()> {
            Ok(())
        }
        async fn remove(&self, _name: &str) -> Result<()> {
            Ok(())
        }
        async fn sync(&self, _name: &str) -> Result<()> {
            Ok(())
        }
    }

    struct MockExecutor;

    #[async_trait]
    impl Executor for MockExecutor {
        type Params = ();
        type Result = ();

        async fn execute(&self, _query: &str, _params: Self::Params) -> Result<Self::Result> {
            Ok(())
        }

        fn is_mutation(&self, query: &str) -> bool {
            query.starts_with("INSERT")
                || query.starts_with("UPDATE")
                || query.starts_with("DELETE")
                || query.starts_with("CREATE")
        }
    }

    struct MockLeaseStore;

    #[async_trait]
    impl LeaseStore for MockLeaseStore {
        async fn read(&self, _key: &str) -> Result<Option<(Vec<u8>, String)>> {
            Ok(None)
        }
        async fn write_if_not_exists(&self, _key: &str, _data: Vec<u8>) -> Result<CasResult> {
            Ok(CasResult { success: true, etag: Some("etag1".into()) })
        }
        async fn write_if_match(&self, _key: &str, _data: Vec<u8>, _etag: &str) -> Result<CasResult> {
            Ok(CasResult { success: true, etag: Some("etag2".into()) })
        }
        async fn delete(&self, _key: &str) -> Result<()> {
            Ok(())
        }
    }

    struct MockStorageBackend;

    #[async_trait]
    impl StorageBackend for MockStorageBackend {
        async fn upload(&self, _key: &str, _data: &[u8]) -> Result<()> {
            Ok(())
        }
        async fn download(&self, _key: &str) -> Result<Vec<u8>> {
            Ok(vec![])
        }
        async fn list(&self, _prefix: &str, _max_keys: Option<usize>) -> Result<Vec<String>> {
            Ok(vec![])
        }
        async fn delete(&self, _key: &str) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_traits_compile() {
        // Ensure all traits compile and mock implementations work
        let _replicator = MockReplicator;
        let _executor = MockExecutor;
        let _lease_store = MockLeaseStore;
        let _storage = MockStorageBackend;
    }

    #[test]
    fn test_cas_result_equality() {
        let r1 = CasResult { success: true, etag: Some("v1".into()) };
        let r2 = CasResult { success: true, etag: Some("v1".into()) };
        let r3 = CasResult { success: false, etag: None };

        assert_eq!(r1, r2);
        assert_ne!(r1, r3);
    }

    #[test]
    fn test_executor_is_mutation() {
        let executor = MockExecutor;

        // Positive: mutations
        assert!(executor.is_mutation("INSERT INTO users VALUES (1)"));
        assert!(executor.is_mutation("UPDATE users SET name = 'Alice'"));
        assert!(executor.is_mutation("DELETE FROM users WHERE id = 1"));
        assert!(executor.is_mutation("CREATE TABLE foo (id INT)"));

        // Negative: reads
        assert!(!executor.is_mutation("SELECT * FROM users"));
        assert!(!executor.is_mutation("MATCH (n) RETURN n"));
    }

    // ========================================================================
    // Async tests with mock implementations
    // ========================================================================

    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// MockReplicator that can fail on demand
    struct FailableReplicator {
        should_fail: Arc<Mutex<bool>>,
    }

    impl FailableReplicator {
        fn new() -> Self {
            Self {
                should_fail: Arc::new(Mutex::new(false)),
            }
        }

        async fn set_fail(&self, fail: bool) {
            *self.should_fail.lock().await = fail;
        }
    }

    #[async_trait]
    impl Replicator for FailableReplicator {
        async fn add(&self, _name: &str, _path: &Path) -> Result<()> {
            if *self.should_fail.lock().await {
                anyhow::bail!("replicator add failed");
            }
            Ok(())
        }
        async fn pull(&self, _name: &str, _path: &Path) -> Result<()> {
            if *self.should_fail.lock().await {
                anyhow::bail!("replicator pull failed");
            }
            Ok(())
        }
        async fn remove(&self, _name: &str) -> Result<()> {
            if *self.should_fail.lock().await {
                anyhow::bail!("replicator remove failed");
            }
            Ok(())
        }
        async fn sync(&self, _name: &str) -> Result<()> {
            if *self.should_fail.lock().await {
                anyhow::bail!("replicator sync failed");
            }
            Ok(())
        }
    }

    /// MockLeaseStore that tracks operations and can simulate CAS conflicts
    struct StatefulLeaseStore {
        data: Arc<Mutex<std::collections::HashMap<String, (Vec<u8>, String)>>>,
    }

    impl StatefulLeaseStore {
        fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(std::collections::HashMap::new())),
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
                // CAS conflict - key already exists
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
                    // Etag matches - update
                    let new_etag = format!("etag-{}", uuid::Uuid::new_v4());
                    store.insert(key.to_string(), (data, new_etag.clone()));
                    Ok(CasResult {
                        success: true,
                        etag: Some(new_etag),
                    })
                } else {
                    // CAS conflict - etag mismatch
                    Ok(CasResult {
                        success: false,
                        etag: None,
                    })
                }
            } else {
                // Key doesn't exist - CAS conflict
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

    #[tokio::test]
    async fn test_replicator_add_success() {
        let replicator = MockReplicator;
        let result = replicator.add("testdb", Path::new("/tmp/test.db")).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_replicator_error_handling() {
        let replicator = FailableReplicator::new();

        // Should succeed initially
        assert!(replicator.add("testdb", Path::new("/tmp/test.db")).await.is_ok());

        // Set to fail mode
        replicator.set_fail(true).await;

        // Should fail now
        assert!(replicator.add("testdb", Path::new("/tmp/test.db")).await.is_err());
        assert!(replicator.pull("testdb", Path::new("/tmp/test.db")).await.is_err());
        assert!(replicator.remove("testdb").await.is_err());
        assert!(replicator.sync("testdb").await.is_err());
    }

    #[tokio::test]
    async fn test_lease_store_cas_create() {
        let store = StatefulLeaseStore::new();

        // First write should succeed (key doesn't exist)
        let result = store
            .write_if_not_exists("lease1", b"node1".to_vec())
            .await
            .unwrap();
        assert!(result.success);
        assert!(result.etag.is_some());

        // Second write should fail (key exists)
        let result = store
            .write_if_not_exists("lease1", b"node2".to_vec())
            .await
            .unwrap();
        assert!(!result.success);
        assert!(result.etag.is_none());
    }

    #[tokio::test]
    async fn test_lease_store_cas_update() {
        let store = StatefulLeaseStore::new();

        // Create initial lease
        let result = store
            .write_if_not_exists("lease1", b"node1-v1".to_vec())
            .await
            .unwrap();
        assert!(result.success);
        let etag = result.etag.unwrap();

        // Update with correct etag should succeed
        let result = store
            .write_if_match("lease1", b"node1-v2".to_vec(), &etag)
            .await
            .unwrap();
        assert!(result.success);
        let new_etag = result.etag.unwrap();
        assert_ne!(etag, new_etag);

        // Update with old etag should fail
        let result = store
            .write_if_match("lease1", b"node1-v3".to_vec(), &etag)
            .await
            .unwrap();
        assert!(!result.success);
    }

    #[tokio::test]
    async fn test_lease_store_read() {
        let store = StatefulLeaseStore::new();

        // Read non-existent key
        let result = store.read("lease1").await.unwrap();
        assert!(result.is_none());

        // Create a lease
        let write_result = store
            .write_if_not_exists("lease1", b"node1".to_vec())
            .await
            .unwrap();
        let etag = write_result.etag.unwrap();

        // Read existing key
        let result = store.read("lease1").await.unwrap();
        assert!(result.is_some());
        let (data, read_etag) = result.unwrap();
        assert_eq!(data, b"node1");
        assert_eq!(read_etag, etag);
    }

    #[tokio::test]
    async fn test_lease_store_delete() {
        let store = StatefulLeaseStore::new();

        // Create and delete
        store
            .write_if_not_exists("lease1", b"node1".to_vec())
            .await
            .unwrap();
        assert!(store.read("lease1").await.unwrap().is_some());

        store.delete("lease1").await.unwrap();
        assert!(store.read("lease1").await.unwrap().is_none());

        // Delete non-existent key (should be idempotent)
        assert!(store.delete("lease1").await.is_ok());
    }

    #[tokio::test]
    async fn test_storage_backend_operations() {
        let storage = MockStorageBackend;

        // Upload
        assert!(storage.upload("key1", b"data").await.is_ok());

        // Download
        assert!(storage.download("key1").await.is_ok());

        // List
        let keys = storage.list("prefix/", None).await.unwrap();
        assert_eq!(keys.len(), 0);

        // Delete
        assert!(storage.delete("key1").await.is_ok());
    }

    #[tokio::test]
    async fn test_executor_async_execute() {
        let executor = MockExecutor;

        // Execute query
        let result = executor.execute("SELECT * FROM users", ()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_cas_result_edge_cases() {
        // Both None
        let r1 = CasResult {
            success: false,
            etag: None,
        };
        let r2 = CasResult {
            success: false,
            etag: None,
        };
        assert_eq!(r1, r2);

        // Empty string etag
        let r3 = CasResult {
            success: true,
            etag: Some("".to_string()),
        };
        let r4 = CasResult {
            success: true,
            etag: Some("".to_string()),
        };
        assert_eq!(r3, r4);

        // Success=true with None etag (edge case, shouldn't happen but test it)
        let r5 = CasResult {
            success: true,
            etag: None,
        };
        assert!(r5.success);
        assert!(r5.etag.is_none());
    }
}
