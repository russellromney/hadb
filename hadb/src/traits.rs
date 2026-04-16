//! Core traits for database-agnostic HA coordination.
//!
//! These traits abstract away database-specific details (SQL vs graph),
//! storage backends (S3 vs etcd), and execution mechanisms.

use anyhow::Result;
use async_trait::async_trait;
use std::path::Path;

use crate::manifest::{HaManifest, ManifestMeta};

// `LeaseStore` and `CasResult` live in `hadb-lease`. Re-exported here so
// existing callers that did `use hadb::{LeaseStore, CasResult}` keep working.
pub use hadb_lease::{CasResult, LeaseStore};

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

    /// Resume replication as leader after promotion from follower.
    ///
    /// Unlike add(), this continues from the existing seq counter in S3
    /// so followers can discover new changesets via discover_after(). Skips
    /// snapshot upload since the promoted node already has the data.
    /// Default: falls back to add().
    async fn add_continuing(&self, name: &str, path: &Path) -> Result<()> {
        self.add(name, path).await
    }

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
// ManifestStore: Manifest coordination abstraction
// ============================================================================

/// Trait for manifest storage with CAS semantics.
///
/// Same pattern as LeaseStore: trait in hadb, implementations in
/// hadb-manifest-s3, hadb-manifest-nats, etc.
///
/// Three methods:
/// - `get`: full manifest fetch (used on catch-up)
/// - `put`: CAS publish with version fencing
/// - `meta`: lightweight HEAD for cheap polling
#[async_trait]
pub trait ManifestStore: Send + Sync {
    /// Fetch the full manifest for a key. Returns None if no manifest exists.
    async fn get(&self, key: &str) -> Result<Option<HaManifest>>;

    /// Publish a new manifest with CAS on expected_version.
    ///
    /// - `expected_version: None` means the key must not exist (first publish).
    /// - `expected_version: Some(v)` means the current version must equal v.
    ///
    /// The `version` field in the provided manifest is ignored; the store
    /// assigns the next version (1 for first publish, expected_version + 1
    /// for updates). Implementors must enforce this.
    ///
    /// Returns CasResult { success: true, .. } on success, or
    /// CasResult { success: false, .. } on version mismatch.
    async fn put(
        &self,
        key: &str,
        manifest: &HaManifest,
        expected_version: Option<u64>,
    ) -> Result<CasResult>;

    /// Cheap metadata check without fetching the full storage payload.
    /// Returns version + leadership info for fencing.
    async fn meta(&self, key: &str) -> Result<Option<ManifestMeta>>;
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
        let _storage = MockStorageBackend;
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
}
