//! ShardedLeaseStore: distribute lease operations across multiple backends.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use crate::{CasResult, LeaseStore};

/// A LeaseStore that shards keys across multiple backends.
///
/// Routes each key to a deterministic shard via `hash(key) % shards.len()`.
/// Works with any `LeaseStore` implementation (NATS, S3, Redis, mixed).
///
/// ```ignore
/// let sharded = ShardedLeaseStore::new(vec![
///     Arc::new(NatsLeaseStore::connect("nats://node1:4222", "leases").await?),
///     Arc::new(NatsLeaseStore::connect("nats://node2:4222", "leases").await?),
///     Arc::new(NatsLeaseStore::connect("nats://node3:4222", "leases").await?),
/// ])?;
/// ```
pub struct ShardedLeaseStore {
    shards: Vec<Arc<dyn LeaseStore>>,
}

impl ShardedLeaseStore {
    /// Create a sharded lease store from a list of backends.
    ///
    /// Panics if `shards` is empty.
    pub fn new(shards: Vec<Arc<dyn LeaseStore>>) -> Result<Self> {
        if shards.is_empty() {
            return Err(anyhow!("ShardedLeaseStore requires at least one shard"));
        }
        Ok(Self { shards })
    }

    fn shard_for(&self, key: &str) -> &dyn LeaseStore {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let idx = (hasher.finish() as usize) % self.shards.len();
        self.shards[idx].as_ref()
    }
}

#[async_trait]
impl LeaseStore for ShardedLeaseStore {
    async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        self.shard_for(key).read(key).await
    }

    async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult> {
        self.shard_for(key).write_if_not_exists(key, data).await
    }

    async fn write_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<CasResult> {
        self.shard_for(key).write_if_match(key, data, etag).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.shard_for(key).delete(key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryLeaseStore;

    #[test]
    fn test_new_empty_shards_errors() {
        let result = ShardedLeaseStore::new(vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_deterministic_routing() {
        let shards: Vec<Arc<dyn LeaseStore>> = (0..4)
            .map(|_| Arc::new(InMemoryLeaseStore::new()) as Arc<dyn LeaseStore>)
            .collect();
        let store = ShardedLeaseStore::new(shards).unwrap();

        // Same key always routes to the same shard.
        let shard_a = store.shard_for("lease:db-1") as *const dyn LeaseStore;
        let shard_b = store.shard_for("lease:db-1") as *const dyn LeaseStore;
        assert_eq!(shard_a, shard_b);
    }

    #[test]
    fn test_distribution_across_shards() {
        let n_shards = 4;
        let shards: Vec<Arc<dyn LeaseStore>> = (0..n_shards)
            .map(|_| Arc::new(InMemoryLeaseStore::new()) as Arc<dyn LeaseStore>)
            .collect();
        let _store = ShardedLeaseStore::new(shards).unwrap();

        // Hash 100 different keys and verify we hit more than one shard.
        let mut shard_indices = std::collections::HashSet::new();
        for i in 0..100 {
            let key = format!("lease:db-{}", i);
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            shard_indices.insert((hasher.finish() as usize) % n_shards);
        }
        // With 100 keys across 4 shards, we should hit all 4.
        assert_eq!(shard_indices.len(), n_shards);
    }

    #[tokio::test]
    async fn test_single_shard_passthrough() {
        let backend = Arc::new(InMemoryLeaseStore::new());
        let store =
            ShardedLeaseStore::new(vec![backend.clone() as Arc<dyn LeaseStore>]).unwrap();

        // Write through sharded store.
        let cas = store
            .write_if_not_exists("lease:db-1", b"node-a".to_vec())
            .await
            .unwrap();
        assert!(cas.success);

        // Read through sharded store.
        let (data, etag) = store.read("lease:db-1").await.unwrap().unwrap();
        assert_eq!(data, b"node-a");

        // CAS update.
        let update = store
            .write_if_match("lease:db-1", b"node-b".to_vec(), &etag)
            .await
            .unwrap();
        assert!(update.success);

        // Delete.
        store.delete("lease:db-1").await.unwrap();
        assert!(store.read("lease:db-1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_multi_shard_isolation() {
        // Create 4 independent shards.
        let shards: Vec<Arc<dyn LeaseStore>> = (0..4)
            .map(|_| Arc::new(InMemoryLeaseStore::new()) as Arc<dyn LeaseStore>)
            .collect();
        let store = ShardedLeaseStore::new(shards).unwrap();

        // Write to multiple keys (will land on different shards).
        for i in 0..20 {
            let key = format!("lease:db-{}", i);
            let cas = store
                .write_if_not_exists(&key, format!("node-{}", i).into_bytes())
                .await
                .unwrap();
            assert!(cas.success, "create failed for {}", key);
        }

        // Read them all back.
        for i in 0..20 {
            let key = format!("lease:db-{}", i);
            let (data, _) = store.read(&key).await.unwrap().unwrap();
            assert_eq!(data, format!("node-{}", i).into_bytes());
        }
    }

    #[tokio::test]
    async fn test_cas_conflict_through_shard() {
        let shards: Vec<Arc<dyn LeaseStore>> = (0..2)
            .map(|_| Arc::new(InMemoryLeaseStore::new()) as Arc<dyn LeaseStore>)
            .collect();
        let store = ShardedLeaseStore::new(shards).unwrap();

        let cas = store
            .write_if_not_exists("lease:db-1", b"node-a".to_vec())
            .await
            .unwrap();
        assert!(cas.success);

        // Second create should fail (CAS conflict).
        let conflict = store
            .write_if_not_exists("lease:db-1", b"node-b".to_vec())
            .await
            .unwrap();
        assert!(!conflict.success);
    }

    #[tokio::test]
    async fn test_cas_update_multi_shard() {
        let shards: Vec<Arc<dyn LeaseStore>> = (0..4)
            .map(|_| Arc::new(InMemoryLeaseStore::new()) as Arc<dyn LeaseStore>)
            .collect();
        let store = ShardedLeaseStore::new(shards).unwrap();

        // Create, update, read back across multiple keys on different shards.
        for i in 0..10 {
            let key = format!("lease:db-{}", i);
            let cas = store
                .write_if_not_exists(&key, b"v1".to_vec())
                .await
                .unwrap();
            assert!(cas.success);
            let etag = cas.etag.unwrap();

            let update = store
                .write_if_match(&key, b"v2".to_vec(), &etag)
                .await
                .unwrap();
            assert!(update.success);

            let (data, _) = store.read(&key).await.unwrap().unwrap();
            assert_eq!(data, b"v2");
        }
    }

    #[tokio::test]
    async fn test_stale_etag_multi_shard() {
        let shards: Vec<Arc<dyn LeaseStore>> = (0..3)
            .map(|_| Arc::new(InMemoryLeaseStore::new()) as Arc<dyn LeaseStore>)
            .collect();
        let store = ShardedLeaseStore::new(shards).unwrap();

        let cas = store
            .write_if_not_exists("lease:db-7", b"v1".to_vec())
            .await
            .unwrap();
        let first_etag = cas.etag.unwrap();

        // Advance the revision.
        let update = store
            .write_if_match("lease:db-7", b"v2".to_vec(), &first_etag)
            .await
            .unwrap();
        assert!(update.success);

        // Stale etag should fail.
        let stale = store
            .write_if_match("lease:db-7", b"v3".to_vec(), &first_etag)
            .await
            .unwrap();
        assert!(!stale.success);
    }

    #[tokio::test]
    async fn test_delete_then_recreate_multi_shard() {
        let shards: Vec<Arc<dyn LeaseStore>> = (0..4)
            .map(|_| Arc::new(InMemoryLeaseStore::new()) as Arc<dyn LeaseStore>)
            .collect();
        let store = ShardedLeaseStore::new(shards).unwrap();

        for i in 0..10 {
            let key = format!("lease:db-{}", i);
            store
                .write_if_not_exists(&key, b"original".to_vec())
                .await
                .unwrap();
            store.delete(&key).await.unwrap();
            assert!(store.read(&key).await.unwrap().is_none());

            // Recreate after delete.
            let cas = store
                .write_if_not_exists(&key, b"recreated".to_vec())
                .await
                .unwrap();
            assert!(cas.success);
            let (data, _) = store.read(&key).await.unwrap().unwrap();
            assert_eq!(data, b"recreated");
        }
    }

    #[tokio::test]
    async fn test_routing_stable_across_calls() {
        // Verify the same key always hits the same shard by checking
        // that state persists (write on one call, read on another).
        let shards: Vec<Arc<dyn LeaseStore>> = (0..8)
            .map(|_| Arc::new(InMemoryLeaseStore::new()) as Arc<dyn LeaseStore>)
            .collect();
        let store = ShardedLeaseStore::new(shards).unwrap();

        store
            .write_if_not_exists("lease:stability-test", b"data".to_vec())
            .await
            .unwrap();

        // Multiple reads should all find the same data (same shard).
        for _ in 0..10 {
            let result = store.read("lease:stability-test").await.unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap().0, b"data");
        }
    }

    #[tokio::test]
    async fn test_various_shard_counts() {
        // Verify basic operations work with 1, 2, 3, 7, 16 shards.
        for n in [1, 2, 3, 7, 16] {
            let shards: Vec<Arc<dyn LeaseStore>> = (0..n)
                .map(|_| Arc::new(InMemoryLeaseStore::new()) as Arc<dyn LeaseStore>)
                .collect();
            let store = ShardedLeaseStore::new(shards).unwrap();

            let key = format!("lease:shard-count-{}", n);
            let cas = store
                .write_if_not_exists(&key, b"test".to_vec())
                .await
                .unwrap();
            assert!(cas.success, "failed with {} shards", n);

            let (data, _) = store.read(&key).await.unwrap().unwrap();
            assert_eq!(data, b"test");
        }
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        let shards: Vec<Arc<dyn LeaseStore>> = (0..4)
            .map(|_| Arc::new(InMemoryLeaseStore::new()) as Arc<dyn LeaseStore>)
            .collect();
        let store = Arc::new(ShardedLeaseStore::new(shards).unwrap());

        // Spawn 50 concurrent creates on different keys.
        let mut handles = Vec::new();
        for i in 0..50 {
            let store = store.clone();
            handles.push(tokio::spawn(async move {
                let key = format!("lease:concurrent-{}", i);
                let cas = store
                    .write_if_not_exists(&key, format!("node-{}", i).into_bytes())
                    .await
                    .unwrap();
                assert!(cas.success);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // Verify all 50 keys exist.
        for i in 0..50 {
            let key = format!("lease:concurrent-{}", i);
            let (data, _) = store.read(&key).await.unwrap().unwrap();
            assert_eq!(data, format!("node-{}", i).into_bytes());
        }
    }
}
