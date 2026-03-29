//! Tests for HaNode (engine-level HA).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio::sync::watch;

use hadb::*;

// ============================================================================
// Mock Implementations (same as coordinator_test.rs)
// ============================================================================

#[derive(Clone)]
struct MockReplicator {
    calls: Arc<Mutex<Vec<String>>>,
    should_fail: Arc<AtomicBool>,
}

impl MockReplicator {
    fn new() -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            should_fail: Arc::new(AtomicBool::new(false)),
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl Replicator for MockReplicator {
    async fn add(&self, name: &str, _path: &Path) -> Result<()> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(anyhow!("mock replicator.add failed"));
        }
        self.calls.lock().unwrap().push(format!("add({})", name));
        Ok(())
    }

    async fn pull(&self, name: &str, _path: &Path) -> Result<()> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(anyhow!("mock replicator.pull failed"));
        }
        self.calls.lock().unwrap().push(format!("pull({})", name));
        Ok(())
    }

    async fn remove(&self, name: &str) -> Result<()> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(anyhow!("mock replicator.remove failed"));
        }
        self.calls.lock().unwrap().push(format!("remove({})", name));
        Ok(())
    }

    async fn sync(&self, name: &str) -> Result<()> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(anyhow!("mock replicator.sync failed"));
        }
        self.calls.lock().unwrap().push(format!("sync({})", name));
        Ok(())
    }
}

struct MockFollowerBehavior;

#[async_trait]
impl FollowerBehavior for MockFollowerBehavior {
    async fn run_follower_loop(
        &self,
        _replicator: Arc<dyn Replicator>,
        _prefix: &str,
        _db_name: &str,
        _db_path: &PathBuf,
        _poll_interval: Duration,
        _position: Arc<std::sync::atomic::AtomicU64>,
        mut cancel_rx: watch::Receiver<bool>,
        _metrics: Arc<HaMetrics>,
    ) -> Result<()> {
        let _ = cancel_rx.changed().await;
        Ok(())
    }

    async fn catchup_on_promotion(
        &self,
        _prefix: &str,
        _db_name: &str,
        _db_path: &PathBuf,
        _position: u64,
    ) -> Result<()> {
        Ok(())
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Create a Coordinator in single-node mode (no lease store).
fn test_coordinator(replicator: &MockReplicator) -> Arc<Coordinator> {
    Coordinator::new(
        Arc::new(replicator.clone()),
        None, // no lease store = single-node mode
        None,
        Arc::new(MockFollowerBehavior),
        "test/",
        CoordinatorConfig::default(),
    )
}

/// Create an HaNode with InMemoryLeaseStore.
fn test_ha_node(
    coordinator: Arc<Coordinator>,
    replicator: &MockReplicator,
    lease_store: Arc<dyn LeaseStore>,
) -> Arc<HaNode> {
    let config = HaNodeConfig {
        instance_id: "engine-1".to_string(),
        address: "10.0.0.1:6379".to_string(),
        ttl_secs: 5,
        renew_interval: Duration::from_millis(100),
        follower_poll_interval: Duration::from_millis(100),
        max_consecutive_renewal_errors: 3,
        lease_prefix: "test/".to_string(),
    };

    HaNode::new(
        coordinator,
        lease_store,
        Arc::new(MockFollowerBehavior),
        Arc::new(replicator.clone()),
        config,
    )
}

// ============================================================================
// Tests: Coordinator promote/demote (unit tests for the new methods)
// ============================================================================

#[tokio::test]
async fn test_promote_from_follower() {
    let replicator = MockReplicator::new();
    let coordinator = test_coordinator(&replicator);

    let db_path = PathBuf::from("/tmp/test.db");
    coordinator.join("db1", &db_path).await.unwrap();
    assert_eq!(coordinator.role("db1").await, Some(Role::Leader));

    // Demote first
    let demoted = coordinator.demote("db1").await.unwrap();
    assert!(demoted);
    assert_eq!(coordinator.role("db1").await, Some(Role::Follower));

    // Promote back
    let promoted = coordinator.promote("db1").await.unwrap();
    assert!(promoted);
    assert_eq!(coordinator.role("db1").await, Some(Role::Leader));

    let calls = replicator.calls();
    assert_eq!(calls, vec![
        "add(db1)",    // initial join
        "remove(db1)", // demote stops sync
        "add(db1)",    // promote restarts sync
    ]);
}

#[tokio::test]
async fn test_demote_from_leader() {
    let replicator = MockReplicator::new();
    let coordinator = test_coordinator(&replicator);

    let db_path = PathBuf::from("/tmp/test.db");
    coordinator.join("db1", &db_path).await.unwrap();

    let mut rx = coordinator.role_events();

    let demoted = coordinator.demote("db1").await.unwrap();
    assert!(demoted);
    assert_eq!(coordinator.role("db1").await, Some(Role::Follower));

    // Should have fired Demoted event
    let event = rx.recv().await.unwrap();
    assert!(matches!(event, RoleEvent::Demoted { db_name } if db_name == "db1"));
}

#[tokio::test]
async fn test_promote_already_leader_is_noop() {
    let replicator = MockReplicator::new();
    let coordinator = test_coordinator(&replicator);

    let db_path = PathBuf::from("/tmp/test.db");
    coordinator.join("db1", &db_path).await.unwrap();

    let promoted = coordinator.promote("db1").await.unwrap();
    assert!(!promoted); // already Leader, no-op
}

#[tokio::test]
async fn test_demote_already_follower_is_noop() {
    let replicator = MockReplicator::new();
    let coordinator = test_coordinator(&replicator);

    let db_path = PathBuf::from("/tmp/test.db");
    coordinator.join("db1", &db_path).await.unwrap();
    coordinator.demote("db1").await.unwrap();

    let demoted = coordinator.demote("db1").await.unwrap();
    assert!(!demoted); // already Follower, no-op
}

#[tokio::test]
async fn test_promote_nonexistent_returns_false() {
    let replicator = MockReplicator::new();
    let coordinator = test_coordinator(&replicator);

    let result = coordinator.promote("nonexistent").await.unwrap();
    assert!(!result);
}

#[tokio::test]
async fn test_demote_nonexistent_returns_false() {
    let replicator = MockReplicator::new();
    let coordinator = test_coordinator(&replicator);

    let result = coordinator.demote("nonexistent").await.unwrap();
    assert!(!result);
}

#[tokio::test]
async fn test_promote_demote_cycle() {
    let replicator = MockReplicator::new();
    let coordinator = test_coordinator(&replicator);

    let db_path = PathBuf::from("/tmp/test.db");
    coordinator.join("db1", &db_path).await.unwrap();

    // Cycle through multiple transitions
    for _ in 0..3 {
        coordinator.demote("db1").await.unwrap();
        assert_eq!(coordinator.role("db1").await, Some(Role::Follower));
        coordinator.promote("db1").await.unwrap();
        assert_eq!(coordinator.role("db1").await, Some(Role::Leader));
    }

    let calls = replicator.calls();
    // add (join) + 3x (remove + add)
    assert_eq!(calls.len(), 7);
}

#[tokio::test]
async fn test_db_path_stored_and_retrievable() {
    let replicator = MockReplicator::new();
    let coordinator = test_coordinator(&replicator);

    let db_path = PathBuf::from("/data/mydb.sqlite");
    coordinator.join("mydb", &db_path).await.unwrap();

    let stored = coordinator.db_path("mydb").await;
    assert_eq!(stored, Some(db_path));
}

#[tokio::test]
async fn test_all_roles() {
    let replicator = MockReplicator::new();
    let coordinator = test_coordinator(&replicator);

    coordinator.join("db1", &PathBuf::from("/tmp/db1")).await.unwrap();
    coordinator.join("db2", &PathBuf::from("/tmp/db2")).await.unwrap();
    coordinator.demote("db2").await.unwrap();

    let mut roles = coordinator.all_roles().await;
    roles.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(roles.len(), 2);
    assert_eq!(roles[0], ("db1".to_string(), Role::Leader));
    assert_eq!(roles[1], ("db2".to_string(), Role::Follower));
}

// ============================================================================
// Tests: HaNode engine-level HA
// ============================================================================

#[tokio::test]
async fn test_ha_node_starts_as_leader() {
    let replicator = MockReplicator::new();
    let coordinator = test_coordinator(&replicator);
    let lease_store = Arc::new(InMemoryLeaseStore::new());

    let node = test_ha_node(coordinator, &replicator, lease_store);
    let role = node.start().await.unwrap();

    assert_eq!(role, Role::Leader);
    assert_eq!(node.engine_role(), Role::Leader);
}

#[tokio::test]
async fn test_ha_node_join_as_leader() {
    let replicator = MockReplicator::new();
    let coordinator = test_coordinator(&replicator);
    let lease_store = Arc::new(InMemoryLeaseStore::new());

    let node = test_ha_node(coordinator, &replicator, lease_store);
    node.start().await.unwrap();

    let role = node.join("db1", &PathBuf::from("/tmp/db1")).await.unwrap();
    assert_eq!(role, Role::Leader);
    assert_eq!(node.role("db1").await, Some(Role::Leader));
    assert_eq!(node.database_count().await, 1);
}

#[tokio::test]
async fn test_ha_node_starts_as_follower_when_lease_taken() {
    let replicator = MockReplicator::new();
    let coordinator = test_coordinator(&replicator);
    let lease_store = Arc::new(InMemoryLeaseStore::new());

    // Pre-claim the lease as another engine
    let mut other_lease = DbLease::new(
        lease_store.clone(),
        "test/",
        "__engine__",
        "other-engine",
        "10.0.0.2:6379",
        60, // long TTL so it doesn't expire during test
    );
    other_lease.try_claim().await.unwrap();

    // Our node should start as Follower
    let node = test_ha_node(coordinator, &replicator, lease_store);
    let role = node.start().await.unwrap();

    assert_eq!(role, Role::Follower);
    assert_eq!(node.engine_role(), Role::Follower);
}

#[tokio::test]
async fn test_ha_node_join_as_follower_demotes_database() {
    let replicator = MockReplicator::new();
    let coordinator = test_coordinator(&replicator);
    let lease_store = Arc::new(InMemoryLeaseStore::new());

    // Pre-claim the lease as another engine
    let mut other_lease = DbLease::new(
        lease_store.clone(),
        "test/",
        "__engine__",
        "other-engine",
        "10.0.0.2:6379",
        60,
    );
    other_lease.try_claim().await.unwrap();

    let node = test_ha_node(coordinator, &replicator, lease_store);
    node.start().await.unwrap();
    assert_eq!(node.engine_role(), Role::Follower);

    // Join should return Follower
    let role = node.join("db1", &PathBuf::from("/tmp/db1")).await.unwrap();
    assert_eq!(role, Role::Follower);
    assert_eq!(node.role("db1").await, Some(Role::Follower));

    // Replicator should have: add (coordinator join) then remove (demote)
    let calls = replicator.calls();
    assert_eq!(calls, vec!["add(db1)", "remove(db1)"]);
}

#[tokio::test]
async fn test_ha_node_multiple_databases_share_engine_role() {
    let replicator = MockReplicator::new();
    let coordinator = test_coordinator(&replicator);
    let lease_store = Arc::new(InMemoryLeaseStore::new());

    let node = test_ha_node(coordinator, &replicator, lease_store);
    node.start().await.unwrap();

    node.join("db1", &PathBuf::from("/tmp/db1")).await.unwrap();
    node.join("db2", &PathBuf::from("/tmp/db2")).await.unwrap();
    node.join("db3", &PathBuf::from("/tmp/db3")).await.unwrap();

    assert_eq!(node.database_count().await, 3);

    // All should be Leader since engine is Leader
    let roles = node.all_roles().await;
    for (_, role) in &roles {
        assert_eq!(*role, Role::Leader);
    }
}

#[tokio::test]
async fn test_ha_node_shutdown_drains_all() {
    let replicator = MockReplicator::new();
    let coordinator = test_coordinator(&replicator);
    let lease_store = Arc::new(InMemoryLeaseStore::new());

    let node = test_ha_node(coordinator, &replicator, lease_store);
    node.start().await.unwrap();
    node.join("db1", &PathBuf::from("/tmp/db1")).await.unwrap();
    node.join("db2", &PathBuf::from("/tmp/db2")).await.unwrap();

    node.shutdown().await.unwrap();
    // After shutdown, databases should be drained
    assert_eq!(node.database_count().await, 0);
}
