//! Comprehensive tests for the Coordinator.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio::sync::watch;

use hadb::*;

// ============================================================================
// Mock Implementations
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

    fn set_fail(&self, fail: bool) {
        self.should_fail.store(fail, Ordering::SeqCst);
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
        self.calls
            .lock()
            .unwrap()
            .push(format!("remove({})", name));
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

/// Mock follower behavior that does nothing (for testing coordinator orchestration).
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
        _caught_up: Arc<std::sync::atomic::AtomicBool>,
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

// Helper to create a test coordinator
fn test_coordinator(
    lease_store: Option<Arc<dyn LeaseStore>>,
    config: CoordinatorConfig,
) -> (Arc<Coordinator>, MockReplicator) {
    let replicator = MockReplicator::new();

    let coordinator = Coordinator::new(
        Arc::new(replicator.clone()),
        lease_store,
        None,
        Arc::new(MockFollowerBehavior),
        "test-prefix",
        config,
    );

    (coordinator, replicator)
}

// ============================================================================
// Tests: Single-Node Mode (No HA)
// ============================================================================

#[tokio::test]
async fn test_single_node_join_as_leader() {
    let config = CoordinatorConfig::default();
    let (coordinator, replicator) = test_coordinator(None, config);

    let db_path = PathBuf::from("/tmp/test.db");
    let result = coordinator.join("testdb", &db_path).await.unwrap();

    assert_eq!(result.role, Role::Leader);
    assert!(coordinator.contains("testdb").await);
    assert_eq!(coordinator.role("testdb").await, Some(Role::Leader));

    let calls = replicator.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0], "add(testdb)");
}

#[tokio::test]
async fn test_single_node_leave() {
    let config = CoordinatorConfig::default();
    let (coordinator, replicator) = test_coordinator(None, config);

    let db_path = PathBuf::from("/tmp/test.db");
    coordinator.join("testdb", &db_path).await.unwrap();

    coordinator.leave("testdb").await.unwrap();

    assert!(!coordinator.contains("testdb").await);
    assert_eq!(coordinator.role("testdb").await, None);

    let calls = replicator.calls();
    assert!(calls.contains(&"remove(testdb)".to_string()));
}

#[tokio::test]
async fn test_single_node_database_count() {
    let config = CoordinatorConfig::default();
    let (coordinator, _) = test_coordinator(None, config);

    assert_eq!(coordinator.database_count().await, 0);

    let db_path = PathBuf::from("/tmp/test.db");
    coordinator.join("db1", &db_path).await.unwrap();
    assert_eq!(coordinator.database_count().await, 1);

    coordinator.join("db2", &db_path).await.unwrap();
    assert_eq!(coordinator.database_count().await, 2);

    coordinator.leave("db1").await.unwrap();
    assert_eq!(coordinator.database_count().await, 1);
}

// ============================================================================
// Tests: HA Mode - Leader
// ============================================================================

#[tokio::test]
async fn test_ha_join_as_leader() {
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let mut config = CoordinatorConfig::default();
    config.lease = Some(LeaseConfig::new(
        "instance-1".into(),
        "127.0.0.1:8080".into(),
    ));

    let (coordinator, replicator) = test_coordinator(Some(lease_store), config);

    let db_path = PathBuf::from("/tmp/test.db");
    let result = coordinator.join("testdb", &db_path).await.unwrap();

    assert_eq!(result.role, Role::Leader);
    assert!(coordinator.contains("testdb").await);
    assert_eq!(coordinator.role("testdb").await, Some(Role::Leader));

    let addr = coordinator.leader_address("testdb").await;
    assert_eq!(addr, Some("127.0.0.1:8080".to_string()));

    let calls = replicator.calls();
    assert!(calls.contains(&"add(testdb)".to_string()));
}

#[tokio::test]
async fn test_ha_leader_metrics() {
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let mut config = CoordinatorConfig::default();
    config.lease = Some(LeaseConfig::new(
        "instance-1".into(),
        "127.0.0.1:8080".into(),
    ));

    let (coordinator, _) = test_coordinator(Some(lease_store), config);

    let db_path = PathBuf::from("/tmp/test.db");
    coordinator.join("testdb", &db_path).await.unwrap();

    let metrics = coordinator.metrics();
    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.lease_claims_attempted, 1);
    assert_eq!(snapshot.lease_claims_succeeded, 1);
    assert_eq!(snapshot.lease_claims_failed, 0);
}

#[tokio::test]
async fn test_ha_leader_handoff() {
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let mut config = CoordinatorConfig::default();
    config.lease = Some(LeaseConfig::new(
        "instance-1".into(),
        "127.0.0.1:8080".into(),
    ));

    let (coordinator, replicator) = test_coordinator(Some(lease_store), config);

    let db_path = PathBuf::from("/tmp/test.db");
    coordinator.join("testdb", &db_path).await.unwrap();

    let result = coordinator.handoff("testdb").await.unwrap();
    assert!(result);

    assert_eq!(coordinator.role("testdb").await, Some(Role::Follower));

    let calls = replicator.calls();
    assert!(calls.contains(&"remove(testdb)".to_string()));
}

#[tokio::test]
async fn test_ha_handoff_not_leader() {
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let mut config = CoordinatorConfig::default();
    config.lease = Some(LeaseConfig::new(
        "instance-1".into(),
        "127.0.0.1:8080".into(),
    ));

    let (coordinator, _) = test_coordinator(Some(lease_store), config);

    let result = coordinator.handoff("nonexistent").await.unwrap();
    assert!(!result);
}

// ============================================================================
// Tests: HA Mode - Follower
// ============================================================================

#[tokio::test]
async fn test_ha_join_as_follower() {
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let mut config1 = CoordinatorConfig::default();
    config1.lease = Some(LeaseConfig::new(
        "instance-1".into(),
        "127.0.0.1:8080".into(),
    ));

    let (coordinator1, _) = test_coordinator(Some(lease_store.clone()), config1);
    let db_path = PathBuf::from("/tmp/test.db");
    coordinator1.join("testdb", &db_path).await.unwrap();

    let mut config2 = CoordinatorConfig::default();
    config2.lease = Some(LeaseConfig::new(
        "instance-2".into(),
        "127.0.0.1:8081".into(),
    ));

    let (coordinator2, replicator2) = test_coordinator(Some(lease_store), config2);
    let result2 = coordinator2.join("testdb", &db_path).await.unwrap();

    assert_eq!(result2.role, Role::Follower);
    assert_eq!(coordinator2.role("testdb").await, Some(Role::Follower));

    let addr = coordinator2.leader_address("testdb").await;
    assert_eq!(addr, Some("127.0.0.1:8080".to_string()));

    let calls = replicator2.calls();
    assert!(calls.contains(&"pull(testdb)".to_string()));
}

#[tokio::test]
async fn test_ha_follower_metrics() {
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let mut config1 = CoordinatorConfig::default();
    config1.lease = Some(LeaseConfig::new(
        "instance-1".into(),
        "127.0.0.1:8080".into(),
    ));

    let (coordinator1, _) = test_coordinator(Some(lease_store.clone()), config1);
    let db_path = PathBuf::from("/tmp/test.db");
    coordinator1.join("testdb", &db_path).await.unwrap();

    let mut config2 = CoordinatorConfig::default();
    config2.lease = Some(LeaseConfig::new(
        "instance-2".into(),
        "127.0.0.1:8081".into(),
    ));

    let (coordinator2, _) = test_coordinator(Some(lease_store), config2);
    coordinator2.join("testdb", &db_path).await.unwrap();

    let metrics = coordinator2.metrics();
    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.lease_claims_attempted, 1);
    assert_eq!(snapshot.lease_claims_succeeded, 0);
    assert_eq!(snapshot.lease_claims_failed, 1);
}

// ============================================================================
// Tests: Role Events
// ============================================================================

#[tokio::test]
async fn test_role_events_join_leader() {
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let mut config = CoordinatorConfig::default();
    config.lease = Some(LeaseConfig::new(
        "instance-1".into(),
        "127.0.0.1:8080".into(),
    ));

    let (coordinator, _) = test_coordinator(Some(lease_store), config);
    let mut events_rx = coordinator.role_events();

    let db_path = PathBuf::from("/tmp/test.db");
    coordinator.join("testdb", &db_path).await.unwrap();

    let event = events_rx.recv().await.unwrap();
    match event {
        RoleEvent::Joined { db_name, role } => {
            assert_eq!(db_name, "testdb");
            assert_eq!(role, Role::Leader);
        }
        _ => panic!("Expected Joined event"),
    }
}

#[tokio::test]
async fn test_role_events_handoff() {
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let mut config = CoordinatorConfig::default();
    config.lease = Some(LeaseConfig::new(
        "instance-1".into(),
        "127.0.0.1:8080".into(),
    ));

    let (coordinator, _) = test_coordinator(Some(lease_store), config);
    let mut events_rx = coordinator.role_events();

    let db_path = PathBuf::from("/tmp/test.db");
    coordinator.join("testdb", &db_path).await.unwrap();

    let _ = events_rx.recv().await;

    coordinator.handoff("testdb").await.unwrap();

    let event = events_rx.recv().await.unwrap();
    match event {
        RoleEvent::Demoted { db_name } => {
            assert_eq!(db_name, "testdb");
        }
        _ => panic!("Expected Demoted event"),
    }
}

// ============================================================================
// Tests: Edge Cases
// ============================================================================

#[tokio::test]
async fn test_replicator_add_failure() {
    let config = CoordinatorConfig::default();
    let (coordinator, replicator) = test_coordinator(None, config);

    replicator.set_fail(true);

    let db_path = PathBuf::from("/tmp/test.db");
    let result = coordinator.join("testdb", &db_path).await;

    assert!(result.is_err());
    assert!(!coordinator.contains("testdb").await);
}

#[tokio::test]
async fn test_leave_nonexistent_database() {
    let config = CoordinatorConfig::default();
    let (coordinator, _) = test_coordinator(None, config);

    coordinator.leave("nonexistent").await.unwrap();
}

#[tokio::test]
async fn test_concurrent_joins() {
    let config = CoordinatorConfig::default();
    let (coordinator, _) = test_coordinator(None, config);

    let db_path = PathBuf::from("/tmp/test.db");

    let handles: Vec<_> = (0..10)
        .map(|i| {
            let coord = coordinator.clone();
            let path = db_path.clone();
            tokio::spawn(async move { coord.join(&format!("db{}", i), &path).await.unwrap() })
        })
        .collect();

    for handle in handles {
        let result = handle.await.unwrap();
        assert_eq!(result.role, Role::Leader);
    }

    assert_eq!(coordinator.database_count().await, 10);
}

#[tokio::test]
async fn test_discover_replicas_no_registry() {
    let config = CoordinatorConfig::default();
    let (coordinator, _) = test_coordinator(None, config);

    let replicas = coordinator.discover_replicas("testdb").await.unwrap();
    assert!(replicas.is_empty());
}

// ============================================================================
// Tests: database_names() and drain_all()
// ============================================================================

#[tokio::test]
async fn test_database_names_empty() {
    let config = CoordinatorConfig::default();
    let (coordinator, _) = test_coordinator(None, config);

    let names = coordinator.database_names().await;
    assert!(names.is_empty());
}

#[tokio::test]
async fn test_database_names_multiple() {
    let config = CoordinatorConfig::default();
    let (coordinator, _) = test_coordinator(None, config);

    let db_path = PathBuf::from("/tmp/test.db");
    coordinator.join("db_alpha", &db_path).await.unwrap();
    coordinator.join("db_beta", &db_path).await.unwrap();
    coordinator.join("db_gamma", &db_path).await.unwrap();

    let mut names = coordinator.database_names().await;
    names.sort();
    assert_eq!(names, vec!["db_alpha", "db_beta", "db_gamma"]);
}

#[tokio::test]
async fn test_drain_all_empty() {
    let config = CoordinatorConfig::default();
    let (coordinator, _) = test_coordinator(None, config);

    let drained = coordinator.drain_all().await;
    assert_eq!(drained, 0);
    assert_eq!(coordinator.database_count().await, 0);
}

#[tokio::test]
async fn test_drain_all_leaders() {
    let config = CoordinatorConfig::default();
    let (coordinator, replicator) = test_coordinator(None, config);

    let db_path = PathBuf::from("/tmp/test.db");
    coordinator.join("db1", &db_path).await.unwrap();
    coordinator.join("db2", &db_path).await.unwrap();
    coordinator.join("db3", &db_path).await.unwrap();

    assert_eq!(coordinator.database_count().await, 3);

    let drained = coordinator.drain_all().await;
    assert_eq!(drained, 3);
    assert_eq!(coordinator.database_count().await, 0);

    // All databases should have been removed via replicator
    let calls = replicator.calls();
    assert!(calls.contains(&"remove(db1)".to_string()));
    assert!(calls.contains(&"remove(db2)".to_string()));
    assert!(calls.contains(&"remove(db3)".to_string()));
}

#[tokio::test]
async fn test_drain_all_with_ha() {
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let mut config = CoordinatorConfig::default();
    config.lease = Some(LeaseConfig::new(
        "instance-1".into(),
        "127.0.0.1:8080".into(),
    ));

    let (coordinator, _) = test_coordinator(Some(lease_store), config);

    let db_path = PathBuf::from("/tmp/test.db");
    coordinator.join("ha_db1", &db_path).await.unwrap();
    coordinator.join("ha_db2", &db_path).await.unwrap();

    assert_eq!(coordinator.database_count().await, 2);

    let drained = coordinator.drain_all().await;
    assert_eq!(drained, 2);
    assert_eq!(coordinator.database_count().await, 0);
}
