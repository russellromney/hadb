//! Coordinator progress contract: a synchronous VFS/IO bridge in a follower
//! must not suspend the independently progressing lease monitor. Existing VFS
//! adapters legally use block_in_place + Handle::block_on. This protects task
//! scheduling, not a particular spawn/join implementation or fencing receipt.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use hadb::{
    CasResult, Coordinator, CoordinatorConfig, DbLease, FollowerBehavior, HaMetrics,
    InMemoryLeaseStore, LeaseConfig, LeaseStore, NoOpReplicator, Replicator, Role,
};
use tokio::sync::{oneshot, watch, Notify};

#[derive(Default)]
struct Progress {
    inside_bridge: AtomicBool,
    monitor_read: Notify,
}

struct ObservedStore {
    inner: InMemoryLeaseStore,
    progress: Arc<Progress>,
}

#[async_trait]
impl LeaseStore for ObservedStore {
    fn key_for(&self, scope: &str) -> String {
        self.inner.key_for(scope)
    }

    async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        let value = self.inner.read(key).await?;
        if self.progress.inside_bridge.load(Ordering::Acquire) {
            // A permit survives even if the IO waiter has not been polled yet.
            self.progress.monitor_read.notify_one();
        }
        Ok(value)
    }

    async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult> {
        self.inner.write_if_not_exists(key, data).await
    }

    async fn write_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<CasResult> {
        self.inner.write_if_match(key, data, etag).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }
}

struct BlockingFollower {
    progress: Arc<Progress>,
    result: Mutex<Option<oneshot::Sender<bool>>>,
}

#[async_trait]
impl FollowerBehavior for BlockingFollower {
    async fn run_follower_loop(
        &self,
        _replicator: Arc<dyn Replicator>,
        _prefix: &str,
        _db_name: &str,
        _db_path: &PathBuf,
        _poll_interval: Duration,
        _position: Arc<AtomicU64>,
        _caught_up: Arc<AtomicBool>,
        _cancel_rx: watch::Receiver<bool>,
        _metrics: Arc<HaMetrics>,
    ) -> Result<()> {
        self.progress.inside_bridge.store(true, Ordering::Release);
        let progressed = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                tokio::time::timeout(
                    Duration::from_millis(500),
                    self.progress.monitor_read.notified(),
                )
                .await
                .is_ok()
            })
        });
        // Capture the result BEFORE returning from the bridge. A monitor that
        // only resumes after the 500 ms timeout cannot turn failure into success.
        self.result
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(progressed)
            .ok();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lease_monitor_progresses_during_follower_blocking_io() {
    let progress = Arc::new(Progress::default());
    let store = Arc::new(ObservedStore {
        inner: InMemoryLeaseStore::new(),
        progress: progress.clone(),
    });
    // The live foreign owner prevents promotion. No fence writer is attached:
    // missing monitor progress cannot be explained by an authority guard.
    let key = store.key_for("db");
    let mut owner = DbLease::new(store.clone(), &key, "other", "other-address", 60);
    assert_eq!(owner.try_claim().await.unwrap(), Role::Leader);

    let mut lease = LeaseConfig::new(store, "follower".into(), "follower-address".into());
    lease.ttl_secs = 60;
    lease.follower_poll_interval = Duration::from_millis(10);
    let (result_tx, result_rx) = oneshot::channel();
    let coordinator = Coordinator::new(
        Arc::new(NoOpReplicator::new()),
        None,
        None,
        Arc::new(BlockingFollower {
            progress,
            result: Mutex::new(Some(result_tx)),
        }),
        "",
        CoordinatorConfig {
            lease: Some(lease),
            ..Default::default()
        },
    );
    let joined = tokio::time::timeout(
        Duration::from_secs(2),
        coordinator.join("db", &PathBuf::from("unused-progress-contract.db")),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(joined.role, Role::Follower);
    let progressed = tokio::time::timeout(Duration::from_secs(2), result_rx)
        .await
        .expect("the internal IO watchdog must finish before the outer watchdog")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), coordinator.leave("db"))
        .await
        .unwrap()
        .unwrap();
    assert!(progressed, "lease monitor must read while the follower is inside its blocking IO bridge, before the internal 500ms watchdog expires");
}
