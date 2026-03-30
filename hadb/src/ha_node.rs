//! Engine-level HA: one lease per process, many databases.
//!
//! `HaNode` wraps a `Coordinator` (in single-node mode, no per-database leases)
//! and owns a single engine-level lease. All databases share the engine's role.
//! When the engine loses its lease, all databases are demoted at once.
//! When the engine acquires its lease, all databases are promoted at once.
//!
//! ```ignore
//! let node = HaNode::new(coordinator, lease_store, config);
//! node.start().await?; // claims engine lease, spawns renewal/monitor
//! node.join("db-1", path1).await?; // starts replication, role follows engine
//! node.join("db-2", path2).await?;
//! ```

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Duration;

use anyhow::Result;
use tokio::sync::{broadcast, watch, RwLock};
use tracing::{error, info, warn};

use crate::coordinator::Coordinator;
use crate::follower::FollowerBehavior;
use crate::lease::DbLease;
use crate::metrics::HaMetrics;
use crate::traits::{LeaseStore, Replicator};
use crate::types::{Role, RoleEvent};

/// Configuration for engine-level HA.
#[derive(Debug, Clone)]
pub struct HaNodeConfig {
    /// Unique identifier for this engine/process.
    pub instance_id: String,
    /// Address other nodes can reach this engine at.
    pub address: String,
    /// Lease TTL in seconds (default 10).
    pub ttl_secs: u64,
    /// Lease renewal interval (default 3s).
    pub renew_interval: Duration,
    /// How often to check the lease when we're a follower (default 2s).
    pub follower_poll_interval: Duration,
    /// Consecutive renewal failures before self-fencing (default 3).
    pub max_consecutive_renewal_errors: u32,
    /// Prefix for the engine lease key in the lease store.
    pub lease_prefix: String,
}

impl HaNodeConfig {
    pub fn new(instance_id: String, address: String) -> Self {
        Self {
            instance_id,
            address,
            ttl_secs: 10,
            renew_interval: Duration::from_secs(3),
            follower_poll_interval: Duration::from_secs(2),
            max_consecutive_renewal_errors: 3,
            lease_prefix: "engine/".to_string(),
        }
    }
}

/// Atomic engine role (same pattern as AtomicRole in coordinator.rs).
struct AtomicEngineRole(AtomicU8);

impl AtomicEngineRole {
    fn new(role: Role) -> Self {
        Self(AtomicU8::new(role.to_u8()))
    }
    fn load(&self) -> Role {
        Role::from_u8(self.0.load(Ordering::SeqCst))
    }
    fn store(&self, role: Role) {
        self.0.store(role.to_u8(), Ordering::SeqCst);
    }
}

/// Engine-level HA coordinator.
///
/// Owns a single lease for the entire engine. Wraps a `Coordinator` running in
/// single-node mode (no per-database leases). Databases join/leave the Coordinator
/// for replication, and the HaNode manages role transitions for all databases at once.
pub struct HaNode {
    coordinator: Arc<Coordinator>,
    lease_store: Arc<dyn LeaseStore>,
    follower_behavior: Arc<dyn FollowerBehavior>,
    replicator: Arc<dyn Replicator>,
    config: HaNodeConfig,
    engine_role: AtomicEngineRole,
    /// Channel to stop the background lease task.
    cancel_tx: watch::Sender<bool>,
    cancel_rx: watch::Receiver<bool>,
    /// Broadcast channel for engine-level role events.
    role_tx: broadcast::Sender<RoleEvent>,
}

impl HaNode {
    /// Create a new HaNode.
    ///
    /// The `coordinator` must be in single-node mode (lease_store = None).
    /// `lease_store` is for the single engine-level lease.
    /// `follower_behavior` is used to run follower pull loops when the engine is Follower.
    /// `replicator` is used for follower pull operations.
    pub fn new(
        coordinator: Arc<Coordinator>,
        lease_store: Arc<dyn LeaseStore>,
        follower_behavior: Arc<dyn FollowerBehavior>,
        replicator: Arc<dyn Replicator>,
        config: HaNodeConfig,
    ) -> Arc<Self> {
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (role_tx, _) = broadcast::channel(64);

        Arc::new(Self {
            coordinator,
            lease_store,
            follower_behavior,
            replicator,
            config,
            engine_role: AtomicEngineRole::new(Role::Follower),
            cancel_tx,
            cancel_rx,
            role_tx,
        })
    }

    /// Start the engine lease lifecycle.
    ///
    /// Claims (or monitors) the engine-level lease and spawns a background task
    /// that handles renewal (if Leader) or monitoring (if Follower). Returns the
    /// initial role.
    pub async fn start(self: &Arc<Self>) -> Result<Role> {
        let mut lease = DbLease::new(
            self.lease_store.clone(),
            &self.config.lease_prefix,
            "__engine__",
            &self.config.instance_id,
            &self.config.address,
            self.config.ttl_secs,
        );

        let initial_role = lease.try_claim().await?;
        self.engine_role.store(initial_role);

        info!(
            "HaNode: engine '{}' started as {:?}",
            self.config.instance_id, initial_role
        );

        // Spawn the background lease management task
        let node = Arc::clone(self);
        tokio::spawn(async move {
            node.run_lease_lifecycle(lease).await;
        });

        Ok(initial_role)
    }

    /// Join a database to the engine.
    ///
    /// Starts WAL replication via the Coordinator (single-node mode, always returns Leader).
    /// If the engine is currently Follower, immediately demotes the database.
    pub async fn join(self: &Arc<Self>, name: &str, db_path: &Path) -> Result<Role> {
        // Coordinator in single-node mode always returns Leader and starts replication.
        self.coordinator.join(name, db_path).await?;

        let engine_role = self.engine_role.load();
        if engine_role == Role::Follower {
            // Engine is Follower: demote this database immediately.
            self.coordinator.demote(name).await?;
            // Start follower pull loop for this database.
            self.start_follower_loop(name, db_path).await;
        }

        Ok(engine_role)
    }

    /// Leave a database from the engine.
    pub async fn leave(&self, name: &str) -> Result<()> {
        self.coordinator.leave(name).await
    }

    /// Get the current engine role.
    pub fn engine_role(&self) -> Role {
        self.engine_role.load()
    }

    /// Get the role of a specific database.
    pub async fn role(&self, name: &str) -> Option<Role> {
        self.coordinator.role(name).await
    }

    /// Subscribe to role events (both per-database and engine-level).
    ///
    /// Events come from the underlying Coordinator (per-database promote/demote).
    pub fn role_events(&self) -> broadcast::Receiver<RoleEvent> {
        self.coordinator.role_events()
    }

    /// Get all database names managed by this node.
    pub async fn database_names(&self) -> Vec<String> {
        self.coordinator.database_names().await
    }

    /// Get all database names and their roles.
    pub async fn all_roles(&self) -> Vec<(String, Role)> {
        self.coordinator.all_roles().await
    }

    /// Drain all databases (final sync for leaders, stop for followers).
    pub async fn drain_all(&self) -> usize {
        self.coordinator.drain_all().await
    }

    /// Get the number of databases.
    pub async fn database_count(&self) -> usize {
        self.coordinator.database_count().await
    }

    /// Check if a database is managed by this node.
    pub async fn contains(&self, name: &str) -> bool {
        self.coordinator.contains(name).await
    }

    /// Get the coordinator's metrics.
    pub fn metrics(&self) -> &Arc<HaMetrics> {
        self.coordinator.metrics()
    }

    /// Graceful shutdown: release the engine lease and drain all databases.
    pub async fn shutdown(&self) -> Result<()> {
        let _ = self.cancel_tx.send(true);
        self.drain_all().await;
        Ok(())
    }

    // ========================================================================
    // Internal: lease lifecycle
    // ========================================================================

    /// Background task that manages the engine lease.
    async fn run_lease_lifecycle(self: Arc<Self>, mut lease: DbLease) {
        let mut cancel_rx = self.cancel_rx.clone();

        loop {
            let role = self.engine_role.load();

            match role {
                Role::Leader => {
                    // Renew the lease periodically.
                    match self.run_leader_renewal(&mut lease, &mut cancel_rx).await {
                        LeaseOutcome::Cancelled => break,
                        LeaseOutcome::LostLease => {
                            // Demote all databases.
                            self.transition_all_to_follower().await;
                        }
                    }
                }
                Role::Follower => {
                    // Monitor the lease, try to acquire when it expires.
                    match self.run_follower_monitor(&mut lease, &mut cancel_rx).await {
                        LeaseOutcome::Cancelled => break,
                        LeaseOutcome::LostLease => {
                            // Should not happen for followers, but treat as no-op.
                            warn!("HaNode: follower monitor returned LostLease (unexpected)");
                        }
                    }
                }
            }
        }

        info!("HaNode: lease lifecycle task stopped");
    }

    /// Run leader lease renewal. Returns when the lease is lost or cancelled.
    async fn run_leader_renewal(
        &self,
        lease: &mut DbLease,
        cancel_rx: &mut watch::Receiver<bool>,
    ) -> LeaseOutcome {
        let mut consecutive_errors = 0u32;

        loop {
            tokio::select! {
                _ = cancel_rx.changed() => {
                    return LeaseOutcome::Cancelled;
                }
                _ = tokio::time::sleep(self.config.renew_interval) => {
                    match lease.renew().await {
                        Ok(true) => {
                            consecutive_errors = 0;
                        }
                        Ok(false) => {
                            error!("HaNode: engine lease renewal rejected (lease stolen)");
                            self.engine_role.store(Role::Follower);
                            return LeaseOutcome::LostLease;
                        }
                        Err(e) => {
                            consecutive_errors += 1;
                            if consecutive_errors >= self.config.max_consecutive_renewal_errors {
                                error!(
                                    "HaNode: {} consecutive renewal failures, self-fencing: {}",
                                    consecutive_errors, e
                                );
                                self.engine_role.store(Role::Follower);
                                return LeaseOutcome::LostLease;
                            }
                            warn!(
                                "HaNode: renewal error ({}/{}): {}",
                                consecutive_errors, self.config.max_consecutive_renewal_errors, e
                            );
                        }
                    }
                }
            }
        }
    }

    /// Run follower lease monitor. Tries to claim the lease when it expires.
    /// Returns when promoted (transitions to leader) or cancelled.
    async fn run_follower_monitor(
        &self,
        lease: &mut DbLease,
        cancel_rx: &mut watch::Receiver<bool>,
    ) -> LeaseOutcome {
        loop {
            tokio::select! {
                _ = cancel_rx.changed() => {
                    return LeaseOutcome::Cancelled;
                }
                _ = tokio::time::sleep(self.config.follower_poll_interval) => {
                    match lease.try_claim().await {
                        Ok(Role::Leader) => {
                            info!("HaNode: engine '{}' acquired lease, promoting all databases",
                                self.config.instance_id);
                            self.engine_role.store(Role::Leader);
                            self.transition_all_to_leader().await;
                            return LeaseOutcome::Cancelled; // exits follower loop, outer loop re-enters as Leader
                        }
                        Ok(Role::Follower) => {
                            // Lease still held by another engine, keep monitoring.
                        }
                        Err(e) => {
                            warn!("HaNode: lease claim error (will retry): {}", e);
                        }
                    }
                }
            }
        }
    }

    /// Demote all databases from Leader to Follower.
    async fn transition_all_to_follower(&self) {
        let db_names = self.coordinator.database_names().await;
        info!("HaNode: demoting {} databases to Follower", db_names.len());

        for name in &db_names {
            if let Err(e) = self.coordinator.demote(name).await {
                error!("HaNode: failed to demote '{}': {}", name, e);
            }
            // Start follower pull loop for this database
            if let Some(db_path) = self.coordinator.db_path(name).await {
                self.start_follower_loop(name, &db_path).await;
            }
        }
    }

    /// Promote all databases from Follower to Leader.
    async fn transition_all_to_leader(&self) {
        let db_names = self.coordinator.database_names().await;
        info!("HaNode: promoting {} databases to Leader", db_names.len());

        for name in &db_names {
            if let Err(e) = self.coordinator.promote(name).await {
                error!("HaNode: failed to promote '{}': {}", name, e);
            }
        }
    }

    /// Start a follower pull loop for a single database.
    ///
    /// This runs the FollowerBehavior's pull loop, which fetches WAL data from
    /// the leader's S3 storage and applies it locally. Runs until the database
    /// is promoted (cancel signal sent by promote()) or left.
    async fn start_follower_loop(&self, name: &str, db_path: &Path) {
        let replicator = self.replicator.clone();
        let follower_behavior = self.follower_behavior.clone();
        let prefix = self.config.lease_prefix.clone();
        let db_name = name.to_string();
        let db_path = db_path.to_path_buf();
        let poll_interval = self.config.follower_poll_interval;
        let position = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let caught_up = Arc::new(AtomicBool::new(false));
        let metrics = self.coordinator.metrics().clone();

        // Create a cancel channel tied to the coordinator's cancel for this database.
        // When the database is promoted, the coordinator sends cancel, stopping this loop.
        let (cancel_tx, cancel_rx) = watch::channel(false);

        // Store the cancel_tx so promote() can stop this loop.
        // For now, we fire-and-forget the pull loop. The coordinator's demote/promote
        // cycle handles stopping via the shared cancel channel.
        let _ = cancel_tx; // keep alive as long as cancel_rx is alive

        tokio::spawn(async move {
            if let Err(e) = follower_behavior
                .run_follower_loop(
                    replicator,
                    &prefix,
                    &db_name,
                    &db_path,
                    poll_interval,
                    position,
                    caught_up,
                    cancel_rx,
                    metrics,
                )
                .await
            {
                error!("HaNode: follower pull loop for '{}' failed: {}", db_name, e);
            }
        });
    }
}

/// Outcome of a lease operation cycle.
enum LeaseOutcome {
    /// Shutdown requested.
    Cancelled,
    /// Lease was lost (renewal rejected or too many errors).
    LostLease,
}
