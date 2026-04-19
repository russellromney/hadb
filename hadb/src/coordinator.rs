//! HA coordinator — zero type parameters, all trait objects.
//!
//! Coordinates leader election, replication, and write forwarding across
//! any database (SQL, graph, document) and any storage backend (S3, etcd, Consul).
//!
//! All dependencies are trait objects (`Arc<dyn Replicator>`, etc.) so there's no
//! generic type gymnastics. The vtable overhead is unmeasurable — every trait method
//! does network I/O.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::{broadcast, watch, RwLock};
use tokio::task::JoinHandle;

use crate::follower::{run_leader_renewal, run_lease_monitor, FollowerBehavior, LeaseMonitorContext};
use crate::lease::DbLease;
use crate::metrics::HaMetrics;
use crate::node_registry::{NodeRegistration, NodeRegistry};
use crate::manifest::ManifestStore;
use crate::traits::{LeaseStore, Replicator};
use crate::types::{CoordinatorConfig, Role, RoleEvent};

/// Resolve the effective lease key for a database.
/// If `override_key` is set, returns it unchanged. Otherwise builds the
/// legacy compound key `"{prefix}{name}/_lease.json"` used by S3/NATS backends.
fn resolve_lease_key(override_key: &Option<String>, prefix: &str, name: &str) -> String {
    match override_key {
        Some(k) => k.clone(),
        None => format!("{}{}/_lease.json", prefix, name),
    }
}

/// Atomic wrapper around Role for lock-free reads.
pub(crate) struct AtomicRole(AtomicU8);

impl AtomicRole {
    fn new(role: Role) -> Self {
        Self(AtomicU8::new(role.to_u8()))
    }

    pub(crate) fn load(&self) -> Role {
        Role::from_u8(self.0.load(Ordering::SeqCst))
    }

    pub(crate) fn store(&self, role: Role) {
        self.0.store(role.to_u8(), Ordering::SeqCst);
    }
}

/// Result of joining a database to the coordinator.
///
/// Contains Arc refs to the coordinator's per-database atomics. Database layers
/// cache these for zero-overhead health checks (single atomic load, no locks).
#[derive(Debug)]
pub struct JoinResult {
    pub role: Role,
    pub caught_up: Arc<AtomicBool>,
    pub position: Arc<AtomicU64>,
}

/// Per-database entry in the coordinator.
struct DbEntry {
    role: Arc<AtomicRole>,
    leader_address: Arc<RwLock<String>>,
    cancel_tx: watch::Sender<bool>,
    task_handle: JoinHandle<()>,
    /// Path to the database file. Stored for follower → leader catch-up
    /// inside the lease monitor task (see `follower::run_lease_monitor`).
    db_path: PathBuf,
    caught_up: Arc<AtomicBool>,
    position: Arc<AtomicU64>,
}

/// HA coordinator.
///
/// Coordinates leader election, replication, and write forwarding across
/// any database and any storage backend. All dependencies are trait objects.
pub struct Coordinator {
    replicator: Arc<dyn Replicator>,
    lease_store: Option<Arc<dyn LeaseStore>>,
    manifest_store: Option<Arc<dyn ManifestStore>>,
    node_registry: Option<Arc<dyn NodeRegistry>>,
    follower_behavior: Arc<dyn FollowerBehavior>,
    config: CoordinatorConfig,
    prefix: String,
    databases: RwLock<HashMap<String, DbEntry>>,
    role_tx: broadcast::Sender<RoleEvent>,
    metrics: Arc<HaMetrics>,
}

impl Coordinator {
    /// Create a new Coordinator.
    ///
    /// `replicator` handles replication (snapshot + incremental updates).
    /// `lease_store` is for CAS lease operations (None = no HA, always Leader).
    /// `manifest_store` is for manifest polling and publishing (None = no manifest).
    /// `node_registry` is for read replica discovery (None = no registry).
    /// `follower_behavior` defines database-specific follower pull and promotion logic.
    /// `prefix` is the storage key prefix for all databases (e.g. "wal/" or "ha/").
    /// `config` is the coordinator configuration.
    pub fn new(
        replicator: Arc<dyn Replicator>,
        lease_store: Option<Arc<dyn LeaseStore>>,
        manifest_store: Option<Arc<dyn ManifestStore>>,
        node_registry: Option<Arc<dyn NodeRegistry>>,
        follower_behavior: Arc<dyn FollowerBehavior>,
        prefix: &str,
        config: CoordinatorConfig,
    ) -> Arc<Self> {
        let (role_tx, _) = broadcast::channel(64);

        Arc::new(Self {
            replicator,
            lease_store,
            manifest_store,
            node_registry,
            follower_behavior,
            config,
            prefix: prefix.to_string(),
            databases: RwLock::new(HashMap::new()),
            role_tx,
            metrics: Arc::new(HaMetrics::new()),
        })
    }

    /// Join a database to the HA cluster.
    ///
    /// If leases are enabled: claims the lease -> Leader, or follows -> Follower.
    /// If leases are disabled: starts leader sync -> always Leader.
    pub async fn join(self: &Arc<Self>, name: &str, db_path: &Path) -> Result<JoinResult> {
        let lease_store = match &self.lease_store {
            Some(ls) => ls.clone(),
            None => {
                // No HA — always leader.
                tokio::time::timeout(
                    self.config.replicator_timeout,
                    self.replicator.add(name, db_path),
                )
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "Coordinator: replicator.add('{}') timed out after {:?}",
                        name,
                        self.config.replicator_timeout
                    )
                })??;

                let (cancel_tx, _) = watch::channel(false);
                let caught_up = Arc::new(AtomicBool::new(true));
                let position = Arc::new(AtomicU64::new(0));
                self.metrics.follower_caught_up.store(1, Ordering::Relaxed);
                self.databases.write().await.insert(
                    name.to_string(),
                    DbEntry {
                        role: Arc::new(AtomicRole::new(Role::Leader)),
                        leader_address: Arc::new(RwLock::new(String::new())),
                        cancel_tx,
                        task_handle: tokio::spawn(async {}),
                        db_path: db_path.to_path_buf(),
                        caught_up: caught_up.clone(),
                        position: position.clone(),
                    },
                );
                let _ = self.role_tx.send(RoleEvent::Joined {
                    db_name: name.to_string(),
                    role: Role::Leader,
                });
                tracing::info!("Coordinator: '{}' joined as Leader (no HA)", name);
                return Ok(JoinResult { role: Role::Leader, caught_up, position });
            }
        };

        // HA mode: try to claim lease.
        let lease_config = self
            .config
            .lease
            .as_ref()
            .expect("lease config must be present when lease_store is Some");

        let lease_key = resolve_lease_key(&self.config.lease_key, &self.prefix, name);
        let mut lease = DbLease::new(
            lease_store,
            &lease_key,
            &lease_config.instance_id,
            &lease_config.address,
            lease_config.ttl_secs,
        );
        if let Some(ref fw) = self.config.fence_writer {
            lease = lease.with_fence_writer(fw.clone());
        }

        self.metrics.inc(&self.metrics.lease_claims_attempted);
        let role = lease.try_claim().await?;
        match role {
            Role::Leader => self.metrics.inc(&self.metrics.lease_claims_succeeded),
            Role::Follower => self.metrics.inc(&self.metrics.lease_claims_failed),
        }

        let (cancel_tx, cancel_rx) = watch::channel(false);
        let shared_role = Arc::new(AtomicRole::new(role));

        let leader_addr = Arc::new(RwLock::new(if role == Role::Leader {
            lease_config.address.clone()
        } else {
            match DbLease::new(
                self.lease_store.as_ref().unwrap().clone(),
                &lease_key,
                &lease_config.instance_id,
                &lease_config.address,
                lease_config.ttl_secs,
            )
            .read()
            .await
            {
                Ok(Some((data, _))) => data.address,
                _ => String::new(),
            }
        }));

        match role {
            Role::Leader => {
                // Restore from S3 first: a previous leader may have written
                // data that this node doesn't have locally. pull() is a no-op
                // if no snapshot exists (first-ever leader).
                let _ = tokio::time::timeout(
                    self.config.replicator_timeout,
                    self.replicator.pull(name, db_path),
                )
                .await;

                // Start leader sync.
                tokio::time::timeout(
                    self.config.replicator_timeout,
                    self.replicator.add(name, db_path),
                )
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "Coordinator: replicator.add('{}') timed out after {:?}",
                        name,
                        self.config.replicator_timeout
                    )
                })??;

                // Spawn lease renewal loop.
                let replicator = self.replicator.clone();
                let db_name = name.to_string();
                let role_tx = self.role_tx.clone();
                let renew_interval = lease_config.renew_interval;
                let max_errors = lease_config.max_consecutive_renewal_errors;
                let replicator_timeout = self.config.replicator_timeout;
                let role_ref = shared_role.clone();
                let metrics = self.metrics.clone();
                let mut renewal_cancel_rx = cancel_rx.clone();

                let task_handle = tokio::spawn(async move {
                    let demoted = run_leader_renewal(
                        &mut lease,
                        &replicator,
                        &db_name,
                        &role_tx,
                        renew_interval,
                        &mut renewal_cancel_rx,
                        max_errors,
                        replicator_timeout,
                        metrics,
                    )
                    .await;
                    if demoted {
                        role_ref.store(Role::Follower);
                    }
                });

                let leader_caught_up = Arc::new(AtomicBool::new(true));
                let leader_position = Arc::new(AtomicU64::new(0));
                self.metrics.follower_caught_up.store(1, Ordering::Relaxed);
                self.databases.write().await.insert(
                    name.to_string(),
                    DbEntry {
                        role: shared_role,
                        leader_address: leader_addr,
                        cancel_tx,
                        task_handle,
                        db_path: db_path.to_path_buf(),
                        caught_up: leader_caught_up.clone(),
                        position: leader_position.clone(),
                    },
                );

                let _ = self.role_tx.send(RoleEvent::Joined {
                    db_name: name.to_string(),
                    role: Role::Leader,
                });
                tracing::info!("Coordinator: '{}' joined as Leader", name);
                Ok(JoinResult { role: Role::Leader, caught_up: leader_caught_up, position: leader_position })
            }
            Role::Follower => {
                // Restore from storage to get a base snapshot.
                tokio::time::timeout(
                    self.config.replicator_timeout,
                    self.replicator.pull(name, db_path),
                )
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "Coordinator: replicator.pull('{}') timed out after {:?}",
                        name,
                        self.config.replicator_timeout
                    )
                })??;

                // Register as follower for read replica discovery.
                if let Some(ref registry) = self.node_registry {
                    if let Ok(Some((leader_data, _))) = lease.read().await {
                        let reg = NodeRegistration {
                            instance_id: lease_config.instance_id.clone(),
                            address: lease_config.address.clone(),
                            role: "follower".to_string(),
                            leader_session_id: leader_data.session_id,
                            last_seen: chrono::Utc::now().timestamp() as u64,
                        };
                        if let Err(e) = registry.register(&self.prefix, name, &reg).await {
                            tracing::error!(
                                "Coordinator: failed to register follower '{}': {}",
                                name,
                                e
                            );
                        }
                    }
                }

                let (follower_stop_tx, follower_stop_rx) = watch::channel(false);

                // Create shared atomics here so DbEntry, run_follower_tasks, and JoinResult
                // all share the same Arc refs.
                let follower_position = Arc::new(AtomicU64::new(0));
                let follower_caught_up = Arc::new(AtomicBool::new(false));

                let self_clone = self.clone();
                let db_name = name.to_string();
                let db_path_buf = db_path.to_path_buf();
                let role_for_entry = shared_role.clone();
                let addr_for_entry = leader_addr.clone();
                let pos_for_tasks = follower_position.clone();
                let cu_for_tasks = follower_caught_up.clone();

                let task_handle = tokio::spawn(async move {
                    self_clone
                        .run_follower_tasks(
                            db_name,
                            db_path_buf,
                            lease,
                            shared_role,
                            leader_addr,
                            follower_stop_tx,
                            follower_stop_rx,
                            cancel_rx,
                            pos_for_tasks,
                            cu_for_tasks,
                        )
                        .await;
                });

                self.databases.write().await.insert(
                    name.to_string(),
                    DbEntry {
                        role: role_for_entry,
                        leader_address: addr_for_entry,
                        cancel_tx,
                        task_handle,
                        db_path: db_path.to_path_buf(),
                        caught_up: follower_caught_up.clone(),
                        position: follower_position.clone(),
                    },
                );

                let _ = self.role_tx.send(RoleEvent::Joined {
                    db_name: name.to_string(),
                    role: Role::Follower,
                });
                tracing::info!("Coordinator: '{}' joined as Follower", name);
                Ok(JoinResult { role: Role::Follower, caught_up: follower_caught_up, position: follower_position })
            }
        }
    }

    /// Run follower tasks (pull loop + lease monitor).
    async fn run_follower_tasks(
        self: Arc<Self>,
        db_name: String,
        db_path: std::path::PathBuf,
        lease: DbLease,
        shared_role: Arc<AtomicRole>,
        leader_addr: Arc<RwLock<String>>,
        follower_stop_tx: watch::Sender<bool>,
        follower_stop_rx: watch::Receiver<bool>,
        cancel_rx: watch::Receiver<bool>,
        shared_position: Arc<AtomicU64>,
        shared_caught_up: Arc<AtomicBool>,
    ) {
        let lease_config = self
            .config
            .lease
            .as_ref()
            .expect("lease must be present");

        // Spawn follower pull loop
        let follower_handle = {
            let behavior = self.follower_behavior.clone();
            let replicator = self.replicator.clone();
            let prefix = self.prefix.clone();
            let db_name_clone = db_name.clone();
            let db_path_clone = db_path.clone();
            let pull_interval = self.config.follower_pull_interval;
            let metrics = self.metrics.clone();
            let position = shared_position.clone();
            let caught_up = shared_caught_up.clone();

            tokio::spawn(async move {
                let _ = behavior
                    .run_follower_loop(
                        replicator,
                        &prefix,
                        &db_name_clone,
                        &db_path_clone,
                        pull_interval,
                        position,
                        caught_up,
                        follower_stop_rx,
                        metrics,
                    )
                    .await;
            })
        };

        // Spawn lease monitor (generic — calls follower_behavior.catchup_on_promotion for DB-specific part)
        let monitor_handle = {
            let ctx = LeaseMonitorContext {
                lease,
                replicator: self.replicator.clone(),
                manifest_store: self.manifest_store.clone(),
                manifest_poll_interval: self.config.manifest_poll_interval,
                follower_behavior: self.follower_behavior.clone(),
                prefix: self.prefix.clone(),
                db_name,
                db_path,
                follower_stop_tx,
                role_tx: self.role_tx.clone(),
                config: lease_config.clone(),
                replicator_timeout: self.config.replicator_timeout,
                leader_address: leader_addr,
                role_ref: shared_role.clone(),
                self_address: lease_config.address.clone(),
                follower_position: shared_position.clone(),
                follower_caught_up: shared_caught_up.clone(),
                cancel_rx,
                node_registry: self.node_registry.clone(),
                metrics: self.metrics.clone(),
            };

            tokio::spawn(async move {
                let _ = run_lease_monitor(ctx).await;
            })
        };

        let _ = tokio::join!(follower_handle, monitor_handle);
    }

    /// Simulate process-level crash for tests: abort all background tasks
    /// for every joined database WITHOUT releasing leases or deregistering.
    ///
    /// A real process crash stops the leader's lease-renewal and follower-pull
    /// loops in the middle of their work, so the lease expires on TTL and other
    /// nodes detect it. `leave()` is the wrong tool for this kind of test
    /// because it gracefully releases the lease — promotion then looks
    /// deceptively fast. This method leaves the on-disk/S3 state exactly as
    /// it would be after `kill -9`: the lease file still points at us, the
    /// manifest is whatever was last flushed, and nothing local is cleaned up.
    ///
    /// After calling this, the coordinator's `databases` map is cleared so
    /// further calls on this coordinator are no-ops; the coordinator itself
    /// is effectively dead from a test's perspective. Use a fresh coordinator
    /// on another node (with the same lease store) to observe failover.
    ///
    /// **Not** part of the production API — this intentionally violates the
    /// graceful-shutdown invariants. Gated to `#[cfg(any(test, feature = "test-utils"))]`
    /// would be nice but is impractical while multiple downstream repos
    /// consume hadb without a shared feature flag. Treat as test-only.
    pub async fn abort_tasks_for_test(&self) {
        let mut dbs = self.databases.write().await;
        let entries: Vec<(String, DbEntry)> = dbs.drain().collect();
        drop(dbs);
        for (name, entry) in entries {
            entry.task_handle.abort();
            tracing::warn!("Coordinator: ABORTED tasks for '{}' (test-only crash simulation)", name);
        }
    }

    /// Leave the HA cluster for a database.
    pub async fn leave(&self, name: &str) -> Result<()> {
        let entry = self.databases.write().await.remove(name);
        let entry = match entry {
            Some(e) => e,
            None => return Ok(()),
        };

        let _ = entry.cancel_tx.send(true);
        let role = entry.role.load();

        match role {
            Role::Leader => {
                if tokio::time::timeout(
                    self.config.replicator_timeout,
                    self.replicator.remove(name),
                )
                .await
                .is_err()
                {
                    tracing::error!(
                        "Coordinator: replicator.remove('{}') timed out after {:?}, continuing leave",
                        name,
                        self.config.replicator_timeout
                    );
                }

                if let Some(ls) = &self.lease_store {
                    if let Some(lease_config) = &self.config.lease {
                        let lease_key = resolve_lease_key(&self.config.lease_key, &self.prefix, name);
                        let lease = DbLease::new(
                            ls.clone(),
                            &lease_key,
                            &lease_config.instance_id,
                            &lease_config.address,
                            lease_config.ttl_secs,
                        );
                        match lease.read().await {
                            Ok(Some((data, _)))
                                if data.instance_id == lease_config.instance_id =>
                            {
                                if let Err(e) = ls.delete(lease.lease_key()).await {
                                    tracing::error!(
                                        "Coordinator: failed to release lease for '{}': {}",
                                        name,
                                        e
                                    );
                                }
                            }
                            Ok(Some((data, _))) => {
                                tracing::info!(
                                    "Coordinator: lease for '{}' held by {} (not us), skipping release",
                                    name,
                                    data.instance_id
                                );
                            }
                            Ok(None) => {}
                            Err(e) => {
                                tracing::error!(
                                    "Coordinator: failed to read lease for '{}' during leave: {}",
                                    name,
                                    e
                                );
                            }
                        }
                    }
                }
                tracing::info!("Coordinator: '{}' left (was Leader)", name);
            }
            Role::Follower => {
                if let (Some(ref registry), Some(ref lease_config)) =
                    (&self.node_registry, &self.config.lease)
                {
                    if let Err(e) = registry
                        .deregister(&self.prefix, name, &lease_config.instance_id)
                        .await
                    {
                        tracing::error!(
                            "Coordinator: failed to deregister follower '{}': {}",
                            name,
                            e
                        );
                    }
                }
                tracing::info!("Coordinator: '{}' left (was Follower)", name);
            }
        }

        let _ = entry.task_handle.await;
        Ok(())
    }

    /// Get the current role of a database.
    pub async fn role(&self, name: &str) -> Option<Role> {
        self.databases
            .read()
            .await
            .get(name)
            .map(|e| e.role.load())
    }

    /// Get the current leader's address for a database.
    pub async fn leader_address(&self, name: &str) -> Option<String> {
        let addr = self
            .databases
            .read()
            .await
            .get(name)
            .map(|e| e.leader_address.clone());
        match addr {
            Some(a) => Some(a.read().await.clone()),
            None => None,
        }
    }

    /// Subscribe to role change events.
    pub fn role_events(&self) -> broadcast::Receiver<RoleEvent> {
        self.role_tx.subscribe()
    }

    /// Get shared metrics reference.
    pub fn metrics(&self) -> &Arc<HaMetrics> {
        &self.metrics
    }

    /// Get the manifest store, if configured.
    ///
    /// Database-specific layers (walrust, turbolite) use this to publish
    /// manifests after each replication sync. The Coordinator itself only
    /// polls manifests for followers; publishing is the database layer's job.
    pub fn manifest_store(&self) -> Option<&Arc<dyn ManifestStore>> {
        self.manifest_store.as_ref()
    }

    /// Discover registered replicas for a database.
    pub async fn discover_replicas(&self, name: &str) -> Result<Vec<NodeRegistration>> {
        let registry = match &self.node_registry {
            Some(r) => r,
            None => return Ok(Vec::new()),
        };
        let lease_config = match &self.config.lease {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };

        let all = registry.discover_all(&self.prefix, name).await?;

        let current_session_id = match &self.lease_store {
            Some(ls) => {
                let lease_key = resolve_lease_key(&self.config.lease_key, &self.prefix, name);
                let lease = DbLease::new(
                    ls.clone(),
                    &lease_key,
                    &lease_config.instance_id,
                    &lease_config.address,
                    lease_config.ttl_secs,
                );
                match lease.read().await {
                    Ok(Some((data, _))) => data.session_id,
                    _ => return Ok(Vec::new()),
                }
            }
            None => return Ok(Vec::new()),
        };

        Ok(all
            .into_iter()
            .filter(|r| r.is_valid(&current_session_id, lease_config.ttl_secs * 6))
            .collect())
    }

    /// Check if a specific database is in the coordinator.
    pub async fn contains(&self, name: &str) -> bool {
        self.databases.read().await.contains_key(name)
    }

    /// Number of databases currently managed.
    pub async fn database_count(&self) -> usize {
        self.databases.read().await.len()
    }

    /// List all managed database names.
    pub async fn database_names(&self) -> Vec<String> {
        self.databases.read().await.keys().cloned().collect()
    }

    /// Drain all databases — leave every database gracefully.
    ///
    /// For Leaders: final WAL sync + release lease.
    /// For Followers: stop tailing + deregister.
    ///
    /// Returns the number of databases successfully drained.
    pub async fn drain_all(&self) -> usize {
        let names = self.database_names().await;
        let total = names.len();
        let mut drained = 0;
        for name in &names {
            match self.leave(name).await {
                Ok(()) => {
                    drained += 1;
                }
                Err(e) => {
                    tracing::error!("drain_all: failed to leave '{}': {}", name, e);
                }
            }
        }
        tracing::info!("drain_all: {}/{} databases drained", drained, total);
        drained
    }

    /// Graceful leader handoff — release leadership without leaving the cluster.
    pub async fn handoff(&self, name: &str) -> Result<bool> {
        let entry = self.databases.read().await;
        let entry = match entry.get(name) {
            Some(e) => e,
            None => return Ok(false),
        };

        if entry.role.load() != Role::Leader {
            return Ok(false);
        }

        let _ = entry.cancel_tx.send(true);

        // Flush pending data before releasing leadership.
        if let Err(e) = tokio::time::timeout(
            self.config.replicator_timeout,
            self.replicator.sync(name),
        )
        .await
        {
            tracing::error!(
                "Coordinator: handoff replicator.sync('{}') timed out after {:?}: {}",
                name, self.config.replicator_timeout, e
            );
        }

        if tokio::time::timeout(
            self.config.replicator_timeout,
            self.replicator.remove(name),
        )
        .await
        .is_err()
        {
            tracing::error!(
                "Coordinator: handoff replicator.remove('{}') timed out after {:?}",
                name,
                self.config.replicator_timeout
            );
        }

        if let Some(ls) = &self.lease_store {
            if let Some(lease_config) = &self.config.lease {
                let lease_key = resolve_lease_key(&self.config.lease_key, &self.prefix, name);
                let lease = DbLease::new(
                    ls.clone(),
                    &lease_key,
                    &lease_config.instance_id,
                    &lease_config.address,
                    lease_config.ttl_secs,
                );
                match lease.read().await {
                    Ok(Some((data, _)))
                        if data.instance_id == lease_config.instance_id =>
                    {
                        if let Err(e) = ls.delete(lease.lease_key()).await {
                            tracing::error!(
                                "Coordinator: failed to release lease during handoff for '{}': {}",
                                name,
                                e
                            );
                        }
                    }
                    _ => {}
                }
            }
        }

        entry.role.store(Role::Follower);
        let _ = self.role_tx.send(RoleEvent::Demoted {
            db_name: name.to_string(),
        });

        tracing::info!("Coordinator: '{}' handed off leadership", name);
        Ok(true)
    }
}
