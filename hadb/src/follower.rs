//! Follower behavior abstraction for database-specific replication logic.
//!
//! The `FollowerBehavior` trait allows each database implementation to define how
//! followers pull incremental updates and how they catch up during promotion.
//!
//! The lease monitor logic is fully generic — `run_lease_monitor` handles polling,
//! consecutive expired reads, claiming, events, and renewal. The only DB-specific
//! part is `catchup_on_promotion` (e.g. walrust pull_incremental for SQLite).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::{broadcast, watch, RwLock};

use crate::coordinator::AtomicRole;
use crate::lease::DbLease;
use crate::metrics::HaMetrics;
use crate::node_registry::NodeRegistry;
use crate::traits::Replicator;
use crate::types::{LeaseConfig, Role, RoleEvent};
use turbodb::ManifestStore;

/// Follower behavior abstraction for database-specific replication.
///
/// Each database (SQLite, Kuzu, etc.) implements this trait to define:
/// - How followers poll for and apply incremental updates (pull loop)
/// - How followers catch up when promoted to leader (warm promotion)
///
/// The lease monitor (polling for leader death, claiming, events) is fully
/// generic and handled by [`run_lease_monitor`]. Only `catchup_on_promotion`
/// is database-specific.
#[async_trait]
pub trait FollowerBehavior: Send + Sync {
    /// Run the follower pull loop: poll storage for new data and apply it.
    ///
    /// This is database-specific:
    /// - SQLite: tracks TXIDs, calls `walrust::sync::pull_incremental`
    /// - Kuzu: tracks checkpoints, calls `graphstream::pull_from`
    ///
    /// `position` must be updated atomically as new data is applied — the lease
    /// monitor reads it to know where to resume during warm promotion.
    async fn run_follower_loop(
        &self,
        replicator: Arc<dyn Replicator>,
        prefix: &str,
        db_name: &str,
        db_path: &PathBuf,
        poll_interval: Duration,
        position: Arc<AtomicU64>,
        caught_up: Arc<AtomicBool>,
        cancel_rx: watch::Receiver<bool>,
        metrics: Arc<HaMetrics>,
    ) -> Result<()>;

    /// Database-specific warm catch-up during promotion.
    ///
    /// Called after a follower claims the lease, before starting leader sync.
    /// Should apply any remaining incremental updates from `position` forward.
    ///
    /// - SQLite: calls `walrust::sync::pull_incremental` from the given TXID
    /// - Kuzu: calls `graphstream::pull_from` from the given checkpoint
    ///
    /// The caller wraps this in a timeout — implementations should not add their own.
    /// Return Ok(()) if no catch-up is needed (e.g. position == 0).
    async fn catchup_on_promotion(
        &self,
        prefix: &str,
        db_name: &str,
        db_path: &PathBuf,
        position: u64,
    ) -> Result<()>;
}

// ============================================================================
// Generic lease monitor
// ============================================================================

/// Context for [`run_lease_monitor`]. Bundles all parameters so the function
/// signature stays clean despite needing many collaborators.
pub struct LeaseMonitorContext {
    pub lease: DbLease,
    pub replicator: Arc<dyn Replicator>,
    pub manifest_store: Option<Arc<dyn ManifestStore>>,
    pub manifest_poll_interval: Duration,
    pub follower_behavior: Arc<dyn FollowerBehavior>,
    pub prefix: String,
    pub db_name: String,
    pub db_path: PathBuf,
    pub follower_stop_tx: watch::Sender<bool>,
    pub role_tx: broadcast::Sender<RoleEvent>,
    pub config: LeaseConfig,
    pub replicator_timeout: Duration,
    pub leader_address: Arc<RwLock<String>>,
    pub(crate) role_ref: Arc<AtomicRole>,
    pub self_address: String,
    pub follower_position: Arc<AtomicU64>,
    pub follower_caught_up: Arc<AtomicBool>,
    pub cancel_rx: watch::Receiver<bool>,
    pub node_registry: Option<Arc<dyn NodeRegistry>>,
    pub metrics: Arc<HaMetrics>,
}

/// Run the follower lease monitor: watch for leader death and auto-promote.
///
/// This is fully generic (not database-specific) — all databases monitor leases
/// the same way. The only DB-specific part is warm catch-up on promotion, which
/// is delegated to [`FollowerBehavior::catchup_on_promotion`].
///
/// Returns true if promoted to leader, false if cancelled.
pub async fn run_lease_monitor(mut ctx: LeaseMonitorContext) -> Result<bool> {
    let mut interval = tokio::time::interval(ctx.config.follower_poll_interval);
    let mut consecutive_expired: u32 = 0;
    let mut last_manifest_version: Option<u64> = None;
    let mut manifest_interval = tokio::time::interval(ctx.manifest_poll_interval);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let lease_data = ctx.lease.read().await;

                // Handle sleeping: leader signaled scale-to-zero.
                if let Ok(Some((ref data, _))) = lease_data {
                    if data.sleeping {
                        tracing::info!(
                            "Lease monitor '{}': leader signaled sleep, emitting Sleeping",
                            ctx.db_name
                        );
                        let _ = ctx.role_tx.send(RoleEvent::Sleeping {
                            db_name: ctx.db_name.clone(),
                        });
                        return Ok(false);
                    }
                }

                // Takeover decision uses the MARGINED claimer check
                // (`is_claimable_by_other`): a follower may only claim once
                // the lease is expired PAST the skew margin, which is
                // strictly after the holder's own relinquish deadline. This
                // keeps the holder ahead of any claimer so they never
                // contend at the TTL boundary (Finding 1).
                let is_claimable = match &lease_data {
                    Ok(None) => true,
                    Ok(Some((data, _etag))) => {
                        if !data.is_claimable_by_other() {
                            // Lease not yet takeable — keep tracking the
                            // (possibly recently-expired) leader's address
                            // and heartbeat node registration.
                            *ctx.leader_address.write().await = data.address.clone();
                            if let Some(ref registry) = ctx.node_registry {
                                let reg = crate::NodeRegistration {
                                    instance_id: ctx.lease.instance_id().to_string(),
                                    address: ctx.self_address.clone(),
                                    role: "follower".to_string(),
                                    leader_session_id: data.session_id.clone(),
                                    last_seen: chrono::Utc::now().timestamp() as u64,
                                };
                                if let Err(e) = registry.register(&ctx.prefix, &ctx.db_name, &reg).await {
                                    tracing::error!(
                                        "Lease monitor '{}': heartbeat registration failed: {}",
                                        ctx.db_name, e
                                    );
                                }
                            }
                        }
                        data.is_claimable_by_other()
                    }
                    Err(e) => {
                        tracing::error!(
                            "Lease monitor '{}': read failed: {}", ctx.db_name, e
                        );
                        consecutive_expired = 0;
                        false
                    }
                };

                if is_claimable {
                    consecutive_expired += 1;
                    if consecutive_expired < ctx.config.required_expired_reads {
                        tracing::debug!(
                            "Lease monitor '{}': expired read {}/{}, waiting",
                            ctx.db_name, consecutive_expired, ctx.config.required_expired_reads
                        );
                        continue;
                    }

                    ctx.metrics.inc(&ctx.metrics.lease_claims_attempted);
                    match ctx.lease.try_claim().await {
                        Ok(crate::Role::Leader) => {
                            tracing::info!(
                                "Lease monitor '{}': claimed lease — promoting to leader",
                                ctx.db_name
                            );
                            ctx.metrics.inc(&ctx.metrics.lease_claims_succeeded);
                            ctx.metrics.inc(&ctx.metrics.promotions_attempted);
                            let promotion_start = std::time::Instant::now();
                            consecutive_expired = 0;

                            // 1. Stop the follower pull loop.
                            let _ = ctx.follower_stop_tx.send(true);

                            // 2. Warm promotion: DB-specific catch-up.
                            let txid = ctx.follower_position.load(Ordering::SeqCst);
                            if txid > 0 {
                                tracing::info!(
                                    "Lease monitor '{}': warm promotion, catching up from position {}",
                                    ctx.db_name, txid
                                );
                                let catchup_start = std::time::Instant::now();
                                let catchup_result = tokio::time::timeout(
                                    ctx.replicator_timeout,
                                    ctx.follower_behavior.catchup_on_promotion(
                                        &ctx.prefix,
                                        &ctx.db_name,
                                        &ctx.db_path,
                                        txid,
                                    ),
                                ).await;
                                match catchup_result {
                                    Ok(Ok(())) => {
                                        ctx.metrics.record_duration(&ctx.metrics.last_catchup_duration_us, catchup_start);
                                        tracing::info!(
                                            "Lease monitor '{}': catch-up complete from position {}",
                                            ctx.db_name, txid
                                        );
                                    }
                                    Ok(Err(e)) => {
                                        tracing::error!(
                                            "Lease monitor '{}': warm catch-up failed: {} — aborting promotion",
                                            ctx.db_name, e
                                        );
                                        ctx.metrics.inc(&ctx.metrics.promotions_aborted_catchup);
                                        let _ = ctx.lease.release().await;
                                        continue;
                                    }
                                    Err(_) => {
                                        tracing::error!(
                                            "Lease monitor '{}': warm catch-up timed out — aborting promotion",
                                            ctx.db_name
                                        );
                                        ctx.metrics.inc(&ctx.metrics.promotions_aborted_timeout);
                                        let _ = ctx.lease.release().await;
                                        continue;
                                    }
                                }
                            }

                            // 3. Resume leader sync from the replicator's authoritative
                            // durable cursor so followers can discover new changesets.
                            let add_result = tokio::time::timeout(
                                ctx.replicator_timeout,
                                ctx.replicator.add_continuing(&ctx.db_name, &ctx.db_path),
                            ).await;
                            match add_result {
                                Ok(Ok(())) => {}
                                Ok(Err(e)) => {
                                    tracing::error!(
                                        "Lease monitor '{}': failed to start leader sync: {} — aborting promotion",
                                        ctx.db_name, e
                                    );
                                    ctx.metrics.inc(&ctx.metrics.promotions_aborted_replicator);
                                    let _ = ctx.lease.release().await;
                                    continue;
                                }
                                Err(_) => {
                                    tracing::error!(
                                        "Lease monitor '{}': replicator.add() timed out — aborting promotion",
                                        ctx.db_name
                                    );
                                    ctx.metrics.inc(&ctx.metrics.promotions_aborted_timeout);
                                    let _ = ctx.lease.release().await;
                                    continue;
                                }
                            }

                            // 4. Deregister from node registry (now leader, not a follower).
                            if let Some(ref registry) = ctx.node_registry {
                                if let Err(e) = registry
                                    .deregister(&ctx.prefix, &ctx.db_name, ctx.lease.instance_id())
                                    .await
                                {
                                    tracing::error!(
                                        "Lease monitor '{}': deregister on promotion failed: {}",
                                        ctx.db_name, e
                                    );
                                }
                            }

                            // 5. Update shared role + address + readiness IMMEDIATELY.
                            ctx.role_ref.store(Role::Leader);
                            ctx.follower_caught_up.store(true, Ordering::SeqCst);
                            ctx.metrics.follower_caught_up.store(1, Ordering::Relaxed);
                            *ctx.leader_address.write().await = ctx.self_address.clone();

                            // 6. Emit Promoted event.
                            ctx.metrics.inc(&ctx.metrics.promotions_succeeded);
                            ctx.metrics.record_duration(&ctx.metrics.last_promotion_duration_us, promotion_start);
                            let _ = ctx.role_tx.send(RoleEvent::Promoted {
                                db_name: ctx.db_name.clone(),
                            });

                            // 7. Switch to lease renewal mode.
                            let demoted = run_leader_renewal(
                                &mut ctx.lease,
                                &ctx.replicator,
                                &ctx.db_name,
                                &ctx.role_tx,
                                ctx.config.renew_interval,
                                &mut ctx.cancel_rx,
                                ctx.config.max_consecutive_renewal_errors,
                                ctx.replicator_timeout,
                                ctx.metrics.clone(),
                            ).await;

                            if demoted {
                                ctx.role_ref.store(Role::Follower);
                                ctx.follower_caught_up.store(false, Ordering::SeqCst);
                                ctx.metrics.follower_caught_up.store(0, Ordering::Relaxed);
                            }

                            return Ok(true); // promoted
                        }
                        Ok(crate::Role::Follower) => {
                            ctx.metrics.inc(&ctx.metrics.lease_claims_failed);
                            consecutive_expired = 0;
                        }
                        Ok(crate::Role::Client | crate::Role::LatentWriter) => {
                            unreachable!("lease claiming only yields Leader or Follower")
                        }
                        Err(e) => {
                            ctx.metrics.inc(&ctx.metrics.lease_claims_failed);
                            tracing::error!(
                                "Lease monitor '{}': claim failed: {}", ctx.db_name, e
                            );
                        }
                    }
                } else {
                    consecutive_expired = 0;
                }
            }
            _ = manifest_interval.tick(), if ctx.manifest_store.is_some() => {
                let store = ctx.manifest_store.as_ref().expect("checked by select guard");
                let manifest_key = format!("{}{}/manifest", ctx.prefix, ctx.db_name);
                match store.meta(&manifest_key).await {
                    Ok(Some(meta)) => {
                        let changed = match last_manifest_version {
                            Some(prev) => meta.version != prev,
                            None => true, // first poll, record version but don't emit event
                        };
                        if changed && last_manifest_version.is_some() {
                            tracing::info!(
                                "Lease monitor '{}': manifest version changed {} -> {}",
                                ctx.db_name,
                                last_manifest_version.unwrap_or(0),
                                meta.version
                            );
                            let _ = ctx.role_tx.send(RoleEvent::ManifestChanged {
                                db_name: ctx.db_name.clone(),
                                version: meta.version,
                            });
                        }
                        last_manifest_version = Some(meta.version);
                    }
                    Ok(None) => {
                        // No manifest yet, nothing to do.
                    }
                    Err(e) => {
                        tracing::error!(
                            "Lease monitor '{}': manifest meta poll failed: {}",
                            ctx.db_name, e
                        );
                    }
                }
            }
            _ = ctx.cancel_rx.changed() => {
                tracing::info!("Lease monitor '{}': cancelled", ctx.db_name);
                return Ok(false);
            }
        }
    }
}

/// Run the leader lease renewal loop.
///
/// This is fully generic (not database-specific) — all databases renew leases the same way.
///
/// Returns true if demoted (CAS conflict, sustained errors, or proactive
/// self-fence), false if cancelled.
///
/// **Proactive self-fence (the safety backstop):** on every tick — including
/// ticks where the renewal call itself errored — the leader checks whether
/// the wall clock has passed its lease's relinquish deadline
/// (`claimed_at + ttl - skew_margin`, via `DbLease::should_self_fence`). If
/// so it fences itself immediately, INDEPENDENT of the error count. This
/// guarantees a partitioned leader (whose renewals all error rather than
/// CAS-conflict) relinquishes BEFORE TTL — before any follower's margined
/// claim window opens — closing the ~N·interval split-brain window the
/// error-count path alone left open.
///
/// **Self-demotion on sustained errors:** retained as a fast-path. If
/// `max_consecutive_errors` consecutive renewals fail with transient errors,
/// the leader self-demotes even if the proactive deadline has not yet passed.
pub async fn run_leader_renewal(
    lease: &mut DbLease,
    replicator: &Arc<dyn Replicator>,
    db_name: &str,
    role_tx: &broadcast::Sender<RoleEvent>,
    renew_interval: Duration,
    cancel_rx: &mut watch::Receiver<bool>,
    max_consecutive_errors: u32,
    replicator_timeout: Duration,
    metrics: Arc<HaMetrics>,
) -> bool {
    let mut interval = tokio::time::interval(renew_interval);
    let mut consecutive_errors: u32 = 0;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let renewal_start = std::time::Instant::now();
                match lease.renew().await {
                    Ok(true) => {
                        metrics.inc(&metrics.lease_renewals_succeeded);
                        metrics.record_duration(&metrics.last_renewal_duration_us, renewal_start);
                        consecutive_errors = 0;
                    }
                    Ok(false) => {
                        // CAS conflict — another node holds the lease.
                        metrics.inc(&metrics.lease_renewals_cas_conflict);
                        metrics.inc(&metrics.demotions_cas_conflict);
                        tracing::error!(
                            "Leader '{}': lease renewal CAS conflict — demoting",
                            db_name
                        );
                        if tokio::time::timeout(replicator_timeout, replicator.remove(db_name)).await.is_err() {
                            tracing::error!(
                                "Leader '{}': replicator.remove() timed out during demotion", db_name
                            );
                        }
                        let _ = role_tx.send(RoleEvent::Demoted {
                            db_name: db_name.to_string(),
                        });
                        return true;
                    }
                    Err(e) => {
                        metrics.inc(&metrics.lease_renewals_error);
                        consecutive_errors += 1;
                        if consecutive_errors >= max_consecutive_errors {
                            metrics.inc(&metrics.demotions_sustained_errors);
                            tracing::error!(
                                "Leader '{}': {} consecutive renewal errors — \
                                 self-demoting to prevent split-brain",
                                db_name, consecutive_errors
                            );
                            if tokio::time::timeout(replicator_timeout, replicator.remove(db_name)).await.is_err() {
                                tracing::error!(
                                    "Leader '{}': replicator.remove() timed out during self-demotion", db_name
                                );
                            }
                            let _ = role_tx.send(RoleEvent::Demoted {
                                db_name: db_name.to_string(),
                            });
                            return true;
                        }
                        tracing::error!(
                            "Leader '{}': lease renewal error ({}/{}), will retry: {}",
                            db_name, consecutive_errors, max_consecutive_errors, e
                        );
                    }
                }

                // Proactive self-fence backstop. Runs on every tick that did
                // not already return (i.e. after a successful renew, which
                // pushes the deadline forward and so won't fire, and after a
                // renewal error, where the deadline is stale and WILL fire
                // once passed). Independent of consecutive_errors: a leader
                // whose renewals only ever error still relinquishes before
                // TTL, before any follower can claim.
                if lease.should_self_fence() {
                    metrics.inc(&metrics.demotions_sustained_errors);
                    tracing::error!(
                        "Leader '{}': lease relinquish deadline passed (proactive self-fence) — \
                         demoting before TTL to prevent split-brain",
                        db_name
                    );
                    if tokio::time::timeout(replicator_timeout, replicator.remove(db_name)).await.is_err() {
                        tracing::error!(
                            "Leader '{}': replicator.remove() timed out during proactive self-fence", db_name
                        );
                    }
                    let _ = role_tx.send(RoleEvent::Demoted {
                        db_name: db_name.to_string(),
                    });
                    return true;
                }
            }
            _ = cancel_rx.changed() => {
                tracing::info!("Leader '{}': renewal cancelled", db_name);
                return false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Replicator;
    use crate::types::RoleEvent;
    use crate::{CasResult, LeaseStore};
    use hadb_lease_mem::InMemoryLeaseStore;
    use std::path::Path;
    use std::sync::Arc;

    /// Replicator stub that records whether `remove` was called.
    struct NoopReplicator;

    #[async_trait]
    impl Replicator for NoopReplicator {
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

    /// LeaseStore that allows the initial claim (write_if_not_exists) but
    /// then makes every renewal (write_if_match) fail with a transient
    /// error — modelling a partitioned leader that can no longer reach the
    /// store. Reads are delegated so the lease is still visible.
    struct RenewAlwaysErrorsStore {
        inner: InMemoryLeaseStore,
    }

    impl RenewAlwaysErrorsStore {
        fn new() -> Self {
            Self {
                inner: InMemoryLeaseStore::new(),
            }
        }
    }

    #[async_trait]
    impl LeaseStore for RenewAlwaysErrorsStore {
        async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
            self.inner.read(key).await
        }
        async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult> {
            self.inner.write_if_not_exists(key, data).await
        }
        async fn write_if_match(
            &self,
            _key: &str,
            _data: Vec<u8>,
            _etag: &str,
        ) -> Result<CasResult> {
            anyhow::bail!("simulated storage partition: renewal cannot reach the lease store")
        }
        async fn delete(&self, key: &str) -> Result<()> {
            self.inner.delete(key).await
        }
    }

    /// Finding 2: a partitioned leader whose renewals only ever ERROR (never
    /// CAS-conflict) must fence itself via the proactive deadline BEFORE TTL
    /// — not after `max_consecutive_renewal_errors` ticks. Here
    /// `max_consecutive_renewal_errors` is set absurdly high (10_000) so the
    /// error-count path can never fire; the only thing that can demote is
    /// the proactive self-fence. We assert it demotes well within the TTL
    /// window.
    #[tokio::test]
    async fn proactive_self_fence_fires_before_ttl() {
        let store = Arc::new(RenewAlwaysErrorsStore::new());
        // ttl=1s, margin defaults to 2s, so the relinquish deadline is at
        // claim time (claimed_at + 1 - 2, saturating) — already past on the
        // first renewal tick. The leader must demote on that first error
        // tick via the proactive backstop, not via the error count.
        let mut lease = DbLease::new(store, "db/_lease.json", "inst-1", "addr-1", 1);
        assert_eq!(
            lease.try_claim().await.unwrap(),
            Role::Leader,
            "initial claim should succeed"
        );

        let replicator: Arc<dyn Replicator> = Arc::new(NoopReplicator);
        let (role_tx, mut role_rx) = tokio::sync::broadcast::channel(8);
        let (_cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);
        let metrics = Arc::new(crate::metrics::HaMetrics::new());

        let started = std::time::Instant::now();
        let demoted = tokio::time::timeout(
            Duration::from_secs(5),
            run_leader_renewal(
                &mut lease,
                &replicator,
                "db",
                &role_tx,
                Duration::from_millis(50), // fast tick
                &mut cancel_rx,
                10_000, // error-count path effectively disabled
                Duration::from_secs(1),
                metrics.clone(),
            ),
        )
        .await
        .expect("renewal loop must terminate, not hang");

        assert!(demoted, "leader must self-demote");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "proactive fence must fire well before the error-count path could ({:?})",
            started.elapsed()
        );

        // A Demoted event must have been emitted.
        let evt = role_rx.try_recv().expect("a role event should be queued");
        assert!(matches!(evt, RoleEvent::Demoted { .. }), "got {evt:?}");

        // The proactive path increments the sustained-errors demotion metric.
        assert!(
            metrics
                .demotions_sustained_errors
                .load(std::sync::atomic::Ordering::Relaxed)
                >= 1,
            "proactive self-fence should record a demotion metric"
        );
    }
}
