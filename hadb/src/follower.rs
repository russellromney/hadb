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

                let is_expired = match &lease_data {
                    Ok(None) => true,
                    Ok(Some((data, _etag))) => {
                        if !data.is_expired() {
                            *ctx.leader_address.write().await = data.address.clone();
                            // Heartbeat node registration with current leader session.
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
                        data.is_expired()
                    }
                    Err(e) => {
                        tracing::error!(
                            "Lease monitor '{}': read failed: {}", ctx.db_name, e
                        );
                        consecutive_expired = 0;
                        false
                    }
                };

                if is_expired {
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

                            // 3. Resume leader sync, continuing from existing S3 seq.
                            // add_continuing() loads state.json to get the right seq
                            // so followers can discover new changesets.
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
/// Returns true if demoted (CAS conflict or sustained errors), false if cancelled.
///
/// **Self-demotion on sustained errors:** If `max_consecutive_errors` consecutive
/// renewals fail with transient errors, the leader self-demotes. During a storage outage,
/// the lease is expiring while we retry — another node may claim it, creating
/// split-brain. Self-demotion prevents this.
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
            }
            _ = cancel_rx.changed() => {
                tracing::info!("Leader '{}': renewal cancelled", db_name);
                return false;
            }
        }
    }
}
