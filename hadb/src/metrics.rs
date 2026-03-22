use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Lightweight HA metrics using atomic counters.
/// Thread-safe, zero-allocation on reads, no external dependencies.
///
/// Access via `coordinator.metrics()` and snapshot via `metrics.snapshot()`.
pub struct HaMetrics {
    // Lease operations
    pub lease_claims_attempted: AtomicU64,
    pub lease_claims_succeeded: AtomicU64,
    pub lease_claims_failed: AtomicU64,
    pub lease_renewals_succeeded: AtomicU64,
    pub lease_renewals_cas_conflict: AtomicU64,
    pub lease_renewals_error: AtomicU64,

    // Promotion lifecycle
    pub promotions_attempted: AtomicU64,
    pub promotions_succeeded: AtomicU64,
    pub promotions_aborted_catchup: AtomicU64,
    pub promotions_aborted_replicator: AtomicU64,
    pub promotions_aborted_timeout: AtomicU64,

    // Demotion
    pub demotions_cas_conflict: AtomicU64,
    pub demotions_sustained_errors: AtomicU64,

    // Follower operations
    pub follower_pulls_succeeded: AtomicU64,
    pub follower_pulls_failed: AtomicU64,
    pub follower_pulls_no_new_data: AtomicU64,

    // Timing (stored as microseconds for atomic access)
    pub last_promotion_duration_us: AtomicU64,
    pub last_catchup_duration_us: AtomicU64,
    pub last_renewal_duration_us: AtomicU64,
}

impl HaMetrics {
    pub fn new() -> Self {
        Self {
            lease_claims_attempted: AtomicU64::new(0),
            lease_claims_succeeded: AtomicU64::new(0),
            lease_claims_failed: AtomicU64::new(0),
            lease_renewals_succeeded: AtomicU64::new(0),
            lease_renewals_cas_conflict: AtomicU64::new(0),
            lease_renewals_error: AtomicU64::new(0),
            promotions_attempted: AtomicU64::new(0),
            promotions_succeeded: AtomicU64::new(0),
            promotions_aborted_catchup: AtomicU64::new(0),
            promotions_aborted_replicator: AtomicU64::new(0),
            promotions_aborted_timeout: AtomicU64::new(0),
            demotions_cas_conflict: AtomicU64::new(0),
            demotions_sustained_errors: AtomicU64::new(0),
            follower_pulls_succeeded: AtomicU64::new(0),
            follower_pulls_failed: AtomicU64::new(0),
            follower_pulls_no_new_data: AtomicU64::new(0),
            last_promotion_duration_us: AtomicU64::new(0),
            last_catchup_duration_us: AtomicU64::new(0),
            last_renewal_duration_us: AtomicU64::new(0),
        }
    }

    /// Take a point-in-time snapshot of all metrics as a plain struct.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            lease_claims_attempted: self.lease_claims_attempted.load(Ordering::Relaxed),
            lease_claims_succeeded: self.lease_claims_succeeded.load(Ordering::Relaxed),
            lease_claims_failed: self.lease_claims_failed.load(Ordering::Relaxed),
            lease_renewals_succeeded: self.lease_renewals_succeeded.load(Ordering::Relaxed),
            lease_renewals_cas_conflict: self.lease_renewals_cas_conflict.load(Ordering::Relaxed),
            lease_renewals_error: self.lease_renewals_error.load(Ordering::Relaxed),
            promotions_attempted: self.promotions_attempted.load(Ordering::Relaxed),
            promotions_succeeded: self.promotions_succeeded.load(Ordering::Relaxed),
            promotions_aborted_catchup: self.promotions_aborted_catchup.load(Ordering::Relaxed),
            promotions_aborted_replicator: self.promotions_aborted_replicator.load(Ordering::Relaxed),
            promotions_aborted_timeout: self.promotions_aborted_timeout.load(Ordering::Relaxed),
            demotions_cas_conflict: self.demotions_cas_conflict.load(Ordering::Relaxed),
            demotions_sustained_errors: self.demotions_sustained_errors.load(Ordering::Relaxed),
            follower_pulls_succeeded: self.follower_pulls_succeeded.load(Ordering::Relaxed),
            follower_pulls_failed: self.follower_pulls_failed.load(Ordering::Relaxed),
            follower_pulls_no_new_data: self.follower_pulls_no_new_data.load(Ordering::Relaxed),
            last_promotion_duration_us: self.last_promotion_duration_us.load(Ordering::Relaxed),
            last_catchup_duration_us: self.last_catchup_duration_us.load(Ordering::Relaxed),
            last_renewal_duration_us: self.last_renewal_duration_us.load(Ordering::Relaxed),
        }
    }

    // Convenience increment helpers.

    pub fn inc(&self, counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_duration(&self, target: &AtomicU64, start: Instant) {
        target.store(start.elapsed().as_micros() as u64, Ordering::Relaxed);
    }
}

impl Default for HaMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of all metrics. Serializable.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MetricsSnapshot {
    pub lease_claims_attempted: u64,
    pub lease_claims_succeeded: u64,
    pub lease_claims_failed: u64,
    pub lease_renewals_succeeded: u64,
    pub lease_renewals_cas_conflict: u64,
    pub lease_renewals_error: u64,
    pub promotions_attempted: u64,
    pub promotions_succeeded: u64,
    pub promotions_aborted_catchup: u64,
    pub promotions_aborted_replicator: u64,
    pub promotions_aborted_timeout: u64,
    pub demotions_cas_conflict: u64,
    pub demotions_sustained_errors: u64,
    pub follower_pulls_succeeded: u64,
    pub follower_pulls_failed: u64,
    pub follower_pulls_no_new_data: u64,
    pub last_promotion_duration_us: u64,
    pub last_catchup_duration_us: u64,
    pub last_renewal_duration_us: u64,
}

impl MetricsSnapshot {
    /// Format as Prometheus exposition text.
    pub fn to_prometheus(&self) -> String {
        let mut out = String::with_capacity(2048);

        // Counters
        Self::counter(&mut out, "hadb_lease_claims_attempted_total", "Total lease claim attempts", self.lease_claims_attempted);
        Self::counter(&mut out, "hadb_lease_claims_succeeded_total", "Successful lease claims", self.lease_claims_succeeded);
        Self::counter(&mut out, "hadb_lease_claims_failed_total", "Failed lease claims", self.lease_claims_failed);
        Self::counter(&mut out, "hadb_lease_renewals_succeeded_total", "Successful lease renewals", self.lease_renewals_succeeded);
        Self::counter(&mut out, "hadb_lease_renewals_cas_conflict_total", "Lease renewals lost to CAS conflict", self.lease_renewals_cas_conflict);
        Self::counter(&mut out, "hadb_lease_renewals_error_total", "Lease renewal errors", self.lease_renewals_error);

        Self::counter(&mut out, "hadb_promotions_attempted_total", "Total promotion attempts", self.promotions_attempted);
        Self::counter(&mut out, "hadb_promotions_succeeded_total", "Successful promotions", self.promotions_succeeded);
        Self::counter(&mut out, "hadb_promotions_aborted_catchup_total", "Promotions aborted due to catchup failure", self.promotions_aborted_catchup);
        Self::counter(&mut out, "hadb_promotions_aborted_replicator_total", "Promotions aborted due to replicator failure", self.promotions_aborted_replicator);
        Self::counter(&mut out, "hadb_promotions_aborted_timeout_total", "Promotions aborted due to timeout", self.promotions_aborted_timeout);

        Self::counter(&mut out, "hadb_demotions_cas_conflict_total", "Demotions from CAS conflict", self.demotions_cas_conflict);
        Self::counter(&mut out, "hadb_demotions_sustained_errors_total", "Demotions from sustained renewal errors", self.demotions_sustained_errors);

        Self::counter(&mut out, "hadb_follower_pulls_succeeded_total", "Successful follower pulls", self.follower_pulls_succeeded);
        Self::counter(&mut out, "hadb_follower_pulls_failed_total", "Failed follower pulls", self.follower_pulls_failed);
        Self::counter(&mut out, "hadb_follower_pulls_no_new_data_total", "Follower pulls with no new data", self.follower_pulls_no_new_data);

        // Gauges (last observed timing)
        Self::gauge(&mut out, "hadb_last_promotion_duration_seconds", "Duration of last promotion", self.last_promotion_duration_us as f64 / 1_000_000.0);
        Self::gauge(&mut out, "hadb_last_catchup_duration_seconds", "Duration of last catchup", self.last_catchup_duration_us as f64 / 1_000_000.0);
        Self::gauge(&mut out, "hadb_last_renewal_duration_seconds", "Duration of last lease renewal", self.last_renewal_duration_us as f64 / 1_000_000.0);

        out
    }

    fn counter(out: &mut String, name: &str, help: &str, value: u64) {
        out.push_str(&format!("# HELP {} {}\n# TYPE {} counter\n{} {}\n", name, help, name, name, value));
    }

    fn gauge(out: &mut String, name: &str, help: &str, value: f64) {
        out.push_str(&format!("# HELP {} {}\n# TYPE {} gauge\n{} {:.6}\n", name, help, name, name, value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_new() {
        let metrics = HaMetrics::new();
        assert_eq!(metrics.lease_claims_attempted.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.promotions_attempted.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_metrics_inc() {
        let metrics = HaMetrics::new();
        metrics.inc(&metrics.lease_claims_attempted);
        metrics.inc(&metrics.lease_claims_attempted);
        assert_eq!(metrics.lease_claims_attempted.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_metrics_record_duration() {
        let metrics = HaMetrics::new();
        let start = Instant::now();
        std::thread::sleep(std::time::Duration::from_micros(100));
        metrics.record_duration(&metrics.last_promotion_duration_us, start);

        let duration_us = metrics.last_promotion_duration_us.load(Ordering::Relaxed);
        assert!(duration_us >= 100); // At least 100us
    }

    #[test]
    fn test_metrics_snapshot() {
        let metrics = HaMetrics::new();
        metrics.inc(&metrics.lease_claims_attempted);
        metrics.inc(&metrics.promotions_succeeded);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.lease_claims_attempted, 1);
        assert_eq!(snapshot.promotions_succeeded, 1);
        assert_eq!(snapshot.lease_claims_succeeded, 0);
    }

    #[test]
    fn test_prometheus_format() {
        let snapshot = MetricsSnapshot {
            lease_claims_attempted: 10,
            lease_claims_succeeded: 8,
            lease_claims_failed: 2,
            lease_renewals_succeeded: 100,
            lease_renewals_cas_conflict: 1,
            lease_renewals_error: 0,
            promotions_attempted: 5,
            promotions_succeeded: 4,
            promotions_aborted_catchup: 0,
            promotions_aborted_replicator: 1,
            promotions_aborted_timeout: 0,
            demotions_cas_conflict: 1,
            demotions_sustained_errors: 0,
            follower_pulls_succeeded: 50,
            follower_pulls_failed: 2,
            follower_pulls_no_new_data: 10,
            last_promotion_duration_us: 500_000,
            last_catchup_duration_us: 100_000,
            last_renewal_duration_us: 5_000,
        };

        let prom = snapshot.to_prometheus();

        // Verify key metrics are present
        assert!(prom.contains("hadb_lease_claims_attempted_total 10"));
        assert!(prom.contains("hadb_promotions_succeeded_total 4"));
        assert!(prom.contains("hadb_last_promotion_duration_seconds 0.500000"));

        // Verify TYPE directives
        assert!(prom.contains("# TYPE hadb_lease_claims_attempted_total counter"));
        assert!(prom.contains("# TYPE hadb_last_promotion_duration_seconds gauge"));
    }

    #[test]
    fn test_metrics_default() {
        let metrics = HaMetrics::default();
        assert_eq!(metrics.lease_claims_attempted.load(Ordering::Relaxed), 0);
    }
}
