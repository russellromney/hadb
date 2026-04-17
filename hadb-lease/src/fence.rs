//! `FenceSource`: the trait storage adapters use to fetch the current
//! lease revision before issuing a fenced write.
//!
//! # What is a fence?
//!
//! A "fence" is the HA invariant that prevents a former leader from
//! writing stale data after it loses its lease. Every fenced write carries
//! the revision the writer thinks it holds; the server rejects the write
//! if that revision is older than the current lease.
//!
//! The revision flow:
//!
//! 1. The lease manager claims or renews a lease. The lease store returns
//!    a monotonic revision (NATS KV rev, Cinch lease rev, etc.).
//! 2. The lease manager publishes that revision into a `FenceSource`
//!    implementation.
//! 3. Every fenced-protected write by the lease holder asks the source
//!    for the current revision via `require()` and attaches it to the
//!    request. No active lease, no write.
//! 4. The server compares the client's revision against its own view of
//!    the current lease and rejects stale writers.
//!
//! # Why a trait?
//!
//! The trait is the abstraction. A concrete in-memory implementation
//! (`arc-swap`-backed, in `hadb-lease-cinch`) is one possible source; a
//! lease backend that produces fence revisions intrinsically (NATS KV's
//! revision on a PUT) could implement `FenceSource` directly without a
//! separate atomic cell. `hadb-storage-cinch` only needs the trait.
//!
//! Keeping the trait in `hadb-lease` (not `hadb-storage`) records the
//! architectural intent: fencing is a lease-layer concern flowing down
//! into storage, not something the abstract storage trait knows about.

/// Returned by `FenceSource::require` when no lease is currently held.
/// Callers must surface this as a hard error rather than proceeding with
/// an unfenced write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoActiveLease;

impl std::fmt::Display for NoActiveLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "no active lease; refusing write")
    }
}

impl std::error::Error for NoActiveLease {}

/// A source of the current lease revision for fenced writes.
///
/// Implementations are typically held behind `Arc<dyn FenceSource>` so
/// the same source can be shared across the lease manager (which updates
/// it) and every storage adapter that performs fenced writes.
pub trait FenceSource: Send + Sync {
    /// Read the current revision, or `None` if no lease is held.
    fn current(&self) -> Option<u64>;

    /// Require a revision. Returns `Err(NoActiveLease)` when no lease is
    /// held; callers that hit this must fail the write rather than
    /// issuing an unfenced request.
    fn require(&self) -> Result<u64, NoActiveLease> {
        self.current().ok_or(NoActiveLease)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Static(Option<u64>);
    impl FenceSource for Static {
        fn current(&self) -> Option<u64> {
            self.0
        }
    }

    #[test]
    fn require_returns_current_when_set() {
        let s = Static(Some(42));
        assert_eq!(s.current(), Some(42));
        assert_eq!(s.require(), Ok(42));
    }

    #[test]
    fn require_errors_when_unset() {
        let s = Static(None);
        assert_eq!(s.current(), None);
        assert_eq!(s.require(), Err(NoActiveLease));
    }

    #[test]
    fn no_active_lease_is_error_type() {
        let e: &dyn std::error::Error = &NoActiveLease;
        assert_eq!(e.to_string(), "no active lease; refusing write");
    }
}
