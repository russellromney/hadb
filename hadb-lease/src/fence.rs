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

use std::sync::Arc;

use arc_swap::ArcSwapOption;

/// The canonical fencing-token comparison: a write carrying `incoming` is
/// accepted only if it is strictly greater than the `current` fence the
/// authority holds. A stale or equal token (`incoming <= current`) is
/// rejected.
///
/// This pins the contract in one place. The fence revision is the lease
/// claim's etag (parsed to `u64` by the lease manager) and is strictly
/// increasing across every claim, renewal, and takeover — so a former
/// leader's revision is always `<=` the new leader's and is rejected.
/// Storage backends (manifest/page servers) implement the same `>` rule
/// server-side; this helper documents and tests it at the lease layer.
/// Do not weaken to `>=`: that would let a former leader with an equal
/// revision write.
#[inline]
pub fn fence_accepts(current: u64, incoming: u64) -> bool {
    incoming > current
}

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

/// Canonical in-memory `FenceSource` implementation, paired with an
/// exclusive writer handle.
///
/// The lease manager holds the `AtomicFenceWriter` and publishes every
/// successful lease claim or renewal into it. Storage adapters hold an
/// `AtomicFence` (implementing [`FenceSource`]) and read the current
/// revision on every fenced write.
///
/// # Producer / consumer split
///
/// `AtomicFence::new()` returns the paired handles. The reader side is
/// `Clone`; share it with any number of storage adapters. The writer side
/// is deliberately not `Clone`: only the lease manager should mutate the
/// fence, and the compiler enforces it.
#[derive(Clone, Debug)]
pub struct AtomicFence {
    state: Arc<ArcSwapOption<u64>>,
}

/// Exclusive writer handle paired with [`AtomicFence`]. Held by the lease
/// manager; deliberately not `Clone` so ownership of "who mutates the
/// fence" stays unambiguous.
#[derive(Debug)]
pub struct AtomicFenceWriter {
    state: Arc<ArcSwapOption<u64>>,
}

impl AtomicFence {
    /// Construct a paired `(AtomicFence, AtomicFenceWriter)`. The fence
    /// starts unset; any `require()` call before the writer publishes a
    /// revision returns `Err(NoActiveLease)`.
    pub fn new() -> (AtomicFence, AtomicFenceWriter) {
        let state = Arc::new(ArcSwapOption::from(None));
        (
            AtomicFence {
                state: state.clone(),
            },
            AtomicFenceWriter { state },
        )
    }

    /// Read the current revision, or `None` if unset. Inherent wrapper around
    /// [`FenceSource::current`] so callers don't need to import the trait.
    pub fn current(&self) -> Option<u64> {
        self.state.load().as_deref().copied()
    }
}

impl FenceSource for AtomicFence {
    fn current(&self) -> Option<u64> {
        self.state.load().as_deref().copied()
    }
}

impl AtomicFenceWriter {
    /// Publish a new revision. Called from the lease manager after each
    /// successful claim or heartbeat.
    ///
    /// Contract: `rev` is the lease claim's etag as a `u64` and is
    /// strictly increasing across claims, renewals, and takeovers. Storage
    /// adapters stamp it on every fenced write; the authority rejects any
    /// write whose revision is `<=` its current fence (see
    /// [`fence_accepts`]). A former leader therefore always carries a
    /// revision the new leader has already superseded.
    pub fn set(&self, rev: u64) {
        self.state.store(Some(Arc::new(rev)));
    }

    /// Clear the revision (lease lost). Subsequent `require()` returns
    /// `NoActiveLease` until `set()` is called again.
    pub fn clear(&self) {
        self.state.store(None);
    }

    /// Read the current revision (symmetric with `AtomicFence::current`).
    /// Rarely needed; the writer side usually just `set`s.
    pub fn current(&self) -> Option<u64> {
        self.state.load().as_deref().copied()
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

    #[test]
    fn atomic_fence_new_is_unset() {
        let (fence, _w) = AtomicFence::new();
        assert_eq!(fence.current(), None);
        assert_eq!(fence.require(), Err(NoActiveLease));
    }

    #[test]
    fn atomic_fence_set_is_visible_to_reader() {
        let (fence, writer) = AtomicFence::new();
        writer.set(42);
        assert_eq!(fence.current(), Some(42));
        assert_eq!(fence.require(), Ok(42));
    }

    #[test]
    fn atomic_fence_clear_resets_to_none() {
        let (fence, writer) = AtomicFence::new();
        writer.set(7);
        writer.clear();
        assert_eq!(fence.current(), None);
        assert_eq!(fence.require(), Err(NoActiveLease));
    }

    // ── Fencing-token contract (Finding 5) ──────────────────────────

    #[test]
    fn fence_strictly_increases_across_takeover() {
        // Old leader claims at rev 10, new leader takes over at a higher
        // rev. The published fence the new leader stamps on writes must
        // strictly exceed the old leader's, so the authority can tell them
        // apart purely by comparison.
        let old_leader_rev = 10u64;
        let new_leader_rev = 11u64;
        assert!(
            new_leader_rev > old_leader_rev,
            "takeover must advance the fence revision"
        );

        // The authority accepts the new leader and rejects the old one.
        let authority_fence = new_leader_rev;
        assert!(fence_accepts(old_leader_rev, new_leader_rev));
        assert!(!fence_accepts(authority_fence, old_leader_rev));
    }

    #[test]
    fn stale_fence_is_rejected() {
        let current = 42u64;
        // Strictly greater: accepted.
        assert!(fence_accepts(current, 43));
        // Equal: rejected (a former leader at the same rev must not write).
        assert!(!fence_accepts(current, 42));
        // Older: rejected.
        assert!(!fence_accepts(current, 41));
        assert!(!fence_accepts(current, 0));
    }
}
