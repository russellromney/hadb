//! `Fence`: the HA invariant that prevents a former-leader from writing
//! stale data after its lease is lost.
//!
//! # Protocol
//!
//! 1. Lease manager acquires a lease. The lease store returns a monotonic
//!    revision (NATS KV rev, S3 etag, etc.) — the *fence token*.
//! 2. Lease manager stores the revision via `FenceWriter::set(rev)`.
//! 3. Every write by the lease holder goes through a storage adapter that
//!    reads the current fence via `Fence::require()` and attaches it to
//!    the request (HTTP `Fence-Token` header, or equivalent).
//! 4. The server compares the client's fence against the current lease
//!    revision and rejects stale writes (HTTP 412 Precondition Failed).
//! 5. When the lease is lost (CAS conflict on renewal, explicit release,
//!    server-observed takeover), `FenceWriter::clear()` drops the fence.
//!    Subsequent `require()` returns `Err(NoActiveLease)`.
//!
//! # Why a split producer/consumer type?
//!
//! So the compiler enforces "only the lease manager can mutate the fence."
//! Storage adapters hold a clonable `Fence` and can only *read* from it.
//! Returning the `FenceWriter` is `new`'s exclusive prerogative.

use std::sync::Arc;

use arc_swap::ArcSwapOption;

/// Read-only handle to the current fence value. Clonable; share with any
/// number of writers. Calling `require()` before a write is the check
/// that refuses to issue writes when the lease is lost.
#[derive(Clone)]
pub struct Fence {
    state: Arc<ArcSwapOption<u64>>,
}

/// Exclusive handle for updating the fence. Held by the lease manager.
/// Not `Clone` — only one owner, which makes "who mutates the fence"
/// unambiguous.
pub struct FenceWriter {
    state: Arc<ArcSwapOption<u64>>,
}

/// Returned by `Fence::require()` when no lease is held. Callers should
/// propagate this as a hard error, never silently continue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoActiveLease;

impl std::fmt::Display for NoActiveLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "no active lease; refusing write")
    }
}

impl std::error::Error for NoActiveLease {}

impl Fence {
    /// Create a new Fence (initially unset) paired with its writer handle.
    /// Hand the `Fence` to storage adapters and the `FenceWriter` to the
    /// lease manager.
    pub fn new() -> (Fence, FenceWriter) {
        let state = Arc::new(ArcSwapOption::from(None));
        (
            Fence { state: state.clone() },
            FenceWriter { state },
        )
    }

    /// Read the current fence revision, or `None` if no lease is held.
    pub fn current(&self) -> Option<u64> {
        self.state.load().as_deref().copied()
    }

    /// Require a fence revision for a write. Use this before issuing any
    /// write that's fencing-protected. Returns `Err(NoActiveLease)` if
    /// the lease is not currently held.
    pub fn require(&self) -> Result<u64, NoActiveLease> {
        self.current().ok_or(NoActiveLease)
    }
}

impl FenceWriter {
    /// Set the fence to a specific revision. Call this from the lease
    /// manager after a successful claim or heartbeat.
    pub fn set(&self, rev: u64) {
        self.state.store(Some(Arc::new(rev)));
    }

    /// Clear the fence (lease lost). Subsequent `Fence::require()` returns
    /// `NoActiveLease` until `set()` is called again.
    pub fn clear(&self) {
        self.state.store(None);
    }

    /// Read the current fence (symmetric with `Fence::current`). Rarely
    /// needed; the writer side usually just `set()`s.
    pub fn current(&self) -> Option<u64> {
        self.state.load().as_deref().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_unset() {
        let (fence, _writer) = Fence::new();
        assert_eq!(fence.current(), None);
        assert_eq!(fence.require(), Err(NoActiveLease));
    }

    #[test]
    fn set_visible_to_reader() {
        let (fence, writer) = Fence::new();
        writer.set(42);
        assert_eq!(fence.current(), Some(42));
        assert_eq!(fence.require(), Ok(42));
    }

    #[test]
    fn clear_resets_to_none() {
        let (fence, writer) = Fence::new();
        writer.set(7);
        writer.clear();
        assert_eq!(fence.current(), None);
        assert_eq!(fence.require(), Err(NoActiveLease));
    }

    #[test]
    fn fence_is_cloneable_writer_is_not() {
        let (fence, writer) = Fence::new();
        let _fence_clone = fence.clone();
        // FenceWriter is deliberately not Clone; the compiler enforces it.
        // This test just demonstrates that Fence *is* cloneable.
        writer.set(1);
    }

    #[test]
    fn updates_are_monotonic_in_practice() {
        let (fence, writer) = Fence::new();
        writer.set(1);
        writer.set(2);
        writer.set(3);
        assert_eq!(fence.current(), Some(3));
        // Nothing stops you from going backward; the trait doesn't enforce
        // monotonicity. That's the lease manager's responsibility.
        writer.set(1);
        assert_eq!(fence.current(), Some(1));
    }
}
