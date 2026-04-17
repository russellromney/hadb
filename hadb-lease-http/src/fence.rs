//! Concrete `FenceSource` implementation for Cinch-style HTTP lease
//! deployments.
//!
//! The lease manager (typically `hadb::DbLease`) holds the `AtomicFenceWriter`
//! and updates it on every successful lease claim or renewal. Storage
//! adapters hold an `AtomicFence` (which implements [`FenceSource`]) and
//! read the current revision on every fenced write.
//!
//! # Producer / consumer split
//!
//! `AtomicFence::new()` returns the paired handles. The reader side is
//! `Clone`; share it with any number of storage adapters. The writer side
//! is not `Clone`: only the lease manager should mutate the fence, and
//! the compiler enforces it.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use hadb_lease::FenceSource;

/// Clonable reader handle. Share with any storage adapter that needs to
/// read the current fence revision for fenced writes.
#[derive(Clone)]
pub struct AtomicFence {
    state: Arc<ArcSwapOption<u64>>,
}

/// Exclusive writer handle. Held by the lease manager; deliberately not
/// `Clone` so ownership of "who mutates the fence" stays unambiguous.
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
}

impl FenceSource for AtomicFence {
    fn current(&self) -> Option<u64> {
        self.state.load().as_deref().copied()
    }
}

impl AtomicFenceWriter {
    /// Publish a new revision. Called from the lease manager after each
    /// successful claim or heartbeat.
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
    use hadb_lease::NoActiveLease;

    #[test]
    fn new_is_unset() {
        let (fence, _w) = AtomicFence::new();
        assert_eq!(fence.current(), None);
        assert_eq!(fence.require(), Err(NoActiveLease));
    }

    #[test]
    fn set_is_visible_to_reader() {
        let (fence, writer) = AtomicFence::new();
        writer.set(42);
        assert_eq!(fence.current(), Some(42));
        assert_eq!(fence.require(), Ok(42));
    }

    #[test]
    fn clear_resets_to_none() {
        let (fence, writer) = AtomicFence::new();
        writer.set(7);
        writer.clear();
        assert_eq!(fence.current(), None);
        assert_eq!(fence.require(), Err(NoActiveLease));
    }

    #[test]
    fn fence_is_clone_writer_is_not() {
        let (fence, writer) = AtomicFence::new();
        let _fence_clone = fence.clone();
        // AtomicFenceWriter is deliberately not Clone; compiler enforces it.
        writer.set(1);
    }

    #[test]
    fn updates_are_last_write_wins() {
        let (fence, writer) = AtomicFence::new();
        writer.set(1);
        writer.set(2);
        writer.set(3);
        assert_eq!(fence.current(), Some(3));
        // Nothing enforces monotonicity at this level; that's the lease
        // manager's responsibility.
        writer.set(1);
        assert_eq!(fence.current(), Some(1));
    }

    #[test]
    fn writer_current_mirrors_reader() {
        let (fence, writer) = AtomicFence::new();
        assert_eq!(writer.current(), None);
        writer.set(99);
        assert_eq!(writer.current(), Some(99));
        assert_eq!(fence.current(), Some(99));
    }
}
