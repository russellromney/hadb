//! `AtomicFence` / `AtomicFenceWriter` moved to `hadb-lease` alongside the
//! `FenceSource` trait itself (Phase Anvil i). This module is a backward-
//! compat re-export; future code should import from `hadb_lease` directly.

pub use hadb_lease::{AtomicFence, AtomicFenceWriter};
