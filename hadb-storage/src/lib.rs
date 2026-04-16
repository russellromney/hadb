//! `hadb-storage`: storage backend trait + Fence primitive.
//!
//! This crate defines the abstractions. It has no backend implementations —
//! those live in sibling crates (`hadb-storage-local`, `hadb-storage-s3`,
//! `hadb-storage-cinch`, `hadb-storage-mem`).
//!
//! # Why trait-only?
//!
//! Crates that implement `StorageBackend` (HTTP adapters, filesystem, etc.)
//! shouldn't pull in the full `hadb` coordinator just to get the trait
//! definition. And consumers (walrust, turbolite) shouldn't pull in S3 +
//! reqwest + tokio just to touch the interface.
//!
//! # The `StorageBackend` trait
//!
//! A minimal byte-level blob store: get / put / delete / list / exists,
//! plus CAS primitives (`put_if_absent`, `put_if_match`) that lease and
//! manifest stores build on. Async throughout; sync consumers wrap in
//! `block_on` themselves.
//!
//! # Fence
//!
//! `Fence` is the HA invariant that prevents a former-leader from writing
//! stale data after losing its lease. The lease manager holds a
//! `FenceWriter`; storage adapters that care about fencing (HTTP-backed
//! ones) hold a `Fence` and call `Fence::require()` before every write.
//! Backends that don't need fencing (local filesystem, tests) simply don't
//! carry one.

pub mod fence;
pub mod storage;

pub use fence::{Fence, FenceWriter, NoActiveLease};
pub use storage::{CasResult, StorageBackend};
