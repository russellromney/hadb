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
//! # Fencing
//!
//! Fencing (the HA invariant that refuses writes from former leaders) is
//! not defined here. `hadb-storage` is a byte-level blob store; whether a
//! particular backend requires a fence revision is the backend's concern,
//! and the fence primitive itself lives alongside the lease layer that
//! produces it: see [`hadb_lease::FenceSource`].

pub mod storage;

pub use storage::{CasResult, StorageBackend};
