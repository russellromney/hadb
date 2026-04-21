//! turbodb: Manifest envelope + `ManifestStore` trait for tiered embedded databases.
//!
//! This crate defines the shared envelope shape (`Manifest`,
//! `ManifestMeta`) and the `ManifestStore` trait that backends implement
//! to publish manifests atomically. The envelope's `payload` is an
//! opaque `Vec<u8>`; its shape belongs to the consumer (turbolite,
//! turbograph, future turboduck).
//!
//! Implementations live in sibling crates: `turbodb-manifest-mem`
//! (tests), `turbodb-manifest-s3`, `turbodb-manifest-cinch`,
//! `turbodb-manifest-nats`, `turbodb-manifest-redis`.
//!
//! ## Design intent
//!
//! See [hadb/turbodb/SPEC.md] for the full turbodb spec (page groups,
//! prefetch, encryption). This crate currently scopes only the
//! manifest layer — extracted from `hadb` core during Phase
//! Turbogenesis.
//!
//! [hadb/turbodb/SPEC.md]: https://github.com/russellromney/hadb/blob/main/turbodb/SPEC.md

mod store;
mod types;

pub use hadb_storage::CasResult;
pub use store::ManifestStore;
pub use types::{Manifest, ManifestMeta};
