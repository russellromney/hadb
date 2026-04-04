//! hadb-manifest-nats: NATS JetStream KV manifest store for hadb.
//!
//! ```ignore
//! use hadb_manifest_nats::NatsManifestStore;
//!
//! let store = NatsManifestStore::connect("nats://localhost:4222", "manifests").await?;
//! ```

pub mod manifest_store;

pub use manifest_store::NatsManifestStore;
