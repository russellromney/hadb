//! turbodb-manifest-nats: NATS JetStream KV ManifestStore for turbodb.
//!
//! ```ignore
//! use turbodb_manifest_nats::NatsManifestStore;
//!
//! let store = NatsManifestStore::connect("nats://localhost:4222", "manifests").await?;
//! ```

pub mod manifest_store;

pub use manifest_store::NatsManifestStore;
