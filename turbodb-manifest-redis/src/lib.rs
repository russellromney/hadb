//! turbodb-manifest-redis: Redis ManifestStore for turbodb.
//!
//! Atomic CAS via Lua scripts. Cluster-safe via colocated hash-tagged
//! data + version side-keys.
//!
//! ```ignore
//! use turbodb_manifest_redis::RedisManifestStore;
//!
//! let store = RedisManifestStore::connect("redis://localhost:6379", "manifest:").await?;
//! ```

pub mod manifest_store;

pub use manifest_store::RedisManifestStore;
