//! hadb-manifest-redis: Redis manifest store for hadb.
//!
//! Atomic CAS via Lua scripts. Full manifest stored as msgpack.
//!
//! ```ignore
//! use hadb_manifest_redis::RedisManifestStore;
//!
//! let store = RedisManifestStore::connect("redis://localhost:6379", "manifests/").await?;
//! ```

pub mod manifest_store;

pub use manifest_store::RedisManifestStore;
