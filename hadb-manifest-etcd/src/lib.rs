//! hadb-manifest-etcd: etcd manifest store for hadb.
//!
//! ```ignore
//! use hadb_manifest_etcd::EtcdManifestStore;
//!
//! let store = EtcdManifestStore::connect(&["http://localhost:2379"], "manifests/").await?;
//! ```

pub mod manifest_store;

pub use manifest_store::EtcdManifestStore;
