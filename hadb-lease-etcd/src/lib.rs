//! hadb-lease-etcd: etcd lease store for hadb.
//!
//! Zero-infra HA on Kubernetes -- every k8s cluster already has etcd.
//! Uses etcd transactions for compare-and-swap leader election.
//!
//! ```ignore
//! use hadb_lease_etcd::EtcdLeaseStore;
//!
//! let lease_store = EtcdLeaseStore::connect(["http://localhost:2379"], "hadb/").await?;
//! ```

pub mod lease_store;

pub use lease_store::EtcdLeaseStore;
