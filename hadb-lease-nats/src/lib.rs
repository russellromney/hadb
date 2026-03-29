//! hadb-lease-nats: NATS JetStream KV lease store for hadb.
//!
//! Provides a NATS-backed implementation of hadb's `LeaseStore` trait
//! using JetStream Key-Value buckets for compare-and-swap leader election.
//!
//! NATS KV operations are 2-5ms (vs S3's 50-200ms), with zero per-request
//! cost. A single NATS server on Fly (~$2/month) handles thousands of
//! lease operations per second.
//!
//! ```ignore
//! use hadb_lease_nats::NatsLeaseStore;
//!
//! let lease_store = NatsLeaseStore::connect("nats://localhost:4222", "hadb-leases").await?;
//! ```

pub mod lease_store;

pub use lease_store::NatsLeaseStore;
