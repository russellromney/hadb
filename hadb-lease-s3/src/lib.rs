//! hadb-lease-s3: S3 coordination primitives for hadb.
//!
//! Provides S3-backed implementations of hadb's coordination traits:
//! - `S3LeaseStore` — Leader election via S3 conditional PUTs (CAS)
//! - `S3NodeRegistry` — Node discovery via S3 object listing
//! - `S3StorageBackend` — Minimal storage for coordination data (node registrations, etc.)
//!
//! ```ignore
//! use hadb::Coordinator;
//! use hadb_lease_s3::{S3LeaseStore, S3StorageBackend, S3NodeRegistry};
//!
//! let s3_config = aws_config::load_from_env().await;
//! let s3_client = aws_sdk_s3::Client::new(&s3_config);
//!
//! let lease_store = S3LeaseStore::new(s3_client.clone(), "my-bucket".into());
//! let storage = S3StorageBackend::new(s3_client.clone(), "my-bucket".into());
//! let registry = S3NodeRegistry::new(Arc::new(storage));
//! ```

mod error;
pub mod lease_store;
pub mod node_registry;
pub mod storage;

pub use lease_store::S3LeaseStore;
pub use node_registry::S3NodeRegistry;
pub use storage::S3StorageBackend;
