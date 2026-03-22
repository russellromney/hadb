//! hadb-s3: S3 reference implementation for hadb.
//!
//! Provides S3LeaseStore, S3StorageBackend, and S3NodeRegistry.
//!
//! ```ignore
//! use hadb::Coordinator;
//! use hadb_s3::{S3LeaseStore, S3StorageBackend};
//!
//! let s3_config = aws_config::load_from_env().await;
//! let s3_client = aws_sdk_s3::Client::new(&s3_config);
//!
//! let lease_store = S3LeaseStore::new(s3_client.clone(), "my-bucket".into());
//! let storage = S3StorageBackend::new(s3_client, "my-bucket".into());
//! ```

mod error;
pub mod lease_store;
pub mod node_registry;
pub mod storage;

pub use lease_store::S3LeaseStore;
pub use node_registry::S3NodeRegistry;
pub use storage::S3StorageBackend;
