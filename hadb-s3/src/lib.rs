//! hadb-s3: S3 reference implementation for hadb.
//!
//! Provides S3LeaseStore and S3StorageBackend implementations using
//! S3 conditional PUTs for leader election and S3 PutObject/GetObject
//! for replication data.
//!
//! ```ignore
//! use hadb::Coordinator;
//! use hadb_s3::{S3LeaseStore, S3StorageBackend, S3Coordinator};
//!
//! let s3_config = aws_config::load_from_env().await;
//! let s3_client = aws_sdk_s3::Client::new(&s3_config);
//!
//! let lease_store = S3LeaseStore::new(s3_client.clone(), "my-bucket".into());
//! let storage = S3StorageBackend::new(s3_client, "my-bucket".into());
//!
//! // Or use the type alias for convenience
//! let coordinator: S3Coordinator<MyReplicator, MyExecutor> = Coordinator::new(
//!     replicator,
//!     executor,
//!     lease_store,
//!     storage,
//!     config,
//! )?;
//! ```

mod error;
pub mod lease_store;
pub mod node_registry;
pub mod storage;

// Re-export for convenience
pub use lease_store::S3LeaseStore;
pub use node_registry::S3NodeRegistry;
pub use storage::S3StorageBackend;

// Type alias for S3-backed coordinator
use hadb::Coordinator;

/// Type alias for Coordinator using S3 implementations.
///
/// Reduces generic type boilerplate for the common case of using S3.
///
/// ```ignore
/// // Instead of:
/// let coordinator: Coordinator<MyReplicator, MyExecutor, S3LeaseStore, S3StorageBackend> = ...;
///
/// // Write:
/// let coordinator: S3Coordinator<MyReplicator, MyExecutor> = ...;
/// ```
pub type S3Coordinator<R, E> = Coordinator<R, E, S3LeaseStore, S3StorageBackend>;

