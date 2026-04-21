//! turbodb-manifest-s3: S3 ManifestStore for turbodb.
//!
//! Stores `Manifest` as msgpack in the S3 object body, with
//! version/writer_id/lease_epoch in custom metadata headers for cheap
//! HeadObject polling.
//!
//! ```ignore
//! use turbodb_manifest_s3::S3ManifestStore;
//!
//! let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
//! let client = aws_sdk_s3::Client::new(&config);
//! let store = S3ManifestStore::new(client, "my-bucket".into());
//! ```

mod error;
pub mod manifest_store;

pub use manifest_store::S3ManifestStore;
