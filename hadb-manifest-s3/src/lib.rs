//! DEPRECATED stub.
//!
//! The S3 ManifestStore moved to `turbodb-manifest-s3` during Phase
//! Turbogenesis. This crate is intentionally empty — it exists only so
//! `hakuzu`'s `Cargo.toml` can resolve the path dependency until
//! hakuzu's own Phase GraphTurbogenesis lands and swaps over to
//! `turbodb-manifest-s3`. After that, delete this directory.
//!
//! Hakuzu's source does still call `hadb_manifest_s3::S3ManifestStore`
//! at the use site; that code only compiles when consumers enable the
//! `graph-ha` feature, which is off by default everywhere right now.
//! For consumers that DO need a working S3 manifest store today,
//! depend on `turbodb-manifest-s3` directly.
