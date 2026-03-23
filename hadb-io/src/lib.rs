//! Shared S3/retry/upload infrastructure for the hadb ecosystem.
//!
//! `hadb-io` provides the common infrastructure layer used by all hadb replication
//! engines (walrust-core, graphstream) and product crates (haqlite, hakuzu):
//!
//! - **Retry** — Exponential backoff with full jitter, error classification, circuit breaker
//! - **S3** — Client helpers for S3-compatible storage (feature-gated)
//! - **ObjectStore** — Rich storage trait for bulk data operations + S3Backend
//! - **Webhook** — HTTP POST notifications with HMAC-SHA256 signing
//! - **Retention** — GFS (Grandfather/Father/Son) snapshot rotation
//! - **Config** — Shared configuration types (S3, webhook, cache, duration parsing)

pub mod config;
pub mod retention;
pub mod retry;
pub mod storage;
pub mod uploader;
pub mod webhook;

#[cfg(feature = "s3")]
pub mod s3;

// Re-export AWS SDK crates so consumers don't need direct dependencies
#[cfg(feature = "s3")]
pub use aws_sdk_s3;

// Re-export primary types for convenience
pub use config::{CacheConfig, S3Config, WebhookConfig};
pub use config::parse_duration_string;
pub use retention::{RetentionPlan, RetentionPolicy, SnapshotEntry, Tier, analyze_retention};
pub use retry::{
    CircuitBreaker, CircuitState, ErrorKind, OnCircuitOpen, RetryConfig, RetryOutcome,
    RetryPolicy, classify_error, is_retryable,
};
pub use storage::ObjectStore;
pub use uploader::{ConcurrentUploader, UploadHandler, UploadMessage, UploaderStats, spawn_uploader};
pub use webhook::{WebhookEvent, WebhookPayload, WebhookSender, compute_hmac_signature};

#[cfg(feature = "s3")]
pub use storage::S3Backend;
