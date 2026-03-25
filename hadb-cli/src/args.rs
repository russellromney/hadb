//! Shared clap arg structs for hadb CLIs.
//!
//! Each product CLI uses `#[command(flatten)]` to embed these consistently.

use clap::Args;
use std::path::PathBuf;

/// S3 connection arguments. Used by every subcommand that touches S3.
#[derive(Args, Debug, Clone)]
pub struct S3Args {
    /// S3 bucket (with optional s3:// prefix and /path suffix).
    #[arg(short, long, env = "HADB_BUCKET")]
    pub bucket: String,

    /// S3 endpoint URL (for Tigris, MinIO, R2, etc).
    #[arg(long, env = "AWS_ENDPOINT_URL_S3")]
    pub endpoint: Option<String>,

    /// S3 key prefix for database storage. Product-specific default applies if empty.
    #[arg(long, env = "HADB_PREFIX", default_value = "")]
    pub prefix: String,
}

/// Lease/leader-election arguments for `serve`.
#[derive(Args, Debug, Clone)]
pub struct LeaseArgs {
    /// Instance ID for leader election. Auto-generated if omitted.
    #[arg(long, env = "HADB_INSTANCE_ID")]
    pub instance_id: Option<String>,

    /// Lease TTL in seconds.
    #[arg(long, default_value = "5")]
    pub lease_ttl: u64,

    /// Lease renew interval in milliseconds.
    #[arg(long, default_value = "2000")]
    pub renew_interval_ms: u64,

    /// Lease poll interval in milliseconds (follower check frequency).
    #[arg(long, default_value = "3000")]
    pub poll_interval_ms: u64,
}

/// Retry policy arguments.
#[derive(Args, Debug, Clone)]
pub struct RetryArgs {
    /// Maximum retry attempts for transient S3 failures.
    #[arg(long)]
    pub max_retries: Option<u32>,

    /// Base delay between retries in milliseconds.
    #[arg(long)]
    pub base_delay_ms: Option<u64>,

    /// Maximum delay between retries in milliseconds.
    #[arg(long)]
    pub max_delay_ms: Option<u64>,
}

/// Arguments for `restore` subcommand.
#[derive(Args, Debug, Clone)]
pub struct RestoreArgs {
    /// Database name in S3 to restore.
    pub name: String,

    /// Output path for the restored database file.
    #[arg(short, long)]
    pub output: PathBuf,

    /// S3 connection.
    #[command(flatten)]
    pub s3: S3Args,

    /// Point-in-time restore (ISO 8601 timestamp).
    #[arg(long)]
    pub point_in_time: Option<String>,
}

/// Arguments for `list` subcommand.
#[derive(Args, Debug, Clone)]
pub struct ListArgs {
    /// S3 connection.
    #[command(flatten)]
    pub s3: S3Args,
}

/// Arguments for `verify` subcommand.
#[derive(Args, Debug, Clone)]
pub struct VerifyArgs {
    /// Database name in S3 to verify.
    pub name: String,

    /// S3 connection.
    #[command(flatten)]
    pub s3: S3Args,
}

/// Arguments for `compact` subcommand (snapshot retention).
#[derive(Args, Debug, Clone)]
pub struct CompactArgs {
    /// Database name in S3 to compact.
    pub name: String,

    /// S3 connection.
    #[command(flatten)]
    pub s3: S3Args,

    /// Number of most recent snapshots to keep.
    #[arg(long, default_value = "47")]
    pub keep: usize,

    /// Actually delete files (default is dry-run).
    #[arg(long)]
    pub force: bool,
}

/// Arguments for `replicate` subcommand (read replica).
#[derive(Args, Debug, Clone)]
pub struct ReplicateArgs {
    /// Source database name in S3.
    pub source: String,

    /// Local database path to replicate into.
    #[arg(short, long)]
    pub local: PathBuf,

    /// S3 connection.
    #[command(flatten)]
    pub s3: S3Args,

    /// Poll interval in seconds.
    #[arg(long, default_value = "1")]
    pub interval: u64,
}

/// Arguments for `snapshot` subcommand (immediate snapshot).
#[derive(Args, Debug, Clone)]
pub struct SnapshotArgs {
    /// Path to a local database file to snapshot.
    pub database: PathBuf,

    /// S3 connection.
    #[command(flatten)]
    pub s3: S3Args,
}
