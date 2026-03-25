//! CLI runner: CliBackend trait + run_cli() dispatch.
//!
//! Each hadb product (haqlite, hakuzu, etc.) implements `CliBackend` to get
//! a consistent CLI with serve, restore, list, verify, compact, replicate,
//! snapshot, and explain subcommands.

use std::fmt::Debug;
use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Result;
use async_trait::async_trait;
use clap::{Parser, Subcommand};
use serde::de::DeserializeOwned;

use crate::args::*;
use crate::config::{self, SharedConfig};
use crate::errors::{self, ExitStatus};

/// Trait that each hadb product implements for CLI dispatch.
///
/// The runner calls the appropriate method based on the subcommand.
/// Products only implement the methods they support; unsupported ones
/// return an error by default.
#[async_trait]
pub trait CliBackend: Send + Sync {
    /// Product-specific config section (deserialized from TOML).
    type Config: DeserializeOwned + Default + Debug + Send + Sync;

    /// Product name for display (e.g. "haqlite", "hakuzu").
    fn product_name(&self) -> &'static str;

    /// Default config filename (e.g. "haqlite.toml").
    fn config_filename(&self) -> &'static str;

    /// Run the HA server (leader election + replication + API).
    async fn serve(&self, shared: &SharedConfig, product: &Self::Config) -> Result<()>;

    /// Restore a database from S3.
    async fn restore(&self, shared: &SharedConfig, args: &RestoreArgs) -> Result<()> {
        let _ = (shared, args);
        anyhow::bail!("{} does not support restore", self.product_name())
    }

    /// List databases in the S3 bucket.
    async fn list(&self, shared: &SharedConfig, args: &ListArgs) -> Result<()> {
        let _ = (shared, args);
        anyhow::bail!("{} does not support list", self.product_name())
    }

    /// Verify integrity of a database's S3 backup chain.
    async fn verify(&self, shared: &SharedConfig, args: &VerifyArgs) -> Result<()> {
        let _ = (shared, args);
        anyhow::bail!("{} does not support verify", self.product_name())
    }

    /// Compact snapshots using retention policy.
    async fn compact(&self, shared: &SharedConfig, args: &CompactArgs) -> Result<()> {
        let _ = (shared, args);
        anyhow::bail!("{} does not support compact", self.product_name())
    }

    /// Run as a read replica, polling S3 for changes.
    async fn replicate(&self, shared: &SharedConfig, args: &ReplicateArgs) -> Result<()> {
        let _ = (shared, args);
        anyhow::bail!("{} does not support replicate", self.product_name())
    }

    /// Take an immediate snapshot.
    async fn snapshot(&self, shared: &SharedConfig, args: &SnapshotArgs) -> Result<()> {
        let _ = (shared, args);
        anyhow::bail!("{} does not support snapshot", self.product_name())
    }

    /// Print resolved config without running anything.
    async fn explain(&self, shared: &SharedConfig, product: &Self::Config) -> Result<()> {
        println!("=== {} Configuration ===\n", self.product_name());
        println!("Shared:");
        println!("  S3 bucket:    {}", shared.s3.bucket);
        println!(
            "  S3 endpoint:  {}",
            shared.s3.endpoint.as_deref().unwrap_or("(default)")
        );
        println!("  Lease TTL:    {}s", shared.lease.ttl_secs);
        println!("  Lease renew:  {}ms", shared.lease.renew_interval_ms);
        println!("  Lease poll:   {}ms", shared.lease.poll_interval_ms);
        println!("  Retention:    keep {} snapshots", shared.retention.keep);
        println!("\nProduct-specific:");
        println!("  {product:#?}");
        Ok(())
    }
}

/// Shared top-level CLI structure.
///
/// Products wrap this inside their own `#[derive(Parser)]` struct to add
/// product-specific global args or subcommands.
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Run the HA database server.
    Serve,
    /// Restore a database from S3 backup.
    Restore(RestoreArgs),
    /// List databases in the S3 bucket.
    List(ListArgs),
    /// Verify backup integrity.
    Verify(VerifyArgs),
    /// Compact snapshots (retention).
    Compact(CompactArgs),
    /// Run as a read replica.
    Replicate(ReplicateArgs),
    /// Take an immediate snapshot.
    Snapshot(SnapshotArgs),
    /// Show resolved config.
    Explain,
}

/// Top-level CLI parser that products can use directly.
#[derive(Parser, Debug)]
pub struct Cli {
    /// Path to config file.
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Commands,
}

/// Initialize tracing with env filter.
fn init_tracing() {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

/// Run a hadb CLI to completion. Returns an ExitCode.
///
/// This is the main entry point that each product's `main()` calls:
///
/// ```ignore
/// fn main() -> ExitCode {
///     hadb_cli::run_cli(MyBackend)
/// }
/// ```
pub fn run_cli<B: CliBackend + 'static>(backend: B) -> ExitCode {
    init_tracing();

    let cli = Cli::parse();

    let config_path = cli.config.as_deref();
    let (shared, product) = match config::load_config::<B::Config>(config_path, backend.config_filename()) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("{}: config error: {e:#}", backend.product_name());
            return ExitStatus::Config.into();
        }
    };

    let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");

    let result = rt.block_on(async {
        match cli.command {
            Commands::Serve => backend.serve(&shared, &product).await,
            Commands::Restore(args) => backend.restore(&shared, &args).await,
            Commands::List(args) => backend.list(&shared, &args).await,
            Commands::Verify(args) => backend.verify(&shared, &args).await,
            Commands::Compact(args) => backend.compact(&shared, &args).await,
            Commands::Replicate(args) => backend.replicate(&shared, &args).await,
            Commands::Snapshot(args) => backend.snapshot(&shared, &args).await,
            Commands::Explain => backend.explain(&shared, &product).await,
        }
    });

    match result {
        Ok(()) => ExitStatus::Success.into(),
        Err(e) => {
            let status = errors::classify_error(&e);
            tracing::error!("{}: {e:#}", backend.product_name());
            status.into()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestBackend;

    #[derive(Debug, Default, serde::Deserialize)]
    struct TestConfig {}

    #[async_trait]
    impl CliBackend for TestBackend {
        type Config = TestConfig;

        fn product_name(&self) -> &'static str {
            "test-db"
        }

        fn config_filename(&self) -> &'static str {
            "test.toml"
        }

        async fn serve(&self, _shared: &SharedConfig, _product: &Self::Config) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_default_methods_return_unsupported() {
        let backend = TestBackend;
        let shared = SharedConfig::default();
        let rt = tokio::runtime::Runtime::new().unwrap();

        let list_args = ListArgs {
            s3: S3Args {
                bucket: "test".to_string(),
                endpoint: None,
                prefix: String::new(),
            },
        };
        let err = rt.block_on(backend.list(&shared, &list_args)).unwrap_err();
        assert!(err.to_string().contains("does not support list"));
    }

    #[test]
    fn test_explain_default() {
        let backend = TestBackend;
        let shared = SharedConfig::default();
        let product = TestConfig {};
        let rt = tokio::runtime::Runtime::new().unwrap();
        // Should not error
        rt.block_on(backend.explain(&shared, &product)).unwrap();
    }
}
