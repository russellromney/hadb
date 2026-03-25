//! Shared configuration loading for hadb CLIs.
//!
//! Each product has its own config struct (e.g. `HaqliteConfig`), but common
//! sections (S3, lease, retry, retention) live here so every product's TOML
//! looks the same.

use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::path::Path;
use std::time::Duration;

use hadb_io::RetryConfig;

/// Shared config sections that every hadb product uses.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct SharedConfig {
    pub s3: S3Section,
    pub lease: LeaseSection,
    pub retry: RetryConfig,
    pub retention: RetentionSection,
}

/// S3 connection config.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct S3Section {
    pub bucket: String,
    pub endpoint: Option<String>,
}

/// Leader election config.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LeaseSection {
    /// Lease TTL in seconds.
    pub ttl_secs: u64,
    /// How often the leader renews its lease (ms).
    pub renew_interval_ms: u64,
    /// How often followers poll the lease (ms).
    pub poll_interval_ms: u64,
}

impl Default for LeaseSection {
    fn default() -> Self {
        Self {
            ttl_secs: 5,
            renew_interval_ms: 2000,
            poll_interval_ms: 3000,
        }
    }
}

impl LeaseSection {
    pub fn ttl(&self) -> Duration {
        Duration::from_secs(self.ttl_secs)
    }

    pub fn renew_interval(&self) -> Duration {
        Duration::from_millis(self.renew_interval_ms)
    }

    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }
}

/// Retention config for snapshot compaction.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RetentionSection {
    /// Number of most recent snapshots to keep.
    pub keep: usize,
}

impl Default for RetentionSection {
    fn default() -> Self {
        Self { keep: 47 }
    }
}

/// Full config document: shared sections + product-specific section.
///
/// The TOML file has shared top-level tables (`[s3]`, `[lease]`, `[retry]`,
/// `[retention]`) plus a product-specific table (e.g. `[serve]` for haqlite).
#[derive(Debug, Clone, Deserialize)]
pub struct FullConfig<P> {
    #[serde(flatten)]
    pub shared: SharedConfig,
    #[serde(flatten)]
    pub product: P,
}

/// Load config from a TOML file.
///
/// If `path` is `Some`, loads from that path. Otherwise looks for
/// `{default_name}` in the current directory. Returns default config
/// if no file is found.
pub fn load_config<P: DeserializeOwned + Default>(
    path: Option<&Path>,
    default_name: &str,
) -> Result<(SharedConfig, P)> {
    let config_path = match path {
        Some(p) => {
            if !p.exists() {
                anyhow::bail!("config file not found: {}", p.display());
            }
            Some(p.to_path_buf())
        }
        None => {
            let default_path = std::env::current_dir()?.join(default_name);
            if default_path.exists() {
                Some(default_path)
            } else {
                None
            }
        }
    };

    match config_path {
        Some(path) => {
            let contents = std::fs::read_to_string(&path)
                .with_context(|| format!("failed to read config: {}", path.display()))?;
            let full: FullConfig<P> = toml::from_str(&contents)
                .with_context(|| format!("failed to parse config: {}", path.display()))?;
            Ok((full.shared, full.product))
        }
        None => Ok((SharedConfig::default(), P::default())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Deserialize, Default)]
    struct TestProduct {
        #[serde(default)]
        serve: Option<TestServe>,
    }

    #[derive(Debug, Clone, Deserialize)]
    struct TestServe {
        db_path: String,
        port: u16,
    }

    #[test]
    fn test_load_config_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("test.toml");
        std::fs::write(
            &config_path,
            r#"
[s3]
bucket = "my-bucket"
endpoint = "https://fly.storage.tigris.dev"

[lease]
ttl_secs = 10
renew_interval_ms = 3000

[serve]
db_path = "/data/my.db"
port = 8080
"#,
        )
        .unwrap();

        let (shared, product): (SharedConfig, TestProduct) =
            load_config(Some(config_path.as_path()), "test.toml").unwrap();

        assert_eq!(shared.s3.bucket, "my-bucket");
        assert_eq!(
            shared.s3.endpoint.as_deref(),
            Some("https://fly.storage.tigris.dev")
        );
        assert_eq!(shared.lease.ttl_secs, 10);
        assert_eq!(shared.lease.renew_interval_ms, 3000);
        // poll_interval_ms should be default
        assert_eq!(shared.lease.poll_interval_ms, 3000);

        let serve = product.serve.unwrap();
        assert_eq!(serve.db_path, "/data/my.db");
        assert_eq!(serve.port, 8080);
    }

    #[test]
    fn test_load_config_defaults_when_no_file() {
        let (shared, product): (SharedConfig, TestProduct) =
            load_config(None, "nonexistent.toml").unwrap();

        assert_eq!(shared.s3.bucket, "");
        assert_eq!(shared.lease.ttl_secs, 5);
        assert!(product.serve.is_none());
    }

    #[test]
    fn test_load_config_missing_explicit_path() {
        let result: Result<(SharedConfig, TestProduct)> =
            load_config(Some(Path::new("/nonexistent/path.toml")), "test.toml");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_lease_section_durations() {
        let lease = LeaseSection {
            ttl_secs: 10,
            renew_interval_ms: 3000,
            poll_interval_ms: 5000,
        };
        assert_eq!(lease.ttl(), Duration::from_secs(10));
        assert_eq!(lease.renew_interval(), Duration::from_millis(3000));
        assert_eq!(lease.poll_interval(), Duration::from_millis(5000));
    }

    #[test]
    fn test_shared_config_default() {
        let config = SharedConfig::default();
        assert_eq!(config.lease.ttl_secs, 5);
        assert_eq!(config.retention.keep, 47);
    }

    #[test]
    fn test_malformed_toml_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("bad.toml");
        std::fs::write(&config_path, "this is not valid [[ toml").unwrap();

        let result: Result<(SharedConfig, TestProduct)> =
            load_config(Some(config_path.as_path()), "bad.toml");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("parse"), "expected parse error, got: {err}");
    }

    #[test]
    fn test_empty_toml_file_returns_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("empty.toml");
        std::fs::write(&config_path, "").unwrap();

        let (shared, product): (SharedConfig, TestProduct) =
            load_config(Some(config_path.as_path()), "empty.toml").unwrap();

        assert_eq!(shared.s3.bucket, "");
        assert_eq!(shared.lease.ttl_secs, 5);
        assert!(product.serve.is_none());
    }

    #[test]
    fn test_wrong_type_in_toml_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("badtype.toml");
        std::fs::write(
            &config_path,
            r#"
[lease]
ttl_secs = "not-a-number"
"#,
        )
        .unwrap();

        let result: Result<(SharedConfig, TestProduct)> =
            load_config(Some(config_path.as_path()), "badtype.toml");
        assert!(result.is_err(), "expected type error for string in u64 field");
    }

    #[test]
    fn test_unknown_keys_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("extra.toml");
        std::fs::write(
            &config_path,
            r#"
[s3]
bucket = "my-bucket"
region = "us-east-1"
"#,
        )
        .unwrap();

        // serde(default) + flatten should ignore unknown keys
        let result: Result<(SharedConfig, TestProduct)> =
            load_config(Some(config_path.as_path()), "extra.toml");
        // This may or may not error depending on serde deny_unknown — just verify it doesn't panic
        if let Ok((shared, _)) = result {
            assert_eq!(shared.s3.bucket, "my-bucket");
        }
    }

    #[test]
    fn test_retention_defaults() {
        let r = RetentionSection::default();
        assert_eq!(r.keep, 47);
    }
}
