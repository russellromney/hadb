//! Shared configuration types for the hadb ecosystem.
//!
//! These types are used across walrust, graphstream, haqlite, hakuzu, etc.
//! Engine-specific config (checkpoint intervals, WAL thresholds) stays in each product.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

/// S3 storage configuration.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct S3Config {
    /// S3 bucket URL (e.g., "s3://backups/prod").
    pub bucket: Option<String>,
    /// S3 endpoint URL (for Tigris/MinIO).
    pub endpoint: Option<String>,
}

/// Webhook configuration for failure notifications.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebhookConfig {
    /// URL to POST notifications to.
    pub url: String,
    /// Events to notify on (default: all).
    #[serde(default = "default_webhook_events")]
    pub events: Vec<String>,
    /// Optional secret for HMAC signing (header: X-Hadb-Signature).
    pub secret: Option<String>,
}

fn default_webhook_events() -> Vec<String> {
    vec![
        "upload_failed".to_string(),
        "auth_failure".to_string(),
        "corruption_detected".to_string(),
        "circuit_breaker_open".to_string(),
    ]
}

/// Cache configuration for disk-based upload queue.
///
/// When enabled, data files are written to disk cache before uploading to S3.
/// Provides crash recovery, decouples encoding from uploads, and enables fast local restore.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CacheConfig {
    /// Enable disk cache (default: false).
    #[serde(default)]
    pub enabled: bool,
    /// How long to keep uploaded files in cache (default: "24h").
    /// Supports: "1h", "24h", "7d", etc.
    #[serde(default = "default_cache_retention")]
    pub retention: String,
    /// Maximum cache size in bytes before cleanup (default: 5GB).
    #[serde(default = "default_cache_max_size")]
    pub max_size: u64,
    /// Override default cache location.
    pub path: Option<String>,
    /// Max concurrent S3 uploads per database (default: 4).
    #[serde(default = "default_uploader_concurrency")]
    pub uploader_concurrency: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            retention: "24h".to_string(),
            max_size: 5 * 1024 * 1024 * 1024, // 5GB
            path: None,
            uploader_concurrency: 4,
        }
    }
}

fn default_cache_retention() -> String {
    "24h".to_string()
}

fn default_cache_max_size() -> u64 {
    5 * 1024 * 1024 * 1024 // 5GB
}

fn default_uploader_concurrency() -> usize {
    4
}

/// Parse duration string like "24h", "7d", "30m", "60s" into chrono::Duration.
pub fn parse_duration_string(s: &str) -> Result<chrono::Duration> {
    let s = s.trim();
    if s.is_empty() {
        return Err(anyhow!("Empty duration string"));
    }

    let (num_str, unit) = if let Some(n) = s.strip_suffix('h') {
        (n, 'h')
    } else if let Some(n) = s.strip_suffix('d') {
        (n, 'd')
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 'm')
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 's')
    } else {
        return Err(anyhow!(
            "Invalid duration format '{}': must end with h, d, m, or s",
            s
        ));
    };

    let num: i64 = num_str
        .parse()
        .map_err(|_| anyhow!("Invalid duration number '{}' in '{}'", num_str, s))?;

    match unit {
        'h' => Ok(chrono::Duration::hours(num)),
        'd' => Ok(chrono::Duration::days(num)),
        'm' => Ok(chrono::Duration::minutes(num)),
        's' => Ok(chrono::Duration::seconds(num)),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_config_default() {
        let config = S3Config::default();
        assert!(config.bucket.is_none());
        assert!(config.endpoint.is_none());
    }

    #[test]
    fn test_s3_config_serde() {
        let json = r#"{"bucket": "s3://test", "endpoint": "https://fly.storage.tigris.dev"}"#;
        let config: S3Config = serde_json::from_str(json).unwrap();
        assert_eq!(config.bucket, Some("s3://test".to_string()));
        assert_eq!(
            config.endpoint,
            Some("https://fly.storage.tigris.dev".to_string())
        );
    }

    #[test]
    fn test_webhook_config_defaults() {
        let json = r#"{"url": "https://example.com/hook"}"#;
        let config: WebhookConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.url, "https://example.com/hook");
        assert_eq!(config.events.len(), 4);
        assert!(config.events.contains(&"upload_failed".to_string()));
        assert!(config.secret.is_none());
    }

    #[test]
    fn test_webhook_config_with_secret() {
        let json =
            r#"{"url": "https://example.com", "events": ["auth_failure"], "secret": "s3cret"}"#;
        let config: WebhookConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.events.len(), 1);
        assert_eq!(config.secret, Some("s3cret".to_string()));
    }

    #[test]
    fn test_cache_config_defaults() {
        let config = CacheConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.retention, "24h");
        assert_eq!(config.max_size, 5 * 1024 * 1024 * 1024);
        assert!(config.path.is_none());
        assert_eq!(config.uploader_concurrency, 4);
    }

    #[test]
    fn test_cache_config_serde() {
        let json = r#"{"enabled": true, "retention": "7d", "max_size": 1073741824, "uploader_concurrency": 8}"#;
        let config: CacheConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.retention, "7d");
        assert_eq!(config.max_size, 1073741824);
        assert_eq!(config.uploader_concurrency, 8);
    }

    #[test]
    fn test_parse_duration_hours() {
        let duration = parse_duration_string("24h").unwrap();
        assert_eq!(duration.num_hours(), 24);
    }

    #[test]
    fn test_parse_duration_days() {
        let duration = parse_duration_string("7d").unwrap();
        assert_eq!(duration.num_days(), 7);
    }

    #[test]
    fn test_parse_duration_minutes() {
        let duration = parse_duration_string("30m").unwrap();
        assert_eq!(duration.num_minutes(), 30);
    }

    #[test]
    fn test_parse_duration_seconds() {
        let duration = parse_duration_string("60s").unwrap();
        assert_eq!(duration.num_seconds(), 60);
    }

    #[test]
    fn test_parse_duration_whitespace() {
        let duration = parse_duration_string("  12h  ").unwrap();
        assert_eq!(duration.num_hours(), 12);
    }

    #[test]
    fn test_parse_duration_invalid() {
        assert!(parse_duration_string("").is_err());
        assert!(parse_duration_string("24").is_err());
        assert!(parse_duration_string("abc").is_err());
        assert!(parse_duration_string("24x").is_err());
    }

    #[test]
    fn test_parse_duration_invalid_number() {
        assert!(parse_duration_string("abch").is_err());
    }
}
