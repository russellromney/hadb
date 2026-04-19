//! S3LeaseStore: LeaseStore implementation using S3 conditional PUTs.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use hadb_lease::{CasResult, LeaseStore};

use crate::error::{is_not_found, is_precondition_failed};

/// LeaseStore backed by S3 conditional PUTs.
///
/// Uses `if_none_match("*")` for create and `if_match(etag)` for update.
/// Handles 412 PreconditionFailed detection for CAS conflicts.
///
/// # Key layout
///
/// `key_for(scope)` returns `"{prefix}{scope}/_lease.json"`. The prefix is
/// configured at construction via [`with_prefix`]; downstream callers
/// (`read` / `write_if_*` / `delete`) treat their `key` argument as the
/// already-built object key — no further mangling.
///
/// **Retry behavior:** AWS SDK has built-in retry logic with exponential backoff
/// (default: 3 attempts for transient errors like 500, 503). Configure via
/// `aws_config::retry::RetryConfig` when building the S3 client.
pub struct S3LeaseStore {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
}

impl S3LeaseStore {
    pub fn new(client: aws_sdk_s3::Client, bucket: String) -> Self {
        Self { client, bucket, prefix: String::new() }
    }

    /// Set a key prefix used by `key_for`. Scope-named lease objects land
    /// under `{prefix}{scope}/_lease.json` in the bucket.
    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.prefix = prefix.to_string();
        self
    }
}

#[async_trait]
impl LeaseStore for S3LeaseStore {
    /// `{prefix}{scope}/_lease.json` — the legacy compound key the coordinator
    /// previously formatted itself.
    fn key_for(&self, scope: &str) -> String {
        format!("{}{}/_lease.json", self.prefix, scope)
    }

    async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;

        match result {
            Ok(output) => {
                let etag = output
                    .e_tag()
                    .ok_or_else(|| anyhow!("S3 GetObject returned no ETag"))?
                    .to_string();
                let body = output.body.collect().await?.into_bytes().to_vec();
                Ok(Some((body, etag)))
            }
            Err(e) => {
                if is_not_found(&e) {
                    Ok(None)
                } else {
                    Err(anyhow!("S3 GetObject failed: {}", e))
                }
            }
        }
    }

    async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult> {
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(data.into())
            .if_none_match("*")
            .send()
            .await;

        match result {
            Ok(output) => Ok(CasResult {
                success: true,
                etag: output.e_tag().map(|s| s.to_string()),
            }),
            Err(e) if is_precondition_failed(&e) => Ok(CasResult {
                success: false,
                etag: None,
            }),
            Err(e) => Err(anyhow!("S3 PutObject (if-none-match) failed: {}", e)),
        }
    }

    async fn write_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<CasResult> {
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(data.into())
            .if_match(etag)
            .send()
            .await;

        match result {
            Ok(output) => Ok(CasResult {
                success: true,
                etag: output.e_tag().map(|s| s.to_string()),
            }),
            Err(e) if is_precondition_failed(&e) => Ok(CasResult {
                success: false,
                etag: None,
            }),
            Err(e) => Err(anyhow!("S3 PutObject (if-match) failed: {}", e)),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| anyhow!("S3 DeleteObject failed: {}", e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_client() -> aws_sdk_s3::Client {
        let conf = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "test", None, None, "test",
            ))
            .build();
        aws_sdk_s3::Client::from_conf(conf)
    }

    #[test]
    fn key_for_no_prefix() {
        let s = S3LeaseStore::new(dummy_client(), "bkt".into());
        assert_eq!(s.key_for("mydb"), "mydb/_lease.json");
    }

    #[test]
    fn key_for_with_prefix() {
        let s = S3LeaseStore::new(dummy_client(), "bkt".into()).with_prefix("placement/");
        assert_eq!(s.key_for("placement-db"), "placement/placement-db/_lease.json");
    }
}
