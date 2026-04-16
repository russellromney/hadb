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

    /// Set a key prefix. All lease operations will prepend this to the key,
    /// scoping leases to a namespace within the bucket.
    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.prefix = prefix.to_string();
        self
    }

    fn prefixed_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}{}", self.prefix, key)
        }
    }
}

#[async_trait]
impl LeaseStore for S3LeaseStore {
    async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        let full_key = self.prefixed_key(key);
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&full_key)
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
        let full_key = self.prefixed_key(key);
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&full_key)
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
        let full_key = self.prefixed_key(key);
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&full_key)
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
        let full_key = self.prefixed_key(key);
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(|e| anyhow!("S3 DeleteObject failed: {}", e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_s3_lease_store_new() {
        // Just verify it compiles
        // Real S3 tests require credentials and are in integration tests
    }
}
