//! S3StorageBackend: StorageBackend implementation using S3.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use hadb::StorageBackend;

/// StorageBackend backed by S3.
///
/// Used for replication data (WAL frames, journal entries, snapshots).
/// Separate from S3LeaseStore which is for small CAS lease operations.
///
/// **Retry behavior:** AWS SDK has built-in retry logic with exponential backoff
/// (default: 3 attempts for transient errors like 500, 503). Configure via
/// `aws_config::retry::RetryConfig` when building the S3 client.
pub struct S3StorageBackend {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl S3StorageBackend {
    pub fn new(client: aws_sdk_s3::Client, bucket: String) -> Self {
        Self { client, bucket }
    }

    /// Check if an S3 error is a 404 NoSuchKey (key doesn't exist).
    fn is_not_found(
        err: &aws_sdk_s3::error::SdkError<impl ProvideErrorMetadata + std::fmt::Debug>,
    ) -> bool {
        if let Some(service_err) = err.as_service_error() {
            if let Some(code) = service_err.code() {
                if code == "NoSuchKey" || code == "NotFound" {
                    return true;
                }
            }
        }
        if let Some(raw) = err.raw_response() {
            if raw.status().as_u16() == 404 {
                return true;
            }
        }
        false
    }
}

#[async_trait]
impl StorageBackend for S3StorageBackend {
    async fn upload(&self, key: &str, data: &[u8]) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(data.to_vec().into())
            .send()
            .await
            .map_err(|e| anyhow!("S3 PutObject failed: {}", e))?;
        Ok(())
    }

    async fn download(&self, key: &str) -> Result<Vec<u8>> {
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;

        match result {
            Ok(output) => {
                let body = output.body.collect().await?.into_bytes().to_vec();
                Ok(body)
            }
            Err(e) => {
                if Self::is_not_found(&e) {
                    Err(anyhow!("Key not found: {}", key))
                } else {
                    Err(anyhow!("S3 GetObject failed: {}", e))
                }
            }
        }
    }

    async fn list(&self, prefix: &str, max_keys: Option<usize>) -> Result<Vec<String>> {
        let mut result = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            // Check if we've hit the limit
            if let Some(max) = max_keys {
                if result.len() >= max {
                    break;
                }
            }

            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);

            // Set page size - cap at 1000 (S3 max) or remaining keys
            if let Some(max) = max_keys {
                let remaining = max.saturating_sub(result.len());
                let page_size = remaining.min(1000) as i32;
                request = request.max_keys(page_size);
            }

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request
                .send()
                .await
                .map_err(|e| anyhow!("S3 ListObjectsV2 failed: {}", e))?;

            // response.contents() returns &[Object], not Option<&[Object]>
            for object in response.contents() {
                if let Some(key) = object.key() {
                    result.push(key.to_string());
                    // Stop if we've hit the limit mid-page
                    if let Some(max) = max_keys {
                        if result.len() >= max {
                            break;
                        }
                    }
                }
            }

            // Check if there are more pages
            if response.is_truncated() == Some(true) {
                continuation_token = response.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        Ok(result)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        // Idempotent - no error if key doesn't exist
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
    #[test]
    fn test_s3_storage_backend_new() {
        // Just verify it compiles
        // Real S3 tests require credentials and are in integration tests
    }
}
