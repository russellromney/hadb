//! S3 client helpers for the hadb ecosystem.
//!
//! Provides a set of utility functions for interacting with S3-compatible storage
//! (AWS S3, Tigris, MinIO, Cloudflare R2, Wasabi, etc.).
//!
//! Feature-gated behind `s3` (enabled by default).

#![cfg(feature = "s3")]

use anyhow::{anyhow, Result};
use aws_sdk_s3::{primitives::ByteStream, Client};
use std::path::Path;

/// Parse bucket string like "s3://bucket-name/prefix" or just "bucket-name".
/// Returns (bucket_name, prefix) where prefix includes trailing slash if non-empty.
pub fn parse_bucket(bucket: &str) -> (String, String) {
    let bucket = bucket.strip_prefix("s3://").unwrap_or(bucket);
    if let Some((bucket, prefix)) = bucket.split_once('/') {
        let prefix = if prefix.is_empty() {
            String::new()
        } else if prefix.ends_with('/') {
            prefix.to_string()
        } else {
            format!("{}/", prefix)
        };
        (bucket.to_string(), prefix)
    } else {
        (bucket.to_string(), String::new())
    }
}

/// Create S3 client with optional custom endpoint (for Tigris/MinIO).
pub async fn create_client(endpoint: Option<&str>) -> Result<Client> {
    // Load base config for credentials + region.
    // Don't set endpoint on aws_config -- set it on S3-specific config to avoid
    // AWS_ENDPOINT_URL_S3 env var conflicts with virtual-hosted-style URLs.
    let base_config = aws_config::from_env().load().await;
    let mut s3_config = aws_sdk_s3::config::Builder::from(&base_config);
    if let Some(endpoint) = endpoint {
        s3_config = s3_config.endpoint_url(endpoint).force_path_style(true);
    }
    Ok(Client::from_conf(s3_config.build()))
}

/// Upload bytes to S3.
pub async fn upload_bytes(client: &Client, bucket: &str, key: &str, data: Vec<u8>) -> Result<()> {
    let len = data.len();
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(data))
        .send()
        .await?;

    tracing::debug!("Uploaded {} bytes to s3://{}/{}", len, bucket, key);
    Ok(())
}

/// Upload a file to S3.
pub async fn upload_file(client: &Client, bucket: &str, key: &str, path: &Path) -> Result<()> {
    let body = ByteStream::from_path(path).await?;

    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await?;

    tracing::debug!("Uploaded {} to s3://{}/{}", path.display(), bucket, key);
    Ok(())
}

/// Download bytes from S3.
pub async fn download_bytes(client: &Client, bucket: &str, key: &str) -> Result<Vec<u8>> {
    let resp = client.get_object().bucket(bucket).key(key).send().await?;

    let data = resp.body.collect().await?.into_bytes().to_vec();
    tracing::debug!(
        "Downloaded {} bytes from s3://{}/{}",
        data.len(),
        bucket,
        key
    );
    Ok(data)
}

/// Download to file.
pub async fn download_file(client: &Client, bucket: &str, key: &str, path: &Path) -> Result<()> {
    let data = download_bytes(client, bucket, key).await?;
    tokio::fs::write(path, &data).await?;
    Ok(())
}

/// List objects with prefix, paginating through all results.
pub async fn list_objects(client: &Client, bucket: &str, prefix: &str) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut req = client.list_objects_v2().bucket(bucket).prefix(prefix);

        if let Some(token) = &continuation_token {
            req = req.continuation_token(token);
        }

        let resp = req.send().await?;

        if let Some(contents) = resp.contents {
            for obj in contents {
                if let Some(key) = obj.key {
                    keys.push(key);
                }
            }
        }

        if resp.is_truncated.unwrap_or(false) {
            continuation_token = resp.next_continuation_token;
        } else {
            break;
        }
    }

    Ok(keys)
}

/// List objects with prefix, starting after a specific key (exclusive).
/// Uses S3's `start_after` parameter for efficient pagination —
/// S3 skips directly to the right position in the index, no scanning.
pub async fn list_objects_after(
    client: &Client,
    bucket: &str,
    prefix: &str,
    start_after: &str,
) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut req = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .start_after(start_after);

        if let Some(token) = &continuation_token {
            req = req.continuation_token(token);
        }

        let resp = req.send().await?;

        if let Some(contents) = resp.contents {
            for obj in contents {
                if let Some(key) = obj.key {
                    keys.push(key);
                }
            }
        }

        if resp.is_truncated.unwrap_or(false) {
            continuation_token = resp.next_continuation_token;
        } else {
            break;
        }
    }

    Ok(keys)
}

/// Upload bytes with SHA256 metadata for integrity verification.
pub async fn upload_bytes_with_checksum(
    client: &Client,
    bucket: &str,
    key: &str,
    data: Vec<u8>,
    checksum: &str,
) -> Result<()> {
    let len = data.len();
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .metadata("x-amz-meta-sha256", checksum)
        .body(ByteStream::from(data))
        .send()
        .await?;

    tracing::debug!(
        "Uploaded {} bytes to s3://{}/{} with checksum {}",
        len,
        bucket,
        key,
        checksum
    );
    Ok(())
}

/// Upload file with SHA256 metadata.
pub async fn upload_file_with_checksum(
    client: &Client,
    bucket: &str,
    key: &str,
    path: &Path,
    checksum: &str,
) -> Result<()> {
    let body = ByteStream::from_path(path).await?;

    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .metadata("x-amz-meta-sha256", checksum)
        .body(body)
        .send()
        .await?;

    tracing::debug!(
        "Uploaded {} to s3://{}/{} with checksum {}",
        path.display(),
        bucket,
        key,
        checksum
    );
    Ok(())
}

/// Get SHA256 checksum from object metadata.
pub async fn get_checksum(client: &Client, bucket: &str, key: &str) -> Result<Option<String>> {
    match client.head_object().bucket(bucket).key(key).send().await {
        Ok(resp) => {
            if let Some(metadata) = resp.metadata {
                if let Some(checksum) = metadata.get("x-amz-meta-sha256") {
                    return Ok(Some(checksum.clone()));
                }
            }
            Ok(None)
        }
        Err(_) => Ok(None),
    }
}

/// Check if object exists via HeadObject.
pub async fn exists(client: &Client, bucket: &str, key: &str) -> Result<bool> {
    use aws_sdk_s3::error::SdkError;
    use aws_sdk_s3::operation::head_object::HeadObjectError;

    match client.head_object().bucket(bucket).key(key).send().await {
        Ok(_) => Ok(true),
        Err(SdkError::ServiceError(err)) => match err.err() {
            HeadObjectError::NotFound(_) => Ok(false),
            _ => {
                let msg = format!("{:?}", err.err());
                if msg.contains("NotFound") || msg.contains("404") || msg.contains("NoSuchKey") {
                    Ok(false)
                } else {
                    Err(anyhow!("Failed to check object existence: {:?}", err.err()))
                }
            }
        },
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("NotFound") || msg.contains("404") || msg.contains("NoSuchKey") {
                Ok(false)
            } else {
                Err(anyhow!("Failed to check object existence: {}", e))
            }
        }
    }
}

/// Delete a single object from S3.
pub async fn delete_object(client: &Client, bucket: &str, key: &str) -> Result<()> {
    client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    tracing::debug!("Deleted s3://{}/{}", bucket, key);
    Ok(())
}

/// Delete multiple objects from S3 (batch operation).
/// Uses DeleteObjects API for efficiency (up to 1000 objects per request).
/// Returns the number of successfully deleted objects.
pub async fn delete_objects(client: &Client, bucket: &str, keys: &[String]) -> Result<usize> {
    use aws_sdk_s3::types::{Delete, ObjectIdentifier};

    if keys.is_empty() {
        return Ok(0);
    }

    let mut total_deleted = 0;

    for chunk in keys.chunks(1000) {
        let objects: Vec<ObjectIdentifier> = chunk
            .iter()
            .filter_map(|key| ObjectIdentifier::builder().key(key).build().ok())
            .collect();

        if objects.is_empty() {
            continue;
        }

        let delete = Delete::builder()
            .set_objects(Some(objects))
            .quiet(true)
            .build()?;

        let result = client
            .delete_objects()
            .bucket(bucket)
            .delete(delete)
            .send()
            .await?;

        let error_count = result.errors().len();
        let deleted_count = chunk.len() - error_count;
        total_deleted += deleted_count;

        if error_count > 0 {
            tracing::warn!(
                "Failed to delete {} objects from s3://{}/",
                error_count,
                bucket
            );
            for err in result.errors() {
                tracing::debug!(
                    "Delete error: {} - {}",
                    err.key().unwrap_or("unknown"),
                    err.message().unwrap_or("unknown error")
                );
            }
        }

        tracing::debug!("Deleted {} objects from s3://{}/", deleted_count, bucket);
    }

    Ok(total_deleted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bucket_simple() {
        let (bucket, prefix) = parse_bucket("my-bucket");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
    }

    #[test]
    fn test_parse_bucket_with_prefix() {
        let (bucket, prefix) = parse_bucket("my-bucket/some/prefix");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "some/prefix/");
    }

    #[test]
    fn test_parse_bucket_s3_url() {
        let (bucket, prefix) = parse_bucket("s3://my-bucket/backups");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "backups/");
    }

    #[test]
    fn test_parse_bucket_s3_url_no_prefix() {
        let (bucket, prefix) = parse_bucket("s3://my-bucket");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
    }

    #[test]
    fn test_parse_bucket_with_trailing_slash() {
        let (bucket, prefix) = parse_bucket("s3://my-bucket/prefix/");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "prefix/");
    }

    #[test]
    fn test_parse_bucket_empty_prefix_after_slash() {
        let (bucket, prefix) = parse_bucket("my-bucket/");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
    }
}
