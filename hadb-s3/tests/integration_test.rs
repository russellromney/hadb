//! Integration tests for hadb-s3.
//!
//! These tests require AWS credentials or a LocalStack instance.
//! By default, they are skipped. To run them:
//!
//! ```bash
//! # Set S3_TEST_BUCKET environment variable
//! export S3_TEST_BUCKET=my-test-bucket
//! export AWS_REGION=us-east-1
//! export AWS_ACCESS_KEY_ID=...
//! export AWS_SECRET_ACCESS_KEY=...
//! cargo test --package hadb-s3 --tests -- --ignored
//! ```

use anyhow::Result;
use hadb::{LeaseStore, StorageBackend};
use hadb_s3::{S3LeaseStore, S3StorageBackend};

async fn s3_client() -> aws_sdk_s3::Client {
    let config = aws_config::load_from_env().await;
    aws_sdk_s3::Client::new(&config)
}

fn test_bucket() -> Option<String> {
    std::env::var("S3_TEST_BUCKET").ok()
}

#[tokio::test]
#[ignore] // Requires S3 credentials
async fn test_s3_lease_store_create_and_read() -> Result<()> {
    let bucket = test_bucket().expect("S3_TEST_BUCKET not set");
    let client = s3_client().await;
    let store = S3LeaseStore::new(client, bucket);

    let key = format!("test-lease-{}", uuid::Uuid::new_v4());

    // Read non-existent key
    let result = store.read(&key).await?;
    assert!(result.is_none());

    // Write
    let cas_result = store.write_if_not_exists(&key, b"test-data".to_vec()).await?;
    assert!(cas_result.success);
    assert!(cas_result.etag.is_some());

    // Read back
    let result = store.read(&key).await?;
    assert!(result.is_some());
    let (data, etag) = result.unwrap();
    assert_eq!(data, b"test-data");
    assert_eq!(etag, cas_result.etag.unwrap());

    // Cleanup
    store.delete(&key).await?;

    Ok(())
}

#[tokio::test]
#[ignore] // Requires S3 credentials
async fn test_s3_lease_store_cas_conflict() -> Result<()> {
    let bucket = test_bucket().expect("S3_TEST_BUCKET not set");
    let client = s3_client().await;
    let store = S3LeaseStore::new(client, bucket);

    let key = format!("test-lease-{}", uuid::Uuid::new_v4());

    // First write succeeds
    let result1 = store.write_if_not_exists(&key, b"node1".to_vec()).await?;
    assert!(result1.success);

    // Second write fails (key exists)
    let result2 = store.write_if_not_exists(&key, b"node2".to_vec()).await?;
    assert!(!result2.success);

    // Cleanup
    store.delete(&key).await?;

    Ok(())
}

#[tokio::test]
#[ignore] // Requires S3 credentials
async fn test_s3_lease_store_cas_update() -> Result<()> {
    let bucket = test_bucket().expect("S3_TEST_BUCKET not set");
    let client = s3_client().await;
    let store = S3LeaseStore::new(client, bucket);

    let key = format!("test-lease-{}", uuid::Uuid::new_v4());

    // Create
    let result = store.write_if_not_exists(&key, b"v1".to_vec()).await?;
    assert!(result.success);
    let etag1 = result.etag.unwrap();

    // Update with correct etag
    let result = store.write_if_match(&key, b"v2".to_vec(), &etag1).await?;
    assert!(result.success);
    let etag2 = result.etag.unwrap();
    assert_ne!(etag1, etag2);

    // Update with stale etag fails
    let result = store.write_if_match(&key, b"v3".to_vec(), &etag1).await?;
    assert!(!result.success);

    // Cleanup
    store.delete(&key).await?;

    Ok(())
}

#[tokio::test]
#[ignore] // Requires S3 credentials
async fn test_s3_storage_backend_upload_download() -> Result<()> {
    let bucket = test_bucket().expect("S3_TEST_BUCKET not set");
    let client = s3_client().await;
    let storage = S3StorageBackend::new(client, bucket);

    let key = format!("test-data-{}", uuid::Uuid::new_v4());
    let data = b"test data payload";

    // Upload
    storage.upload(&key, data).await?;

    // Download
    let downloaded = storage.download(&key).await?;
    assert_eq!(downloaded, data);

    // Cleanup
    storage.delete(&key).await?;

    Ok(())
}

#[tokio::test]
#[ignore] // Requires S3 credentials
async fn test_s3_storage_backend_list() -> Result<()> {
    let bucket = test_bucket().expect("S3_TEST_BUCKET not set");
    let client = s3_client().await;
    let storage = S3StorageBackend::new(client, bucket);

    let prefix = format!("test-prefix-{}/", uuid::Uuid::new_v4());

    // Upload multiple objects
    for i in 0..3 {
        let key = format!("{}{}", prefix, i);
        storage.upload(&key, b"data").await?;
    }

    // List (no limit)
    let keys = storage.list(&prefix, None).await?;
    assert_eq!(keys.len(), 3);

    // Cleanup
    for key in keys {
        storage.delete(&key).await?;
    }

    Ok(())
}

#[tokio::test]
#[ignore] // Requires S3 credentials
async fn test_s3_storage_backend_download_not_found() -> Result<()> {
    let bucket = test_bucket().expect("S3_TEST_BUCKET not set");
    let client = s3_client().await;
    let storage = S3StorageBackend::new(client, bucket);

    let key = format!("non-existent-{}", uuid::Uuid::new_v4());

    // Download should return error
    let result = storage.download(&key).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Key not found"));

    Ok(())
}

#[tokio::test]
#[ignore] // Requires S3 credentials
async fn test_s3_storage_backend_delete_idempotent() -> Result<()> {
    let bucket = test_bucket().expect("S3_TEST_BUCKET not set");
    let client = s3_client().await;
    let storage = S3StorageBackend::new(client, bucket);

    let key = format!("test-delete-{}", uuid::Uuid::new_v4());

    // Delete non-existent key (should succeed)
    storage.delete(&key).await?;

    // Create and delete
    storage.upload(&key, b"data").await?;
    storage.delete(&key).await?;

    // Delete again (should still succeed)
    storage.delete(&key).await?;

    Ok(())
}
