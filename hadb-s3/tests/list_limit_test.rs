//! Tests for list() max_keys limit.

use anyhow::Result;
use futures::stream::{self, StreamExt};
use hadb::StorageBackend;
use hadb_s3::S3StorageBackend;
use std::sync::Arc;

async fn s3_client() -> aws_sdk_s3::Client {
    let config = aws_config::load_from_env().await;
    aws_sdk_s3::Client::new(&config)
}

fn test_bucket() -> Option<String> {
    std::env::var("S3_TEST_BUCKET").ok()
}

const CONCURRENCY: usize = 50;

#[tokio::test]
#[ignore] // Requires S3 credentials
async fn test_list_with_limit() -> Result<()> {
    let bucket = test_bucket().expect("S3_TEST_BUCKET not set");
    let client = s3_client().await;
    let storage = Arc::new(S3StorageBackend::new(client, bucket));

    let prefix = format!("test-list-limit-{}/", uuid::Uuid::new_v4());

    // Upload 10 objects concurrently.
    let keys: Vec<String> = (0..10).map(|i| format!("{}{}", prefix, i)).collect();
    stream::iter(keys.clone())
        .for_each_concurrent(CONCURRENCY, |key| {
            let s = storage.clone();
            async move { s.upload(&key, b"data").await.unwrap(); }
        })
        .await;

    // List with limit 5
    let listed = storage.list(&prefix, Some(5)).await?;
    assert_eq!(listed.len(), 5);

    // List with limit 20 (more than available)
    let listed = storage.list(&prefix, Some(20)).await?;
    assert_eq!(listed.len(), 10);

    // List with no limit
    let listed = storage.list(&prefix, None).await?;
    assert_eq!(listed.len(), 10);

    // Cleanup concurrently.
    stream::iter(keys)
        .for_each_concurrent(CONCURRENCY, |key| {
            let s = storage.clone();
            async move { s.delete(&key).await.unwrap(); }
        })
        .await;

    Ok(())
}

#[tokio::test]
#[ignore] // Requires S3 credentials
async fn test_list_pagination_with_limit() -> Result<()> {
    let bucket = test_bucket().expect("S3_TEST_BUCKET not set");
    let client = s3_client().await;
    let storage = Arc::new(S3StorageBackend::new(client, bucket));

    let prefix = format!("test-list-page-{}/", uuid::Uuid::new_v4());

    // 1050 objects — just over one S3 page (1000). Enough to prove pagination.
    let count = 1050;
    let keys: Vec<String> = (0..count).map(|i| format!("{}{:04}", prefix, i)).collect();

    // Upload concurrently.
    stream::iter(keys.clone())
        .for_each_concurrent(CONCURRENCY, |key| {
            let s = storage.clone();
            async move { s.upload(&key, b"data").await.unwrap(); }
        })
        .await;

    // List with limit 500 (within first page)
    let listed = storage.list(&prefix, Some(500)).await?;
    assert_eq!(listed.len(), 500);

    // List with limit 1025 (crosses page boundary)
    let listed = storage.list(&prefix, Some(1025)).await?;
    assert_eq!(listed.len(), 1025);

    // List with no limit (should get all)
    let listed = storage.list(&prefix, None).await?;
    assert_eq!(listed.len(), count);

    // Cleanup concurrently.
    stream::iter(keys)
        .for_each_concurrent(CONCURRENCY, |key| {
            let s = storage.clone();
            async move { s.delete(&key).await.unwrap(); }
        })
        .await;

    Ok(())
}
