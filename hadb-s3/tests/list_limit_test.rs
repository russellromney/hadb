//! Tests for list() max_keys limit.

use anyhow::Result;
use hadb::StorageBackend;
use hadb_s3::S3StorageBackend;

fn s3_client() -> aws_sdk_s3::Client {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let config = aws_config::load_from_env().await;
        aws_sdk_s3::Client::new(&config)
    })
}

fn test_bucket() -> Option<String> {
    std::env::var("S3_TEST_BUCKET").ok()
}

#[tokio::test]
#[ignore] // Requires S3 credentials
async fn test_list_with_limit() -> Result<()> {
    let bucket = test_bucket().expect("S3_TEST_BUCKET not set");
    let client = s3_client();
    let storage = S3StorageBackend::new(client, bucket);

    let prefix = format!("test-list-limit-{}/", uuid::Uuid::new_v4());

    // Upload 10 objects
    for i in 0..10 {
        let key = format!("{}{}", prefix, i);
        storage.upload(&key, b"data").await?;
    }

    // List with limit 5
    let keys = storage.list(&prefix, Some(5)).await?;
    assert_eq!(keys.len(), 5);

    // List with limit 20 (more than available)
    let keys = storage.list(&prefix, Some(20)).await?;
    assert_eq!(keys.len(), 10);

    // List with no limit
    let keys = storage.list(&prefix, None).await?;
    assert_eq!(keys.len(), 10);

    // Cleanup
    for key in keys {
        storage.delete(&key).await?;
    }

    Ok(())
}

#[tokio::test]
#[ignore] // Requires S3 credentials
async fn test_list_pagination_with_limit() -> Result<()> {
    let bucket = test_bucket().expect("S3_TEST_BUCKET not set");
    let client = s3_client();
    let storage = S3StorageBackend::new(client, bucket);

    let prefix = format!("test-list-page-{}/", uuid::Uuid::new_v4());

    // Upload 2500 objects (more than 2 S3 pages at 1000 per page)
    for i in 0..2500 {
        let key = format!("{}{:04}", prefix, i);
        storage.upload(&key, b"data").await?;
    }

    // List with limit 1500 (crosses page boundary)
    let keys = storage.list(&prefix, Some(1500)).await?;
    assert_eq!(keys.len(), 1500);

    // List with no limit (should get all 2500)
    let keys = storage.list(&prefix, None).await?;
    assert_eq!(keys.len(), 2500);

    // Cleanup
    for key in keys {
        storage.delete(&key).await?;
    }

    Ok(())
}
