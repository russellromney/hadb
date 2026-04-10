use std::collections::BTreeMap;

use hadb::{LeaseStore, ManifestStore, StorageBackend};
use hadb_lease_s3::{S3LeaseStore, S3StorageBackend};
use hadb_manifest_s3::S3ManifestStore;
use hadb::manifest::{HaManifest, StorageManifest};

fn make_turbo_manifest(page_count: u64) -> HaManifest {
    HaManifest {
        version: 1,
        writer_id: "test-writer".to_string(),
        lease_epoch: 1,
        timestamp_ms: 1234567890,
        storage: StorageManifest::Turbolite { 
            page_count, 
            page_size: 4096,
            pages_per_group: 2048,
            sub_pages_per_frame: 1,
            strategy: "default".to_string(),
            page_group_keys: vec![],
            frame_tables: vec![],
            group_pages: vec![],
            btrees: BTreeMap::new(),
            interior_chunk_keys: BTreeMap::new(),
            index_chunk_keys: BTreeMap::new(),
            subframe_overrides: vec![],
            turbolite_version: 0,
            db_header: None,
        },
    }
}

async fn make_s3_client() -> aws_sdk_s3::Client {
    let config = aws_config::from_env()
        .endpoint_url("http://localhost:9002")
        .region("us-east-1")
        .behavior_version(aws_config::BehaviorVersion::latest())
        .load()
        .await;
    let mut s3_config = aws_sdk_s3::config::Builder::from(&config);
    s3_config = s3_config.force_path_style(true);
    aws_sdk_s3::Client::from_conf(s3_config.build())
}

#[tokio::test]
async fn s3_lease_store_roundtrip() {
    let client = make_s3_client().await;
    let store = S3LeaseStore::new(client.clone(), "test-bucket".into());

    let lease_key = "test/integration/lease";
    let lease_data = serde_json::to_vec(&serde_json::json!({
        "instance_id": "test-leader",
        "timestamp": 1234567890u64,
        "ttl_secs": 10u64,
    })).unwrap();

    let result = store.write_if_not_exists(lease_key, lease_data.clone()).await;
    assert!(result.is_ok(), "first write should succeed: {:?}", result);
    assert!(result.unwrap().success, "first write should succeed");

    let result = store.write_if_not_exists(lease_key, lease_data).await;
    assert!(result.is_ok(), "second write should not panic");
    assert!(!result.unwrap().success, "second write should fail");

    let delete = store.delete(lease_key).await;
    assert!(delete.is_ok(), "delete should succeed");
}

#[tokio::test]
async fn s3_lease_store_cas() {
    let client = make_s3_client().await;
    let store = S3LeaseStore::new(client.clone(), "test-bucket".into());

    let lease_key = "test/integration/cas";
    let v1_data = serde_json::to_vec(&serde_json::json!({"version": 1})).unwrap();
    let v2_data = serde_json::to_vec(&serde_json::json!({"version": 2})).unwrap();

    let initial = store.write_if_not_exists(lease_key, v1_data).await.unwrap();
    assert!(initial.success);

    let read = store.read(lease_key).await.unwrap();
    let version = read.unwrap().1;

    let cas_result = store.write_if_match(lease_key, v2_data, &version).await;
    assert!(cas_result.is_ok(), "CAS should succeed");
    assert!(cas_result.unwrap().success);
}

#[tokio::test]
async fn s3_manifest_store_roundtrip() {
    let client = make_s3_client().await;
    let store = S3ManifestStore::new(client.clone(), "test-bucket".into());

    let key = "test/integration/manifest";
    let manifest = make_turbo_manifest(0);

    let result = store.put(key, &manifest, None).await;
    assert!(result.is_ok(), "put should succeed: {:?}", result);
}

#[tokio::test]
async fn s3_manifest_store_cas() {
    let client = make_s3_client().await;
    let store = S3ManifestStore::new(client.clone(), "test-bucket".into());

    let key = "test/integration/manifest-cas";
    store.put(key, &make_turbo_manifest(0), None).await.unwrap();

    let meta = store.meta(key).await.unwrap().unwrap();
    let mut v2 = make_turbo_manifest(1);
    v2.version = 2;
    v2.writer_id = "writer-2".to_string();
    v2.timestamp_ms = 2;

    let cas_result = store.put(key, &v2, Some(meta.version)).await;
    assert!(cas_result.is_ok(), "CAS should succeed");
}

#[tokio::test]
async fn s3_storage_backend_upload_download() {
    let client = make_s3_client().await;
    let storage = S3StorageBackend::new(client.clone(), "test-bucket".into());

    let key = "test/integration/data.bin";
    let data = b"Hello, S3 integration test!".to_vec();

    let upload = storage.upload(key, &data).await;
    assert!(upload.is_ok(), "upload should succeed: {:?}", upload);

    let delete = storage.delete(key).await;
    assert!(delete.is_ok(), "delete should succeed");
}

#[tokio::test]
async fn s3_storage_backend_list() {
    let client = make_s3_client().await;
    let storage = S3StorageBackend::new(client.clone(), "test-bucket".into());

    let prefix = "test/integration/list/";
    let keys = vec![
        format!("{}a", prefix),
        format!("{}b", prefix),
        format!("{}c", prefix),
    ];

    for key in &keys {
        storage.upload(key, b"test").await.unwrap();
    }

    let list = storage.list(prefix, None).await;
    assert!(list.is_ok(), "list should succeed");
    assert_eq!(list.unwrap().len(), 3);

    for key in &keys {
        storage.delete(key).await.ok();
    }
}