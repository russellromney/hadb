//! S3ManifestStore: ManifestStore implementation using S3 conditional PUTs.
//!
//! Stores the full HaManifest as msgpack in the S3 object body.
//! Stores version, writer_id, and lease_epoch in S3 custom metadata headers
//! so that `meta()` can use HeadObject without downloading the body.
//!
//! CAS uses S3 ETag conditional writes (If-Match / If-None-Match).
//! The manifest version is managed client-side; ETag prevents races.

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use hadb::{CasResult, HaManifest, ManifestMeta, ManifestStore};

use crate::error::{is_not_found, is_precondition_failed};

const META_VERSION: &str = "manifest-version";
const META_WRITER_ID: &str = "manifest-writer-id";
const META_LEASE_EPOCH: &str = "manifest-lease-epoch";

/// ManifestStore backed by S3 conditional PUTs.
///
/// - `get`: GetObject, deserialize msgpack body
/// - `put`: PutObject with custom metadata headers, If-Match/If-None-Match for CAS
/// - `meta`: HeadObject, read version/writer_id/lease_epoch from metadata headers
pub struct S3ManifestStore {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl S3ManifestStore {
    pub fn new(client: aws_sdk_s3::Client, bucket: String) -> Self {
        Self { client, bucket }
    }
}

#[async_trait]
impl ManifestStore for S3ManifestStore {
    async fn get(&self, key: &str) -> Result<Option<HaManifest>> {
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
                let manifest: HaManifest = rmp_serde::from_slice(&body)
                    .map_err(|e| anyhow!("failed to deserialize manifest: {}", e))?;
                Ok(Some(manifest))
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

    async fn put(
        &self,
        key: &str,
        manifest: &HaManifest,
        expected_version: Option<u64>,
    ) -> Result<CasResult> {
        let new_version = match expected_version {
            None => 1u64,
            Some(v) => v + 1,
        };

        let mut stored = manifest.clone();
        stored.version = new_version;

        let body = rmp_serde::to_vec(&stored)
            .map_err(|e| anyhow!("failed to serialize manifest: {}", e))?;

        let metadata: HashMap<String, String> = HashMap::from([
            (META_VERSION.to_string(), new_version.to_string()),
            (META_WRITER_ID.to_string(), stored.writer_id.clone()),
            (META_LEASE_EPOCH.to_string(), stored.lease_epoch.to_string()),
        ]);

        // To do CAS we need the current ETag first, then use If-Match.
        // For "must not exist" (expected_version: None), use If-None-Match: *.
        match expected_version {
            None => {
                let result = self
                    .client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .body(body.into())
                    .set_metadata(Some(metadata))
                    .content_type("application/msgpack")
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
                    Err(e) => Err(anyhow!("S3 PutObject (create) failed: {}", e)),
                }
            }
            Some(expected) => {
                // First, get the current ETag via HeadObject.
                let head = self
                    .client
                    .head_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .send()
                    .await;

                let (current_etag, current_version) = match head {
                    Ok(output) => {
                        let etag = output
                            .e_tag()
                            .ok_or_else(|| anyhow!("S3 HeadObject returned no ETag"))?
                            .to_string();
                        let version = output
                            .metadata()
                            .and_then(|m| m.get(META_VERSION))
                            .and_then(|v| v.parse::<u64>().ok())
                            .ok_or_else(|| anyhow!("S3 HeadObject missing manifest-version metadata"))?;
                        (etag, version)
                    }
                    Err(e) => {
                        if is_not_found(&e) {
                            // Key doesn't exist, but caller expected a version.
                            return Ok(CasResult {
                                success: false,
                                etag: None,
                            });
                        }
                        return Err(anyhow!("S3 HeadObject failed: {}", e));
                    }
                };

                // Check version matches what caller expects.
                if current_version != expected {
                    return Ok(CasResult {
                        success: false,
                        etag: None,
                    });
                }

                // Conditional PUT with If-Match on current ETag.
                let result = self
                    .client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .body(body.into())
                    .set_metadata(Some(metadata))
                    .content_type("application/msgpack")
                    .if_match(&current_etag)
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
                    Err(e) => Err(anyhow!("S3 PutObject (update) failed: {}", e)),
                }
            }
        }
    }

    async fn meta(&self, key: &str) -> Result<Option<ManifestMeta>> {
        let result = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;

        match result {
            Ok(output) => {
                let metadata = output.metadata()
                    .ok_or_else(|| anyhow!("S3 HeadObject returned no metadata"))?;

                let version = metadata
                    .get(META_VERSION)
                    .ok_or_else(|| anyhow!("missing {} header", META_VERSION))?
                    .parse::<u64>()
                    .map_err(|e| anyhow!("invalid {} header: {}", META_VERSION, e))?;

                let writer_id = metadata
                    .get(META_WRITER_ID)
                    .ok_or_else(|| anyhow!("missing {} header", META_WRITER_ID))?
                    .clone();

                let lease_epoch = metadata
                    .get(META_LEASE_EPOCH)
                    .ok_or_else(|| anyhow!("missing {} header", META_LEASE_EPOCH))?
                    .parse::<u64>()
                    .map_err(|e| anyhow!("invalid {} header: {}", META_LEASE_EPOCH, e))?;

                Ok(Some(ManifestMeta {
                    version,
                    writer_id,
                    lease_epoch,
                }))
            }
            Err(e) => {
                if is_not_found(&e) {
                    Ok(None)
                } else {
                    Err(anyhow!("S3 HeadObject failed: {}", e))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hadb::{StorageManifest, FrameEntry, BTreeInfo};
    use std::collections::BTreeMap;

    fn make_manifest(writer: &str, epoch: u64) -> HaManifest {
        HaManifest {
            version: 0,
            writer_id: writer.to_string(),
            lease_epoch: epoch,
            timestamp_ms: 1000,
            storage: StorageManifest::Walrust {
                txid: 1,
                changeset_prefix: "cs/".to_string(),
                latest_changeset_key: "cs/1".to_string(),
                snapshot_key: None,
                snapshot_txid: None,
            },
        }
    }

    fn make_turbolite_manifest(writer: &str, epoch: u64) -> HaManifest {
        HaManifest {
            version: 0,
            writer_id: writer.to_string(),
            lease_epoch: epoch,
            timestamp_ms: 2000,
            storage: StorageManifest::Turbolite {
                page_count: 100,
                page_size: 4096,
                page_group_keys: vec!["pg-0".to_string()],
                frame_tables: vec![vec![FrameEntry { page_number: 1, frame_offset: 0 }]],
                group_pages: vec![vec![1, 2, 3]],
                btrees: BTreeMap::from([(1, BTreeInfo { root_page: 1, depth: 2 })]),
                interior_chunk_keys: vec!["ic-0".to_string()],
                index_chunk_keys: vec!["idx-0".to_string()],
            },
        }
    }

    /// Test fixture: connects to S3 using AWS_* env vars.
    /// Requires S3_BUCKET env var. Returns None if not set.
    struct TestFixture {
        store: S3ManifestStore,
        prefix: String,
        client: aws_sdk_s3::Client,
        bucket: String,
    }

    impl TestFixture {
        async fn new() -> Option<Self> {
            let bucket = match std::env::var("S3_BUCKET") {
                Ok(b) => b,
                Err(_) => return None,
            };
            let prefix = format!(
                "test-manifest-{}/",
                uuid::Uuid::new_v4().to_string().replace('-', "")
            );
            let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            let client = aws_sdk_s3::Client::new(&config);
            let store = S3ManifestStore::new(client.clone(), bucket.clone());
            Some(Self {
                store,
                prefix,
                client,
                bucket,
            })
        }

        fn key(&self, name: &str) -> String {
            format!("{}{}", self.prefix, name)
        }

        async fn cleanup(self) {
            // List and delete all objects under the test prefix.
            let list = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&self.prefix)
                .send()
                .await;
            if let Ok(output) = list {
                for obj in output.contents() {
                    if let Some(key) = obj.key() {
                        let _ = self
                            .client
                            .delete_object()
                            .bucket(&self.bucket)
                            .key(key)
                            .send()
                            .await;
                    }
                }
            }
        }
    }

    impl std::ops::Deref for TestFixture {
        type Target = S3ManifestStore;
        fn deref(&self) -> &Self::Target {
            &self.store
        }
    }

    // ========================================================================
    // Happy path
    // ========================================================================

    #[tokio::test]
    async fn test_get_nonexistent() {
        let Some(f) = TestFixture::new().await else { return };
        let result = f.get(&f.key("nope")).await.unwrap();
        assert!(result.is_none());
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_create_and_get() {
        let Some(f) = TestFixture::new().await else { return };
        let m = make_manifest("node-1", 1);
        let key = f.key("db1");

        let res = f.put(&key, &m, None).await.unwrap();
        assert!(res.success);
        assert!(res.etag.is_some());

        let fetched = f.get(&key).await.unwrap().expect("should exist");
        assert_eq!(fetched.version, 1);
        assert_eq!(fetched.writer_id, "node-1");
        assert_eq!(fetched.lease_epoch, 1);
        assert_eq!(fetched.timestamp_ms, 1000);
        assert!(matches!(fetched.storage, StorageManifest::Walrust { .. }));
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_create_turbolite_roundtrip() {
        let Some(f) = TestFixture::new().await else { return };
        let m = make_turbolite_manifest("node-1", 1);
        let key = f.key("db-turbo");

        f.put(&key, &m, None).await.unwrap();
        let fetched = f.get(&key).await.unwrap().expect("should exist");
        match &fetched.storage {
            StorageManifest::Turbolite {
                page_count,
                page_size,
                page_group_keys,
                frame_tables,
                ..
            } => {
                assert_eq!(*page_count, 100);
                assert_eq!(*page_size, 4096);
                assert_eq!(page_group_keys, &vec!["pg-0".to_string()]);
                assert_eq!(frame_tables.len(), 1);
            }
            _ => panic!("expected Turbolite"),
        }
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_update_with_correct_version() {
        let Some(f) = TestFixture::new().await else { return };
        let key = f.key("db1");

        f.put(&key, &make_manifest("node-1", 1), None).await.unwrap();

        let res = f.put(&key, &make_manifest("node-1", 2), Some(1)).await.unwrap();
        assert!(res.success);

        let fetched = f.get(&key).await.unwrap().expect("should exist");
        assert_eq!(fetched.version, 2);
        assert_eq!(fetched.lease_epoch, 2);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_meta_returns_correct_fields() {
        let Some(f) = TestFixture::new().await else { return };
        let key = f.key("db1");

        f.put(&key, &make_manifest("node-42", 7), None).await.unwrap();

        let meta = f.meta(&key).await.unwrap().expect("should exist");
        assert_eq!(meta.version, 1);
        assert_eq!(meta.writer_id, "node-42");
        assert_eq!(meta.lease_epoch, 7);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_meta_nonexistent() {
        let Some(f) = TestFixture::new().await else { return };
        let result = f.meta(&f.key("nope")).await.unwrap();
        assert!(result.is_none());
        f.cleanup().await;
    }

    // ========================================================================
    // Failure modes
    // ========================================================================

    #[tokio::test]
    async fn test_put_create_conflict() {
        let Some(f) = TestFixture::new().await else { return };
        let key = f.key("db1");

        let first = f.put(&key, &make_manifest("node-1", 1), None).await.unwrap();
        assert!(first.success);

        let second = f.put(&key, &make_manifest("node-2", 1), None).await.unwrap();
        assert!(!second.success);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_stale_version() {
        let Some(f) = TestFixture::new().await else { return };
        let key = f.key("db1");

        f.put(&key, &make_manifest("node-1", 1), None).await.unwrap();
        f.put(&key, &make_manifest("node-1", 1), Some(1)).await.unwrap();

        // Stale version 1 (current is 2)
        let res = f.put(&key, &make_manifest("node-1", 1), Some(1)).await.unwrap();
        assert!(!res.success);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_version_on_nonexistent() {
        let Some(f) = TestFixture::new().await else { return };
        let key = f.key("db1");

        let res = f.put(&key, &make_manifest("node-1", 1), Some(1)).await.unwrap();
        assert!(!res.success);
        f.cleanup().await;
    }

    // ========================================================================
    // Edge cases
    // ========================================================================

    #[tokio::test]
    async fn test_sequential_version_increments() {
        let Some(f) = TestFixture::new().await else { return };
        let key = f.key("db1");

        f.put(&key, &make_manifest("node-1", 1), None).await.unwrap();
        assert_eq!(f.get(&key).await.unwrap().unwrap().version, 1);

        f.put(&key, &make_manifest("node-1", 1), Some(1)).await.unwrap();
        assert_eq!(f.get(&key).await.unwrap().unwrap().version, 2);

        f.put(&key, &make_manifest("node-1", 1), Some(2)).await.unwrap();
        assert_eq!(f.get(&key).await.unwrap().unwrap().version, 3);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_meta_updates_after_put() {
        let Some(f) = TestFixture::new().await else { return };
        let key = f.key("db1");

        f.put(&key, &make_manifest("node-1", 1), None).await.unwrap();
        let meta1 = f.meta(&key).await.unwrap().unwrap();
        assert_eq!(meta1.version, 1);
        assert_eq!(meta1.writer_id, "node-1");

        f.put(&key, &make_manifest("node-2", 5), Some(1)).await.unwrap();
        let meta2 = f.meta(&key).await.unwrap().unwrap();
        assert_eq!(meta2.version, 2);
        assert_eq!(meta2.writer_id, "node-2");
        assert_eq!(meta2.lease_epoch, 5);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_multiple_keys_independent() {
        let Some(f) = TestFixture::new().await else { return };
        let k1 = f.key("db1");
        let k2 = f.key("db2");

        f.put(&k1, &make_manifest("node-1", 1), None).await.unwrap();
        f.put(&k2, &make_turbolite_manifest("node-2", 2), None).await.unwrap();

        let m1 = f.get(&k1).await.unwrap().unwrap();
        let m2 = f.get(&k2).await.unwrap().unwrap();
        assert_eq!(m1.writer_id, "node-1");
        assert_eq!(m2.writer_id, "node-2");
        assert!(matches!(m1.storage, StorageManifest::Walrust { .. }));
        assert!(matches!(m2.storage, StorageManifest::Turbolite { .. }));
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_ignores_caller_version() {
        let Some(f) = TestFixture::new().await else { return };
        let key = f.key("db1");

        let mut m = make_manifest("node-1", 1);
        m.version = 999;
        f.put(&key, &m, None).await.unwrap();

        let fetched = f.get(&key).await.unwrap().unwrap();
        assert_eq!(fetched.version, 1, "store must assign version 1, not caller's 999");
        f.cleanup().await;
    }
}
