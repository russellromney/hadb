//! RedisManifestStore: ManifestStore implementation using Redis with Lua scripts.
//!
//! Full manifest stored as msgpack value in a Redis key. CAS via Lua
//! script: atomically read current value, check version, set if match.
//! `meta()` fetches the full value and extracts ManifestMeta (Redis GET
//! is fast enough).

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use hadb_storage::CasResult;
use redis::AsyncCommands;
use turbodb::{Manifest, ManifestMeta, ManifestStore};

/// Lua script for atomic first publish that also sets the version side-key.
/// Both KEYS[1] (data) and KEYS[2] (version) are declared so Redis Cluster
/// can verify they hash to the same slot.
const CREATE_SCRIPT: &str = r#"
local key = KEYS[1]
local version_key = KEYS[2]
local new_data = ARGV[1]
local new_version = ARGV[2]

local exists = redis.call('EXISTS', key)
if exists == 1 then
    return "EXISTS"
end
redis.call('SET', key, new_data)
redis.call('SET', version_key, new_version)
return "OK"
"#;

/// Lua script for atomic CAS update using the version side-key.
/// Both KEYS[1] (data) and KEYS[2] (version) are declared so Redis Cluster
/// can verify they hash to the same slot.
const UPDATE_SCRIPT: &str = r#"
local key = KEYS[1]
local version_key = KEYS[2]
local new_data = ARGV[1]
local expected = ARGV[2]
local new_version = ARGV[3]

local current_version = redis.call('GET', version_key)
if current_version == false then
    return "MISSING"
end
if current_version ~= expected then
    return "CONFLICT"
end
redis.call('SET', key, new_data)
redis.call('SET', version_key, new_version)
return "OK"
"#;

/// ManifestStore backed by Redis with Lua-script CAS.
///
/// Each manifest uses two Redis keys: a data key and a
/// `{data_key}:version` side-key for cheap CAS comparison in Lua
/// scripts. Both keys use Redis hash tags (`{...}`) so they route to
/// the same slot in Redis Cluster.
///
/// External deletion of one key without the other creates inconsistent
/// state. Only use this store's API to manage manifest keys.
pub struct RedisManifestStore {
    client: redis::aio::ConnectionManager,
    prefix: String,
}

impl RedisManifestStore {
    pub fn new(client: redis::aio::ConnectionManager, prefix: String) -> Self {
        Self { client, prefix }
    }

    pub async fn connect(url: &str, prefix: &str) -> Result<Self> {
        let client =
            redis::Client::open(url).map_err(|e| anyhow!("Redis client open failed: {}", e))?;
        let conn = redis::aio::ConnectionManager::new(client)
            .await
            .map_err(|e| anyhow!("Redis connection failed: {}", e))?;
        Ok(Self {
            client: conn,
            prefix: prefix.to_string(),
        })
    }

    fn prefixed_key(&self, key: &str) -> String {
        format!("{{{}{}}}", self.prefix, key)
    }

    fn version_key(&self, key: &str) -> String {
        format!("{{{}{}}}:version", self.prefix, key)
    }
}

#[async_trait]
impl ManifestStore for RedisManifestStore {
    async fn get(&self, key: &str) -> Result<Option<Manifest>> {
        let pkey = self.prefixed_key(key);
        let mut conn = self.client.clone();

        let data: Option<Vec<u8>> = conn
            .get(&pkey)
            .await
            .map_err(|e| anyhow!("Redis GET failed: {}", e))?;

        match data {
            Some(bytes) => {
                let manifest: Manifest = rmp_serde::from_slice(&bytes)
                    .map_err(|e| anyhow!("failed to deserialize manifest: {}", e))?;
                Ok(Some(manifest))
            }
            None => Ok(None),
        }
    }

    async fn put(
        &self,
        key: &str,
        manifest: &Manifest,
        expected_version: Option<u64>,
    ) -> Result<CasResult> {
        let pkey = self.prefixed_key(key);
        let vkey = self.version_key(key);
        let mut conn = self.client.clone();

        let new_version = match expected_version {
            None => 1u64,
            Some(v) => v + 1,
        };

        let mut stored = manifest.clone();
        stored.version = new_version;

        let body = rmp_serde::to_vec(&stored)
            .map_err(|e| anyhow!("failed to serialize manifest: {}", e))?;

        let result: String = match expected_version {
            None => redis::Script::new(CREATE_SCRIPT)
                .key(&pkey)
                .key(&vkey)
                .arg(body.as_slice())
                .arg(new_version.to_string())
                .invoke_async(&mut conn)
                .await
                .map_err(|e| anyhow!("Redis Lua script failed: {}", e))?,
            Some(expected) => redis::Script::new(UPDATE_SCRIPT)
                .key(&pkey)
                .key(&vkey)
                .arg(body.as_slice())
                .arg(expected.to_string())
                .arg(new_version.to_string())
                .invoke_async(&mut conn)
                .await
                .map_err(|e| anyhow!("Redis Lua script failed: {}", e))?,
        };

        match result.as_str() {
            "OK" => Ok(CasResult {
                success: true,
                etag: Some(new_version.to_string()),
            }),
            "EXISTS" | "CONFLICT" | "MISSING" => Ok(CasResult {
                success: false,
                etag: None,
            }),
            other => Err(anyhow!("unexpected Lua script result: {}", other)),
        }
    }

    async fn meta(&self, key: &str) -> Result<Option<ManifestMeta>> {
        match self.get(key).await? {
            Some(manifest) => Ok(Some(ManifestMeta::from(&manifest))),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use turbodb::{BTreeManifestEntry, Backend, FrameEntry};

    fn make_manifest(writer: &str, epoch: u64) -> Manifest {
        Manifest {
            version: 0,
            writer_id: writer.to_string(),
            lease_epoch: epoch,
            timestamp_ms: 1000,
            storage: Backend::Walrust {
                txid: 1,
                changeset_prefix: "cs/".to_string(),
                latest_changeset_key: "cs/1".to_string(),
                snapshot_key: None,
                snapshot_txid: None,
            },
        }
    }

    fn make_turbolite_manifest(writer: &str, epoch: u64) -> Manifest {
        Manifest {
            version: 0,
            writer_id: writer.to_string(),
            lease_epoch: epoch,
            timestamp_ms: 2000,
            storage: Backend::Turbolite {
                page_count: 100,
                page_size: 4096,
                pages_per_group: 256,
                sub_pages_per_frame: 16,
                strategy: "Positional".to_string(),
                page_group_keys: vec!["pg-0".to_string()],
                frame_tables: vec![vec![FrameEntry {
                    offset: 0,
                    len: 4096,
                    page_count: 0,
                }]],
                group_pages: vec![vec![1, 2, 3]],
                btrees: BTreeMap::from([(
                    1,
                    BTreeManifestEntry {
                        name: "sqlite_master".to_string(),
                        obj_type: "table".to_string(),
                        group_ids: vec![0, 1],
                    },
                )]),
                interior_chunk_keys: BTreeMap::from([(0, "ic-0".to_string())]),
                index_chunk_keys: BTreeMap::from([(0, "idx-0".to_string())]),
                subframe_overrides: vec![BTreeMap::new()],
                turbolite_version: 0,
                db_header: None,
            },
        }
    }

    struct TestFixture {
        store: RedisManifestStore,
        prefix: String,
        client: redis::aio::ConnectionManager,
    }

    impl TestFixture {
        /// Requires `REDIS_URL` env var pointed at a live Redis
        /// (single-node or Cluster). Panics loudly if unset — silent
        /// skips let broken tests look green.
        async fn new() -> Self {
            let url = std::env::var("REDIS_URL").expect(
                "REDIS_URL must be set for turbodb-manifest-redis integration tests \
                 (e.g. redis://127.0.0.1:6379)",
            );
            let prefix = format!(
                "test-manifest-{}/",
                uuid::Uuid::new_v4().to_string().replace('-', "")
            );
            let redis_client = redis::Client::open(url.as_str()).expect("open redis client");
            let conn = redis::aio::ConnectionManager::new(redis_client)
                .await
                .expect("connect");
            let store = RedisManifestStore::new(conn.clone(), prefix.clone());
            Self {
                store,
                prefix,
                client: conn,
            }
        }

        async fn cleanup(self) {
            let mut conn = self.client.clone();
            let keys: Vec<String> = redis::cmd("KEYS")
                .arg(format!("{}*", self.prefix))
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            for key in keys {
                let _: () = conn.del(&key).await.unwrap_or_default();
            }
        }
    }

    impl std::ops::Deref for TestFixture {
        type Target = RedisManifestStore;
        fn deref(&self) -> &Self::Target {
            &self.store
        }
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let f = TestFixture::new().await;
        assert!(f.get("nope").await.unwrap().is_none());
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_create_and_get() {
        let f = TestFixture::new().await;

        let res = f.put("db1", &make_manifest("node-1", 1), None).await.unwrap();
        assert!(res.success);

        let fetched = f.get("db1").await.unwrap().expect("should exist");
        assert_eq!(fetched.version, 1);
        assert_eq!(fetched.writer_id, "node-1");
        assert_eq!(fetched.lease_epoch, 1);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_create_turbolite_roundtrip() {
        let f = TestFixture::new().await;

        f.put("db1", &make_turbolite_manifest("node-1", 1), None)
            .await
            .unwrap();
        let fetched = f.get("db1").await.unwrap().unwrap();
        match &fetched.storage {
            Backend::Turbolite { page_count, page_size, .. } => {
                assert_eq!(*page_count, 100);
                assert_eq!(*page_size, 4096);
            }
            _ => panic!("expected Turbolite"),
        }
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_update_with_correct_version() {
        let f = TestFixture::new().await;

        f.put("db1", &make_manifest("node-1", 1), None).await.unwrap();
        let res = f.put("db1", &make_manifest("node-1", 2), Some(1)).await.unwrap();
        assert!(res.success);

        let fetched = f.get("db1").await.unwrap().unwrap();
        assert_eq!(fetched.version, 2);
        assert_eq!(fetched.lease_epoch, 2);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_meta_returns_correct_fields() {
        let f = TestFixture::new().await;

        f.put("db1", &make_manifest("node-42", 7), None).await.unwrap();
        let meta = f.meta("db1").await.unwrap().expect("should exist");
        assert_eq!(meta.version, 1);
        assert_eq!(meta.writer_id, "node-42");
        assert_eq!(meta.lease_epoch, 7);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_meta_nonexistent() {
        let f = TestFixture::new().await;
        assert!(f.meta("nope").await.unwrap().is_none());
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_create_conflict() {
        let f = TestFixture::new().await;

        f.put("db1", &make_manifest("node-1", 1), None).await.unwrap();
        let res = f.put("db1", &make_manifest("node-2", 1), None).await.unwrap();
        assert!(!res.success);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_stale_version() {
        let f = TestFixture::new().await;

        f.put("db1", &make_manifest("node-1", 1), None).await.unwrap();
        f.put("db1", &make_manifest("node-1", 1), Some(1)).await.unwrap();

        let res = f.put("db1", &make_manifest("node-1", 1), Some(1)).await.unwrap();
        assert!(!res.success);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_version_on_nonexistent() {
        let f = TestFixture::new().await;
        let res = f.put("db1", &make_manifest("node-1", 1), Some(1)).await.unwrap();
        assert!(!res.success);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_sequential_version_increments() {
        let f = TestFixture::new().await;

        f.put("db1", &make_manifest("node-1", 1), None).await.unwrap();
        assert_eq!(f.get("db1").await.unwrap().unwrap().version, 1);

        f.put("db1", &make_manifest("node-1", 1), Some(1)).await.unwrap();
        assert_eq!(f.get("db1").await.unwrap().unwrap().version, 2);

        f.put("db1", &make_manifest("node-1", 1), Some(2)).await.unwrap();
        assert_eq!(f.get("db1").await.unwrap().unwrap().version, 3);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_meta_updates_after_put() {
        let f = TestFixture::new().await;

        f.put("db1", &make_manifest("node-1", 1), None).await.unwrap();
        f.put("db1", &make_manifest("node-2", 5), Some(1)).await.unwrap();

        let meta = f.meta("db1").await.unwrap().unwrap();
        assert_eq!(meta.version, 2);
        assert_eq!(meta.writer_id, "node-2");
        assert_eq!(meta.lease_epoch, 5);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_put_ignores_caller_version() {
        let f = TestFixture::new().await;

        let mut m = make_manifest("node-1", 1);
        m.version = 999;
        f.put("db1", &m, None).await.unwrap();
        assert_eq!(f.get("db1").await.unwrap().unwrap().version, 1);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn test_multiple_keys_independent() {
        let f = TestFixture::new().await;

        f.put("db1", &make_manifest("node-1", 1), None).await.unwrap();
        f.put("db2", &make_turbolite_manifest("node-2", 2), None)
            .await
            .unwrap();

        let m1 = f.get("db1").await.unwrap().unwrap();
        let m2 = f.get("db2").await.unwrap().unwrap();
        assert_eq!(m1.writer_id, "node-1");
        assert_eq!(m2.writer_id, "node-2");
        f.cleanup().await;
    }
}
