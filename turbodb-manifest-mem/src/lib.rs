//! In-memory `ManifestStore` for tests.
//!
//! Enforces CAS semantics on `put`. The store assigns the version
//! (1 on first publish, `expected_version + 1` on update); the caller's
//! `manifest.version` is ignored.

use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use tokio::sync::Mutex;

use hadb_storage::CasResult;
use turbodb::{check_epoch_fence, Manifest, ManifestMeta, ManifestStore};

pub struct MemManifestStore {
    data: Mutex<HashMap<String, Manifest>>,
}

impl Default for MemManifestStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemManifestStore {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }

    /// Remove a manifest by key. Test-only (the trait has no delete).
    pub async fn delete(&self, key: &str) {
        self.data.lock().await.remove(key);
    }
}

#[async_trait]
impl ManifestStore for MemManifestStore {
    async fn get(&self, key: &str) -> Result<Option<Manifest>> {
        Ok(self.data.lock().await.get(key).cloned())
    }

    async fn put(
        &self,
        key: &str,
        manifest: &Manifest,
        expected_version: Option<u64>,
    ) -> Result<CasResult> {
        let mut store = self.data.lock().await;

        // Epoch fence before version-CAS: a stale writer (lower epoch)
        // is rejected even if it would otherwise win the version race.
        if let Some(existing) = store.get(key) {
            check_epoch_fence(existing.epoch, manifest.epoch)?;
        }

        match expected_version {
            None => {
                if store.contains_key(key) {
                    return Ok(CasResult {
                        success: false,
                        etag: None,
                    });
                }
                let mut m = manifest.clone();
                m.version = 1;
                let etag = m.version.to_string();
                store.insert(key.to_string(), m);
                Ok(CasResult {
                    success: true,
                    etag: Some(etag),
                })
            }
            Some(expected) => {
                let current = store.get(key);
                match current {
                    Some(existing) if existing.version == expected => {
                        let mut m = manifest.clone();
                        m.version = expected + 1;
                        let etag = m.version.to_string();
                        store.insert(key.to_string(), m);
                        Ok(CasResult {
                            success: true,
                            etag: Some(etag),
                        })
                    }
                    _ => Ok(CasResult {
                        success: false,
                        etag: None,
                    }),
                }
            }
        }
    }

    async fn meta(&self, key: &str) -> Result<Option<ManifestMeta>> {
        Ok(self.data.lock().await.get(key).map(ManifestMeta::from))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use turbodb::LeaseFenceError;

    fn manifest(epoch: u64, writer: &str, payload: &[u8]) -> Manifest {
        Manifest {
            version: 0,
            epoch,
            writer_id: writer.to_string(),
            timestamp_ms: 0,
            payload: payload.to_vec(),
        }
    }

    /// Plan-named test: a CAS write with a stale (lower) epoch fails
    /// with LeaseFenceError; a write at the matching-or-higher epoch
    /// succeeds and updates the pointer atomically.
    #[tokio::test]
    async fn root_pointer_fenced_cas() {
        let store = MemManifestStore::new();

        // Leader at epoch 5 publishes the first base.
        let v1 = store
            .put("db", &manifest(5, "leader-A", b"base-e5"), None)
            .await
            .expect("first publish");
        assert!(v1.success);
        let stored = store.meta("db").await.unwrap().unwrap();
        assert_eq!(stored.version, 1);
        assert_eq!(stored.epoch, 5);

        // A NEW leader promotes to epoch 6 and publishes — allowed
        // (epoch >= stored), wins the version-CAS at version 1.
        let v2 = store
            .put("db", &manifest(6, "leader-B", b"base-e6"), Some(1))
            .await
            .expect("promotion publish");
        assert!(v2.success);
        let stored = store.meta("db").await.unwrap().unwrap();
        assert_eq!(stored.epoch, 6);
        assert_eq!(stored.writer_id, "leader-B");

        // The OLD leader (epoch 5) tries to publish again at the right
        // version — fenced. Returns LeaseFenceError, NOT a version miss.
        let err = store
            .put("db", &manifest(5, "leader-A", b"stale-write"), Some(2))
            .await
            .expect_err("stale-epoch write must be fenced");
        let fence = err
            .downcast_ref::<LeaseFenceError>()
            .expect("must be a LeaseFenceError, not a generic error");
        assert_eq!(fence.stored, 6);
        assert_eq!(fence.attempted, 5);

        // The stale write did not land — store still holds leader-B's base.
        let current = store.get("db").await.unwrap().unwrap();
        assert_eq!(current.payload, b"base-e6");
        assert_eq!(current.writer_id, "leader-B");
    }

    /// Equal epochs fall through to pure version-CAS (the normal
    /// same-leader successive-publish path is unaffected by fencing).
    #[tokio::test]
    async fn equal_epoch_uses_version_cas() {
        let store = MemManifestStore::new();
        store
            .put("db", &manifest(3, "leader", b"v1"), None)
            .await
            .expect("create");

        // Same epoch, correct version -> success.
        let ok = store
            .put("db", &manifest(3, "leader", b"v2"), Some(1))
            .await
            .expect("same-epoch update");
        assert!(ok.success);

        // Same epoch, wrong version -> version miss (success:false), NOT a fence error.
        let miss = store
            .put("db", &manifest(3, "leader", b"v3"), Some(1))
            .await
            .expect("stale-version put returns Ok(false), not Err");
        assert!(!miss.success);
    }
}
