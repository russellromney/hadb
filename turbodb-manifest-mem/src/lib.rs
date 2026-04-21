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
use turbodb::{Manifest, ManifestMeta, ManifestStore};

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
