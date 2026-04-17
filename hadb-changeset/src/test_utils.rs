use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use hadb_storage::{CasResult, StorageBackend};
use tokio::sync::Mutex;

/// In-memory `StorageBackend` for hadb-changeset tests.
///
/// Implements the new byte-level trait (`get`/`put`/`delete`/`list` + CAS),
/// matching the pattern used by `hadb-storage-mem::MemStorage`. Kept inline
/// here (not depending on `hadb-storage-mem`) so this tiny helper stays
/// hermetic to the `hadb-changeset` crate.
pub struct InMemoryObjectStore {
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

impl InMemoryObjectStore {
    pub fn new() -> Self {
        Self {
            objects: Mutex::new(HashMap::new()),
        }
    }

    #[allow(dead_code)]
    pub async fn insert(&self, key: &str, data: Vec<u8>) {
        self.objects.lock().await.insert(key.to_string(), data);
    }

    #[allow(dead_code)]
    pub async fn keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.objects.lock().await.keys().cloned().collect();
        keys.sort();
        keys
    }
}

impl Default for InMemoryObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageBackend for InMemoryObjectStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.objects.lock().await.get(key).cloned())
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.objects
            .lock()
            .await
            .insert(key.to_string(), data.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.objects.lock().await.remove(key);
        Ok(())
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        let map = self.objects.lock().await;
        let mut keys: Vec<String> = map
            .keys()
            .filter(|k| k.starts_with(prefix))
            .filter(|k| after.map(|a| k.as_str() > a).unwrap_or(true))
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.objects.lock().await.contains_key(key))
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        let mut map = self.objects.lock().await;
        if map.contains_key(key) {
            return Ok(CasResult { success: false, etag: None });
        }
        map.insert(key.to_string(), data.to_vec());
        Ok(CasResult {
            success: true,
            etag: Some("test".into()),
        })
    }

    async fn put_if_match(&self, key: &str, data: &[u8], _etag: &str) -> Result<CasResult> {
        let mut map = self.objects.lock().await;
        if !map.contains_key(key) {
            return Ok(CasResult { success: false, etag: None });
        }
        map.insert(key.to_string(), data.to_vec());
        Ok(CasResult {
            success: true,
            etag: Some("test".into()),
        })
    }
}
