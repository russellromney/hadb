//! `hadb-storage-mem`: in-memory `StorageBackend` implementation.
//!
//! Used as the test fixture across the hadb/walrust/turbolite/haqlite
//! workspaces. Not intended for production: everything lives in a single
//! `Mutex<HashMap>` and etags are a monotonic counter.
//!
//! # Etags
//!
//! An etag is an opaque string that survives across the CAS boundary:
//! `put_if_match(key, data, etag)` only commits if the current stored etag
//! equals `etag`. We use a monotonic `u64` counter as a string so etags are
//! unique per-write and totally ordered.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::Mutex;

use hadb_storage::{CasResult, StorageBackend};

/// In-memory blob store. Cheap to clone via `Arc`.
#[derive(Default)]
pub struct MemStorage {
    entries: Mutex<HashMap<String, (Vec<u8>, String)>>,
    etag_counter: AtomicU64,
}

impl MemStorage {
    pub fn new() -> Self {
        Self::default()
    }

    fn next_etag(&self) -> String {
        let n = self.etag_counter.fetch_add(1, Ordering::SeqCst);
        // `+ 1` because fetch_add returns the previous value; we want 1-based
        // etags so "0" never appears (some implementors treat 0 as "missing").
        format!("{}", n + 1)
    }
}

#[async_trait]
impl StorageBackend for MemStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self
            .entries
            .lock()
            .await
            .get(key)
            .map(|(bytes, _)| bytes.clone()))
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        let etag = self.next_etag();
        self.entries
            .lock()
            .await
            .insert(key.to_string(), (data.to_vec(), etag));
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.entries.lock().await.remove(key);
        Ok(())
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        let guard = self.entries.lock().await;
        let mut keys: Vec<String> = guard
            .keys()
            .filter(|k| k.starts_with(prefix))
            .filter(|k| match after {
                Some(cursor) => k.as_str() > cursor,
                None => true,
            })
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.entries.lock().await.contains_key(key))
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        let mut guard = self.entries.lock().await;
        if guard.contains_key(key) {
            return Ok(CasResult {
                success: false,
                etag: None,
            });
        }
        let etag = self.next_etag();
        guard.insert(key.to_string(), (data.to_vec(), etag.clone()));
        Ok(CasResult {
            success: true,
            etag: Some(etag),
        })
    }

    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult> {
        let mut guard = self.entries.lock().await;
        match guard.get(key) {
            Some((_, current)) if current == etag => {
                let new_etag = self.next_etag();
                guard.insert(key.to_string(), (data.to_vec(), new_etag.clone()));
                Ok(CasResult {
                    success: true,
                    etag: Some(new_etag),
                })
            }
            _ => Ok(CasResult {
                success: false,
                etag: None,
            }),
        }
    }

    async fn range_get(&self, key: &str, start: u64, len: u32) -> Result<Option<Vec<u8>>> {
        let guard = self.entries.lock().await;
        let Some((bytes, _)) = guard.get(key) else {
            return Ok(None);
        };
        let start = start as usize;
        if start >= bytes.len() {
            return Ok(Some(Vec::new()));
        }
        let end = start.saturating_add(len as usize).min(bytes.len());
        Ok(Some(bytes[start..end].to_vec()))
    }

    fn backend_name(&self) -> &str {
        "mem"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn put_then_get_roundtrips() {
        let s = MemStorage::new();
        s.put("k", b"v").await.unwrap();
        assert_eq!(s.get("k").await.unwrap().unwrap(), b"v");
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let s = MemStorage::new();
        assert!(s.get("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn put_overwrites() {
        let s = MemStorage::new();
        s.put("k", b"first").await.unwrap();
        s.put("k", b"second").await.unwrap();
        assert_eq!(s.get("k").await.unwrap().unwrap(), b"second");
    }

    #[tokio::test]
    async fn delete_removes_and_is_idempotent() {
        let s = MemStorage::new();
        s.put("k", b"v").await.unwrap();
        s.delete("k").await.unwrap();
        assert!(s.get("k").await.unwrap().is_none());
        // Second delete must not error.
        s.delete("k").await.unwrap();
    }

    #[tokio::test]
    async fn exists_reflects_state() {
        let s = MemStorage::new();
        assert!(!s.exists("k").await.unwrap());
        s.put("k", b"").await.unwrap();
        assert!(s.exists("k").await.unwrap());
        s.delete("k").await.unwrap();
        assert!(!s.exists("k").await.unwrap());
    }

    #[tokio::test]
    async fn list_filters_by_prefix_and_sorts() {
        let s = MemStorage::new();
        s.put("a/1", b"").await.unwrap();
        s.put("a/3", b"").await.unwrap();
        s.put("a/2", b"").await.unwrap();
        s.put("b/1", b"").await.unwrap();
        assert_eq!(s.list("a/", None).await.unwrap(), vec!["a/1", "a/2", "a/3"]);
        assert_eq!(s.list("b/", None).await.unwrap(), vec!["b/1"]);
        assert!(s.list("c/", None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_after_is_exclusive() {
        let s = MemStorage::new();
        for k in ["a/1", "a/2", "a/3"] {
            s.put(k, b"").await.unwrap();
        }
        let got = s.list("a/", Some("a/1")).await.unwrap();
        assert_eq!(got, vec!["a/2", "a/3"]);
    }

    #[tokio::test]
    async fn put_if_absent_first_wins() {
        let s = MemStorage::new();
        let a = s.put_if_absent("k", b"first").await.unwrap();
        assert!(a.success);
        let b = s.put_if_absent("k", b"second").await.unwrap();
        assert!(!b.success);
        assert_eq!(s.get("k").await.unwrap().unwrap(), b"first");
    }

    #[tokio::test]
    async fn put_if_match_advances_etag() {
        let s = MemStorage::new();
        let a = s.put_if_absent("k", b"v1").await.unwrap();
        let e1 = a.etag.unwrap();

        let b = s.put_if_match("k", b"v2", &e1).await.unwrap();
        assert!(b.success);
        let e2 = b.etag.unwrap();
        assert_ne!(e1, e2);

        // Stale etag now fails.
        let c = s.put_if_match("k", b"v3", &e1).await.unwrap();
        assert!(!c.success);
        assert_eq!(s.get("k").await.unwrap().unwrap(), b"v2");
    }

    #[tokio::test]
    async fn put_if_match_on_missing_fails() {
        let s = MemStorage::new();
        let r = s.put_if_match("nope", b"x", "any").await.unwrap();
        assert!(!r.success);
        assert!(r.etag.is_none());
    }

    #[tokio::test]
    async fn concurrent_put_if_absent_exactly_one_wins() {
        let s = Arc::new(MemStorage::new());
        let mut handles = Vec::new();
        for i in 0..16 {
            let s = Arc::clone(&s);
            handles.push(tokio::spawn(async move {
                s.put_if_absent("lease", format!("node-{i}").as_bytes())
                    .await
                    .unwrap()
            }));
        }
        let mut wins = 0;
        for h in handles {
            if h.await.unwrap().success {
                wins += 1;
            }
        }
        assert_eq!(wins, 1);
    }

    #[tokio::test]
    async fn range_get_slices_correctly() {
        let s = MemStorage::new();
        s.put("k", b"abcdefghij").await.unwrap();
        assert_eq!(s.range_get("k", 0, 3).await.unwrap().unwrap(), b"abc");
        assert_eq!(s.range_get("k", 2, 4).await.unwrap().unwrap(), b"cdef");
        assert_eq!(s.range_get("k", 8, 10).await.unwrap().unwrap(), b"ij");
        assert_eq!(
            s.range_get("k", 50, 10).await.unwrap().unwrap(),
            Vec::<u8>::new()
        );
    }

    #[tokio::test]
    async fn range_get_on_missing_returns_none() {
        let s = MemStorage::new();
        assert!(s.range_get("nope", 0, 10).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn etags_are_unique_per_write() {
        let s = MemStorage::new();
        let a = s.put_if_absent("k", b"v1").await.unwrap();
        let b = s
            .put_if_match("k", b"v2", a.etag.as_ref().unwrap())
            .await
            .unwrap();
        let c = s
            .put_if_match("k", b"v3", b.etag.as_ref().unwrap())
            .await
            .unwrap();
        let etags = [a.etag.unwrap(), b.etag.unwrap(), c.etag.unwrap()];
        assert_eq!(
            etags.iter().collect::<std::collections::HashSet<_>>().len(),
            3
        );
    }

    // Compile-time: exercises the trait object shape our consumers depend on.
    #[allow(dead_code)]
    fn _usable_as_arc_dyn(_: Arc<dyn StorageBackend>) {}

    #[test]
    fn backend_name_is_stable() {
        assert_eq!(MemStorage::new().backend_name(), "mem");
    }
}
