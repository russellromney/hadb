//! `hadb-lease-mem`: in-memory `LeaseStore` implementation.
//!
//! Used as the standard test fixture across the hadb / haqlite / walrust /
//! turbolite workspaces. Not intended for production: all state lives in a
//! single process `Mutex<HashMap>`, and there's no persistence.
//!
//! Etags are monotonically increasing `u64` values serialised as decimal
//! strings, matching NATS KV's revision semantics closely enough that
//! tests written against this backend behave the same when swapped onto a
//! real NATS cluster.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use anyhow::Result;
use async_trait::async_trait;

use hadb_lease::{CasResult, LeaseStore};

/// In-memory `LeaseStore`. Cheap to construct; wrap in `Arc` for sharing.
pub struct InMemoryLeaseStore {
    leases: Mutex<HashMap<String, (Vec<u8>, String)>>,
    revision: AtomicU64,
}

impl InMemoryLeaseStore {
    pub fn new() -> Self {
        Self {
            leases: Mutex::new(HashMap::new()),
            revision: AtomicU64::new(0),
        }
    }

    fn next_revision(&self) -> String {
        self.revision
            .fetch_add(1, Ordering::SeqCst)
            .wrapping_add(1)
            .to_string()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, (Vec<u8>, String)>> {
        self.leases
            .lock()
            .expect("InMemoryLeaseStore mutex poisoned")
    }
}

impl Default for InMemoryLeaseStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LeaseStore for InMemoryLeaseStore {
    async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        Ok(self.lock().get(key).cloned())
    }

    async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult> {
        let mut leases = self.lock();
        if leases.contains_key(key) {
            Ok(CasResult {
                success: false,
                etag: None,
            })
        } else {
            let etag = self.next_revision();
            leases.insert(key.to_string(), (data, etag.clone()));
            Ok(CasResult {
                success: true,
                etag: Some(etag),
            })
        }
    }

    async fn write_if_match(
        &self,
        key: &str,
        data: Vec<u8>,
        expected_etag: &str,
    ) -> Result<CasResult> {
        let mut leases = self.lock();
        match leases.get(key) {
            Some((_, current_etag)) if current_etag == expected_etag => {
                let new_etag = self.next_revision();
                leases.insert(key.to_string(), (data, new_etag.clone()));
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

    async fn delete(&self, key: &str) -> Result<()> {
        self.lock().remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn read_missing_returns_none() {
        let store = InMemoryLeaseStore::new();
        assert!(store.read("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn write_if_not_exists_creates_on_empty() {
        let store = InMemoryLeaseStore::new();
        let r = store
            .write_if_not_exists("k", b"holder-a".to_vec())
            .await
            .unwrap();
        assert!(r.success);
        assert!(r.etag.is_some());

        let read = store.read("k").await.unwrap().unwrap();
        assert_eq!(read.0, b"holder-a");
        assert_eq!(read.1, r.etag.unwrap());
    }

    #[tokio::test]
    async fn write_if_not_exists_rejects_existing() {
        let store = InMemoryLeaseStore::new();
        store.write_if_not_exists("k", b"a".to_vec()).await.unwrap();
        let r = store.write_if_not_exists("k", b"b".to_vec()).await.unwrap();
        assert!(!r.success);
        assert!(r.etag.is_none());
        assert_eq!(store.read("k").await.unwrap().unwrap().0, b"a");
    }

    #[tokio::test]
    async fn write_if_match_advances_and_rejects_stale() {
        let store = InMemoryLeaseStore::new();
        let first = store
            .write_if_not_exists("k", b"v1".to_vec())
            .await
            .unwrap();
        let e1 = first.etag.unwrap();

        let second = store
            .write_if_match("k", b"v2".to_vec(), &e1)
            .await
            .unwrap();
        assert!(second.success);
        let e2 = second.etag.unwrap();
        assert_ne!(e1, e2);

        let stale = store
            .write_if_match("k", b"v3".to_vec(), &e1)
            .await
            .unwrap();
        assert!(!stale.success);
        assert_eq!(store.read("k").await.unwrap().unwrap().0, b"v2");
    }

    #[tokio::test]
    async fn write_if_match_on_missing_fails() {
        let store = InMemoryLeaseStore::new();
        let r = store
            .write_if_match("nope", b"x".to_vec(), "any-etag")
            .await
            .unwrap();
        assert!(!r.success);
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let store = InMemoryLeaseStore::new();
        store.write_if_not_exists("k", b"v".to_vec()).await.unwrap();
        store.delete("k").await.unwrap();
        assert!(store.read("k").await.unwrap().is_none());
        // Second delete must not error.
        store.delete("k").await.unwrap();
    }

    #[tokio::test]
    async fn revisions_are_strictly_monotonic() {
        let store = InMemoryLeaseStore::new();
        let a = store
            .write_if_not_exists("k", b"v1".to_vec())
            .await
            .unwrap();
        let a_rev = a.etag.unwrap().parse::<u64>().unwrap();

        let b = store
            .write_if_match("k", b"v2".to_vec(), &a_rev.to_string())
            .await
            .unwrap();
        let b_rev = b.etag.unwrap().parse::<u64>().unwrap();

        let c = store
            .write_if_match("k", b"v3".to_vec(), &b_rev.to_string())
            .await
            .unwrap();
        let c_rev = c.etag.unwrap().parse::<u64>().unwrap();

        assert!(a_rev < b_rev);
        assert!(b_rev < c_rev);
    }

    #[tokio::test]
    async fn multiple_keys_are_independent() {
        let store = Arc::new(InMemoryLeaseStore::new());
        store
            .write_if_not_exists("a", b"holder-a".to_vec())
            .await
            .unwrap();
        store
            .write_if_not_exists("b", b"holder-b".to_vec())
            .await
            .unwrap();
        assert_eq!(store.read("a").await.unwrap().unwrap().0, b"holder-a");
        assert_eq!(store.read("b").await.unwrap().unwrap().0, b"holder-b");
    }

    #[tokio::test]
    async fn concurrent_claims_exactly_one_wins() {
        let store = Arc::new(InMemoryLeaseStore::new());
        let mut handles = Vec::new();
        for i in 0..16 {
            let s = Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                s.write_if_not_exists("lease", format!("node-{i}").into_bytes())
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

    #[allow(dead_code)]
    fn _usable_as_arc_dyn(_: Arc<dyn LeaseStore>) {}
}
