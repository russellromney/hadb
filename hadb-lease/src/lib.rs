//! `hadb-lease`: LeaseStore trait for HA leader election.
//!
//! Trait-only. Backend implementations live in sibling crates
//! (`hadb-lease-s3`, `hadb-lease-nats`, `hadb-lease-cinch`,
//! `hadb-lease-mem`).
//!
//! # Why trait-only?
//!
//! Backend crates shouldn't pull in the full hadb coordinator just to get
//! the LeaseStore trait definition. Consumers that only need to call the
//! trait (haqlite, hadb) can depend on this tiny crate directly.
//!
//! # Relationship to `hadb-storage`
//!
//! `CasResult` is the shared CAS primitive; it lives in `hadb-storage` and is
//! re-exported here for ergonomic use.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use hadb_storage::CasResult;

pub mod fence;
pub use fence::{AtomicFence, AtomicFenceWriter, FenceSource, NoActiveLease};

/// Canonical lease payload written into any `LeaseStore` backend by any
/// hadb-driven writer (the coordinator's `DbLease`, the cinch-cloud
/// lease orchestrator, embedded libsql replicas, etc.). Lives in
/// `hadb-lease` rather than `hadb` so every `LeaseStore` impl + every
/// lease-aware client can agree on the on-wire shape without taking a
/// dep on the full `hadb` coordinator crate.
///
/// Byte-opaque by intent: `LeaseStore` backends never parse it — they
/// round-trip the serialized bytes untouched. Interoperability between
/// impls is automatic as long as all writers use this struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseData {
    pub instance_id: String,
    /// Network address of the leader (for client discovery).
    #[serde(default)]
    pub address: String,
    pub claimed_at: u64,
    pub ttl_secs: u64,
    /// Unique per leadership claim. Changes on every promotion, even if the same
    /// instance reclaims. Clients poll for a new session_id after failures.
    #[serde(default)]
    pub session_id: String,
    /// When true, the leader is shutting down gracefully (e.g., Fly scale-to-zero).
    /// Followers should call on_sleep and exit rather than promote.
    #[serde(default)]
    pub sleeping: bool,
}

impl LeaseData {
    /// Whether this lease has expired based on current time.
    pub fn is_expired(&self) -> bool {
        let now = chrono::Utc::now().timestamp() as u64;
        now >= self.claimed_at + self.ttl_secs
    }
}

/// Trait for CAS lease operations on a key-value store.
///
/// Used for leader election via conditional writes. Any storage system
/// with CAS support can implement this: S3 (conditional PUT),
/// Consul (check-and-set), Redis (SETNX), DynamoDB (conditional writes).
///
/// The coordinator uses this for:
/// - Leader claims lease via `write_if_not_exists`
/// - Leader renews lease via `write_if_match`
/// - Followers read lease to discover leader via `read`
/// - Leader releases lease via `delete` on graceful shutdown
#[async_trait]
pub trait LeaseStore: Send + Sync {
    /// Build the on-store key for a given coordinator scope (typically a
    /// database name). Backends that wrap each scope in their own
    /// container (e.g., S3 buckets with structured key paths) override
    /// this; pass-through backends (`CinchLeaseStore` server-scopes by
    /// token, `InMemoryLeaseStore` namespaces by HashMap) use the default.
    ///
    /// Coordinator calls this once per database at join time and then
    /// uses the result in the basic `read` / `write_if_*` / `delete`
    /// operations below.
    fn key_for(&self, scope: &str) -> String {
        scope.to_string()
    }

    /// Read a key, returning (data, etag). None if key doesn't exist.
    ///
    /// The etag is an opaque version token used for CAS operations.
    /// For S3: the ETag header. For NATS KV: the revision number.
    async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>>;

    /// Write only if key doesn't exist (create). CAS.
    ///
    /// Used by followers to claim an expired lease. Returns success=true
    /// if the write succeeded (lease was claimed), false if another node
    /// already claimed it (CAS conflict).
    async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult>;

    /// Write only if current etag matches (update). CAS.
    ///
    /// Used by leader to renew its lease. Returns success=true if the
    /// renewal succeeded (still leader), false if another node claimed
    /// the lease (CAS conflict - self-fencing).
    async fn write_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<CasResult>;

    /// Delete a key (best-effort for lease release).
    ///
    /// Used by leader on graceful shutdown to release the lease immediately
    /// instead of waiting for TTL expiration. Failures are logged but not fatal.
    async fn delete(&self, key: &str) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    // ── LeaseData tests (moved from hadb::lease when the struct
    //    lifted into this crate, Phase Shoal) ─────────────────────────

    #[test]
    fn lease_data_not_expired_when_fresh() {
        let now = chrono::Utc::now().timestamp() as u64;
        let lease = LeaseData {
            instance_id: "test".into(),
            address: "localhost:8080".into(),
            claimed_at: now,
            ttl_secs: 60,
            session_id: "session-1".into(),
            sleeping: false,
        };
        assert!(!lease.is_expired());
    }

    #[test]
    fn lease_data_expired_past_ttl() {
        let now = chrono::Utc::now().timestamp() as u64;
        let lease = LeaseData {
            instance_id: "test".into(),
            address: "localhost:8080".into(),
            claimed_at: now.saturating_sub(120),
            ttl_secs: 60,
            session_id: "session-1".into(),
            sleeping: false,
        };
        assert!(lease.is_expired());
    }

    #[test]
    fn lease_data_serialization_roundtrip() {
        let lease = LeaseData {
            instance_id: "test-instance".into(),
            address: "localhost:9000".into(),
            claimed_at: 1234567890,
            ttl_secs: 30,
            session_id: "session-abc".into(),
            sleeping: true,
        };
        let json = serde_json::to_string(&lease).expect("serialize");
        let back: LeaseData = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.instance_id, "test-instance");
        assert_eq!(back.address, "localhost:9000");
        assert_eq!(back.claimed_at, 1234567890);
        assert_eq!(back.ttl_secs, 30);
        assert_eq!(back.session_id, "session-abc");
        assert!(back.sleeping);
    }

    /// Cross-language interop: a fixture string representing exactly
    /// what cinchfs-go (`cinchfs-go/auth_test.go::cinchfsLeaseDataInteropFixture`)
    /// emits must deserialize here into a `LeaseData` with matching
    /// field values. This is the contract: any hadb-driven writer in
    /// any language serializes this shape, every Rust reader decodes
    /// it. If the Go side drifts (renames a json tag, changes a type),
    /// this test fails; if this test drifts, the Go side's matching
    /// test fails. Either failure points at which side moved.
    ///
    /// The fixture string MUST be kept byte-identical to cinchfs-go's.
    #[test]
    fn lease_data_interop_fixture_from_cinchfs_go() {
        const CINCHFS_GO_INTEROP_FIXTURE: &str = r#"{"instance_id":"iid-interop","address":"cinchfs://host","claimed_at":1700000000,"ttl_secs":30,"session_id":"sid-interop","sleeping":false}"#;

        let lease: LeaseData =
            serde_json::from_str(CINCHFS_GO_INTEROP_FIXTURE).expect("deserialize cinchfs fixture");

        assert_eq!(lease.instance_id, "iid-interop");
        assert_eq!(lease.address, "cinchfs://host");
        assert_eq!(lease.claimed_at, 1_700_000_000);
        assert_eq!(lease.ttl_secs, 30);
        assert_eq!(lease.session_id, "sid-interop");
        assert!(!lease.sleeping);

        // And Rust's serializer should produce the same byte shape
        // Go's does (stable key order: struct-declaration order).
        // If this ever fails, check whether anyone added serde
        // attributes that change emission order.
        let reemitted = serde_json::to_string(&lease).expect("serialize");
        assert_eq!(
            reemitted, CINCHFS_GO_INTEROP_FIXTURE,
            "Rust re-emitted JSON drifted from the cinchfs-go interop fixture.\n\
             Either cinchfs-go needs updating, or serde attributes on LeaseData changed."
        );
    }

    /// Tolerate the hadb-historical legacy shape where callers used
    /// to omit the optional fields (they're `#[serde(default)]`).
    /// Guards against accidentally breaking an older on-the-wire
    /// payload a long-running lease holder might have written before
    /// an upgrade.
    #[test]
    fn lease_data_deserializes_with_missing_defaults() {
        let minimal = r#"{"instance_id":"i","claimed_at":100,"ttl_secs":5}"#;
        let lease: LeaseData =
            serde_json::from_str(minimal).expect("missing-defaults should deserialize");
        assert_eq!(lease.instance_id, "i");
        assert_eq!(lease.address, "");
        assert_eq!(lease.session_id, "");
        assert!(!lease.sleeping);
    }

    #[allow(dead_code)]
    fn _object_safe(_: &dyn LeaseStore) {}

    #[allow(dead_code)]
    fn _send_sync<T: LeaseStore>() {}

    /// Stateful in-memory mock used to exercise the trait contract here at
    /// the abstraction layer. Real impls (`hadb-lease-s3`, `-http`, `-nats`,
    /// `-etcd`) have their own tests; this one just pins the trait semantics.
    struct StatefulLeaseStore {
        data: Arc<Mutex<HashMap<String, (Vec<u8>, String)>>>,
    }

    impl StatefulLeaseStore {
        fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    #[async_trait]
    impl LeaseStore for StatefulLeaseStore {
        async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
            Ok(self.data.lock().await.get(key).cloned())
        }

        async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult> {
            let mut store = self.data.lock().await;
            if store.contains_key(key) {
                Ok(CasResult {
                    success: false,
                    etag: None,
                })
            } else {
                let etag = format!("etag-{}", uuid::Uuid::new_v4());
                store.insert(key.to_string(), (data, etag.clone()));
                Ok(CasResult {
                    success: true,
                    etag: Some(etag),
                })
            }
        }

        async fn write_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<CasResult> {
            let mut store = self.data.lock().await;
            if let Some((_, current_etag)) = store.get(key) {
                if current_etag == etag {
                    let new_etag = format!("etag-{}", uuid::Uuid::new_v4());
                    store.insert(key.to_string(), (data, new_etag.clone()));
                    Ok(CasResult {
                        success: true,
                        etag: Some(new_etag),
                    })
                } else {
                    Ok(CasResult {
                        success: false,
                        etag: None,
                    })
                }
            } else {
                Ok(CasResult {
                    success: false,
                    etag: None,
                })
            }
        }

        async fn delete(&self, key: &str) -> Result<()> {
            self.data.lock().await.remove(key);
            Ok(())
        }
    }

    #[test]
    fn cas_result_equality() {
        let r1 = CasResult {
            success: true,
            etag: Some("v1".into()),
        };
        let r2 = CasResult {
            success: true,
            etag: Some("v1".into()),
        };
        let r3 = CasResult {
            success: false,
            etag: None,
        };
        assert_eq!(r1, r2);
        assert_ne!(r1, r3);
    }

    #[test]
    fn cas_result_edge_cases() {
        let r1 = CasResult {
            success: false,
            etag: None,
        };
        let r2 = CasResult {
            success: false,
            etag: None,
        };
        assert_eq!(r1, r2);

        let r3 = CasResult {
            success: true,
            etag: Some(String::new()),
        };
        let r4 = CasResult {
            success: true,
            etag: Some(String::new()),
        };
        assert_eq!(r3, r4);

        let r5 = CasResult {
            success: true,
            etag: None,
        };
        assert!(r5.success);
        assert!(r5.etag.is_none());
    }

    #[tokio::test]
    async fn lease_store_cas_create() {
        let store = StatefulLeaseStore::new();

        let result = store
            .write_if_not_exists("lease1", b"node1".to_vec())
            .await
            .unwrap();
        assert!(result.success);
        assert!(result.etag.is_some());

        let result = store
            .write_if_not_exists("lease1", b"node2".to_vec())
            .await
            .unwrap();
        assert!(!result.success);
        assert!(result.etag.is_none());
    }

    #[tokio::test]
    async fn lease_store_cas_update() {
        let store = StatefulLeaseStore::new();

        let result = store
            .write_if_not_exists("lease1", b"node1-v1".to_vec())
            .await
            .unwrap();
        assert!(result.success);
        let etag = result.etag.unwrap();

        let result = store
            .write_if_match("lease1", b"node1-v2".to_vec(), &etag)
            .await
            .unwrap();
        assert!(result.success);
        let new_etag = result.etag.unwrap();
        assert_ne!(etag, new_etag);

        let result = store
            .write_if_match("lease1", b"node1-v3".to_vec(), &etag)
            .await
            .unwrap();
        assert!(!result.success);
    }

    #[tokio::test]
    async fn lease_store_read() {
        let store = StatefulLeaseStore::new();

        let result = store.read("lease1").await.unwrap();
        assert!(result.is_none());

        let write_result = store
            .write_if_not_exists("lease1", b"node1".to_vec())
            .await
            .unwrap();
        let etag = write_result.etag.unwrap();

        let result = store.read("lease1").await.unwrap();
        assert!(result.is_some());
        let (data, read_etag) = result.unwrap();
        assert_eq!(data, b"node1");
        assert_eq!(read_etag, etag);
    }

    #[tokio::test]
    async fn lease_store_delete() {
        let store = StatefulLeaseStore::new();

        store
            .write_if_not_exists("lease1", b"node1".to_vec())
            .await
            .unwrap();
        assert!(store.read("lease1").await.unwrap().is_some());

        store.delete("lease1").await.unwrap();
        assert!(store.read("lease1").await.unwrap().is_none());

        assert!(store.delete("lease1").await.is_ok());
    }
}
