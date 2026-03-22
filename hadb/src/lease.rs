use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::{LeaseStore, Role};

/// Lease data stored in LeaseStore (S3, etcd, etc.).
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

/// Per-database CAS lease manager.
///
/// Handles claim, renew, release for a single database's lease key.
/// Uses post-claim verification with jitter to handle backends (like Tigris)
/// that don't enforce conditional PUTs.
pub struct DbLease {
    store: Arc<dyn LeaseStore>,
    lease_key: String,
    instance_id: String,
    address: String,
    ttl_secs: u64,
    current_etag: Option<String>,
    /// Generated fresh on every new claim (not renewal).
    session_id: String,
}

impl DbLease {
    pub fn new(
        store: Arc<dyn LeaseStore>,
        prefix: &str,
        db_name: &str,
        instance_id: &str,
        address: &str,
        ttl_secs: u64,
    ) -> Self {
        let lease_key = format!("{}{}/_lease.json", prefix, db_name);
        Self {
            store,
            lease_key,
            instance_id: instance_id.to_string(),
            address: address.to_string(),
            ttl_secs,
            current_etag: None,
            session_id: uuid::Uuid::new_v4().to_string(),
        }
    }

    /// Try to claim the lease. Returns Leader if we got it, Follower if someone else holds it.
    pub async fn try_claim(&mut self) -> Result<Role> {
        match self.read().await? {
            None => {
                // No lease exists — try to create one.
                self.try_claim_new().await
            }
            Some((lease, etag)) => {
                if lease.sleeping {
                    // Cluster was sleeping — claim to wake it up.
                    self.session_id = uuid::Uuid::new_v4().to_string();
                    self.try_claim_expired(&etag).await
                } else if lease.instance_id == self.instance_id {
                    // We already hold it (e.g. restart). Renew in place.
                    self.current_etag = Some(etag.clone());
                    self.session_id = uuid::Uuid::new_v4().to_string();
                    match self.renew().await? {
                        true => Ok(Role::Leader),
                        false => Ok(Role::Follower),
                    }
                } else if lease.is_expired() {
                    // Expired lease by another instance — try to take over.
                    self.session_id = uuid::Uuid::new_v4().to_string();
                    self.try_claim_expired(&etag).await
                } else {
                    // Active lease by another instance.
                    Ok(Role::Follower)
                }
            }
        }
    }

    /// Renew an existing lease we hold. Returns false if we lost it (CAS conflict).
    /// Keeps the same session_id (no post-claim verify needed — only leader renews).
    pub async fn renew(&mut self) -> Result<bool> {
        let etag = self
            .current_etag
            .as_ref()
            .ok_or_else(|| anyhow!("No ETag — cannot renew without prior claim"))?
            .clone();

        let body = serde_json::to_vec(&self.make_lease())?;
        let result = self.store.write_if_match(&self.lease_key, body, &etag).await?;

        if result.success {
            self.current_etag = result.etag;
            Ok(true)
        } else {
            tracing::warn!("Lease renewal failed (ETag mismatch) — lost lease");
            self.current_etag = None;
            Ok(false)
        }
    }

    /// Release the lease (best-effort delete).
    pub async fn release(&mut self) -> Result<()> {
        self.store.delete(&self.lease_key).await?;
        self.current_etag = None;
        Ok(())
    }

    /// Read the current lease data + etag. None if no lease exists.
    pub async fn read(&self) -> Result<Option<(LeaseData, String)>> {
        match self.store.read(&self.lease_key).await? {
            Some((data, etag)) => {
                let lease: LeaseData = serde_json::from_slice(&data)?;
                Ok(Some((lease, etag)))
            }
            None => Ok(None),
        }
    }

    /// The lease key used in the store.
    pub fn lease_key(&self) -> &str {
        &self.lease_key
    }

    /// The instance ID of this node.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Current session ID (set on each new claim).
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Whether this lease currently holds a valid etag (i.e. we believe we hold the lease).
    pub fn has_etag(&self) -> bool {
        self.current_etag.is_some()
    }

    // ========================================================================
    // Internal
    // ========================================================================

    /// Try to create a new lease (key doesn't exist).
    async fn try_claim_new(&mut self) -> Result<Role> {
        // Generate a new session ID for this claim
        self.session_id = uuid::Uuid::new_v4().to_string();

        let body = serde_json::to_vec(&self.make_lease())?;
        let result = self
            .store
            .write_if_not_exists(&self.lease_key, body)
            .await?;

        if result.success {
            self.current_etag = result.etag;
            self.post_claim_verify().await
        } else {
            // Someone else created it between our read and write.
            Ok(Role::Follower)
        }
    }

    /// Try to take over an expired lease (CAS on the old etag).
    async fn try_claim_expired(&mut self, expired_etag: &str) -> Result<Role> {
        let body = serde_json::to_vec(&self.make_lease())?;
        let result = self
            .store
            .write_if_match(&self.lease_key, body, expired_etag)
            .await?;

        if result.success {
            self.current_etag = result.etag;
            self.post_claim_verify().await
        } else {
            // Someone else took over the expired lease first.
            Ok(Role::Follower)
        }
    }

    /// Post-claim verification: sleep with jitter then read back.
    ///
    /// Handles backends like Tigris that don't enforce conditional PUTs.
    /// The jitter ensures concurrent claimants read after all writes complete,
    /// and strong read consistency means both see the same last-writer-wins result.
    async fn post_claim_verify(&mut self) -> Result<Role> {
        let jitter_ms = {
            use rand::Rng;
            rand::thread_rng().gen_range(50..200)
        };
        tokio::time::sleep(Duration::from_millis(jitter_ms)).await;

        match self.read().await? {
            Some((lease, read_etag)) => {
                if lease.instance_id == self.instance_id
                    && lease.session_id == self.session_id
                {
                    self.current_etag = Some(read_etag);
                    Ok(Role::Leader)
                } else {
                    tracing::warn!(
                        "Post-claim verify failed: lease held by {}:{} (not us: {}:{})",
                        lease.instance_id,
                        lease.session_id,
                        self.instance_id,
                        self.session_id,
                    );
                    self.current_etag = None;
                    Ok(Role::Follower)
                }
            }
            None => {
                tracing::warn!("Post-claim verify: lease vanished after write");
                self.current_etag = None;
                Ok(Role::Follower)
            }
        }
    }

    /// Write the lease with `sleeping: true` — signals followers to shut down gracefully.
    /// Only valid when we hold the lease (have an etag).
    pub async fn set_sleeping(&mut self) -> Result<bool> {
        let etag = self
            .current_etag
            .as_ref()
            .ok_or_else(|| anyhow!("No ETag — cannot set sleeping without prior claim"))?
            .clone();

        let lease = LeaseData {
            instance_id: self.instance_id.clone(),
            address: self.address.clone(),
            claimed_at: chrono::Utc::now().timestamp() as u64,
            ttl_secs: self.ttl_secs,
            session_id: self.session_id.clone(),
            sleeping: true,
        };
        let body = serde_json::to_vec(&lease)?;
        let result = self.store.write_if_match(&self.lease_key, body, &etag).await?;

        if result.success {
            self.current_etag = result.etag;
            Ok(true)
        } else {
            tracing::warn!("set_sleeping CAS conflict — lost lease");
            self.current_etag = None;
            Ok(false)
        }
    }

    fn make_lease(&self) -> LeaseData {
        LeaseData {
            instance_id: self.instance_id.clone(),
            address: self.address.clone(),
            claimed_at: chrono::Utc::now().timestamp() as u64,
            ttl_secs: self.ttl_secs,
            session_id: self.session_id.clone(),
            sleeping: false,
        }
    }
}

// ============================================================================
// InMemoryLeaseStore — for testing without S3
// ============================================================================

/// In-memory lease store for testing.
///
/// Thread-safe via `std::sync::Mutex` (held briefly, never across await points).
/// Use this in unit/integration tests instead of requiring real S3.
pub struct InMemoryLeaseStore {
    leases: std::sync::Mutex<std::collections::HashMap<String, (Vec<u8>, String)>>,
}

impl InMemoryLeaseStore {
    pub fn new() -> Self {
        Self {
            leases: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

impl Default for InMemoryLeaseStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl crate::LeaseStore for InMemoryLeaseStore {
    async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        Ok(self.leases.lock().unwrap().get(key).cloned())
    }

    async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<crate::CasResult> {
        let mut leases = self.leases.lock().unwrap();
        if leases.contains_key(key) {
            Ok(crate::CasResult {
                success: false,
                etag: None,
            })
        } else {
            let etag = format!("etag-{}", uuid::Uuid::new_v4());
            leases.insert(key.to_string(), (data, etag.clone()));
            Ok(crate::CasResult {
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
    ) -> Result<crate::CasResult> {
        let mut leases = self.leases.lock().unwrap();
        match leases.get(key) {
            Some((_, current_etag)) if current_etag == expected_etag => {
                let new_etag = format!("etag-{}", uuid::Uuid::new_v4());
                leases.insert(key.to_string(), (data, new_etag.clone()));
                Ok(crate::CasResult {
                    success: true,
                    etag: Some(new_etag),
                })
            }
            _ => Ok(crate::CasResult {
                success: false,
                etag: None,
            }),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.leases.lock().unwrap().remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CasResult, LeaseStore};
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Mock LeaseStore for testing.
    struct MockLeaseStore {
        data: Arc<Mutex<HashMap<String, (Vec<u8>, String)>>>,
        next_etag: Arc<Mutex<u64>>,
    }

    impl MockLeaseStore {
        fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(HashMap::new())),
                next_etag: Arc::new(Mutex::new(0)),
            }
        }

        fn gen_etag(&self) -> String {
            let mut counter = self.next_etag.lock().unwrap();
            *counter += 1;
            format!("etag-{}", *counter)
        }
    }

    #[async_trait::async_trait]
    impl LeaseStore for MockLeaseStore {
        async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
            let data = self.data.lock().unwrap();
            Ok(data.get(key).cloned())
        }

        async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult> {
            let mut store = self.data.lock().unwrap();
            if store.contains_key(key) {
                Ok(CasResult {
                    success: false,
                    etag: None,
                })
            } else {
                let etag = self.gen_etag();
                store.insert(key.to_string(), (data, etag.clone()));
                Ok(CasResult {
                    success: true,
                    etag: Some(etag),
                })
            }
        }

        async fn write_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<CasResult> {
            let mut store = self.data.lock().unwrap();
            match store.get(key) {
                Some((_, current_etag)) if current_etag == etag => {
                    let new_etag = self.gen_etag();
                    store.insert(key.to_string(), (data, new_etag.clone()));
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
            let mut store = self.data.lock().unwrap();
            store.remove(key);
            Ok(())
        }
    }

    // ========================================================================
    // LeaseData tests
    // ========================================================================

    #[test]
    fn test_lease_data_not_expired() {
        let now = chrono::Utc::now().timestamp() as u64;
        let lease = LeaseData {
            instance_id: "test".to_string(),
            address: "localhost:8080".to_string(),
            claimed_at: now,
            ttl_secs: 60,
            session_id: "session-1".to_string(),
            sleeping: false,
        };
        assert!(!lease.is_expired());
    }

    #[test]
    fn test_lease_data_expired() {
        let now = chrono::Utc::now().timestamp() as u64;
        let lease = LeaseData {
            instance_id: "test".to_string(),
            address: "localhost:8080".to_string(),
            claimed_at: now.saturating_sub(120), // 120 seconds ago
            ttl_secs: 60,
            session_id: "session-1".to_string(),
            sleeping: false,
        };
        assert!(lease.is_expired());
    }

    #[test]
    fn test_lease_data_serialization() {
        let lease = LeaseData {
            instance_id: "test-instance".to_string(),
            address: "localhost:9000".to_string(),
            claimed_at: 1234567890,
            ttl_secs: 30,
            session_id: "session-abc".to_string(),
            sleeping: true,
        };

        let json = serde_json::to_string(&lease).unwrap();
        let deserialized: LeaseData = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.instance_id, "test-instance");
        assert_eq!(deserialized.address, "localhost:9000");
        assert_eq!(deserialized.claimed_at, 1234567890);
        assert_eq!(deserialized.ttl_secs, 30);
        assert_eq!(deserialized.session_id, "session-abc");
        assert!(deserialized.sleeping);
    }

    // ========================================================================
    // DbLease initialization
    // ========================================================================

    #[test]
    fn test_db_lease_new() {
        let store = Arc::new(MockLeaseStore::new());
        let lease = DbLease::new(
            store,
            "test-prefix/",
            "mydb",
            "instance-1",
            "localhost:8080",
            60,
        );

        assert_eq!(lease.lease_key(), "test-prefix/mydb/_lease.json");
        assert_eq!(lease.instance_id(), "instance-1");
        assert!(!lease.session_id().is_empty());
        assert!(!lease.has_etag());
    }

    // ========================================================================
    // try_claim() tests
    // ========================================================================

    #[tokio::test]
    async fn test_try_claim_no_lease_success() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease = DbLease::new(store, "", "db1", "inst-1", "addr-1", 60);

        let role = lease.try_claim().await.unwrap();
        assert_eq!(role, Role::Leader);
        assert!(lease.has_etag());
    }

    #[tokio::test]
    async fn test_try_claim_sleeping_lease() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease1 = DbLease::new(store.clone(), "", "db1", "inst-1", "addr-1", 60);

        // First instance claims and sets sleeping
        lease1.try_claim().await.unwrap();
        lease1.set_sleeping().await.unwrap();

        // Second instance wakes it up
        let mut lease2 = DbLease::new(store, "", "db1", "inst-2", "addr-2", 60);
        let role = lease2.try_claim().await.unwrap();
        assert_eq!(role, Role::Leader);
        assert!(lease2.has_etag());
    }

    #[tokio::test]
    async fn test_try_claim_already_held_by_us() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease = DbLease::new(store.clone(), "", "db1", "inst-1", "addr-1", 60);

        // First claim
        let role1 = lease.try_claim().await.unwrap();
        assert_eq!(role1, Role::Leader);
        let session1 = lease.session_id().to_string();

        // Second claim (simulating restart)
        let role2 = lease.try_claim().await.unwrap();
        assert_eq!(role2, Role::Leader);
        let session2 = lease.session_id().to_string();

        // Session ID should change on new claim
        assert_ne!(session1, session2);
    }

    #[tokio::test]
    async fn test_try_claim_active_lease_by_other() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease1 = DbLease::new(store.clone(), "", "db1", "inst-1", "addr-1", 60);
        let mut lease2 = DbLease::new(store, "", "db1", "inst-2", "addr-2", 60);

        // Instance 1 claims
        lease1.try_claim().await.unwrap();

        // Instance 2 tries to claim (should be follower)
        let role = lease2.try_claim().await.unwrap();
        assert_eq!(role, Role::Follower);
        assert!(!lease2.has_etag());
    }

    #[tokio::test]
    async fn test_try_claim_expired_lease() {
        let store = Arc::new(MockLeaseStore::new());

        // Create an expired lease manually
        let expired_lease = LeaseData {
            instance_id: "inst-1".to_string(),
            address: "addr-1".to_string(),
            claimed_at: chrono::Utc::now().timestamp() as u64 - 120, // 120 seconds ago
            ttl_secs: 60,
            session_id: "old-session".to_string(),
            sleeping: false,
        };
        let lease_key = "db1/_lease.json";
        store
            .write_if_not_exists(lease_key, serde_json::to_vec(&expired_lease).unwrap())
            .await
            .unwrap();

        // New instance tries to claim the expired lease
        let mut lease = DbLease::new(store, "", "db1", "inst-2", "addr-2", 60);
        let role = lease.try_claim().await.unwrap();
        assert_eq!(role, Role::Leader);
        assert!(lease.has_etag());
    }

    // ========================================================================
    // renew() tests
    // ========================================================================

    #[tokio::test]
    async fn test_renew_success() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease = DbLease::new(store, "", "db1", "inst-1", "addr-1", 60);

        lease.try_claim().await.unwrap();
        let session_before = lease.session_id().to_string();

        let renewed = lease.renew().await.unwrap();
        assert!(renewed);
        assert!(lease.has_etag());

        // Session ID should NOT change on renewal (only on new claims)
        assert_eq!(lease.session_id(), session_before);
    }

    #[tokio::test]
    async fn test_renew_cas_conflict() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease1 = DbLease::new(store.clone(), "", "db1", "inst-1", "addr-1", 60);
        let mut lease2 = DbLease::new(store, "", "db1", "inst-2", "addr-2", 60);

        // Instance 1 claims
        lease1.try_claim().await.unwrap();

        // Manually create an expired lease so instance 2 can take over
        let expired_lease = LeaseData {
            instance_id: "inst-1".to_string(),
            address: "addr-1".to_string(),
            claimed_at: chrono::Utc::now().timestamp() as u64 - 120,
            ttl_secs: 60,
            session_id: "old-session".to_string(),
            sleeping: false,
        };
        lease2.store.write_if_match(
            lease2.lease_key(),
            serde_json::to_vec(&expired_lease).unwrap(),
            lease1.current_etag.as_ref().unwrap(),
        ).await.unwrap();

        // Instance 2 takes over
        lease2.try_claim().await.unwrap();

        // Instance 1 tries to renew (should fail due to CAS conflict)
        let renewed = lease1.renew().await.unwrap();
        assert!(!renewed);
        assert!(!lease1.has_etag());
    }

    #[tokio::test]
    async fn test_renew_without_etag() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease = DbLease::new(store, "", "db1", "inst-1", "addr-1", 60);

        // Try to renew without claiming first
        let result = lease.renew().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No ETag"));
    }

    // ========================================================================
    // release() tests
    // ========================================================================

    #[tokio::test]
    async fn test_release() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease = DbLease::new(store.clone(), "", "db1", "inst-1", "addr-1", 60);

        lease.try_claim().await.unwrap();
        assert!(lease.has_etag());

        lease.release().await.unwrap();
        assert!(!lease.has_etag());

        // Verify lease is gone from store
        let read_result = lease.read().await.unwrap();
        assert!(read_result.is_none());
    }

    // ========================================================================
    // set_sleeping() tests
    // ========================================================================

    #[tokio::test]
    async fn test_set_sleeping_success() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease = DbLease::new(store, "", "db1", "inst-1", "addr-1", 60);

        lease.try_claim().await.unwrap();
        let set = lease.set_sleeping().await.unwrap();
        assert!(set);
        assert!(lease.has_etag());

        // Verify sleeping flag is set
        let (lease_data, _) = lease.read().await.unwrap().unwrap();
        assert!(lease_data.sleeping);
    }

    #[tokio::test]
    async fn test_set_sleeping_without_etag() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease = DbLease::new(store, "", "db1", "inst-1", "addr-1", 60);

        // Try to set sleeping without claiming first
        let result = lease.set_sleeping().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No ETag"));
    }

    #[tokio::test]
    async fn test_set_sleeping_cas_conflict() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease1 = DbLease::new(store.clone(), "", "db1", "inst-1", "addr-1", 60);
        let mut lease2 = DbLease::new(store, "", "db1", "inst-2", "addr-2", 60);

        // Instance 1 claims
        lease1.try_claim().await.unwrap();

        // Manually expire and let instance 2 take over
        let expired_lease = LeaseData {
            instance_id: "inst-1".to_string(),
            address: "addr-1".to_string(),
            claimed_at: chrono::Utc::now().timestamp() as u64 - 120,
            ttl_secs: 60,
            session_id: "old-session".to_string(),
            sleeping: false,
        };
        lease2.store.write_if_match(
            lease2.lease_key(),
            serde_json::to_vec(&expired_lease).unwrap(),
            lease1.current_etag.as_ref().unwrap(),
        ).await.unwrap();
        lease2.try_claim().await.unwrap();

        // Instance 1 tries to set sleeping (should fail)
        let set = lease1.set_sleeping().await.unwrap();
        assert!(!set);
        assert!(!lease1.has_etag());
    }

    // ========================================================================
    // read() tests
    // ========================================================================

    #[tokio::test]
    async fn test_read_no_lease() {
        let store = Arc::new(MockLeaseStore::new());
        let lease = DbLease::new(store, "", "db1", "inst-1", "addr-1", 60);

        let result = lease.read().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_read_existing_lease() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease = DbLease::new(store, "", "db1", "inst-1", "addr-1", 60);

        lease.try_claim().await.unwrap();

        let (lease_data, etag) = lease.read().await.unwrap().unwrap();
        assert_eq!(lease_data.instance_id, "inst-1");
        assert_eq!(lease_data.address, "addr-1");
        assert!(!etag.is_empty());
    }

    // ========================================================================
    // Session ID tests
    // ========================================================================

    #[tokio::test]
    async fn test_session_id_changes_on_new_claim() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease1 = DbLease::new(store.clone(), "", "db1", "inst-1", "addr-1", 60);

        lease1.try_claim().await.unwrap();
        let session1 = lease1.session_id().to_string();

        lease1.release().await.unwrap();

        lease1.try_claim().await.unwrap();
        let session2 = lease1.session_id().to_string();

        assert_ne!(session1, session2);
    }

    #[tokio::test]
    async fn test_session_id_persists_on_renewal() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease = DbLease::new(store, "", "db1", "inst-1", "addr-1", 60);

        lease.try_claim().await.unwrap();
        let session1 = lease.session_id().to_string();

        lease.renew().await.unwrap();
        let session2 = lease.session_id().to_string();

        assert_eq!(session1, session2);
    }

    // ========================================================================
    // Integration / edge case tests
    // ========================================================================

    #[tokio::test]
    async fn test_concurrent_claim_both_cannot_be_leader() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease1 = DbLease::new(store.clone(), "", "db1", "inst-1", "addr-1", 60);
        let mut lease2 = DbLease::new(store, "", "db1", "inst-2", "addr-2", 60);

        // Both try to claim simultaneously
        let (role1, role2) = tokio::join!(lease1.try_claim(), lease2.try_claim());

        let role1 = role1.unwrap();
        let role2 = role2.unwrap();

        // One must be leader, one must be follower (post-claim verify ensures this)
        let leaders = [role1, role2]
            .iter()
            .filter(|r| **r == Role::Leader)
            .count();
        assert_eq!(leaders, 1, "Exactly one instance should become leader");
    }

    #[tokio::test]
    async fn test_lease_lifecycle() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease = DbLease::new(store, "", "db1", "inst-1", "addr-1", 60);

        // Initial claim
        let role = lease.try_claim().await.unwrap();
        assert_eq!(role, Role::Leader);
        let session1 = lease.session_id().to_string();

        // Renew
        assert!(lease.renew().await.unwrap());
        assert_eq!(lease.session_id(), session1); // Session unchanged

        // Set sleeping
        assert!(lease.set_sleeping().await.unwrap());

        // Release
        lease.release().await.unwrap();

        // Claim again (new session)
        let role = lease.try_claim().await.unwrap();
        assert_eq!(role, Role::Leader);
        let session2 = lease.session_id().to_string();
        assert_ne!(session1, session2);
    }
}
