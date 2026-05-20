use anyhow::{anyhow, Result};
use std::sync::Arc;

use crate::{LeaseStore, Role};
use hadb_lease::AtomicFenceWriter;

// Phase Shoal: `LeaseData` now lives in the `hadb-lease` trait crate so
// every `LeaseStore` impl + every lease-aware client (embedded
// replicas, cinch's lease orchestrator, ...) can agree on the on-wire
// shape without depending on the full `hadb` coordinator crate.
// Re-exported for back-compat so existing imports (`hadb::LeaseData`,
// `haqlite::LeaseData`) keep working.
pub use hadb_lease::LeaseData;

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
    /// Exclusive writer side of the fence. The paired `AtomicFence` is the
    /// reader that storage adapters hold. `None` = no external observer
    /// (tests and single-writer deployments that don't fence).
    fence_writer: Option<Arc<AtomicFenceWriter>>,
    /// Wall-clock second at which the lease we currently hold was stamped
    /// (`claimed_at` of the last successful claim/renew/sleeping write).
    /// `None` when we hold no lease. Used to compute the proactive
    /// self-fence deadline so a partitioned leader relinquishes before
    /// TTL even when renewals are erroring rather than CAS-conflicting.
    current_claimed_at: Option<u64>,
}

impl DbLease {
    /// Create a new DbLease for a specific lease key.
    ///
    /// The caller owns the lease key format. S3/NATS backends typically use
    /// compound keys like `"{prefix}{db_name}/_lease.json"`; HTTP/token-scoped
    /// backends typically use a simple semantic name like `"writer"`.
    pub fn new(
        store: Arc<dyn LeaseStore>,
        lease_key: &str,
        instance_id: &str,
        address: &str,
        ttl_secs: u64,
    ) -> Self {
        Self {
            store,
            lease_key: lease_key.to_string(),
            instance_id: instance_id.to_string(),
            address: address.to_string(),
            ttl_secs,
            current_etag: None,
            session_id: uuid::Uuid::new_v4().to_string(),
            fence_writer: None,
            current_claimed_at: None,
        }
    }

    /// Attach the writer half of an `AtomicFence`. Callers pair this with
    /// an `AtomicFence` reader handed to every storage adapter that needs
    /// fenced writes.
    pub fn with_fence_writer(mut self, writer: Arc<AtomicFenceWriter>) -> Self {
        self.fence_writer = Some(writer);
        self
    }

    /// Publish the current etag (parsed as a `u64` lease revision) into
    /// the fence. No-op if no writer was attached.
    ///
    /// If a writer *is* attached but the etag doesn't parse as `u64`, we
    /// clear the fence and log. Letting it silently skip the update would
    /// leave a stale revision in place while the lease believes itself
    /// fresh -- the writer then issues fenced writes with a revision the
    /// server has long superseded and gets confusing "former leader"
    /// rejections. This is the "no silent failures" rule: either the
    /// lease backend produces u64 etags (Cinch, NATS KV) or the caller
    /// should not attach a fence_writer.
    fn update_fence_token(&self) {
        if let (Some(ref writer), Some(ref etag)) = (&self.fence_writer, &self.current_etag) {
            match etag.parse::<u64>() {
                Ok(rev) => writer.set(rev),
                Err(_) => {
                    tracing::error!(
                        lease_key = %self.lease_key,
                        etag = %etag,
                        "DbLease has a fence_writer but the lease store returned a non-u64 etag; \
                         clearing fence to force writes to fail fast rather than carry a stale revision"
                    );
                    writer.clear();
                }
            }
        }
    }

    /// Clear the fence (lease lost). No-op if no writer was attached.
    fn clear_fence_token(&self) {
        if let Some(ref writer) = self.fence_writer {
            writer.clear();
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
                } else if lease.is_claimable_by_other() {
                    // Another instance's lease is expired PAST the skew
                    // margin — only then is it safe to take over. Using the
                    // margined claimer check (not bare is_expired) keeps the
                    // holder's self-relinquish window strictly ahead of ours,
                    // so we never contend at the TTL boundary.
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

        let lease = self.make_lease();
        let claimed_at = lease.claimed_at;
        let body = serde_json::to_vec(&lease)?;
        let result = self
            .store
            .write_if_match(&self.lease_key, body, &etag)
            .await?;

        if result.success {
            self.current_etag = result.etag;
            self.current_claimed_at = Some(claimed_at);
            self.update_fence_token();
            Ok(true)
        } else {
            self.recover_ambiguous_renewal_conflict().await
        }
    }

    async fn recover_ambiguous_renewal_conflict(&mut self) -> Result<bool> {
        // The etag we believed we held going INTO the renewal. A genuine
        // "our commit landed but the response was lost" recovery must show
        // the store moved to a NEW etag that advanced consistently from
        // this one. Reusing the same etag, or one that regressed, means we
        // are looking at a stale read of a lease we have actually lost.
        let prior_etag = self.current_etag.clone();

        match self.read().await? {
            Some((lease, etag))
                if lease.instance_id == self.instance_id
                    && lease.session_id == self.session_id
                    && !lease.sleeping
                    && !lease.should_relinquish()
                    && Self::etag_advanced_consistently(prior_etag.as_deref(), &etag) =>
            {
                tracing::warn!(
                    "Lease renewal CAS conflict matched our current session on read-back \
                     with an advanced etag — treating prior ambiguous renewal as accepted"
                );
                self.current_etag = Some(etag);
                self.current_claimed_at = Some(lease.claimed_at);
                self.update_fence_token();
                Ok(true)
            }
            _ => {
                tracing::warn!(
                    "Lease renewal failed (ETag mismatch / unexpected read-back) — lost lease"
                );
                self.current_etag = None;
                self.current_claimed_at = None;
                self.clear_fence_token();
                Ok(false)
            }
        }
    }

    /// Did the read-back etag advance consistently from the one we held?
    ///
    /// A real ambiguous-commit recovery sees a NEW etag (the store applied
    /// our write) that did not regress. We require:
    /// - the read-back differs from our prior etag (equal == the store
    ///   never moved == we are reading a stale lease we lost), and
    /// - when both parse as `u64` revisions, the read-back is strictly
    ///   greater (a numeric regression means another writer's older
    ///   revision — lost leadership).
    ///
    /// Opaque (non-`u64`) etags can only be checked for inequality; that
    /// is the strongest signal those backends give. (Such backends should
    /// not attach a fence_writer — see `update_fence_token`.)
    fn etag_advanced_consistently(prior: Option<&str>, read_back: &str) -> bool {
        match prior {
            // No prior etag: we never held the lease, so a CAS conflict
            // here cannot be "our commit landed". Treat as lost.
            None => false,
            Some(prior) => {
                if prior == read_back {
                    return false;
                }
                match (prior.parse::<u64>(), read_back.parse::<u64>()) {
                    (Ok(prev), Ok(now)) => now > prev,
                    // Opaque etags: inequality is all we can verify.
                    _ => true,
                }
            }
        }
    }

    /// Release the lease (best-effort delete).
    pub async fn release(&mut self) -> Result<()> {
        self.store.delete(&self.lease_key).await?;
        self.current_etag = None;
        self.current_claimed_at = None;
        self.clear_fence_token();
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
        self.session_id = uuid::Uuid::new_v4().to_string();

        let lease = self.make_lease();
        let claimed_at = lease.claimed_at;
        let body = serde_json::to_vec(&lease)?;
        let result = self
            .store
            .write_if_not_exists(&self.lease_key, body)
            .await?;

        if result.success {
            self.current_etag = result.etag;
            self.current_claimed_at = Some(claimed_at);
            self.update_fence_token();
            Ok(Role::Leader)
        } else {
            Ok(Role::Follower)
        }
    }

    /// Try to take over an expired lease (CAS on the old etag).
    async fn try_claim_expired(&mut self, expired_etag: &str) -> Result<Role> {
        let lease = self.make_lease();
        let claimed_at = lease.claimed_at;
        let body = serde_json::to_vec(&lease)?;
        let result = self
            .store
            .write_if_match(&self.lease_key, body, expired_etag)
            .await?;

        if result.success {
            self.current_etag = result.etag;
            self.current_claimed_at = Some(claimed_at);
            self.update_fence_token();
            Ok(Role::Leader)
        } else {
            Ok(Role::Follower)
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

        let claimed_at = chrono::Utc::now().timestamp() as u64;
        let lease = LeaseData {
            instance_id: self.instance_id.clone(),
            address: self.address.clone(),
            claimed_at,
            ttl_secs: self.ttl_secs,
            session_id: self.session_id.clone(),
            sleeping: true,
        };
        let body = serde_json::to_vec(&lease)?;
        let result = self
            .store
            .write_if_match(&self.lease_key, body, &etag)
            .await?;

        if result.success {
            self.current_etag = result.etag;
            self.current_claimed_at = Some(claimed_at);
            self.update_fence_token();
            Ok(true)
        } else {
            tracing::warn!("set_sleeping CAS conflict — lost lease");
            self.current_etag = None;
            self.current_claimed_at = None;
            self.clear_fence_token();
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

    /// Proactive self-fence deadline: the wall-clock second past which the
    /// holder must stop trusting its lease (`claimed_at + ttl - margin`).
    /// `None` when we hold no lease. The renewal loop fences itself once
    /// `now_secs() >= renew_deadline_secs()`, INDEPENDENT of the
    /// consecutive-error count — a partitioned leader whose renewals only
    /// ever error still relinquishes before TTL, before any follower's
    /// claim window opens.
    pub fn renew_deadline_secs(&self) -> Option<u64> {
        self.current_claimed_at.map(|claimed_at| {
            claimed_at
                .saturating_add(self.ttl_secs)
                .saturating_sub(hadb_lease::LEASE_SKEW_MARGIN_SECS)
        })
    }

    /// Whether the holder should self-relinquish NOW (proactive fence).
    /// True once past [`renew_deadline_secs`](Self::renew_deadline_secs);
    /// always false when we hold no lease.
    pub fn should_self_fence(&self) -> bool {
        self.should_self_fence_at(now_secs())
    }

    /// [`should_self_fence`](Self::should_self_fence) with an injected clock.
    pub fn should_self_fence_at(&self, now: u64) -> bool {
        matches!(self.renew_deadline_secs(), Some(deadline) if now >= deadline)
    }
}

/// Current wall-clock in whole seconds, clamped at 0 (mirrors the helper in
/// `hadb-lease`; kept local to avoid widening that crate's public surface).
fn now_secs() -> u64 {
    let ts = chrono::Utc::now().timestamp();
    if ts < 0 {
        0
    } else {
        ts as u64
    }
}

// InMemoryLeaseStore lives in `hadb-lease-mem` now; re-exported from this
// crate for backwards-compatibility with callers that did
// `use hadb::InMemoryLeaseStore`.
pub use hadb_lease_mem::InMemoryLeaseStore;

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
            counter.to_string()
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
    // DbLease initialization
    // ========================================================================

    #[test]
    fn test_db_lease_new() {
        let store = Arc::new(MockLeaseStore::new());
        let lease = DbLease::new(
            store,
            "test-prefix/mydb/_lease.json",
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
        let mut lease = DbLease::new(store, "db1/_lease.json", "inst-1", "addr-1", 60);

        let role = lease.try_claim().await.unwrap();
        assert_eq!(role, Role::Leader);
        assert!(lease.has_etag());
    }

    #[tokio::test]
    async fn test_try_claim_sleeping_lease() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease1 = DbLease::new(store.clone(), "db1/_lease.json", "inst-1", "addr-1", 60);

        // First instance claims and sets sleeping
        lease1.try_claim().await.unwrap();
        lease1.set_sleeping().await.unwrap();

        // Second instance wakes it up
        let mut lease2 = DbLease::new(store, "db1/_lease.json", "inst-2", "addr-2", 60);
        let role = lease2.try_claim().await.unwrap();
        assert_eq!(role, Role::Leader);
        assert!(lease2.has_etag());
    }

    #[tokio::test]
    async fn test_try_claim_already_held_by_us() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease = DbLease::new(store.clone(), "db1/_lease.json", "inst-1", "addr-1", 60);

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
        let mut lease1 = DbLease::new(store.clone(), "db1/_lease.json", "inst-1", "addr-1", 60);
        let mut lease2 = DbLease::new(store, "db1/_lease.json", "inst-2", "addr-2", 60);

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
        let mut lease = DbLease::new(store, "db1/_lease.json", "inst-2", "addr-2", 60);
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
        let mut lease = DbLease::new(store, "db1/_lease.json", "inst-1", "addr-1", 60);

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
        let mut lease1 = DbLease::new(store.clone(), "db1/_lease.json", "inst-1", "addr-1", 60);
        let mut lease2 = DbLease::new(store, "db1/_lease.json", "inst-2", "addr-2", 60);

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
        lease2
            .store
            .write_if_match(
                lease2.lease_key(),
                serde_json::to_vec(&expired_lease).unwrap(),
                lease1.current_etag.as_ref().unwrap(),
            )
            .await
            .unwrap();

        // Instance 2 takes over
        lease2.try_claim().await.unwrap();

        // Instance 1 tries to renew (should fail due to CAS conflict)
        let renewed = lease1.renew().await.unwrap();
        assert!(!renewed);
        assert!(!lease1.has_etag());
    }

    struct AmbiguousCommitStore {
        inner: MockLeaseStore,
        fail_next_match_after_commit: Arc<Mutex<bool>>,
    }

    impl AmbiguousCommitStore {
        fn new() -> Self {
            Self {
                inner: MockLeaseStore::new(),
                fail_next_match_after_commit: Arc::new(Mutex::new(false)),
            }
        }

        fn fail_next_match_after_commit(&self) {
            *self.fail_next_match_after_commit.lock().unwrap() = true;
        }
    }

    #[async_trait::async_trait]
    impl LeaseStore for AmbiguousCommitStore {
        async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
            self.inner.read(key).await
        }

        async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult> {
            self.inner.write_if_not_exists(key, data).await
        }

        async fn write_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<CasResult> {
            if *self.fail_next_match_after_commit.lock().unwrap() {
                let mut store = self.inner.data.lock().unwrap();
                match store.get(key) {
                    Some((_, current_etag)) if current_etag == etag => {
                        let new_etag = self.inner.gen_etag();
                        store.insert(key.to_string(), (data, new_etag));
                        *self.fail_next_match_after_commit.lock().unwrap() = false;
                        return Ok(CasResult {
                            success: false,
                            etag: None,
                        });
                    }
                    _ => {}
                }
            }
            self.inner.write_if_match(key, data, etag).await
        }

        async fn delete(&self, key: &str) -> Result<()> {
            self.inner.delete(key).await
        }
    }

    #[tokio::test]
    async fn test_renew_recovers_when_prior_commit_succeeded_but_response_was_lost() {
        let store = Arc::new(AmbiguousCommitStore::new());
        let (fence, writer) = AtomicFence::new();
        let mut lease = DbLease::new(store.clone(), "db1/_lease.json", "inst-1", "addr-1", 60)
            .with_fence_writer(Arc::new(writer));

        lease.try_claim().await.unwrap();
        let session_before = lease.session_id().to_string();
        let fence_after_claim = fence.current().expect("fence set after claim");

        store.fail_next_match_after_commit();
        let renewed = lease.renew().await.unwrap();

        assert!(renewed, "same-session read-back should retain leadership");
        assert_eq!(lease.session_id(), session_before);
        assert!(lease.has_etag());
        assert!(
            fence.current().expect("fence after recovered renewal") > fence_after_claim,
            "read-back recovery must advance the storage fence"
        );

        assert!(
            lease.renew().await.unwrap(),
            "subsequent renew should use the recovered etag"
        );
    }

    #[tokio::test]
    async fn test_renew_without_etag() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease = DbLease::new(store, "db1/_lease.json", "inst-1", "addr-1", 60);

        // Try to renew without claiming first
        let result = lease.renew().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No ETag"));
    }

    /// Finding 3: a store that reports a CAS conflict on `write_if_match`
    /// but leaves the stored lease UNTOUCHED (same etag as the one we held).
    /// Models a non-atomic / read-after-write-lagging backend where the
    /// read-back still carries our identity from a stale view even though
    /// we have actually lost the lease.
    struct ConflictWithoutAdvanceStore {
        inner: MockLeaseStore,
    }
    impl ConflictWithoutAdvanceStore {
        fn new() -> Self {
            Self {
                inner: MockLeaseStore::new(),
            }
        }
    }
    #[async_trait::async_trait]
    impl LeaseStore for ConflictWithoutAdvanceStore {
        async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
            self.inner.read(key).await
        }
        async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult> {
            self.inner.write_if_not_exists(key, data).await
        }
        async fn write_if_match(
            &self,
            _key: &str,
            _data: Vec<u8>,
            _etag: &str,
        ) -> Result<CasResult> {
            // Always report conflict, never mutate the store. Read-back will
            // show our identity + the SAME etag we already held.
            Ok(CasResult {
                success: false,
                etag: None,
            })
        }
        async fn delete(&self, key: &str) -> Result<()> {
            self.inner.delete(key).await
        }
    }

    #[tokio::test]
    async fn recover_rejects_unexpected_etag() {
        let store = Arc::new(ConflictWithoutAdvanceStore::new());
        let (fence, writer) = AtomicFence::new();
        let mut lease = DbLease::new(store, "db1/_lease.json", "inst-1", "addr-1", 60)
            .with_fence_writer(Arc::new(writer));

        // Claim succeeds (write_if_not_exists path).
        assert_eq!(lease.try_claim().await.unwrap(), Role::Leader);
        assert!(lease.has_etag());
        assert!(fence.current().is_some(), "fence set after claim");

        // Renew CAS-conflicts and the read-back shows our identity but the
        // SAME etag we already held (no consistent advance) — recovery must
        // treat this as LOST leadership, not falsely re-confirm.
        let renewed = lease.renew().await.unwrap();
        assert!(
            !renewed,
            "read-back with a non-advanced etag must be treated as lost leadership"
        );
        assert!(!lease.has_etag(), "etag must be cleared on lost leadership");
        assert!(
            fence.current().is_none(),
            "fence must be cleared so downstream writes fail fast (NoActiveLease)"
        );
    }

    #[test]
    fn etag_advanced_consistently_rules() {
        // No prior etag: never a valid recovery.
        assert!(!DbLease::etag_advanced_consistently(None, "5"));
        // Same etag: store never moved — lost.
        assert!(!DbLease::etag_advanced_consistently(Some("5"), "5"));
        // Numeric regression: an older revision — lost.
        assert!(!DbLease::etag_advanced_consistently(Some("5"), "4"));
        // Numeric strict advance: valid recovery.
        assert!(DbLease::etag_advanced_consistently(Some("5"), "6"));
        // Opaque etags that differ: inequality is the strongest signal.
        assert!(DbLease::etag_advanced_consistently(
            Some("\"aaa\""),
            "\"bbb\""
        ));
        assert!(!DbLease::etag_advanced_consistently(
            Some("\"aaa\""),
            "\"aaa\""
        ));
    }

    // ========================================================================
    // release() tests
    // ========================================================================

    #[tokio::test]
    async fn test_release() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease = DbLease::new(store.clone(), "db1/_lease.json", "inst-1", "addr-1", 60);

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
        let mut lease = DbLease::new(store, "db1/_lease.json", "inst-1", "addr-1", 60);

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
        let mut lease = DbLease::new(store, "db1/_lease.json", "inst-1", "addr-1", 60);

        // Try to set sleeping without claiming first
        let result = lease.set_sleeping().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No ETag"));
    }

    #[tokio::test]
    async fn test_set_sleeping_cas_conflict() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease1 = DbLease::new(store.clone(), "db1/_lease.json", "inst-1", "addr-1", 60);
        let mut lease2 = DbLease::new(store, "db1/_lease.json", "inst-2", "addr-2", 60);

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
        lease2
            .store
            .write_if_match(
                lease2.lease_key(),
                serde_json::to_vec(&expired_lease).unwrap(),
                lease1.current_etag.as_ref().unwrap(),
            )
            .await
            .unwrap();
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
        let lease = DbLease::new(store, "db1/_lease.json", "inst-1", "addr-1", 60);

        let result = lease.read().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_read_existing_lease() {
        let store = Arc::new(MockLeaseStore::new());
        let mut lease = DbLease::new(store, "db1/_lease.json", "inst-1", "addr-1", 60);

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
        let mut lease1 = DbLease::new(store.clone(), "db1/_lease.json", "inst-1", "addr-1", 60);

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
        let mut lease = DbLease::new(store, "db1/_lease.json", "inst-1", "addr-1", 60);

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
        let mut lease1 = DbLease::new(store.clone(), "db1/_lease.json", "inst-1", "addr-1", 60);
        let mut lease2 = DbLease::new(store, "db1/_lease.json", "inst-2", "addr-2", 60);

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
        let mut lease = DbLease::new(store, "db1/_lease.json", "inst-1", "addr-1", 60);

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

    // ========================================================================
    // Fence token integration
    // ========================================================================

    use hadb_lease::AtomicFence;

    #[tokio::test]
    async fn test_fence_token_updated_on_claim() {
        let store = Arc::new(InMemoryLeaseStore::new());
        let (fence, writer) = AtomicFence::new();

        let mut lease = DbLease::new(store, "db1/_lease.json", "inst-1", "addr-1", 60)
            .with_fence_writer(Arc::new(writer));

        assert!(fence.current().is_none());

        let role = lease.try_claim().await.unwrap();
        assert_eq!(role, Role::Leader);

        let fence_val = fence.current().expect("fence should be set after claim");
        assert!(fence_val > 0, "fence revision must be > 0, got {fence_val}");
    }

    #[tokio::test]
    async fn test_fence_token_updated_on_renew() {
        let store = Arc::new(InMemoryLeaseStore::new());
        let (fence, writer) = AtomicFence::new();

        let mut lease = DbLease::new(store, "db1/_lease.json", "inst-1", "addr-1", 60)
            .with_fence_writer(Arc::new(writer));

        lease.try_claim().await.unwrap();
        let fence_after_claim = fence.current().expect("fence set after claim");

        lease.renew().await.unwrap();
        let fence_after_renew = fence.current().expect("fence set after renew");

        assert!(
            fence_after_renew > fence_after_claim,
            "fence should increase on renew: {fence_after_claim} -> {fence_after_renew}",
        );
    }

    #[tokio::test]
    async fn test_fence_token_cleared_on_release() {
        let store = Arc::new(InMemoryLeaseStore::new());
        let (fence, writer) = AtomicFence::new();

        let mut lease = DbLease::new(store, "db1/_lease.json", "inst-1", "addr-1", 60)
            .with_fence_writer(Arc::new(writer));

        lease.try_claim().await.unwrap();
        assert!(fence.current().is_some());

        lease.release().await.unwrap();
        assert!(
            fence.current().is_none(),
            "fence should be cleared on release"
        );
    }

    #[tokio::test]
    async fn test_fence_token_cleared_on_renewal_failure() {
        let store = Arc::new(InMemoryLeaseStore::new());
        let (fence, writer) = AtomicFence::new();

        let mut lease1 = DbLease::new(store.clone(), "db1/_lease.json", "inst-1", "addr-1", 60)
            .with_fence_writer(Arc::new(writer));
        let mut lease2 = DbLease::new(store, "db1/_lease.json", "inst-2", "addr-2", 60);

        lease1.try_claim().await.unwrap();
        assert!(fence.current().is_some());

        lease1.release().await.unwrap();
        lease2.try_claim().await.unwrap();

        // inst-1's release cleared the fence. inst-2 has its own (unattached) lease.
        assert!(fence.current().is_none());
    }

    #[tokio::test]
    async fn test_fence_token_updated_on_set_sleeping() {
        let store = Arc::new(InMemoryLeaseStore::new());
        let (fence, writer) = AtomicFence::new();

        let mut lease = DbLease::new(store, "db1/_lease.json", "inst-1", "addr-1", 60)
            .with_fence_writer(Arc::new(writer));

        lease.try_claim().await.unwrap();
        let fence_after_claim = fence.current().expect("fence set after claim");

        lease.set_sleeping().await.unwrap();
        let fence_after_sleeping = fence.current().expect("fence set after sleeping");

        assert!(
            fence_after_sleeping > fence_after_claim,
            "fence should increase on set_sleeping: {fence_after_claim} -> {fence_after_sleeping}",
        );
    }

    /// Lease store whose etags are opaque strings, not u64s. Models S3's
    /// behaviour (quoted ETag hex) and any other backend that doesn't
    /// produce numeric revisions.
    struct OpaqueEtagStore {
        data: Arc<Mutex<HashMap<String, (Vec<u8>, String)>>>,
    }
    impl OpaqueEtagStore {
        fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }
    #[async_trait::async_trait]
    impl LeaseStore for OpaqueEtagStore {
        async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
            Ok(self.data.lock().unwrap().get(key).cloned())
        }
        async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult> {
            let mut store = self.data.lock().unwrap();
            if store.contains_key(key) {
                return Ok(CasResult {
                    success: false,
                    etag: None,
                });
            }
            let etag = format!("\"{:x}\"", uuid::Uuid::new_v4().as_u128());
            store.insert(key.to_string(), (data, etag.clone()));
            Ok(CasResult {
                success: true,
                etag: Some(etag),
            })
        }
        async fn write_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<CasResult> {
            let mut store = self.data.lock().unwrap();
            match store.get(key) {
                Some((_, current)) if current == etag => {
                    let new_etag = format!("\"{:x}\"", uuid::Uuid::new_v4().as_u128());
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
            self.data.lock().unwrap().remove(key);
            Ok(())
        }
    }

    /// Phase Anvil i review fix: if `DbLease` is handed a `fence_writer` but
    /// the lease store's etag isn't parseable as u64, the fence must be
    /// CLEARED (not silently skipped). Carrying a stale revision while the
    /// lease manager believes itself fresh is the "former leader writes"
    /// data-corruption scenario. Failing fast (NoActiveLease) is the
    /// correct outcome per CLAUDE.md "no silent failures".
    #[tokio::test]
    async fn test_fence_cleared_on_non_u64_etag() {
        let store = Arc::new(OpaqueEtagStore::new());
        let (fence, writer) = AtomicFence::new();

        // Manually seed the writer so we can observe that it gets CLEARED
        // rather than left at a stale value.
        writer.set(42);
        assert_eq!(fence.current(), Some(42), "preseeded fence is visible");

        let mut lease = DbLease::new(store, "db1/_lease.json", "inst-1", "addr-1", 60)
            .with_fence_writer(Arc::new(writer));

        let role = lease.try_claim().await.expect("claim");
        assert_eq!(
            role,
            Role::Leader,
            "opaque-etag backend still supports CAS claim"
        );

        // The fence must now be None. The key invariant: downstream storage
        // adapters calling `fence.require()` get `NoActiveLease` (write fails
        // fast) instead of the stale `42` (write carries a revision the
        // server will reject as former-leader). The old code left `42` here.
        assert_eq!(
            fence.current(),
            None,
            "fence must clear when the etag doesn't parse as u64; leaving a stale \
             revision is the silent-failure bug the Anvil-i review fix closed"
        );
    }

    /// Follow-up: renew doesn't restore the fence either; the opaque etag
    /// keeps producing non-u64 values, so every write must keep failing.
    /// Pins the contract that bad etags are terminal for fence-enforced
    /// writes (rather than intermittent).
    #[tokio::test]
    async fn test_fence_stays_cleared_across_renew_with_opaque_etag() {
        let store = Arc::new(OpaqueEtagStore::new());
        let (fence, writer) = AtomicFence::new();
        let mut lease = DbLease::new(store, "db1/_lease.json", "inst-1", "addr-1", 60)
            .with_fence_writer(Arc::new(writer));

        lease.try_claim().await.unwrap();
        assert_eq!(fence.current(), None, "first claim clears");

        let renewed = lease.renew().await.unwrap();
        assert!(renewed, "renew with valid etag succeeds");
        assert_eq!(
            fence.current(),
            None,
            "renew must keep fence cleared since etag is still opaque"
        );
    }
}
