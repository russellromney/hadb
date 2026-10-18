//! Ownership failures through public DbLease and LeaseStore interfaces.

use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};
use std::time::Duration;

use anyhow::{bail, Result};
use async_trait::async_trait;
use hadb::{CasResult, DbLease, InMemoryLeaseStore, LeaseStore, Role};
use hadb_lease::{AtomicFence, LeaseData};

/// Uses real memory CAS. Only transport outcomes are controlled here.
struct TransportStore {
    inner: InMemoryLeaseStore,
    outcome: AtomicU8,
}

impl TransportStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: InMemoryLeaseStore::new(),
            outcome: AtomicU8::new(0),
        })
    }

    async fn transport(&self) -> Result<()> {
        match self.outcome.load(Ordering::SeqCst) {
            1 => bail!("transport failed"),
            2 => std::future::pending().await,
            5 => {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[async_trait]
impl LeaseStore for TransportStore {
    async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        self.inner.read(key).await
    }
    async fn write_if_not_exists(&self, key: &str, bytes: Vec<u8>) -> Result<CasResult> {
        let mut receipt = self.inner.write_if_not_exists(key, bytes).await?;
        if self.outcome.load(Ordering::SeqCst) == 3 {
            receipt.etag = None;
        }
        Ok(receipt)
    }
    async fn write_if_match(&self, key: &str, bytes: Vec<u8>, expected: &str) -> Result<CasResult> {
        self.transport().await?;
        let mut receipt = self.inner.write_if_match(key, bytes, expected).await?;
        if self.outcome.load(Ordering::SeqCst) == 3 {
            receipt.etag = None;
        }
        if self.outcome.load(Ordering::SeqCst) == 4 {
            receipt.etag = Some(expected.to_owned());
        }
        Ok(receipt)
    }
    async fn delete(&self, key: &str) -> Result<()> {
        self.transport().await?;
        self.inner.delete(key).await
    }
}

#[tokio::test]
async fn default_release_relinquishes_by_cas_without_deleting() {
    let store = TransportStore::new();
    let mut owner = DbLease::new(store.clone(), "db", "old", "address", 60);
    assert_eq!(owner.try_claim().await.unwrap(), Role::Leader);
    let old_session = owner.session_id().to_owned();
    owner.release().await.unwrap();
    let (bytes, _) = store
        .read("db")
        .await
        .unwrap()
        .expect("default release leaves a readable expired sentinel");
    let expired: LeaseData = serde_json::from_slice(&bytes).unwrap();
    assert!(expired.is_claimable_by_other());
    assert!(!expired.sleeping);
    assert_ne!(expired.session_id, old_session);

    let mut successor = DbLease::new(store.clone(), "db", "new", "address", 60);
    assert_eq!(successor.try_claim().await.unwrap(), Role::Leader);
    assert!(successor.renew().await.unwrap());
}

#[tokio::test]
async fn default_release_and_renewal_reject_a_superseded_owner() {
    for release in [true, false] {
        let store = TransportStore::new();
        let (old_fence, old_writer) = AtomicFence::new();
        let (new_fence, new_writer) = AtomicFence::new();
        let mut old = DbLease::new(store.clone(), "db", "same-instance", "old", 60)
            .with_fence_writer(Arc::new(old_writer));
        let mut successor = DbLease::new(store, "db", "same-instance", "new", 60)
            .with_fence_writer(Arc::new(new_writer));
        assert_eq!(old.try_claim().await.unwrap(), Role::Leader);
        assert_eq!(successor.try_claim().await.unwrap(), Role::Leader);
        assert_ne!(old.session_id(), successor.session_id());
        assert!(old_fence.current().is_some());
        let successor_revision = new_fence.current().unwrap();
        // The old owner's FIRST release/renewal happens only after succession.
        if release {
            old.release().await.unwrap();
        } else {
            assert!(!old.renew().await.unwrap());
        }
        assert!(old_fence.current().is_none());
        assert_eq!(new_fence.current(), Some(successor_revision));
        assert_eq!(
            successor.read().await.unwrap().unwrap().0.session_id,
            successor.session_id()
        );
        assert!(successor.renew().await.unwrap());
        assert!(new_fence.current().unwrap() > successor_revision);
    }
}

#[tokio::test(start_paused = true)]
async fn failed_or_cancelled_release_clears_authority_before_io() {
    for outcome in [1, 2] {
        let store = TransportStore::new();
        let (fence, writer) = AtomicFence::new();
        let mut lease = DbLease::new(store.clone(), "db", "owner", "address", 60)
            .with_fence_writer(Arc::new(writer));
        assert_eq!(lease.try_claim().await.unwrap(), Role::Leader);
        assert!(fence.current().is_some());
        store.outcome.store(outcome, Ordering::SeqCst);
        let result = tokio::time::timeout(Duration::from_millis(20), lease.release()).await;
        if outcome == 1 {
            assert!(result.unwrap().is_err());
        } else {
            assert!(result.is_err());
        }
        assert!(
            fence.current().is_none(),
            "failed or cancelled release must clear fence"
        );
        assert!(!lease.has_etag());
    }
}

#[tokio::test(start_paused = true)]
async fn unconfirmed_renewal_never_keeps_an_active_fence() {
    for outcome in [1, 2, 3, 4] {
        let store = TransportStore::new();
        let (fence, writer) = AtomicFence::new();
        let mut lease = DbLease::new(store.clone(), "db", "owner", "address", 60)
            .with_fence_writer(Arc::new(writer));
        assert_eq!(lease.try_claim().await.unwrap(), Role::Leader);
        assert!(fence.current().is_some());
        store.outcome.store(outcome, Ordering::SeqCst);
        let result = tokio::time::timeout(Duration::from_millis(20), lease.renew()).await;
        assert!(
            !matches!(result, Ok(Ok(true))),
            "unconfirmed renewal cannot succeed"
        );
        assert!(
            fence.current().is_none(),
            "outcome {outcome} left stale authority active"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn holder_deadline_revokes_a_blocked_renewal() {
    let store = TransportStore::new();
    let (fence, writer) = AtomicFence::new();
    let mut lease = DbLease::new(store.clone(), "db", "owner", "address", 4)
        .with_fence_writer(Arc::new(writer));
    assert_eq!(lease.try_claim().await.unwrap(), Role::Leader);
    store.outcome.store(2, Ordering::SeqCst);
    let renewal = lease.renew();
    tokio::pin!(renewal);
    tokio::select! {
        biased;
        _ = &mut renewal => panic!("fixture must block inside the lease transport"),
        _ = tokio::task::yield_now() => {}
    }
    assert!(
        fence.current().is_some(),
        "a valid bounded renewal retains confirmed authority"
    );
    let started = tokio::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(5), &mut renewal)
        .await
        .expect("internal holder deadline must fire before the longer test watchdog");
    assert!(result.is_err());
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(fence.current().is_none());
}

#[tokio::test(start_paused = true)]
async fn confirmed_fence_survives_a_bounded_renewal() {
    let store = TransportStore::new();
    let (fence, writer) = AtomicFence::new();
    let mut lease = DbLease::new(store.clone(), "db", "owner", "address", 60)
        .with_fence_writer(Arc::new(writer));
    assert_eq!(lease.try_claim().await.unwrap(), Role::Leader);
    let before = fence.current().unwrap();
    store.outcome.store(5, Ordering::SeqCst);
    let renewal = lease.renew();
    tokio::pin!(renewal);
    tokio::select! {
        biased;
        _ = &mut renewal => panic!("fixture must delay the bounded renewal"),
        _ = tokio::task::yield_now() => {}
    }
    assert_eq!(fence.current(), Some(before));
    assert!(renewal.await.unwrap());
    assert!(fence.current().unwrap() > before);
}

#[tokio::test]
async fn missing_claim_or_sleep_receipt_cannot_publish_a_fence() {
    for sleeping in [false, true] {
        let store = TransportStore::new();
        let (fence, writer) = AtomicFence::new();
        writer.set(42);
        let mut lease = DbLease::new(store.clone(), "db", "owner", "address", 60)
            .with_fence_writer(Arc::new(writer));
        if sleeping {
            assert_eq!(lease.try_claim().await.unwrap(), Role::Leader);
        }
        store.outcome.store(3, Ordering::SeqCst);
        if sleeping {
            assert!(lease.set_sleeping().await.is_err());
        } else {
            assert!(lease.try_claim().await.is_err());
        }
        assert!(
            fence.current().is_none(),
            "missing receipt must revoke stale authority"
        );
    }
}

#[tokio::test]
async fn dropping_owner_clears_its_paired_fence() {
    let (fence, writer) = AtomicFence::new();
    let mut lease = DbLease::new(
        Arc::new(InMemoryLeaseStore::new()),
        "db",
        "owner",
        "address",
        60,
    )
    .with_fence_writer(Arc::new(writer));
    assert_eq!(lease.try_claim().await.unwrap(), Role::Leader);
    assert!(fence.current().is_some());
    drop(lease);
    assert!(
        fence.current().is_none(),
        "dropping the owner must revoke its local fence"
    );
}
