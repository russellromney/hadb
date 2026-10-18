//! Real-S3 lease contracts. Run explicitly with HADB_S3_SERVICE_FILE pointing
//! to private service JSON containing endpoint, bucket, and AWS environment.

use std::{collections::HashMap, sync::Arc};

use hadb::DbLease;
use hadb_lease::{AtomicFence, LeaseStore};
use hadb_lease_s3::S3LeaseStore;
use serde::Deserialize;

mod support;
use support::FaultProxy;

#[derive(Deserialize)]
struct Service {
    endpoint: String,
    bucket: String,
    env: HashMap<String, String>,
}

struct Fixture {
    service: Service,
    prefix: String,
}

impl Fixture {
    fn new() -> Self {
        let path = std::env::var("HADB_S3_SERVICE_FILE")
            .expect("explicit S3 tests require HADB_S3_SERVICE_FILE");
        let service =
            serde_json::from_slice(&std::fs::read(path).expect("read private service config"))
                .expect("parse private service config");
        Self {
            service,
            prefix: format!("hadb-lease-contract/{}/", uuid::Uuid::new_v4()),
        }
    }

    fn client(&self) -> aws_sdk_s3::Client {
        self.client_at(&self.service.endpoint)
    }

    fn client_at(&self, endpoint: &str) -> aws_sdk_s3::Client {
        let conf = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(
                self.service.env["AWS_REGION"].clone(),
            ))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                self.service.env["AWS_ACCESS_KEY_ID"].clone(),
                self.service.env["AWS_SECRET_ACCESS_KEY"].clone(),
                None,
                None,
                "private-test-service",
            ))
            .endpoint_url(endpoint)
            .retry_config(aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1))
            .force_path_style(true)
            .build();
        aws_sdk_s3::Client::from_conf(conf)
    }

    fn store(&self) -> Arc<S3LeaseStore> {
        Arc::new(
            S3LeaseStore::new(self.client(), self.service.bucket.clone()).with_prefix(&self.prefix),
        )
    }

    async fn raw(&self, key: &str, bytes: Vec<u8>, format: Option<&str>) {
        let mut request = self
            .client()
            .put_object()
            .bucket(&self.service.bucket)
            .key(key)
            .body(bytes.into());
        if let Some(format) = format {
            request = request.metadata("hadb-lease-format", format);
        }
        request
            .send()
            .await
            .expect("prepare isolated on-store compatibility fixture");
    }
}

#[tokio::test]
#[ignore = "requires dedicated S3 service; run with HADB_S3_SERVICE_FILE"]
async fn s3_revisions_survive_recreation_and_new_process() {
    let fixture = Fixture::new();
    let store = fixture.store();
    let bytes = vec![0, 255, b'{', b'\n'];
    if let Ok(key) = std::env::var("HADB_S3_CHILD_KEY") {
        let expected = std::env::var("HADB_S3_CHILD_TOKEN").unwrap();
        assert!(
            store
                .write_if_match(&key, bytes, &expected)
                .await
                .unwrap()
                .success
        );
        return;
    }
    let key = store.key_for("bytes");
    let created = store
        .write_if_not_exists(&key, bytes.clone())
        .await
        .unwrap();
    assert!(created.success);
    let first = created.etag.unwrap();
    let revision: u64 = first
        .parse()
        .expect("S3 lease receipts must be numeric revisions");
    assert!(revision > 0);
    assert_eq!(
        store.read(&key).await.unwrap().unwrap(),
        (bytes.clone(), first.clone())
    );

    // A new OS process must advance the persisted sequence, even with identical
    // bytes. No shared cache or process-local counter can provide this proof.
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "s3_revisions_survive_recreation_and_new_process",
        ])
        .env("HADB_S3_CHILD_KEY", &key)
        .env("HADB_S3_CHILD_TOKEN", &first)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "subprocess failed: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let (read_bytes, second) = fixture.store().read(&key).await.unwrap().unwrap();
    assert_eq!(read_bytes, bytes);
    assert!(second.parse::<u64>().unwrap() > revision);
    assert!(
        !store
            .write_if_match(&key, vec![], &first)
            .await
            .unwrap()
            .success
    );

    let empty = store.write_if_match(&key, vec![], &second).await.unwrap();
    assert!(empty.success);
    let empty_token = empty.etag.unwrap();
    assert!(empty_token.parse::<u64>().unwrap() > second.parse::<u64>().unwrap());
    assert_eq!(store.read(&key).await.unwrap().unwrap().0, Vec::<u8>::new());
    store.delete(&key).await.unwrap();
    assert!(store.read(&key).await.unwrap().is_none());
    store.delete(&key).await.unwrap();
    let recreated = fixture
        .store()
        .write_if_not_exists(&key, bytes.clone())
        .await
        .unwrap();
    assert!(recreated.success);
    let recreated_token = recreated.etag.unwrap();
    assert!(recreated_token.parse::<u64>().unwrap() > empty_token.parse::<u64>().unwrap());
    for stale in [&first, &second, &empty_token] {
        assert!(
            !store
                .write_if_match(&key, vec![], stale)
                .await
                .unwrap()
                .success
        );
    }
    assert_eq!(
        store.read(&key).await.unwrap().unwrap(),
        (bytes, recreated_token)
    );
}

#[tokio::test]
#[ignore = "requires dedicated S3 service; run with HADB_S3_SERVICE_FILE"]
async fn s3_stale_owner_cannot_release_a_successor() {
    let fixture = Fixture::new();
    let store = fixture.store();
    let key = store.key_for("db");
    let (old_fence, old_writer) = AtomicFence::new();
    let (new_fence, new_writer) = AtomicFence::new();
    let mut old = DbLease::new(store.clone(), &key, "same-instance", "old", 60)
        .with_fence_writer(Arc::new(old_writer));
    let mut successor = DbLease::new(fixture.store(), &key, "same-instance", "new", 60)
        .with_fence_writer(Arc::new(new_writer));
    assert_eq!(old.try_claim().await.unwrap(), hadb::Role::Leader);
    assert_eq!(successor.try_claim().await.unwrap(), hadb::Role::Leader);
    assert_ne!(old.session_id(), successor.session_id());

    old.release().await.unwrap();
    let (data, _) = successor
        .read()
        .await
        .unwrap()
        .expect("stale release must not delete the successor's lease");
    assert_eq!(data.session_id, successor.session_id());
    assert!(old_fence.current().is_none());
    let before = new_fence.current().expect("successor fence remains active");
    assert!(successor.renew().await.unwrap());
    assert!(new_fence.current().unwrap() > before);
    successor.release().await.unwrap();
    assert!(new_fence.current().is_none());
    assert!(store.read(&key).await.unwrap().is_none());
}

#[tokio::test]
#[ignore = "requires dedicated S3 service; run with HADB_S3_SERVICE_FILE"]
async fn s3_conditional_create_and_tombstone_reclaim_have_one_winner() {
    let fixture = Fixture::new();
    let store = fixture.store();
    let key = store.key_for("contended");
    let mut previous = 0;
    for _ in 0..3 {
        let attempts = (0..12).map(|writer| {
            let client = fixture.store();
            let key = key.clone();
            async move {
                client
                    .write_if_not_exists(&key, vec![writer])
                    .await
                    .unwrap()
            }
        });
        let receipts = futures::future::join_all(attempts).await;
        let winners: Vec<_> = receipts.iter().filter(|receipt| receipt.success).collect();
        assert_eq!(
            winners.len(),
            1,
            "atomic create/reclaim must have one winner"
        );
        let winner = winners[0].etag.as_ref().unwrap();
        assert!(winner.parse::<u64>().unwrap() > previous);
        assert_eq!(store.read(&key).await.unwrap().unwrap().1, *winner);
        if previous > 0 {
            assert!(!store
                .release_if_match(&key, &previous.to_string())
                .await
                .unwrap());
        }
        previous = winner.parse().unwrap();
        assert!(store.release_if_match(&key, winner).await.unwrap());
        assert!(store.read(&key).await.unwrap().is_none());
    }
}

#[tokio::test]
#[ignore = "requires dedicated S3 service; run with HADB_S3_SERVICE_FILE"]
async fn s3_legacy_payloads_migrate_only_on_matched_mutation() {
    let fixture = Fixture::new();
    let store = fixture.store();
    let payloads = [
        vec![],
        vec![0, 255, 128],
        br#"{"version":1,"revision":999,"payload":"tombstone"}"#.to_vec(),
    ];
    for (index, bytes) in payloads.into_iter().enumerate() {
        let key = store.key_for(&format!("raw-{index}"));
        fixture.raw(&key, bytes.clone(), None).await;
        let (read, legacy) = store.read(&key).await.unwrap().unwrap();
        assert_eq!(read, bytes);
        assert!(
            legacy.parse::<u64>().is_err(),
            "legacy state cannot supply numeric fence proof"
        );
        assert!(
            !store
                .write_if_not_exists(&key, vec![1])
                .await
                .unwrap()
                .success
        );
        let receipts = futures::future::join_all((0..2).map(|_| {
            let client = fixture.store();
            let key = key.clone();
            let bytes = bytes.clone();
            let legacy = legacy.clone();
            async move { client.write_if_match(&key, bytes, &legacy).await.unwrap() }
        }))
        .await;
        assert_eq!(receipts.iter().filter(|receipt| receipt.success).count(), 1);
        let (read, migrated) = store.read(&key).await.unwrap().unwrap();
        assert_eq!(read, bytes);
        assert_eq!(
            migrated, "1",
            "conditional conversion begins a new numeric history"
        );
        assert!(!store.release_if_match(&key, &legacy).await.unwrap());
        assert!(
            !store
                .write_if_match(&key, vec![], &legacy)
                .await
                .unwrap()
                .success
        );
    }

    let key = store.key_for("raw-release");
    fixture.raw(&key, vec![255], None).await;
    let (_, legacy) = store.read(&key).await.unwrap().unwrap();
    assert!(store.release_if_match(&key, &legacy).await.unwrap());
    assert!(store.read(&key).await.unwrap().is_none());
    let recreated = fixture
        .store()
        .write_if_not_exists(&key, vec![0])
        .await
        .unwrap();
    assert!(recreated.success);
    assert_eq!(recreated.etag.as_deref(), Some("2"));
    assert!(!store.release_if_match(&key, &legacy).await.unwrap());
}

#[tokio::test]
#[ignore = "requires dedicated S3 service; run with HADB_S3_SERVICE_FILE"]
async fn s3_corrupt_or_exhausted_revision_records_fail_closed() {
    let fixture = Fixture::new();
    let store = fixture.store();
    let key = store.key_for("invalid");
    for (format, bytes) in [
        ("future", br#"{}"#.to_vec()),
        ("1", br#"not json"#.to_vec()),
        ("1", br#"{"version":1,"revision":1}"#.to_vec()),
        (
            "1",
            br#"{"version":1,"revision":1,"payload":null}"#.to_vec(),
        ),
        (
            "1",
            br#"{"version":1,"revision":1,"payload":{"future":[]}}"#.to_vec(),
        ),
        (
            "1",
            br#"{"version":1,"revision":1,"payload":{"live":null}}"#.to_vec(),
        ),
        (
            "1",
            br#"{"version":1,"revision":0,"payload":"tombstone"}"#.to_vec(),
        ),
        (
            "1",
            br#"{"version":2,"revision":1,"payload":"tombstone"}"#.to_vec(),
        ),
    ] {
        fixture.raw(&key, bytes, Some(format)).await;
        assert!(store.read(&key).await.is_err());
        assert!(store.write_if_not_exists(&key, vec![]).await.is_err());
        assert!(store.write_if_match(&key, vec![], "1").await.is_err());
        assert!(store.release_if_match(&key, "1").await.is_err());
        assert!(store.delete(&key).await.is_err());
    }

    let key = store.key_for("overflow");
    fixture
        .raw(
            &key,
            serde_json::to_vec(&serde_json::json!({
                "version": 1, "revision": u64::MAX - 1, "payload": { "live": [255] }
            }))
            .unwrap(),
            Some("1"),
        )
        .await;
    let last = store
        .write_if_match(&key, vec![0], &(u64::MAX - 1).to_string())
        .await
        .unwrap();
    assert!(last.success);
    let last = last.etag.unwrap();
    assert_eq!(last, u64::MAX.to_string());
    assert!(store.write_if_match(&key, vec![1], &last).await.is_err());
    assert!(store.release_if_match(&key, &last).await.is_err());
    assert!(store.delete(&key).await.is_err());
    assert_eq!(store.read(&key).await.unwrap().unwrap(), (vec![0], last));

    fixture
        .raw(
            &key,
            serde_json::to_vec(&serde_json::json!({
                "version": 1, "revision": u64::MAX, "payload": "tombstone"
            }))
            .unwrap(),
            Some("1"),
        )
        .await;
    assert!(store.read(&key).await.unwrap().is_none());
    assert!(store.write_if_not_exists(&key, vec![1]).await.is_err());
}

#[tokio::test]
#[ignore = "requires dedicated S3 service; run with HADB_S3_SERVICE_FILE"]
async fn s3_unsupported_conditional_create_never_falls_back_to_unconditional_put() {
    let fixture = Fixture::new();
    let proxy = FaultProxy::new(&fixture.service.endpoint).await;
    let store = S3LeaseStore::new(
        fixture.client_at(&proxy.endpoint),
        fixture.service.bucket.clone(),
    )
    .with_prefix(&fixture.prefix);
    let key = store.key_for("unsupported");
    for fault in [1, 4, 5] {
        proxy
            .fault
            .store(fault, std::sync::atomic::Ordering::SeqCst);
        let result = store.write_if_not_exists(&key, vec![1]).await;
        if fault == 4 {
            assert!(
                !result
                    .expect("known ConditionalRequestConflict is a CAS conflict")
                    .success
            );
        } else {
            assert!(
                result.is_err(),
                "unsupported or unknown conditional error must never claim by unconditional PUT"
            );
        }
        assert!(fixture.store().read(&key).await.unwrap().is_none());
    }
}

#[tokio::test]
#[ignore = "requires dedicated S3 service; run with HADB_S3_SERVICE_FILE"]
async fn s3_lost_renewal_receipt_can_only_recover_its_current_session() {
    for takeover in [false, true] {
        let fixture = Fixture::new();
        let proxy = FaultProxy::new(&fixture.service.endpoint).await;
        let store = Arc::new(
            S3LeaseStore::new(
                fixture.client_at(&proxy.endpoint),
                fixture.service.bucket.clone(),
            )
            .with_prefix(&fixture.prefix),
        );
        let key = store.key_for("ambiguous");
        let (fence, writer) = AtomicFence::new();
        let mut owner = DbLease::new(store, &key, "same-instance", "old", 60)
            .with_fence_writer(Arc::new(writer));
        assert_eq!(owner.try_claim().await.unwrap(), hadb::Role::Leader);
        let before = fence.current().unwrap();
        proxy.fault.store(2, std::sync::atomic::Ordering::SeqCst);
        assert!(
            owner.renew().await.is_err(),
            "proxy must hide the accepted renewal receipt"
        );
        assert!(
            fence.current().is_none(),
            "an uncertain response must revoke local authority"
        );
        let (committed, revision) = owner.read().await.unwrap().unwrap();
        assert_eq!(committed.session_id, owner.session_id());
        assert!(
            revision.parse::<u64>().unwrap() > before,
            "real S3 must have accepted the renewal"
        );

        if takeover {
            let mut successor = DbLease::new(fixture.store(), &key, "same-instance", "new", 60);
            assert_eq!(successor.try_claim().await.unwrap(), hadb::Role::Leader);
            assert!(
                !owner.renew().await.unwrap(),
                "new-session proof cannot recover the former owner"
            );
            assert!(fence.current().is_none());
            assert!(successor.renew().await.unwrap());
        } else {
            assert!(
                owner.renew().await.unwrap(),
                "current-session advanced proof can recover an ambiguous commit"
            );
            assert!(fence.current().unwrap() > before);
        }
    }
}

#[tokio::test]
#[ignore = "requires dedicated S3 service; run with HADB_S3_SERVICE_FILE"]
async fn s3_claim_and_renew_publish_an_active_ordered_fence() {
    let fixture = Fixture::new();
    let store = fixture.store();
    let key = store.key_for("db");
    let (fence, writer) = AtomicFence::new();
    let mut lease = DbLease::new(store.clone(), &key, "owner", "address", 60)
        .with_fence_writer(Arc::new(writer));

    assert_eq!(lease.try_claim().await.unwrap(), hadb::Role::Leader);
    let claimed = fence
        .current()
        .expect("successful S3 claim must activate fence");
    assert!(claimed > 0);
    assert_eq!(lease.read().await.unwrap().unwrap().1, claimed.to_string());

    assert!(lease.renew().await.unwrap());
    let renewed = fence
        .current()
        .expect("successful S3 renewal must keep fence active");
    assert!(
        renewed > claimed,
        "renewal must advance the ownership fence"
    );
    assert_eq!(lease.read().await.unwrap().unwrap().1, renewed.to_string());
}
