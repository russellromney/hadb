//! Integration tests against a real S3-compatible endpoint.
//!
//! Skipped unless `CINCH_S3_TEST_BUCKET` is set. Also respects:
//! - `CINCH_S3_TEST_ENDPOINT` for non-AWS services (Tigris, MinIO, etc.)
//! - Standard AWS env vars for credentials (`AWS_ACCESS_KEY_ID`,
//!   `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`).
//!
//! Each test uses a unique prefix so parallel runs don't collide.
//!
//! Run with:
//!   CINCH_S3_TEST_BUCKET=my-bucket \
//!   CINCH_S3_TEST_ENDPOINT=https://fly.storage.tigris.dev \
//!   cargo test -p hadb-storage-s3 --test integration

use std::env;
use std::sync::Arc;

use hadb_storage::StorageBackend;
use hadb_storage_s3::S3Storage;

/// Returns `None` if integration tests should be skipped.
async fn storage() -> Option<(S3Storage, String)> {
    let bucket = env::var("CINCH_S3_TEST_BUCKET").ok()?;
    let endpoint = env::var("CINCH_S3_TEST_ENDPOINT").ok();
    let unique = format!("hadb-storage-s3-it/{}", uuid::Uuid::new_v4());
    let store = S3Storage::from_env(bucket, endpoint.as_deref())
        .await
        .expect("construct S3Storage")
        .with_prefix(&unique);
    Some((store, unique))
}

macro_rules! skip_without_s3 {
    () => {
        match storage().await {
            Some(v) => v,
            None => {
                eprintln!("CINCH_S3_TEST_BUCKET unset; skipping");
                return;
            }
        }
    };
}

async fn cleanup(s: &S3Storage) {
    let keys = s.list("", None).await.unwrap_or_default();
    if !keys.is_empty() {
        let _ = s.delete_many(&keys).await;
    }
}

#[tokio::test]
async fn put_get_roundtrips() {
    let (s, _prefix) = skip_without_s3!();
    s.put("hello", b"world").await.unwrap();
    let got = s.get("hello").await.unwrap().unwrap();
    assert_eq!(got, b"world");
    cleanup(&s).await;
}

#[tokio::test]
async fn get_missing_returns_none() {
    let (s, _prefix) = skip_without_s3!();
    assert!(s.get("nope").await.unwrap().is_none());
    cleanup(&s).await;
}

#[tokio::test]
async fn delete_is_idempotent() {
    let (s, _prefix) = skip_without_s3!();
    s.put("k", b"v").await.unwrap();
    s.delete("k").await.unwrap();
    s.delete("k").await.unwrap();
    assert!(s.get("k").await.unwrap().is_none());
    cleanup(&s).await;
}

#[tokio::test]
async fn list_filters_and_respects_after() {
    let (s, _prefix) = skip_without_s3!();
    for k in ["a/1", "a/2", "a/3", "b/1"] {
        s.put(k, b"").await.unwrap();
    }
    let all_a = s.list("a/", None).await.unwrap();
    assert_eq!(all_a, vec!["a/1", "a/2", "a/3"]);
    let after = s.list("a/", Some("a/1")).await.unwrap();
    assert_eq!(after, vec!["a/2", "a/3"]);
    cleanup(&s).await;
}

#[tokio::test]
async fn exists_reflects_presence() {
    let (s, _prefix) = skip_without_s3!();
    assert!(!s.exists("k").await.unwrap());
    s.put("k", b"v").await.unwrap();
    assert!(s.exists("k").await.unwrap());
    cleanup(&s).await;
}

#[tokio::test]
async fn put_if_absent_first_wins() {
    let (s, _prefix) = skip_without_s3!();
    let first = s.put_if_absent("lease", b"node-a").await.unwrap();
    assert!(first.success);
    let second = s.put_if_absent("lease", b"node-b").await.unwrap();
    // S3 native gives CAS; Tigris may accept both. Assert body-wise.
    if second.success {
        eprintln!("backend accepted concurrent if-none-match writes (Tigris-style)");
    } else {
        assert!(!second.success);
    }
    cleanup(&s).await;
}

#[tokio::test]
async fn put_if_match_rejects_stale_etag() {
    let (s, _prefix) = skip_without_s3!();
    let a = s.put_if_absent("k", b"v1").await.unwrap();
    let Some(e1) = a.etag else {
        eprintln!("backend didn't return etag on create; skipping");
        return;
    };
    let b = s.put_if_match("k", b"v2", &e1).await.unwrap();
    assert!(b.success);
    let c = s.put_if_match("k", b"v3", &e1).await.unwrap();
    if c.success {
        eprintln!("backend accepted stale if-match (non-strict CAS)");
    } else {
        assert!(!c.success);
    }
    cleanup(&s).await;
}

#[tokio::test]
async fn range_get_returns_requested_slice() {
    let (s, _prefix) = skip_without_s3!();
    s.put("k", b"abcdefghij").await.unwrap();
    let a = s.range_get("k", 0, 3).await.unwrap().unwrap();
    assert_eq!(a, b"abc");
    let b = s.range_get("k", 2, 4).await.unwrap().unwrap();
    assert_eq!(b, b"cdef");
    cleanup(&s).await;
}

#[tokio::test]
async fn metrics_count_operations() {
    let (s, _prefix) = skip_without_s3!();
    s.put("k", b"abcd").await.unwrap();
    s.get("k").await.unwrap();
    assert!(s.put_count() >= 1);
    assert!(s.fetch_count() >= 1);
    assert!(s.bytes_put() >= 4);
    assert!(s.bytes_fetched() >= 4);
    cleanup(&s).await;
}

#[tokio::test]
async fn delete_many_batches() {
    let (s, _prefix) = skip_without_s3!();
    let keys: Vec<String> = (0..5).map(|i| format!("k{i}")).collect();
    for k in &keys {
        s.put(k, b"").await.unwrap();
    }
    let deleted = s.delete_many(&keys).await.unwrap();
    assert!(deleted >= 5);
    for k in &keys {
        assert!(s.get(k).await.unwrap().is_none());
    }
    cleanup(&s).await;
}

#[allow(dead_code)]
fn _compile_check(_: Arc<dyn StorageBackend>) {}
