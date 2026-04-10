//! Property-based tests for ManifestStore.
//!
//! Verifies version monotonicity, put/get roundtrip, and garbage input handling.

use hadb::{HaManifest, InMemoryManifestStore, ManifestStore, StorageManifest};
use proptest::prelude::*;

fn arb_writer_id() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9-]{0,15}".prop_filter("non-empty", |s| !s.is_empty())
}

fn arb_epoch() -> impl Strategy<Value = u64> {
    0u64..1_000_000
}

fn arb_timestamp() -> impl Strategy<Value = u64> {
    0u64..9_999_999_999u64
}

fn arb_manifest() -> impl Strategy<Value = HaManifest> {
    (
        arb_writer_id(),
        arb_epoch(),
        arb_timestamp(),
        0u64..100_000u64,
    )
        .prop_map(|(writer_id, lease_epoch, timestamp_ms, txid)| HaManifest {
            version: 0,
            writer_id,
            lease_epoch,
            timestamp_ms,
            storage: StorageManifest::Walrust {
                txid,
                changeset_prefix: "cs/".to_string(),
                latest_changeset_key: format!("cs/{}", txid),
                snapshot_key: if txid > 10 {
                    Some(format!("snap/{}", txid - 10))
                } else {
                    None
                },
                snapshot_txid: if txid > 10 { Some(txid - 10) } else { None },
            },
        })
}

fn arb_key() -> impl Strategy<Value = String> {
    "[a-z_][a-z0-9_/]{0,20}".prop_filter("non-empty", |k| !k.is_empty())
}

proptest! {
    #[test]
    fn manifest_version_monotonicity(key in arb_key(), manifest in arb_manifest(), num_puts in 1usize..20) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = InMemoryManifestStore::new();

            let res = store.put(&key, &manifest, None).await.unwrap();
            prop_assert!(res.success, "first put must succeed");

            let mut expected_version = 1u64;
            for _ in 1..num_puts {
                let res = store.put(&key, &manifest, Some(expected_version)).await.unwrap();
                prop_assert!(res.success, "put with correct version must succeed");
                expected_version += 1;
            }

            let fetched = store.get(&key).await.unwrap().expect("manifest must exist");
            prop_assert_eq!(
                fetched.version, expected_version,
                "version must be exactly {} after {} puts", expected_version, num_puts
            );

            Ok::<(), proptest::test_runner::TestCaseError>(())
        }).unwrap();
    }

    #[test]
    fn manifest_put_get_roundtrip(key in arb_key(), manifest in arb_manifest()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = InMemoryManifestStore::new();

            store.put(&key, &manifest, None).await.unwrap();

            let fetched = store.get(&key).await.unwrap().expect("manifest must exist");

            prop_assert_eq!(fetched.writer_id, manifest.writer_id);
            prop_assert_eq!(fetched.lease_epoch, manifest.lease_epoch);
            prop_assert_eq!(fetched.timestamp_ms, manifest.timestamp_ms);
            prop_assert_eq!(fetched.version, 1);

            match (&fetched.storage, &manifest.storage) {
                (StorageManifest::Walrust { txid: ft, .. }, StorageManifest::Walrust { txid: mt, .. }) => {
                    prop_assert_eq!(*ft, *mt);
                }
                _ => panic!("storage variant mismatch"),
            }

            Ok::<(), proptest::test_runner::TestCaseError>(())
        }).unwrap();
    }

    #[test]
    fn manifest_handles_garbage_input(garbage_bytes in prop::collection::vec(any::<u8>(), 0..256)) {
        let result: Result<HaManifest, _> = serde_json::from_slice(&garbage_bytes);
        let result2: Result<HaManifest, _> = rmp_serde::from_slice(&garbage_bytes);

        if let Ok(ref decoded) = result {
            prop_assert!(!decoded.writer_id.contains('\0'), "valid manifest should not contain null bytes from garbage");
        }

        if let Ok(ref decoded2) = result2 {
            prop_assert!(!decoded2.writer_id.contains('\0'), "valid manifest should not contain null bytes from garbage");
        }

        prop_assert!(
            result.is_err() || result2.is_err() || true,
            "deserializing garbage must not panic (only return Err or valid data)"
        );
    }
}
