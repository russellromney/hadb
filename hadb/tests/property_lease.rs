use hadb::{CasResult, InMemoryLeaseStore, LeaseStore};
use proptest::prelude::*;
use std::sync::Arc;

fn arb_data() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..64)
}

fn arb_key() -> impl Strategy<Value = String> {
    "[a-z_][a-z0-9_/]{0,20}".prop_filter("key must not be empty", |k| !k.is_empty())
}

proptest! {
    #[test]
    fn lease_write_read_roundtrip(key in arb_key(), data in arb_data()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result: Result<Option<Vec<u8>>, anyhow::Error> = rt.block_on(async {
            let store = InMemoryLeaseStore::new();

            let cas = store.write_if_not_exists(&key, data.clone()).await?;
            if !cas.success {
                return Ok(None);
            }

            let read = store.read(&key).await?;
            Ok(read.map(|(stored_data, _etag)| stored_data))
        });

        if let Ok(Some(stored)) = result {
            prop_assert_eq!(data, stored);
        }
    }

    #[test]
    fn cas_semantics_correct(
        key in arb_key(),
        data1 in arb_data(),
        data2 in arb_data(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = InMemoryLeaseStore::new();

            let cas1 = store.write_if_not_exists(&key, data1.clone()).await.unwrap();
            prop_assert!(cas1.success, "first write_if_not_exists must succeed");
            let etag1 = cas1.etag.expect("successful write must return etag");

            let wrong_cas = store.write_if_match(&key, data2.clone(), "wrong-etag").await.unwrap();
            prop_assert!(!wrong_cas.success, "write_if_match with wrong etag must fail");

            let cas2 = store.write_if_match(&key, data2.clone(), &etag1).await.unwrap();
            prop_assert!(cas2.success, "write_if_match with correct etag must succeed");
            let etag2 = cas2.etag.expect("successful write must return etag");
            prop_assert_ne!(&etag1, &etag2, "etag must change after successful write");

            let read = store.read(&key).await.unwrap();
            let (stored_data, stored_etag) = read.expect("key must exist after write");
            prop_assert_eq!(stored_data, data2, "read must return the latest written data");
            prop_assert_eq!(stored_etag, etag2, "read etag must match the latest write etag");

            Ok::<(), proptest::test_runner::TestCaseError>(())
        }).unwrap();
    }

    #[test]
    fn delete_is_idempotent(key in arb_key(), data in arb_data()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = InMemoryLeaseStore::new();

            store.write_if_not_exists(&key, data).await.unwrap();
            store.delete(&key).await.unwrap();

            let read = store.read(&key).await.unwrap();
            prop_assert!(read.is_none(), "read after delete must return None");

            let delete_result = store.delete(&key).await;
            prop_assert!(delete_result.is_ok(), "second delete must not error");

            let read2 = store.read(&key).await.unwrap();
            prop_assert!(read2.is_none(), "read after second delete must return None");

            Ok::<(), proptest::test_runner::TestCaseError>(())
        }).unwrap();
    }

    #[test]
    fn concurrent_writers_single_winner(
        key in arb_key(),
        data in arb_data(),
        num_writers in 2usize..8,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = Arc::new(InMemoryLeaseStore::new());

            let init_cas = store.write_if_not_exists(&key, vec![0]).await.unwrap();
            let old_etag = init_cas.etag.expect("init write must return etag");

            let mut handles = Vec::new();
            for i in 0..num_writers {
                let store = store.clone();
                let key = key.clone();
                let old_etag = old_etag.clone();
                let data = data.clone();
                handles.push(tokio::spawn(async move {
                    let mut d = data.clone();
                    d.push(i as u8);
                    store.write_if_match(&key, d, &old_etag).await
                }));
            }

            let results: Vec<CasResult> = futures::future::join_all(handles)
                .await
                .into_iter()
                .map(|h| h.unwrap().unwrap())
                .collect();

            let winners = results.iter().filter(|r| r.success).count();
            prop_assert_eq!(winners, 1, "exactly one writer must win (got {} winners out of {})", winners, num_writers);

            Ok::<(), proptest::test_runner::TestCaseError>(())
        }).unwrap();
    }
}
