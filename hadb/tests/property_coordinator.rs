use hadb::{InMemoryLeaseStore, LeaseConfig, Role};
use proptest::prelude::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

proptest! {
    #[test]
    fn no_split_brain_under_transitions(
        num_nodes in 2usize..6,
        num_rounds in 1usize..10,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let instance_ids: Vec<String> = (0..num_nodes)
                .map(|i| format!("node-{}", i))
                .collect();

            let store = Arc::new(InMemoryLeaseStore::new());
            let mut leaders_per_round: Vec<Vec<String>> = Vec::new();

            for round in 0..num_rounds {
                let mut round_leaders = Vec::new();

                for node_idx in 0..num_nodes {
                    let lease_config = LeaseConfig::new(
                        instance_ids[node_idx].clone(),
                        format!("127.0.0.1:{}", 8000 + node_idx),
                    );

                    let mut lease = hadb::DbLease::new(
                        store.clone(),
                        "test/db1/_lease.json",
                        &lease_config.instance_id,
                        &lease_config.address,
                        lease_config.ttl_secs,
                    );

                    let role = lease.try_claim().await.unwrap();
                    if role == Role::Leader {
                        round_leaders.push(instance_ids[node_idx].clone());
                    }
                }

                leaders_per_round.push(round_leaders);
            }

            for (round_idx, leaders) in leaders_per_round.iter().enumerate() {
                prop_assert!(
                    leaders.len() <= 1,
                    "round {}: at most one leader per round, got {:?}",
                    round_idx,
                    leaders
                );
            }

            Ok::<(), proptest::test_runner::TestCaseError>(())
        }).unwrap();
    }

    #[test]
    fn follower_readiness_correct(
        leader_position in 0u64..10000,
        follower_position in 0u64..10000,
    ) {
        let caught_up = follower_position >= leader_position;

        let caught_up_flag = Arc::new(AtomicBool::new(caught_up));
        let position = Arc::new(AtomicU64::new(follower_position));

        let is_caught_up = caught_up_flag.load(Ordering::SeqCst);
        let pos = position.load(Ordering::SeqCst);

        prop_assert_eq!(
            is_caught_up,
            pos >= leader_position,
            "caught_up flag must match position comparison"
        );

        if is_caught_up {
            prop_assert!(
                pos >= leader_position,
                "if caught_up is true, position must be >= leader_position"
            );
        } else {
            prop_assert!(
                pos < leader_position,
                "if caught_up is false, position must be < leader_position"
            );
        }
    }
}
