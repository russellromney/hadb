use std::time::Duration;
use turbodb::{CheckpointConfig, Durability, FlushPolicy};

#[test]
fn checkpoint_default_values() {
    let c = CheckpointConfig::default();
    assert_eq!(c.interval, Duration::from_secs(15));
    assert_eq!(c.commit_count, 10_000);
    assert_eq!(c.wal_bytes, 64 * 1024 * 1024);
}

#[test]
fn durability_default_is_continuous() {
    let d = Durability::default();
    assert!(
        matches!(
            d,
            Durability::Continuous {
                checkpoint: CheckpointConfig {
                    interval,
                    commit_count,
                    wal_bytes,
                },
                replication_interval,
            } if interval == Duration::from_secs(15)
                && commit_count == 10_000
                && wal_bytes == 64 * 1024 * 1024
                && replication_interval == Duration::from_secs(1)
        ),
        "expected default Continuous with standard params, got {:?}",
        d
    );
}

#[test]
fn checkpoint_flush_policy() {
    let cfg = CheckpointConfig {
        interval: Duration::from_secs(30),
        commit_count: 5,
        wal_bytes: 1024,
    };
    let d = Durability::Checkpoint(cfg);
    assert_eq!(d.flush_policy(), FlushPolicy::Checkpoint(cfg));
}

#[test]
fn continuous_flush_policy() {
    let cfg = CheckpointConfig {
        interval: Duration::from_secs(60),
        commit_count: 100,
        wal_bytes: 4096,
    };
    let d = Durability::Continuous {
        checkpoint: cfg,
        replication_interval: Duration::from_secs(2),
    };
    assert_eq!(d.flush_policy(), FlushPolicy::Checkpoint(cfg));
}

#[test]
fn cloud_flush_policy_is_per_commit() {
    let d = Durability::Cloud;
    assert_eq!(d.flush_policy(), FlushPolicy::PerCommit);
}

#[test]
fn durability_variants_are_distinct() {
    let ckpt = Durability::Checkpoint(CheckpointConfig::default());
    let cont = Durability::default();
    let cloud = Durability::Cloud;

    assert_ne!(ckpt, cont);
    assert_ne!(cont, cloud);
    assert_ne!(cloud, ckpt);
}
