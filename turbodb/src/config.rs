use std::time::Duration;

/// Trigger thresholds for turbolite / turbograph checkpoints. Checkpoint
/// fires whenever the first of (time elapsed, commits since last,
/// WAL/journal bytes since last) trips — not all three together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointConfig {
    pub interval: Duration,
    pub commit_count: u64,
    pub wal_bytes: u64,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(15),
            commit_count: 10_000,
            wal_bytes: 64 * 1024 * 1024,
        }
    }
}

/// The low-level tiering policy a turbodb-backed consumer asks its page-
/// group flusher to follow. Not user-facing — `Durability` wraps it with
/// the right replication pairing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushPolicy {
    /// Pages flushed at checkpoint boundaries. Config above.
    Checkpoint(CheckpointConfig),
    /// Pages + manifest flushed synchronously per commit, before ack.
    /// Consumer runs its storage engine in "pages-are-truth" mode
    /// (turbolite S3Primary = `journal_mode=OFF|DELETE` with xSync-per-commit).
    PerCommit,
}

/// User-facing durability for tiered consumers. The three meaningful combos
/// of (hadb replication, turbodb flush policy). Consumer crates
/// (haqlite-turbolite, hakuzu, future turboduck) take this enum in their
/// builders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Page groups flushed on checkpoint trigger. No log shipping.
    /// Crash between checkpoints = lose everything since last checkpoint;
    /// graceful shutdown MUST force a checkpoint (see step 5).
    /// Best for dev / single-node / desktop-like apps / tests.
    /// RPO = checkpoint interval.
    Checkpoint(CheckpointConfig),

    /// Page groups on checkpoint + log ships continuously on interval.
    /// Crash: replacement restores from last checkpoint + replays shipped
    /// log segments. RPO = replication_interval (default 1s).
    /// Default for tiered consumers (haqlite-turbolite, hakuzu).
    Continuous {
        checkpoint: CheckpointConfig,
        replication_interval: Duration,
    },

    /// Every commit synchronously flushes page groups + manifest before
    /// ack. No WAL/journal shipping (pages are the replication).
    /// Consumer runs its storage engine in S3Primary equivalent mode.
    /// RPO = 0. Expensive S3 PUT volume; write latency = S3 round-trip.
    /// Required for multi-writer (haqlite-turbolite's `HaMode::Shared`).
    Cloud,
}

impl Default for Durability {
    fn default() -> Self {
        Durability::Continuous {
            checkpoint: CheckpointConfig::default(),
            replication_interval: Duration::from_secs(1),
        }
    }
}

impl Durability {
    pub fn flush_policy(&self) -> FlushPolicy {
        match self {
            Self::Checkpoint(c) => FlushPolicy::Checkpoint(*c),
            Self::Continuous { checkpoint, .. } => FlushPolicy::Checkpoint(*checkpoint),
            Self::Cloud => FlushPolicy::PerCommit,
        }
    }

    pub fn is_cloud(&self) -> bool {
        matches!(self, Self::Cloud)
    }

    /// Replication interval for walrust sync loop.
    /// Checkpoint/Cloud default to 1h (no log shipping);
    /// Continuous returns its configured interval.
    pub fn replication_interval(&self) -> std::time::Duration {
        match self {
            Self::Checkpoint(_) => std::time::Duration::from_secs(3600),
            Self::Continuous { replication_interval, .. } => *replication_interval,
            Self::Cloud => std::time::Duration::from_secs(3600),
        }
    }

    /// Whether to skip snapshot creation in walrust.
    /// Cloud mode skips because pages are already durable per-commit.
    pub fn skip_snapshot(&self) -> bool {
        matches!(self, Self::Cloud)
    }
}
