use std::sync::Arc;
use std::time::Duration;

use hadb_lease::LeaseStore;

/// Node behavior inside a HA topology.
///
/// `Leader` and `Follower` are assigned by lease outcome in
/// [`HaMode::SingleWriter`]. `Client` and `LatentWriter` are self-declared
/// roles for topologies that do not participate in leader election; they are
/// visible in the API so higher-level crates can reject or implement them
/// deliberately instead of overloading follower semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Holds the persistent single-writer lease and accepts writes.
    Leader,
    /// Replays from the leader and may promote when the lease expires.
    Follower,
    /// Read-only consumer. Never claims a lease or promotes.
    Client,
    /// Shared-writer peer. Acquires per-write leases instead of leading.
    LatentWriter,
}

/// HA topology mode: how writes are coordinated.
///
/// Two-axis model paired with [`Role`]:
/// - `HaMode` describes the cluster topology.
/// - `Role` describes a node's behavior inside that topology.
///
/// Only [`HaMode::SingleWriter`] paired with [`Role::Leader`] or
/// [`Role::Follower`] is fully implemented today. The other valid
/// combinations are visible in the type system so consumers can choose
/// deliberately, but downstream crates (`haqlite`, `haqlite-turbolite`)
/// will bail at open time with an explicit "not yet implemented"
/// message until the corresponding feature lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HaMode {
    /// One node claims a persistent lease at join and holds it until
    /// death/release. Runtime roles are `Leader`, `Follower`, or `Client`.
    SingleWriter,
    /// Peers acquire-write-release a per-write lease. Runtime roles are
    /// `LatentWriter` or `Client`.
    SharedWriter,
}

/// hadb's sole durability lever: how the Replicator ships the change log
/// (walrust WAL frames, graphstream journal entries, anything else) to the
/// replication target. Pure replication axis — hadb has no notion of
/// tiering, page groups, or checkpoints. Consumers that tier (turbolite,
/// turbograph) layer their own policy on top via `turbodb::Durability`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Replicator is either absent or its ship loop is disabled.
    /// The log stays local; durability comes from the caller's storage
    /// engine (SQLite WAL on local disk, etc). RPO at the log layer = ∞.
    /// Used by `turbodb::Durability::{Checkpoint, Cloud}` — the first
    /// because nothing ships, the second because per-commit page writes
    /// already cover replication.
    Local,

    /// Replicator ships on the given interval. Idle ticks are no-ops
    /// (nothing new to ship → nothing goes out). Default for plain hadb
    /// consumers; also what `turbodb::Durability::Continuous` composes to.
    Replicated(Duration),

    /// Replicator ships synchronously per commit. RPO at the log layer = 0.
    /// Rare — most consumers that want RPO=0 pick `turbodb::Durability::Cloud`
    /// (page-level per-commit) instead of log-level sync shipping.
    SyncReplicated,
}

impl Default for HaMode {
    fn default() -> Self {
        HaMode::SingleWriter
    }
}

impl Default for Durability {
    fn default() -> Self {
        Durability::Replicated(Duration::from_secs(1))
    }
}

impl std::fmt::Display for HaMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HaMode::SingleWriter => write!(f, "SingleWriter"),
            HaMode::SharedWriter => write!(f, "SharedWriter"),
        }
    }
}

impl std::fmt::Display for Durability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Durability::Local => write!(f, "Local"),
            Durability::Replicated(d) => write!(f, "Replicated({}ms)", d.as_millis()),
            Durability::SyncReplicated => write!(f, "SyncReplicated"),
        }
    }
}

/// Validate a mode + durability combination.
///
/// Valid combinations:
/// - SingleWriter + Local → OK
/// - SingleWriter + Replicated(_) → OK
/// - SingleWriter + SyncReplicated → OK
/// - SharedWriter + Local → OK (per-commit page writes are valid)
///
/// Invalid:
/// - SharedWriter + Replicated(_): async log shipping diverges silently
/// - SharedWriter + SyncReplicated: no lease-per-commit mechanism, still a race
pub fn validate_mode_durability(mode: HaMode, durability: Durability) -> Result<(), String> {
    match (mode, durability) {
        (HaMode::SingleWriter, _) => Ok(()),
        (HaMode::SharedWriter, Durability::Local) => Ok(()),
        (HaMode::SharedWriter, Durability::Replicated(_)) => {
            Err("SharedWriter + async log shipping diverges silently across writers".to_string())
        }
        (HaMode::SharedWriter, Durability::SyncReplicated) => {
            Err("SharedWriter + sync log shipping without a lease-per-commit mechanism is still a race - use turbodb::Durability::Cloud".to_string())
        }
    }
}

/// Validate that a node role makes sense for the selected topology.
pub fn validate_mode_role(mode: HaMode, role: Role) -> Result<(), String> {
    match (mode, role) {
        (HaMode::SingleWriter, Role::Leader | Role::Follower | Role::Client) => Ok(()),
        (HaMode::SingleWriter, Role::LatentWriter) => {
            Err("LatentWriter requires SharedWriter mode".to_string())
        }
        (HaMode::SharedWriter, Role::LatentWriter | Role::Client) => Ok(()),
        (HaMode::SharedWriter, Role::Leader | Role::Follower) => {
            Err("SharedWriter has no Leader/Follower; use LatentWriter or Client".to_string())
        }
    }
}

impl Role {
    /// Convert Role to u8 for atomic storage.
    pub fn to_u8(self) -> u8 {
        match self {
            Role::Leader => 0,
            Role::Follower => 1,
            Role::Client => 2,
            Role::LatentWriter => 3,
        }
    }

    /// Convert u8 back to Role.
    ///
    /// Returns `Err` for any value outside the defined role tags. The
    /// in-process [`AtomicRole`](crate::coordinator::AtomicRole) only
    /// stores values it produced via [`Role::to_u8`] and unwraps with
    /// `.expect(...)`. External callers reading a role tag from the
    /// wire or storage must propagate the error instead of panicking.
    pub fn from_u8(val: u8) -> Result<Self, &'static str> {
        match val {
            0 => Ok(Role::Leader),
            1 => Ok(Role::Follower),
            2 => Ok(Role::Client),
            3 => Ok(Role::LatentWriter),
            _ => Err("invalid role tag (expected 0..=3)"),
        }
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Leader => write!(f, "Leader"),
            Role::Follower => write!(f, "Follower"),
            Role::Client => write!(f, "Client"),
            Role::LatentWriter => write!(f, "LatentWriter"),
        }
    }
}

/// Events emitted when a database's role changes.
///
/// Lifecycle hooks that the coordinator triggers:
/// - `Joined` - Database joined the cluster with the given role
/// - `Promoted` - Follower was promoted to leader (previous leader died/left)
/// - `Demoted` - Leader was demoted to follower (lost lease via CAS conflict)
/// - `Fenced` - Leader lost its lease, must stop serving immediately
/// - `Sleeping` - Leader signaled sleep (e.g., Fly scale-to-zero)
/// - `ManifestChanged` - Manifest version changed, followers should pull
#[derive(Debug, Clone)]
pub enum RoleEvent {
    /// Database joined the cluster with the given role.
    Joined { db_name: String, role: Role },
    /// Follower was promoted to leader (previous leader died/left).
    Promoted { db_name: String },
    /// Leader was demoted to follower (lost lease via CAS conflict).
    /// Consumer should stop writes and switch to read-only.
    Demoted { db_name: String },
    /// Leader lost its lease — must stop serving immediately.
    /// Stricter than Demoted: the database should be fully disconnected.
    Fenced { db_name: String },
    /// Leader signaled sleep (e.g., Fly scale-to-zero). Follower should shut down gracefully.
    Sleeping { db_name: String },
    /// Manifest version changed. Followers should pull new data.
    ManifestChanged { db_name: String, version: u64 },
}

/// Configuration for CAS lease coordination.
///
/// Carries both the policy (timing, identity) and the storage backend
/// that holds the lease key. The pairing is enforced at the type level
/// — there's no way to construct a `LeaseConfig` without a `store`, so
/// the Coordinator can rely on "leases configured" meaning "store +
/// policy both present."
#[derive(Clone)]
pub struct LeaseConfig {
    /// Storage backend that holds this database's lease key.
    pub store: Arc<dyn LeaseStore>,
    /// Unique identifier for this instance (e.g. FLY_MACHINE_ID).
    pub instance_id: String,
    /// Network address for this instance (for client discovery).
    pub address: String,
    /// Lease time-to-live in seconds. Default: 5.
    pub ttl_secs: u64,
    /// How often to renew the lease. Default: 2s.
    pub renew_interval: Duration,
    /// How often followers poll the lease for leader death. Default: 1s.
    pub follower_poll_interval: Duration,
    /// Number of consecutive expired reads required before a follower can claim.
    /// Prevents premature takeover on transient storage glitches. Default: 1.
    pub required_expired_reads: u32,
    /// Max consecutive renewal errors before self-demoting.
    /// Prevents split-brain during sustained storage outages: if we can't renew,
    /// our lease is expiring and another node may claim it. Default: 3.
    pub max_consecutive_renewal_errors: u32,
}

impl LeaseConfig {
    pub fn new(store: Arc<dyn LeaseStore>, instance_id: String, address: String) -> Self {
        Self {
            store,
            instance_id,
            address,
            ttl_secs: 5,
            renew_interval: Duration::from_secs(2),
            follower_poll_interval: Duration::from_secs(1),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }
    }
}

// `dyn LeaseStore` doesn't carry Debug; print the policy fields and
// elide the store. Coordinators print this in logs, so keeping a Debug
// impl matters even though the field can't be displayed.
impl std::fmt::Debug for LeaseConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseConfig")
            .field("instance_id", &self.instance_id)
            .field("address", &self.address)
            .field("ttl_secs", &self.ttl_secs)
            .field("renew_interval", &self.renew_interval)
            .field("follower_poll_interval", &self.follower_poll_interval)
            .field("required_expired_reads", &self.required_expired_reads)
            .field(
                "max_consecutive_renewal_errors",
                &self.max_consecutive_renewal_errors,
            )
            .field("store", &"<dyn LeaseStore>")
            .finish()
    }
}

/// Top-level configuration for the Coordinator.
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Replication durability mode. Default: `Durability::Replicated(1s)`.
    /// Carries the sync interval for `Replicated` variant.
    pub durability: Durability,
    /// Snapshot interval. Default: 1h.
    pub snapshot_interval: Duration,
    /// Lease config. None = no leases, always Leader (single-node mode).
    pub lease: Option<LeaseConfig>,
    /// How often followers poll for new replication data. Default: 1s.
    pub follower_pull_interval: Duration,
    /// Timeout for replicator.add() and pull during promotion.
    /// Prevents hanging forever if storage is slow or unresponsive.
    /// Default: 30s. Should be less than lease TTL for safety, but this is
    /// a safety net — self-demotion on renewal errors handles stale leases.
    pub replicator_timeout: Duration,
    /// How often followers poll ManifestStore for version changes. Default: 1s.
    /// Only used when a ManifestStore is configured on the Coordinator.
    pub manifest_poll_interval: Duration,
    /// Writer side of an `AtomicFence`, updated by `DbLease` on every
    /// successful claim/renew. Pair this with an `AtomicFence` reader
    /// handed to storage adapters that perform fenced writes.
    /// `None` = no fence enforcement.
    pub fence_writer: Option<Arc<hadb_lease::AtomicFenceWriter>>,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            durability: Durability::default(),
            snapshot_interval: Duration::from_secs(3600),
            lease: None,
            follower_pull_interval: Duration::from_secs(1),
            replicator_timeout: Duration::from_secs(30),
            manifest_poll_interval: Duration::from_secs(1),
            fence_writer: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_display() {
        assert_eq!(Role::Leader.to_string(), "Leader");
        assert_eq!(Role::Follower.to_string(), "Follower");
        assert_eq!(Role::Client.to_string(), "Client");
        assert_eq!(Role::LatentWriter.to_string(), "LatentWriter");
    }

    #[test]
    fn test_role_equality() {
        assert_eq!(Role::Leader, Role::Leader);
        assert_eq!(Role::Follower, Role::Follower);
        assert_ne!(Role::Leader, Role::Follower);
    }

    #[test]
    fn test_role_to_u8() {
        assert_eq!(Role::Leader.to_u8(), 0);
        assert_eq!(Role::Follower.to_u8(), 1);
        assert_eq!(Role::Client.to_u8(), 2);
        assert_eq!(Role::LatentWriter.to_u8(), 3);
    }

    #[test]
    fn test_role_from_u8() {
        assert_eq!(Role::from_u8(0), Ok(Role::Leader));
        assert_eq!(Role::from_u8(1), Ok(Role::Follower));
        assert_eq!(Role::from_u8(2), Ok(Role::Client));
        assert_eq!(Role::from_u8(3), Ok(Role::LatentWriter));
    }

    #[test]
    fn test_role_roundtrip() {
        for role in [
            Role::Leader,
            Role::Follower,
            Role::Client,
            Role::LatentWriter,
        ] {
            assert_eq!(Role::from_u8(role.to_u8()), Ok(role));
        }
    }

    #[test]
    fn test_role_from_u8_invalid() {
        let err = Role::from_u8(4).expect_err("4 is outside the role tag range");
        assert!(err.contains("invalid role tag"), "unexpected: {err}");
        assert!(Role::from_u8(255).is_err());
    }

    #[test]
    fn test_validate_mode_role_valid() {
        // SingleWriter accepts Leader, Follower, Client.
        assert!(validate_mode_role(HaMode::SingleWriter, Role::Leader).is_ok());
        assert!(validate_mode_role(HaMode::SingleWriter, Role::Follower).is_ok());
        assert!(validate_mode_role(HaMode::SingleWriter, Role::Client).is_ok());

        // SharedWriter accepts LatentWriter, Client.
        assert!(validate_mode_role(HaMode::SharedWriter, Role::LatentWriter).is_ok());
        assert!(validate_mode_role(HaMode::SharedWriter, Role::Client).is_ok());
    }

    #[test]
    fn test_validate_mode_role_rejects_singlewriter_latent() {
        let err =
            validate_mode_role(HaMode::SingleWriter, Role::LatentWriter).expect_err("invalid");
        assert!(
            err.contains("LatentWriter requires SharedWriter mode"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_validate_mode_role_rejects_sharedwriter_leader() {
        let err = validate_mode_role(HaMode::SharedWriter, Role::Leader).expect_err("invalid");
        assert!(
            err.contains("SharedWriter has no Leader/Follower"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_validate_mode_role_rejects_sharedwriter_follower() {
        let err = validate_mode_role(HaMode::SharedWriter, Role::Follower).expect_err("invalid");
        assert!(
            err.contains("SharedWriter has no Leader/Follower"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_hamode_display_round_trip() {
        assert_eq!(HaMode::SingleWriter.to_string(), "SingleWriter");
        assert_eq!(HaMode::SharedWriter.to_string(), "SharedWriter");
    }

    #[test]
    fn test_role_event_variants() {
        let joined = RoleEvent::Joined {
            db_name: "test".into(),
            role: Role::Leader,
        };
        let promoted = RoleEvent::Promoted {
            db_name: "test".into(),
        };
        let demoted = RoleEvent::Demoted {
            db_name: "test".into(),
        };
        let fenced = RoleEvent::Fenced {
            db_name: "test".into(),
        };
        let sleeping = RoleEvent::Sleeping {
            db_name: "test".into(),
        };
        let manifest_changed = RoleEvent::ManifestChanged {
            db_name: "test".into(),
            version: 42,
        };

        // Verify they all compile and match their patterns
        match joined {
            RoleEvent::Joined { .. } => {}
            _ => panic!("wrong variant"),
        }
        match promoted {
            RoleEvent::Promoted { .. } => {}
            _ => panic!("wrong variant"),
        }
        match demoted {
            RoleEvent::Demoted { .. } => {}
            _ => panic!("wrong variant"),
        }
        match fenced {
            RoleEvent::Fenced { .. } => {}
            _ => panic!("wrong variant"),
        }
        match sleeping {
            RoleEvent::Sleeping { .. } => {}
            _ => panic!("wrong variant"),
        }
        match manifest_changed {
            RoleEvent::ManifestChanged { version, .. } => assert_eq!(version, 42),
            _ => panic!("wrong variant"),
        }
    }

    fn test_store() -> Arc<dyn LeaseStore> {
        Arc::new(crate::lease::InMemoryLeaseStore::new())
    }

    #[test]
    fn test_lease_config_defaults() {
        let config = LeaseConfig::new(test_store(), "instance-1".into(), "127.0.0.1:8080".into());

        assert_eq!(config.instance_id, "instance-1");
        assert_eq!(config.address, "127.0.0.1:8080");
        assert_eq!(config.ttl_secs, 5);
        assert_eq!(config.renew_interval, Duration::from_secs(2));
        assert_eq!(config.follower_poll_interval, Duration::from_secs(1));
        assert_eq!(config.required_expired_reads, 1);
        assert_eq!(config.max_consecutive_renewal_errors, 3);
    }

    #[test]
    fn test_lease_config_custom() {
        let mut config =
            LeaseConfig::new(test_store(), "instance-1".into(), "127.0.0.1:8080".into());
        config.ttl_secs = 10;
        config.renew_interval = Duration::from_secs(5);
        config.follower_poll_interval = Duration::from_secs(2);
        config.required_expired_reads = 3;
        config.max_consecutive_renewal_errors = 5;

        assert_eq!(config.ttl_secs, 10);
        assert_eq!(config.renew_interval, Duration::from_secs(5));
        assert_eq!(config.follower_poll_interval, Duration::from_secs(2));
        assert_eq!(config.required_expired_reads, 3);
        assert_eq!(config.max_consecutive_renewal_errors, 5);
    }

    #[test]
    fn test_coordinator_config_default() {
        let config = CoordinatorConfig::default();

        assert_eq!(
            config.durability,
            Durability::Replicated(Duration::from_secs(1))
        );
        assert_eq!(config.snapshot_interval, Duration::from_secs(3600));
        assert!(config.lease.is_none());
        assert_eq!(config.follower_pull_interval, Duration::from_secs(1));
        assert_eq!(config.replicator_timeout, Duration::from_secs(30));
        assert_eq!(config.manifest_poll_interval, Duration::from_secs(1));
    }

    #[test]
    fn test_coordinator_config_with_lease() {
        let lease = LeaseConfig::new(test_store(), "instance-1".into(), "127.0.0.1:8080".into());
        let mut config = CoordinatorConfig::default();
        config.lease = Some(lease);

        assert!(config.lease.is_some());
        assert_eq!(config.lease.as_ref().unwrap().instance_id, "instance-1");
    }

    #[test]
    fn test_coordinator_config_custom() {
        let mut config = CoordinatorConfig::default();
        config.durability = Durability::Replicated(Duration::from_secs(5));
        config.snapshot_interval = Duration::from_secs(7200);
        config.follower_pull_interval = Duration::from_secs(2);
        config.replicator_timeout = Duration::from_secs(60);

        assert_eq!(
            config.durability,
            Durability::Replicated(Duration::from_secs(5))
        );
        assert_eq!(config.snapshot_interval, Duration::from_secs(7200));
        assert_eq!(config.follower_pull_interval, Duration::from_secs(2));
        assert_eq!(config.replicator_timeout, Duration::from_secs(60));
    }
}
