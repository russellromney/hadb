use std::sync::Arc;
use std::time::Duration;

/// Database role in the HA cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Leader,
    Follower,
}

/// HA topology mode: how writes are coordinated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HaMode {
    /// Single persistent leader, followers replay.
    Dedicated,
    /// Multiple writers, lease-serialized per write.
    Shared,
}

/// How data is made durable between writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Replication log shipping (walrust WAL / graphstream journal). RPO = sync_interval.
    Replicated,
    /// Page-level S3 tiering (turbolite / turbograph S3Primary). RPO = 0.
    Synchronous,
}

impl Default for HaMode {
    fn default() -> Self { HaMode::Dedicated }
}

impl Default for Durability {
    fn default() -> Self { Durability::Replicated }
}

impl std::fmt::Display for HaMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HaMode::Dedicated => write!(f, "Dedicated"),
            HaMode::Shared => write!(f, "Shared"),
        }
    }
}

impl std::fmt::Display for Durability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Durability::Replicated => write!(f, "Replicated"),
            Durability::Synchronous => write!(f, "Synchronous"),
        }
    }
}

/// Validate a mode + durability combination.
///
/// Shared + Replicated is invalid: multiwriter with eventual consistency
/// means writers operate on stale data. Valid combinations:
/// - Dedicated + Replicated (journal shipping, RPO = sync_interval)
/// - Dedicated + Synchronous (S3Primary, RPO = 0)
/// - Shared + Synchronous (lease-per-write, RPO = 0)
pub fn validate_mode_durability(mode: HaMode, durability: Durability) -> Result<(), String> {
    match (mode, durability) {
        (HaMode::Dedicated, Durability::Replicated) => Ok(()),
        (HaMode::Dedicated, Durability::Synchronous) => Ok(()),
        (HaMode::Shared, Durability::Synchronous) => Ok(()),
        (HaMode::Shared, Durability::Replicated) => {
            Err("Shared mode requires Synchronous durability (Shared+Replicated is invalid: multiwriter + eventual = writes on stale data)".to_string())
        }
    }
}

impl Role {
    /// Convert Role to u8 for atomic storage.
    pub fn to_u8(self) -> u8 {
        match self {
            Role::Leader => 0,
            Role::Follower => 1,
        }
    }

    /// Convert u8 back to Role. Panics if invalid value.
    pub fn from_u8(val: u8) -> Self {
        match val {
            0 => Role::Leader,
            1 => Role::Follower,
            _ => panic!("Invalid role value: {}", val),
        }
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Leader => write!(f, "Leader"),
            Role::Follower => write!(f, "Follower"),
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
#[derive(Debug, Clone)]
pub struct LeaseConfig {
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
    pub fn new(instance_id: String, address: String) -> Self {
        Self {
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

/// Top-level configuration for the Coordinator.
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Replication sync interval. Default: 1s.
    pub sync_interval: Duration,
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
    /// Shared fence token, updated by DbLease on acquire/renew.
    /// Storage clients read this to include Fence-Token on writes.
    /// None = no fence enforcement (backward compat).
    pub fence_token: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            sync_interval: Duration::from_secs(1),
            snapshot_interval: Duration::from_secs(3600),
            lease: None,
            follower_pull_interval: Duration::from_secs(1),
            replicator_timeout: Duration::from_secs(30),
            manifest_poll_interval: Duration::from_secs(1),
            fence_token: None,
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
    }

    #[test]
    fn test_role_from_u8() {
        assert_eq!(Role::from_u8(0), Role::Leader);
        assert_eq!(Role::from_u8(1), Role::Follower);
    }

    #[test]
    fn test_role_roundtrip() {
        assert_eq!(Role::from_u8(Role::Leader.to_u8()), Role::Leader);
        assert_eq!(Role::from_u8(Role::Follower.to_u8()), Role::Follower);
    }

    #[test]
    #[should_panic(expected = "Invalid role value: 2")]
    fn test_role_from_u8_invalid() {
        Role::from_u8(2);
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

    #[test]
    fn test_lease_config_defaults() {
        let config = LeaseConfig::new("instance-1".into(), "127.0.0.1:8080".into());

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
        let mut config = LeaseConfig::new("instance-1".into(), "127.0.0.1:8080".into());
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

        assert_eq!(config.sync_interval, Duration::from_secs(1));
        assert_eq!(config.snapshot_interval, Duration::from_secs(3600));
        assert!(config.lease.is_none());
        assert_eq!(config.follower_pull_interval, Duration::from_secs(1));
        assert_eq!(config.replicator_timeout, Duration::from_secs(30));
        assert_eq!(config.manifest_poll_interval, Duration::from_secs(1));
    }

    #[test]
    fn test_coordinator_config_with_lease() {
        let lease = LeaseConfig::new("instance-1".into(), "127.0.0.1:8080".into());
        let mut config = CoordinatorConfig::default();
        config.lease = Some(lease);

        assert!(config.lease.is_some());
        assert_eq!(config.lease.as_ref().unwrap().instance_id, "instance-1");
    }

    #[test]
    fn test_coordinator_config_custom() {
        let mut config = CoordinatorConfig::default();
        config.sync_interval = Duration::from_secs(5);
        config.snapshot_interval = Duration::from_secs(7200);
        config.follower_pull_interval = Duration::from_secs(2);
        config.replicator_timeout = Duration::from_secs(60);

        assert_eq!(config.sync_interval, Duration::from_secs(5));
        assert_eq!(config.snapshot_interval, Duration::from_secs(7200));
        assert_eq!(config.follower_pull_interval, Duration::from_secs(2));
        assert_eq!(config.replicator_timeout, Duration::from_secs(60));
    }
}
