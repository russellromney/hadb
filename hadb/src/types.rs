use std::time::Duration;

/// Database role in the HA cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Leader,
    Follower,
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
/// - `Fenced` - Leader lost its lease — must stop serving immediately
/// - `Sleeping` - Leader signaled sleep (e.g., Fly scale-to-zero)
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
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            sync_interval: Duration::from_secs(1),
            snapshot_interval: Duration::from_secs(3600),
            lease: None,
            follower_pull_interval: Duration::from_secs(1),
            replicator_timeout: Duration::from_secs(30),
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
