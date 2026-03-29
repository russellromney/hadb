//! hadb: Database-agnostic HA coordination framework.
//!
//! Pure coordination logic with zero cloud dependencies. Abstracts away
//! databases (SQL, graph, document) and storage backends (S3, etcd, Consul).
//!
//! ```ignore
//! use hadb::{Coordinator, CoordinatorConfig, LeaseConfig};
//!
//! let config = CoordinatorConfig {
//!     lease: Some(LeaseConfig::new(instance_id, address)),
//!     ..Default::default()
//! };
//!
//! let coordinator = Coordinator::new(
//!     replicator,              // Arc<dyn Replicator>
//!     Some(lease_store),       // Option<Arc<dyn LeaseStore>>
//!     None,                    // Option<Arc<dyn NodeRegistry>>
//!     follower_behavior,       // Arc<dyn FollowerBehavior>
//!     "prefix/",
//!     config,
//! );
//!
//! coordinator.join("mydb", path).await?;
//! ```

pub mod client;
pub mod coordinator;
pub mod follower;
pub mod lease;
pub mod metrics;
pub mod node_registry;
pub mod sharded_lease;
pub mod traits;
pub mod types;

// Re-export core traits
pub use traits::{CasResult, LeaseStore, Replicator, StorageBackend};
pub use types::{CoordinatorConfig, LeaseConfig, Role, RoleEvent};
pub use metrics::{HaMetrics, MetricsSnapshot};
pub use lease::{DbLease, InMemoryLeaseStore, LeaseData};
pub use node_registry::{node_key, nodes_prefix, InMemoryNodeRegistry, NodeRegistry, NodeRegistration};
pub use follower::{FollowerBehavior, LeaseMonitorContext, run_leader_renewal, run_lease_monitor};
pub use client::{HaClient, HaClientBuilder};
pub use coordinator::Coordinator;
pub use sharded_lease::ShardedLeaseStore;
