//! hadb: Database-agnostic HA coordination framework.
//!
//! Pure coordination logic with zero cloud dependencies. Abstracts away
//! databases (SQL, graph, document) and storage backends. Manifest types
//! and the `ManifestStore` trait live in the sibling `turbodb` crate
//! (extracted in Phase Turbogenesis); hadb depends on turbodb for that
//! trait surface and owns the lease/coordination side.
//!
//! ```ignore
//! use hadb::{Coordinator, CoordinatorConfig, LeaseConfig};
//! use turbodb::ManifestStore;
//!
//! // The lease store lives on LeaseConfig; None here = no HA (always leader).
//! let config = CoordinatorConfig {
//!     lease: Some(LeaseConfig::new(lease_store, instance_id, address)),
//!     ..Default::default()
//! };
//!
//! let coordinator = Coordinator::new(
//!     replicator,              // Arc<dyn Replicator>
//!     Some(manifest_store),    // Option<Arc<dyn turbodb::ManifestStore>>
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
pub mod traits;
pub mod types;

// Re-export core traits
pub use client::{HaClient, HaClientBuilder};
pub use coordinator::{Coordinator, JoinResult};
pub use follower::{run_leader_renewal, run_lease_monitor, FollowerBehavior, LeaseMonitorContext};
pub use hadb_lease::{CasResult, LeaseStore};
pub use lease::{DbLease, InMemoryLeaseStore, LeaseData};
pub use metrics::{HaMetrics, MetricsSnapshot};
pub use node_registry::{
    node_key, nodes_prefix, InMemoryNodeRegistry, NodeRegistration, NodeRegistry,
};
pub use traits::{NoOpReplicator, Replicator, StorageBackend};
pub use types::{
    validate_mode_durability, validate_mode_role, CoordinatorConfig, Durability, HaMode,
    LeaseConfig, Role, RoleEvent,
};
