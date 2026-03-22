//! hadb: Database-agnostic HA coordination framework.
//!
//! Pure coordination logic with zero cloud dependencies. Abstracts away
//! databases (SQL, graph, document) and storage backends (S3, etcd, Consul).
//!
//! ```ignore
//! use hadb::{Coordinator, Replicator, Executor, LeaseStore, StorageBackend};
//! use hadb_s3::{S3LeaseStore, S3StorageBackend};
//!
//! let coordinator = Coordinator::new(
//!     replicator,
//!     executor,
//!     S3LeaseStore::new(...)?,
//!     S3StorageBackend::new(...)?,
//!     config,
//! )?;
//!
//! coordinator.join("mydb", path).await?;
//! ```

pub mod lease;
pub mod metrics;
pub mod node_registry;
pub mod traits;
pub mod types;

// Re-export core traits
pub use traits::{CasResult, Executor, LeaseStore, Replicator, StorageBackend};
pub use types::{CoordinatorConfig, LeaseConfig, Role, RoleEvent};
pub use metrics::{HaMetrics, MetricsSnapshot};
pub use lease::{DbLease, LeaseData};
pub use node_registry::{node_key, nodes_prefix, InMemoryNodeRegistry, NodeRegistry, NodeRegistration};

// Placeholder Coordinator - will be implemented in next step
use std::sync::Arc;

/// Generic HA coordinator.
///
/// Coordinates leader election, replication, and write forwarding across
/// any database (SQL, graph, document) and any storage backend (S3, etcd, Consul).
pub struct Coordinator<R, E, L, S>
where
    R: Replicator,
    E: Executor,
    L: LeaseStore,
    S: StorageBackend,
{
    _replicator: Arc<R>,
    _executor: Arc<E>,
    _lease_store: Arc<L>,
    _storage: Arc<S>,
}

impl<R, E, L, S> Coordinator<R, E, L, S>
where
    R: Replicator,
    E: Executor,
    L: LeaseStore,
    S: StorageBackend,
{
    // Placeholder - will be implemented later
}
