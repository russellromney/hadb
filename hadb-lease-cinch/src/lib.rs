//! `hadb-lease-cinch`: Cinch-protocol HTTP lease store for hadb.
//!
//! Implements the `LeaseStore` trait over HTTP, speaking the `/v1/lease`
//! routes exposed by Grabby (and engine in embedded mode) with Bearer
//! token auth. CAS semantics use standard HTTP conditional headers
//! (If-Match, If-None-Match).
//!
//! Also exports `AtomicFence`/`AtomicFenceWriter`, the concrete
//! `FenceSource` implementation Cinch deployments use to plumb lease
//! revisions into fenced storage writes (see `hadb-storage-cinch`).
//!
//! ```ignore
//! use hadb_lease_cinch::{CinchLeaseStore, AtomicFence};
//!
//! let store = CinchLeaseStore::new("https://lease-proxy.example.com", "my-token");
//! let (fence, writer) = AtomicFence::new();
//! ```

pub mod fence;
pub mod lease_store;

pub use fence::{AtomicFence, AtomicFenceWriter};
pub use lease_store::CinchLeaseStore;
