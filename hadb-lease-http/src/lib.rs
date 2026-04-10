//! hadb-lease-http: HTTP-based lease store for hadb.
//!
//! Implements the `LeaseStore` trait over HTTP, designed for embedded
//! replicas that coordinate through any HTTP server implementing the
//! lease API. CAS semantics use standard HTTP conditional headers
//! (If-Match, If-None-Match).
//!
//! ```ignore
//! use hadb_lease_http::HttpLeaseStore;
//!
//! let store = HttpLeaseStore::new("https://lease-proxy.example.com", "my-token");
//! ```

pub mod lease_store;

pub use lease_store::HttpLeaseStore;
