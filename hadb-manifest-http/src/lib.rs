//! hadb-manifest-http: HTTP-based manifest store for hadb.
//!
//! Implements the `ManifestStore` trait over HTTP, designed for embedded
//! replicas that coordinate through any HTTP server implementing the
//! manifest API.
//!
//! ```ignore
//! use hadb_manifest_http::HttpManifestStore;
//!
//! let store = HttpManifestStore::new("https://manifest-proxy.example.com", "my-token");
//! ```

pub mod manifest_store;

pub use manifest_store::HttpManifestStore;
