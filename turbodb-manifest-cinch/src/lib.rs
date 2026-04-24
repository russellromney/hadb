//! turbodb-manifest-cinch: ManifestStore over Cinch's `/v1/sync/manifest` HTTP wire.
//!
//! Renamed from hadb-manifest-http during Phase Turbogenesis. The wire
//! contract (Bearer auth, `/v1/sync/manifest?key=...`, custom
//! `X-Manifest-Version` / `X-Writer-Id` / `X-Lease-Epoch` HEAD headers)
//! is cinch-specific — a generic `-http` suffix would lie about
//! interoperability.
//!
//! ```ignore
//! use turbodb_manifest_cinch::CinchManifestStore;
//!
//! let store = CinchManifestStore::new("https://grabby.example.com", "my-token");
//! ```

pub mod manifest_store;

pub use manifest_store::CinchManifestStore;
