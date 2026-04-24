//! Manifest envelope types for the turbodb crate family.
//!
//! The envelope is deliberately thin: version + writer_id + a wall-clock
//! timestamp + an opaque `Vec<u8>` payload. Turbodb does not look inside
//! the payload — its shape is defined by the consumer (turbolite,
//! turbograph, future turboduck). This keeps the abstraction in the same
//! direction as `hadb-lease` and `hadb-storage`: bytes-level trait, open
//! set of consumers.
//!
//! Phase Turbogenesis-b: replaces the closed `Backend` enum (which named
//! `Turbolite` / `TurboliteWalrust` / `Walrust` / `Turbograph` as
//! variants inside turbodb) with this opaque envelope.

use serde::{Deserialize, Serialize};

/// Full manifest for a database. The `payload` bytes are consumer-defined.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    /// Monotonically increasing version, bumped on every successful put.
    pub version: u64,
    /// Instance ID that last published this manifest.
    pub writer_id: String,
    /// Unix timestamp in milliseconds when this manifest was published.
    pub timestamp_ms: u64,
    /// Opaque consumer-defined payload. Encoded as msgpack `bin` (not an
    /// array of numbers) via `serde_bytes`.
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

/// Lightweight metadata extracted from `Manifest`. Returned by `meta()`
/// so pollers can check for version changes without fetching the
/// potentially-large payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestMeta {
    pub version: u64,
    pub writer_id: String,
}

impl From<&Manifest> for ManifestMeta {
    fn from(m: &Manifest) -> Self {
        Self {
            version: m.version,
            writer_id: m.writer_id.clone(),
        }
    }
}
