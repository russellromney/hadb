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
    /// Lease epoch under which this manifest was published.
    ///
    /// Phase 004 fenced root pointer: `put` rejects any write whose
    /// `epoch` is **less than** the currently-stored manifest's epoch
    /// with a [`LeaseFenceError`]. This fences a stale leader (lost its
    /// lease, still at epoch E) from overwriting a manifest published
    /// by the new leader at epoch E+1 — even in the race window where
    /// version-CAS alone could let the stale write through.
    ///
    /// Promotion increments the epoch. Default 0 for pre-phase-004 /
    /// non-fenced publishers (decodes cleanly from old bytes via
    /// serde default; equal epochs fall back to pure version-CAS).
    #[serde(default)]
    pub epoch: u64,
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
    /// Lease epoch of the stored manifest — surfaced so pollers and
    /// would-be writers can detect fencing without fetching the payload.
    #[serde(default)]
    pub epoch: u64,
    pub writer_id: String,
}

impl From<&Manifest> for ManifestMeta {
    fn from(m: &Manifest) -> Self {
        Self {
            version: m.version,
            epoch: m.epoch,
            writer_id: m.writer_id.clone(),
        }
    }
}

/// Error returned by [`ManifestStore::put`] when a write is fenced by a
/// higher stored lease epoch.
///
/// Distinct from a version-CAS miss (which surfaces as
/// `CasResult { success: false, .. }` and is retry-able): a fence
/// violation means the writer has **lost its lease** and must stop
/// publishing entirely — retrying cannot succeed because a newer
/// leader owns the epoch.
///
/// Implementors return this via `anyhow`; callers that need to
/// distinguish it from other errors downcast:
///
/// ```ignore
/// match store.put(key, &m, expected).await {
///     Ok(cas) if cas.success => { /* published */ }
///     Ok(_) => { /* version race — re-read and retry */ }
///     Err(e) if e.downcast_ref::<LeaseFenceError>().is_some() => {
///         /* fenced — stop writing, we lost the lease */
///     }
///     Err(e) => return Err(e),
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("manifest put fenced: stored epoch {stored} > attempted epoch {attempted}")]
pub struct LeaseFenceError {
    /// The epoch currently in the store (the new leader's).
    pub stored: u64,
    /// The epoch the rejected write carried (the stale writer's).
    pub attempted: u64,
}
