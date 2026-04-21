//! Manifest types shared across the turbodb crate family.
//!
//! The manifest collapses coordination + storage state into one small
//! object. One fetch tells a node everything: who last wrote, what
//! epoch, and what the current storage state is.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ============================================================================
// Manifest
// ============================================================================

/// Full manifest for a database, combining coordination metadata with
/// backend-specific state.
///
/// Renamed from `hadb::HaManifest` during Phase Turbogenesis. The
/// `storage` field name is preserved for wire compatibility.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    /// Monotonically increasing version, bumped on every successful put.
    pub version: u64,
    /// Instance ID that last published this manifest.
    pub writer_id: String,
    /// Fencing token from the lease. Prevents stale writers from publishing.
    pub lease_epoch: u64,
    /// Unix timestamp in milliseconds when this manifest was published.
    pub timestamp_ms: u64,
    /// Storage-engine-specific state.
    pub storage: Backend,
}

/// Lightweight metadata extracted from `Manifest`. Returned by `meta()`
/// to allow cheap polling without fetching the full storage payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestMeta {
    pub version: u64,
    pub writer_id: String,
    pub lease_epoch: u64,
}

impl From<&Manifest> for ManifestMeta {
    fn from(m: &Manifest) -> Self {
        Self {
            version: m.version,
            writer_id: m.writer_id.clone(),
            lease_epoch: m.lease_epoch,
        }
    }
}

// ============================================================================
// Backend (storage-engine-specific state)
// ============================================================================

/// Storage-engine-specific manifest data.
///
/// Renamed from `hadb::StorageManifest` during Phase Turbogenesis. Each
/// backend (turbolite, walrust, turbograph, …) adds a variant here.
/// Variant names are preserved on the wire.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Backend {
    Turbolite {
        /// turbolite manifest version (for S3 key uniqueness: pg/{gid}_v{version})
        #[serde(default)]
        turbolite_version: u64,
        page_count: u64,
        page_size: u32,
        pages_per_group: u32,
        sub_pages_per_frame: u32,
        strategy: String,
        page_group_keys: Vec<String>,
        frame_tables: Vec<Vec<FrameEntry>>,
        group_pages: Vec<Vec<u64>>,
        btrees: BTreeMap<u64, BTreeManifestEntry>,
        interior_chunk_keys: BTreeMap<u32, String>,
        index_chunk_keys: BTreeMap<u32, String>,
        subframe_overrides: Vec<BTreeMap<usize, SubframeOverride>>,
        /// Full page 0 content for multiwriter catch-up. Optional for backward compat.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        db_header: Option<Vec<u8>>,
    },
    /// Hybrid: turbolite page groups as base state + walrust WAL frames as deltas.
    /// turbolite page groups in S3 = full database. walrust changesets = incremental
    /// WAL frames since last turbolite checkpoint. No walrust snapshots needed.
    TurboliteWalrust {
        // turbolite base state (same fields as Turbolite variant)
        #[serde(default)]
        turbolite_version: u64,
        page_count: u64,
        page_size: u32,
        pages_per_group: u32,
        sub_pages_per_frame: u32,
        strategy: String,
        page_group_keys: Vec<String>,
        frame_tables: Vec<Vec<FrameEntry>>,
        group_pages: Vec<Vec<u64>>,
        btrees: BTreeMap<u64, BTreeManifestEntry>,
        interior_chunk_keys: BTreeMap<u32, String>,
        index_chunk_keys: BTreeMap<u32, String>,
        subframe_overrides: Vec<BTreeMap<usize, SubframeOverride>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        db_header: Option<Vec<u8>>,
        // walrust delta position
        walrust_txid: u64,
        walrust_changeset_prefix: String,
    },
    Walrust {
        txid: u64,
        changeset_prefix: String,
        latest_changeset_key: String,
        snapshot_key: Option<String>,
        snapshot_txid: Option<u64>,
    },
    /// Turbograph page-group manifest (analogous to Turbolite but for graph databases).
    Turbograph {
        /// turbograph manifest version (for S3 key uniqueness: pg/{gid}_v{version})
        #[serde(default)]
        turbograph_version: u64,
        page_count: u64,
        page_size: u32,
        pages_per_group: u32,
        sub_pages_per_frame: u32,
        page_group_keys: Vec<String>,
        frame_tables: Vec<Vec<FrameEntry>>,
        subframe_overrides: Vec<BTreeMap<usize, SubframeOverride>>,
        encrypted: bool,
        /// Graphstream journal sequence captured at checkpoint.
        /// Followers replay journal entries after this sequence.
        #[serde(default)]
        journal_seq: u64,
    },
    /// Hybrid: turbograph page groups as base state + graphstream journal deltas.
    TurbographGraphstream {
        #[serde(default)]
        turbograph_version: u64,
        page_count: u64,
        page_size: u32,
        pages_per_group: u32,
        sub_pages_per_frame: u32,
        page_group_keys: Vec<String>,
        frame_tables: Vec<Vec<FrameEntry>>,
        subframe_overrides: Vec<BTreeMap<usize, SubframeOverride>>,
        encrypted: bool,
        #[serde(default)]
        journal_seq: u64,
        /// S3 prefix for graphstream delta segments.
        #[serde(default)]
        graphstream_segment_prefix: String,
    },
}

/// A single frame entry in a turbolite/turbograph frame table.
/// Represents a byte range within an S3 object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FrameEntry {
    pub offset: u64,
    pub len: u32,
    /// Actual number of pages encoded in this frame.
    /// Used by turbograph for seekable sub-frames. 0 = legacy format.
    #[serde(default)]
    pub page_count: u32,
}

/// B-tree metadata with group associations for a turbolite page group.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BTreeManifestEntry {
    pub name: String,
    pub obj_type: String,
    pub group_ids: Vec<u64>,
}

/// Override for a specific subframe (future turbolite Phase Drift).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubframeOverride {
    pub key: String,
    pub entry: FrameEntry,
}
