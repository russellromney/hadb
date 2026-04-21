//! `ManifestStore` trait — CAS-based manifest publication.
//!
//! Same trait pattern as hadb-lease's `LeaseStore`: trait in this
//! crate, implementations in sibling crates (turbodb-manifest-mem,
//! turbodb-manifest-s3, turbodb-manifest-cinch, turbodb-manifest-nats,
//! turbodb-manifest-redis).

use anyhow::Result;
use async_trait::async_trait;
use hadb_storage::CasResult;

use crate::types::{Manifest, ManifestMeta};

/// Trait for manifest storage with CAS semantics.
///
/// Three methods:
/// - `get`: full manifest fetch (used on catch-up)
/// - `put`: CAS publish with version fencing
/// - `meta`: lightweight HEAD for cheap polling
#[async_trait]
pub trait ManifestStore: Send + Sync {
    /// Fetch the full manifest for a key. Returns None if no manifest exists.
    async fn get(&self, key: &str) -> Result<Option<Manifest>>;

    /// Publish a new manifest with CAS on `expected_version`.
    ///
    /// - `expected_version: None` means the key must not exist (first publish).
    /// - `expected_version: Some(v)` means the current version must equal `v`.
    ///
    /// The `version` field in the provided manifest is ignored; the
    /// store assigns the next version (1 for first publish,
    /// `expected_version + 1` for updates). Implementors must enforce
    /// this.
    ///
    /// Returns `CasResult { success: true, .. }` on success, or
    /// `CasResult { success: false, .. }` on version mismatch.
    async fn put(
        &self,
        key: &str,
        manifest: &Manifest,
        expected_version: Option<u64>,
    ) -> Result<CasResult>;

    /// Cheap metadata check without fetching the full storage payload.
    /// Returns version + leadership info for fencing.
    async fn meta(&self, key: &str) -> Result<Option<ManifestMeta>>;
}
