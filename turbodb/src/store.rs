//! `ManifestStore` trait — CAS-based manifest publication.
//!
//! Same trait pattern as hadb-lease's `LeaseStore`: trait in this
//! crate, implementations in sibling crates (turbodb-manifest-mem,
//! turbodb-manifest-s3, turbodb-manifest-cinch, turbodb-manifest-nats,
//! turbodb-manifest-redis).

use anyhow::Result;
use async_trait::async_trait;
use hadb_storage::CasResult;

use crate::types::{LeaseFenceError, Manifest, ManifestMeta};

/// Shared epoch-fence check for `ManifestStore::put` implementations.
///
/// Returns `Err(LeaseFenceError)` if `attempted_epoch < stored_epoch`
/// (a stale writer trying to overwrite a newer leader's manifest), or
/// `Ok(())` otherwise. Call this after reading the current stored
/// manifest's epoch and before performing the version-CAS.
///
/// Equal epochs are allowed through to version-CAS — that's the normal
/// case where the same leader publishes successive versions within one
/// lease term.
pub fn check_epoch_fence(stored_epoch: u64, attempted_epoch: u64) -> Result<()> {
    if attempted_epoch < stored_epoch {
        return Err(LeaseFenceError {
            stored: stored_epoch,
            attempted: attempted_epoch,
        }
        .into());
    }
    Ok(())
}

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

    /// Publish a new manifest with CAS on `expected_version`, fenced by
    /// the lease epoch.
    ///
    /// - `expected_version: None` means the key must not exist (first publish).
    /// - `expected_version: Some(v)` means the current version must equal `v`.
    ///
    /// The `version` field in the provided manifest is ignored; the
    /// store assigns the next version (1 for first publish,
    /// `expected_version + 1` for updates). Implementors must enforce
    /// this.
    ///
    /// **Epoch fencing (phase 004).** Before the version-CAS, the store
    /// compares `manifest.epoch` against the currently-stored manifest's
    /// epoch:
    /// - `manifest.epoch < stored.epoch` → the write is **fenced**:
    ///   return `Err(`[`LeaseFenceError`]`)`. The writer has lost its
    ///   lease to a newer leader; retrying cannot succeed.
    /// - `manifest.epoch >= stored.epoch` → proceed to version-CAS.
    ///
    /// The epoch check runs before version-CAS so a stale leader is
    /// fenced even if it happens to hold the current version. On first
    /// publish (no stored manifest) there is nothing to fence against.
    ///
    /// Returns:
    /// - `Ok(CasResult { success: true, .. })` on success
    /// - `Ok(CasResult { success: false, .. })` on **version** mismatch
    ///   (retry-able optimistic-concurrency race)
    /// - `Err(`[`LeaseFenceError`]`)` on **epoch** fence (fatal — stop
    ///   writing)
    ///
    /// [`LeaseFenceError`]: crate::LeaseFenceError
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
