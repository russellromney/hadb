//! `StorageBackend`: the byte-level blob-store trait.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Result of a CAS (compare-and-swap) operation.
///
/// `success=true` means the write was committed and `etag` is the new
/// version identifier. `success=false` means a precondition failed
/// (key already exists for `put_if_absent`, or etag mismatch for
/// `put_if_match`); the caller must re-read and retry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CasResult {
    pub success: bool,
    pub etag: Option<String>,
}

/// Byte-level blob-storage trait.
///
/// This is the minimum turbolite, walrust, and hadb need from a backend.
/// Domain-specific serialization (WAL frames, turbolite manifests, lease
/// holder bytes) lives in the consumer; this trait only moves bytes.
///
/// ## Consistency model
///
/// - `get`/`put`/`delete` are best-effort. No ordering guarantees between
///   writers unless the backend explicitly provides them.
/// - `put_if_absent`/`put_if_match` are the CAS primitives. A `success=true`
///   return means the server committed the write atomically.
/// - `list` returns keys in lexicographic order. `after` is exclusive.
///
/// ## Async-ness
///
/// Trait methods are async because most real backends (S3, HTTP) are async.
/// Sync consumers (turbolite's prefetch workers) wrap their calls in
/// `tokio::runtime::Handle::block_on`. The trait itself stays tokio-agnostic.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    // ── Basic I/O ──────────────────────────────────────────────────────

    /// Fetch an object by key. Returns `Ok(None)` if the key doesn't exist.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Write an object. Overwrites if the key exists.
    async fn put(&self, key: &str, data: &[u8]) -> Result<()>;

    /// Delete an object. Missing keys are not an error.
    async fn delete(&self, key: &str) -> Result<()>;

    /// List object keys with a given prefix.
    ///
    /// `after`: if `Some(key)`, only returns keys lexicographically greater
    /// than `key` (exclusive). Used for pagination.
    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>>;

    /// Check whether an object exists. Cheaper than a full `get` for
    /// backends that support HEAD requests.
    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.get(key).await?.is_some())
    }

    // ── CAS primitives ─────────────────────────────────────────────────

    /// Write iff the key does not exist.
    ///
    /// Used by lease stores to claim a fresh lease, and by manifest stores
    /// to write version 0.
    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult>;

    /// Write iff the current etag matches.
    ///
    /// Used by lease renewal (the lease holder's etag advances on each
    /// heartbeat) and by manifest CAS updates.
    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult>;

    // ── Optional / default-impl ────────────────────────────────────────

    /// Byte-range GET. Default implementation fetches the full object and
    /// slices. Override when the backend supports Range headers (S3, HTTP).
    async fn range_get(&self, key: &str, start: u64, len: u32) -> Result<Option<Vec<u8>>> {
        match self.get(key).await? {
            None => Ok(None),
            Some(bytes) => {
                let start = start as usize;
                let end = start.saturating_add(len as usize).min(bytes.len());
                if start >= bytes.len() {
                    Ok(Some(Vec::new()))
                } else {
                    Ok(Some(bytes[start..end].to_vec()))
                }
            }
        }
    }

    /// Batch delete. Default is serial `delete` calls; override for backends
    /// with native batch primitives (S3 DeleteObjects, HTTP bulk endpoints).
    ///
    /// ## Semantics
    ///
    /// * "Missing key" is success (idempotent delete), the same as `delete`.
    /// * Explicit per-key server failures must produce `Err` with detail
    ///   about the failed keys; do not warn-and-continue. Callers
    ///   (compaction, garbage collection) depend on visible failure.
    /// * The returned `usize` is a count of keys the backend accepted, not a
    ///   diagnostic. Callers needing per-key results should call `delete` in
    ///   a loop instead.
    async fn delete_many(&self, keys: &[String]) -> Result<usize> {
        let mut count = 0;
        for key in keys {
            self.delete(key).await?;
            count += 1;
        }
        Ok(count)
    }

    /// Batch put. Default is serial `put` calls; override for backends
    /// with parallelism (HTTP with concurrent uploads, S3 with multipart).
    async fn put_many(&self, entries: &[(String, Vec<u8>)]) -> Result<()> {
        for (key, data) in entries {
            self.put(key, data).await?;
        }
        Ok(())
    }

    /// Short human-readable identifier for this backend. Used in logs.
    fn backend_name(&self) -> &str {
        "unknown"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Compile-time checks: the trait is object-safe and Send + Sync.
    #[allow(dead_code)]
    fn _object_safe(_: &dyn StorageBackend) {}

    #[allow(dead_code)]
    fn _send_sync<T: StorageBackend>() {}
}
