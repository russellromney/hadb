//! `hadb-storage-local`: filesystem-backed `StorageBackend`.
//!
//! Writes land under a single root directory; the `StorageBackend` key is
//! treated as a relative path. Writes are atomic (write-to-tmp + rename);
//! `put_if_absent` uses `O_CREAT | O_EXCL`, so it's a true filesystem CAS.
//!
//! `put_if_match` uses a per-key `tokio::Mutex` to serialise the
//! "compare etag + commit" sequence. The etag is the stored file's
//! `(size, mtime_nanos)` pair encoded as a string: good enough for local
//! CAS given the serialisation, though not meant for production scale.
//!
//! # Intended scope
//!
//! This backend is for local development, single-node setups, and tests
//! that want a filesystem instead of a HashMap. Two hard limits to keep
//! in mind:
//!
//! 1. **Single-process only.** The per-key `tokio::Mutex` that guards the
//!    `put_if_match` compare-and-swap sequence lives in memory; two
//!    processes writing under the same `root` race each other's renames
//!    and can both commit a CAS update. Use a distributed backend for
//!    multi-process workloads.
//! 2. **Etags are not stable across restarts** or across filesystems.
//!    The etag is `(size, mtime_nanos)`; restoring from backup, rsync'ing,
//!    or any operation that rewrites the mtime without changing content
//!    invalidates previously held etags. Callers that persist an etag
//!    somewhere else (a manifest pointer, for example) must re-read it
//!    after any such operation.

use std::collections::HashMap;
use std::io::{ErrorKind, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rand::RngCore;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

use hadb_storage::{CasResult, StorageBackend};

/// Filesystem-backed `StorageBackend`.
pub struct LocalStorage {
    root: PathBuf,
    /// Per-key mutex to serialise CAS sequences. Entries accumulate as keys
    /// are touched; since this backend is single-process and low-scale, we
    /// don't bother evicting.
    cas_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl LocalStorage {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            cas_locks: Mutex::new(HashMap::new()),
        }
    }

    fn path(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    async fn cas_lock(&self, key: &str) -> Arc<Mutex<()>> {
        let mut guard = self.cas_locks.lock().await;
        guard
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Atomic write: write to `.tmp.{rand}` sibling, then rename over the
    /// target. Rename is atomic on POSIX; best-effort elsewhere.
    async fn atomic_write(&self, path: &Path, data: &[u8]) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let mut rand_buf = [0u8; 8];
        rand::thread_rng().fill_bytes(&mut rand_buf);
        let suffix = rand_buf
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let tmp = path.with_extension(format!("tmp.{suffix}"));

        let mut file = File::create(&tmp).await?;
        file.write_all(data).await?;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        fs::rename(&tmp, path).await?;
        Ok(())
    }

    async fn current_etag(&self, path: &Path) -> Result<Option<String>> {
        match fs::metadata(path).await {
            Ok(meta) => Ok(Some(etag_from_meta(&meta))),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

fn etag_from_meta(meta: &std::fs::Metadata) -> String {
    let size = meta.len();
    let mtime_nanos = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{size}-{mtime_nanos}")
}

#[async_trait]
impl StorageBackend for LocalStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match fs::read(self.path(key)).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.atomic_write(&self.path(key), data).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        match fs::remove_file(self.path(key)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        let root = self.root.clone();
        let prefix = prefix.to_string();
        let after = after.map(str::to_string);
        // Synchronous walk. This backend isn't meant for huge listings.
        let keys = tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
            let mut out = Vec::new();
            walk(&root, &root, &mut out)?;
            out.retain(|k| k.starts_with(&prefix));
            if let Some(cursor) = after {
                out.retain(|k| k.as_str() > cursor.as_str());
            }
            out.sort();
            Ok(out)
        })
        .await
        .map_err(|e| anyhow!("list walk panicked: {e}"))??;
        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(fs::try_exists(self.path(key)).await?)
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        let path = self.path(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        // O_CREAT | O_EXCL: fails with AlreadyExists if the file already exists.
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(mut file) => {
                file.write_all(data).await?;
                file.flush().await?;
                file.sync_all().await?;
                drop(file);
                let etag = match fs::metadata(&path).await {
                    Ok(m) => etag_from_meta(&m),
                    Err(e) => return Err(e.into()),
                };
                Ok(CasResult {
                    success: true,
                    etag: Some(etag),
                })
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(CasResult {
                success: false,
                etag: None,
            }),
            Err(e) => Err(e.into()),
        }
    }

    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult> {
        let lock = self.cas_lock(key).await;
        let _guard = lock.lock().await;

        let path = self.path(key);
        match self.current_etag(&path).await? {
            Some(current) if current == etag => {
                self.atomic_write(&path, data).await?;
                let new_etag = match fs::metadata(&path).await {
                    Ok(m) => etag_from_meta(&m),
                    Err(e) => return Err(e.into()),
                };
                Ok(CasResult {
                    success: true,
                    etag: Some(new_etag),
                })
            }
            _ => Ok(CasResult {
                success: false,
                etag: None,
            }),
        }
    }

    async fn range_get(&self, key: &str, start: u64, len: u32) -> Result<Option<Vec<u8>>> {
        let path = self.path(key);
        let mut file = match File::open(&path).await {
            Ok(f) => f,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let size = file.metadata().await?.len();
        if start >= size {
            return Ok(Some(Vec::new()));
        }
        let end = start.saturating_add(len as u64).min(size);
        let want = (end - start) as usize;
        file.seek(SeekFrom::Start(start)).await?;
        let mut buf = vec![0u8; want];
        file.read_exact(&mut buf).await?;
        Ok(Some(buf))
    }

    fn backend_name(&self) -> &str {
        "local"
    }
}

/// Recursively collect files under `dir`, with paths relative to `root`
/// using `/` as the separator (platform-independent keys).
fn walk(root: &Path, dir: &Path, out: &mut Vec<String>) -> Result<()> {
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    for entry in rd {
        let entry = entry?;
        let path = entry.path();
        let ft = entry.file_type()?;
        if ft.is_dir() {
            walk(root, &path, out)?;
        } else if ft.is_file() {
            // Skip in-flight atomic-write tmp files so callers never observe them.
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.contains(".tmp.") {
                    continue;
                }
            }
            let rel = path
                .strip_prefix(root)
                .map_err(|_| anyhow!("path escaped root"))?;
            let key = rel
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("/");
            out.push(key);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn new_store() -> (LocalStorage, TempDir) {
        let tmp = TempDir::new().unwrap();
        (LocalStorage::new(tmp.path()), tmp)
    }

    #[tokio::test]
    async fn put_then_get_roundtrips() {
        let (s, _t) = new_store();
        s.put("k", b"v").await.unwrap();
        assert_eq!(s.get("k").await.unwrap().unwrap(), b"v");
    }

    #[tokio::test]
    async fn put_creates_parent_dirs() {
        let (s, _t) = new_store();
        s.put("a/b/c/k", b"v").await.unwrap();
        assert_eq!(s.get("a/b/c/k").await.unwrap().unwrap(), b"v");
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let (s, _t) = new_store();
        assert!(s.get("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn put_overwrites_atomically() {
        let (s, _t) = new_store();
        s.put("k", b"first").await.unwrap();
        s.put("k", b"second-much-longer").await.unwrap();
        assert_eq!(s.get("k").await.unwrap().unwrap(), b"second-much-longer");
    }

    #[tokio::test]
    async fn delete_removes_and_is_idempotent() {
        let (s, _t) = new_store();
        s.put("k", b"v").await.unwrap();
        s.delete("k").await.unwrap();
        assert!(s.get("k").await.unwrap().is_none());
        s.delete("k").await.unwrap();
    }

    #[tokio::test]
    async fn exists_reflects_state() {
        let (s, _t) = new_store();
        assert!(!s.exists("k").await.unwrap());
        s.put("k", b"").await.unwrap();
        assert!(s.exists("k").await.unwrap());
    }

    #[tokio::test]
    async fn list_filters_by_prefix_and_sorts() {
        let (s, _t) = new_store();
        for k in ["a/3", "a/1", "a/2", "b/1"] {
            s.put(k, b"").await.unwrap();
        }
        assert_eq!(s.list("a/", None).await.unwrap(), vec!["a/1", "a/2", "a/3"]);
        assert_eq!(s.list("b/", None).await.unwrap(), vec!["b/1"]);
        assert!(s.list("c/", None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_after_is_exclusive() {
        let (s, _t) = new_store();
        for k in ["a/1", "a/2", "a/3"] {
            s.put(k, b"").await.unwrap();
        }
        assert_eq!(
            s.list("a/", Some("a/1")).await.unwrap(),
            vec!["a/2", "a/3"]
        );
    }

    #[tokio::test]
    async fn list_ignores_in_flight_tmp_files() {
        // We can't easily create a real half-written tmp, but we can drop one
        // into the root and confirm `list` skips it.
        let (s, t) = new_store();
        std::fs::write(t.path().join("k.tmp.abc123"), b"partial").unwrap();
        s.put("k", b"final").await.unwrap();
        let keys = s.list("", None).await.unwrap();
        assert!(keys.iter().any(|k| k == "k"));
        assert!(keys.iter().all(|k| !k.contains(".tmp.")));
    }

    #[tokio::test]
    async fn put_if_absent_first_wins() {
        let (s, _t) = new_store();
        let a = s.put_if_absent("k", b"first").await.unwrap();
        assert!(a.success);
        let b = s.put_if_absent("k", b"second").await.unwrap();
        assert!(!b.success);
        assert_eq!(s.get("k").await.unwrap().unwrap(), b"first");
    }

    #[tokio::test]
    async fn concurrent_put_if_absent_exactly_one_wins() {
        let (s, _t) = new_store();
        let s = Arc::new(s);
        let mut handles = Vec::new();
        for i in 0..16 {
            let s = Arc::clone(&s);
            handles.push(tokio::spawn(async move {
                s.put_if_absent("lease", format!("node-{i}").as_bytes())
                    .await
                    .unwrap()
            }));
        }
        let mut wins = 0;
        for h in handles {
            if h.await.unwrap().success {
                wins += 1;
            }
        }
        assert_eq!(wins, 1);
    }

    #[tokio::test]
    async fn put_if_match_advances_and_rejects_stale() {
        let (s, _t) = new_store();
        let a = s.put_if_absent("k", b"v1").await.unwrap();
        let e1 = a.etag.unwrap();

        // Etag changes are based on mtime-nanos + size; if fs mtime granularity
        // is coarse the nanos may tie. Forcing a length change makes size differ
        // and guarantees a distinct etag.
        let b = s.put_if_match("k", b"v2-different-length", &e1).await.unwrap();
        assert!(b.success);
        let e2 = b.etag.unwrap();
        assert_ne!(e1, e2);

        let c = s.put_if_match("k", b"v3", &e1).await.unwrap();
        assert!(!c.success);
    }

    #[tokio::test]
    async fn put_if_match_on_missing_fails() {
        let (s, _t) = new_store();
        let r = s.put_if_match("nope", b"x", "any").await.unwrap();
        assert!(!r.success);
    }

    #[tokio::test]
    async fn range_get_slices_correctly() {
        let (s, _t) = new_store();
        s.put("k", b"abcdefghij").await.unwrap();
        assert_eq!(s.range_get("k", 0, 3).await.unwrap().unwrap(), b"abc");
        assert_eq!(s.range_get("k", 2, 4).await.unwrap().unwrap(), b"cdef");
        assert_eq!(s.range_get("k", 8, 10).await.unwrap().unwrap(), b"ij");
        assert_eq!(
            s.range_get("k", 50, 10).await.unwrap().unwrap(),
            Vec::<u8>::new()
        );
    }

    #[tokio::test]
    async fn range_get_on_missing_returns_none() {
        let (s, _t) = new_store();
        assert!(s.range_get("nope", 0, 10).await.unwrap().is_none());
    }

    #[allow(dead_code)]
    fn _usable_as_arc_dyn(_: Arc<dyn StorageBackend>) {}

    #[test]
    fn backend_name_is_stable() {
        let tmp = TempDir::new().unwrap();
        assert_eq!(LocalStorage::new(tmp.path()).backend_name(), "local");
    }
}
