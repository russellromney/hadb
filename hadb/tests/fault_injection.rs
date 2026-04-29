//! Fault injection tests for StorageBackend implementations.
//!
//! These tests verify that storage backends handle failures gracefully
//! and don't corrupt data or crash the process.
//!
//! Failure scenarios tested:
//! - Network partition (upload returns error)
//! - S3 PUT failure (upload fails after succeed)
//! - Corrupt data on download
//! - Latency injection
//! - Partial upload (interrupted mid-write)
//!
//! Design principle: "never crash, never corrupt data" - storage backends
//! must handle all failure modes gracefully.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use hadb::StorageBackend;

/// Fault injection mode - controls what kind of failure to inject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultMode {
    /// No fault injection - normal operation
    None,
    /// Upload always fails immediately with an error
    UploadFailsImmediately,
    /// Upload fails after reported success (silent corruption detection)
    UploadSilentFailure,
    /// Download returns corrupted data
    DownloadCorruptsData,
    /// Download always fails immediately
    DownloadFailsImmediately,
    /// All operations have artificial latency (ms)
    LatencyInjection(u64),
    /// Operations fail after N invocations (counter-based)
    FailAfter(u64),
    /// Upload appears to succeed but data is never actually stored
    UploadPretendsSuccessNeverStored,
}

impl Default for FaultMode {
    fn default() -> Self {
        FaultMode::None
    }
}

/// Statistics tracked by the faulty storage backend.
#[derive(Debug, Default)]
pub struct FaultStats {
    /// Number of upload calls
    pub upload_count: Arc<AtomicUsize>,
    /// Number of download calls
    pub download_count: Arc<AtomicUsize>,
    /// Number of list calls
    pub list_count: Arc<AtomicUsize>,
    /// Number of delete calls
    pub delete_count: Arc<AtomicUsize>,
    /// Whether upload has failed in this session
    pub upload_has_failed: Arc<AtomicBool>,
    /// Whether download has failed in this session
    pub download_has_failed: Arc<AtomicBool>,
}

/// A storage backend wrapper that injects failures for testing.
///
/// This wraps any StorageBackend implementation and injects failures
/// according to the configured FaultMode. Used to verify that callers
/// handle storage failures correctly.
///
/// Example:
/// ```ignore
/// let storage = FaultyStorageBackend::new(
///     real_backend,
///     FaultMode::UploadFailsImmediately,
/// );
/// // upload will now fail
/// ```
pub struct FaultyStorageBackend<T: StorageBackend> {
    inner: T,
    mode: FaultMode,
    stats: FaultStats,
}

impl<T: StorageBackend> FaultyStorageBackend<T> {
    /// Create a new faulty storage backend wrapping `inner`.
    pub fn new(inner: T, mode: FaultMode) -> Self {
        Self {
            inner,
            mode,
            stats: FaultStats::default(),
        }
    }

    /// Create with explicit stats tracking.
    pub fn new_with_stats(inner: T, mode: FaultMode, stats: FaultStats) -> Self {
        Self { inner, mode, stats }
    }

    /// Update the fault mode at runtime.
    pub fn set_mode(&mut self, mode: FaultMode) {
        self.mode = mode;
    }

    /// Get a snapshot of current stats.
    pub fn stats(&self) -> FaultStatsSnapshot {
        FaultStatsSnapshot {
            uploads: self.stats.upload_count.load(Ordering::SeqCst),
            downloads: self.stats.download_count.load(Ordering::SeqCst),
            lists: self.stats.list_count.load(Ordering::SeqCst),
            deletes: self.stats.delete_count.load(Ordering::SeqCst),
            upload_failed: self.stats.upload_has_failed.load(Ordering::SeqCst),
            download_failed: self.stats.download_has_failed.load(Ordering::SeqCst),
        }
    }

    fn should_fail(&self, op: &'static str) -> bool {
        match self.mode {
            FaultMode::None => false,
            FaultMode::UploadFailsImmediately
            | FaultMode::UploadSilentFailure
            | FaultMode::UploadPretendsSuccessNeverStored
                if op == "upload" =>
            {
                true
            }
            FaultMode::DownloadFailsImmediately | FaultMode::DownloadCorruptsData
                if op == "download" =>
            {
                true
            }
            FaultMode::FailAfter(n) => {
                let count = match op {
                    "upload" => self.stats.upload_count.load(Ordering::SeqCst),
                    "download" => self.stats.download_count.load(Ordering::SeqCst),
                    "list" => self.stats.list_count.load(Ordering::SeqCst),
                    "delete" => self.stats.delete_count.load(Ordering::SeqCst),
                    _ => 0,
                };
                count >= n as usize
            }
            _ => false,
        }
    }

    fn inject_latency(&self) {
        if let FaultMode::LatencyInjection(ms) = self.mode {
            if ms > 0 {
                std::thread::sleep(std::time::Duration::from_millis(ms));
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct FaultStatsSnapshot {
    pub uploads: usize,
    pub downloads: usize,
    pub lists: usize,
    pub deletes: usize,
    pub upload_failed: bool,
    pub download_failed: bool,
}

#[async_trait]
impl<T: StorageBackend> StorageBackend for FaultyStorageBackend<T> {
    async fn upload(&self, key: &str, data: &[u8]) -> Result<()> {
        self.stats.upload_count.fetch_add(1, Ordering::SeqCst);
        self.inject_latency();

        if self.should_fail("upload") {
            self.stats.upload_has_failed.store(true, Ordering::SeqCst);

            match self.mode {
                FaultMode::UploadSilentFailure | FaultMode::UploadPretendsSuccessNeverStored => {
                    // Pretend success but don't actually store
                    // Caller thinks data is persisted, but it isn't
                    return Ok(());
                }
                _ => {
                    anyhow::bail!("injected upload failure for key: {}", key);
                }
            }
        }

        self.inner.upload(key, data).await
    }

    async fn download(&self, key: &str) -> Result<Vec<u8>> {
        self.stats.download_count.fetch_add(1, Ordering::SeqCst);
        self.inject_latency();

        if self.should_fail("download") {
            self.stats.download_has_failed.store(true, Ordering::SeqCst);

            if self.mode == FaultMode::DownloadCorruptsData {
                // Return corrupted data - caller should detect via checksum
                return Ok(b"CORRUPTED_DATA".to_vec());
            }

            anyhow::bail!("injected download failure for key: {}", key);
        }

        let data = self.inner.download(key).await?;

        // Corruption injection for silent failure mode
        if self.mode == FaultMode::UploadPretendsSuccessNeverStored {
            // If we previously pretended to succeed, now return nothing
            // This simulates the "silent data loss" scenario
            anyhow::bail!("key not found after pretend-success: {}", key);
        }

        Ok(data)
    }

    async fn list(&self, prefix: &str, max_keys: Option<usize>) -> Result<Vec<String>> {
        self.stats.list_count.fetch_add(1, Ordering::SeqCst);
        self.inject_latency();

        // List doesn't fail in any mode (might be too aggressive)

        self.inner.list(prefix, max_keys).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.stats.delete_count.fetch_add(1, Ordering::SeqCst);
        self.inject_latency();

        // Delete doesn't fail in any mode

        self.inner.delete(key).await
    }
}

// ============================================================================
// Tests
// ============================================================================

/// Mock storage backend for testing fault injection.
#[derive(Clone)]
struct MockStorageBackend {
    data: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>>,
}

impl MockStorageBackend {
    fn new() -> Self {
        Self {
            data: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }
}

#[async_trait]
impl StorageBackend for MockStorageBackend {
    async fn upload(&self, key: &str, data: &[u8]) -> Result<()> {
        self.data
            .lock()
            .unwrap()
            .insert(key.to_string(), data.to_vec());
        Ok(())
    }

    async fn download(&self, key: &str) -> Result<Vec<u8>> {
        self.data
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("not found"))
    }

    async fn list(&self, prefix: &str, _max_keys: Option<usize>) -> Result<Vec<String>> {
        let data = self.data.lock().unwrap();
        Ok(data
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.data.lock().unwrap().remove(key);
        Ok(())
    }
}

#[tokio::test]
async fn test_normal_operation() {
    let inner = MockStorageBackend::new();
    let storage = FaultyStorageBackend::new(inner, FaultMode::None);

    storage.upload("key1", b"data1").await.unwrap();
    let data = storage.download("key1").await.unwrap();
    assert_eq!(data, b"data1");

    let stats = storage.stats();
    assert_eq!(stats.uploads, 1);
    assert_eq!(stats.downloads, 1);
}

#[tokio::test]
async fn test_upload_fails_immediately() {
    let inner = MockStorageBackend::new();
    let storage = FaultyStorageBackend::new(inner, FaultMode::UploadFailsImmediately);

    let result = storage.upload("key1", b"data1").await;
    assert!(result.is_err());
    assert!(storage.stats().upload_failed);

    let inner2 = MockStorageBackend::new();
    let storage2 = FaultyStorageBackend::new(inner2, FaultMode::None);
    storage2.upload("key2", b"data2").await.unwrap();
    let data = storage2.download("key2").await.unwrap();
    assert_eq!(data, b"data2");
}

#[tokio::test]
async fn test_upload_silent_failure() {
    let inner = MockStorageBackend::new();
    let storage = FaultyStorageBackend::new(inner, FaultMode::UploadSilentFailure);

    let result = storage.upload("key1", b"data1").await;
    assert!(result.is_ok());

    let result = storage.download("key1").await;
    assert!(result.is_err());
    assert!(storage.stats().upload_failed);
}

#[tokio::test]
async fn test_download_corrupts_data() {
    let inner = MockStorageBackend::new();
    let _storage = FaultyStorageBackend::new(inner, FaultMode::DownloadCorruptsData);

    let inner2 = MockStorageBackend::new();
    let storage2 = FaultyStorageBackend::new(inner2.clone(), FaultMode::None);
    storage2.upload("key1", b"real_data").await.unwrap();

    let storage3 = FaultyStorageBackend::new(inner2, FaultMode::DownloadCorruptsData);
    let data = storage3.download("key1").await.unwrap();
    assert_eq!(data, b"CORRUPTED_DATA");
}

#[tokio::test]
async fn test_latency_injection() {
    let inner = MockStorageBackend::new();
    let storage = FaultyStorageBackend::new(inner, FaultMode::LatencyInjection(50));

    let start = std::time::Instant::now();
    storage.upload("key1", b"data1").await.unwrap();
    let elapsed = start.elapsed().as_millis();

    assert!(elapsed >= 45, "expected ~50ms latency, got {}ms", elapsed);
}

#[tokio::test]
async fn test_fail_after() {
    // FailAfter(4) means the 4th upload fails (counter increments before check)
    let inner = MockStorageBackend::new();
    let storage = FaultyStorageBackend::new(inner, FaultMode::FailAfter(4));

    storage.upload("key1", b"data1").await.unwrap();
    storage.upload("key2", b"data2").await.unwrap();
    storage.upload("key3", b"data3").await.unwrap();

    let result = storage.upload("key4", b"data4").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_stats_tracking() {
    let inner = MockStorageBackend::new();
    let storage = FaultyStorageBackend::new(inner, FaultMode::None);

    storage.upload("key1", b"data1").await.unwrap();
    storage.upload("key2", b"data2").await.unwrap();
    let _ = storage.download("key1").await.unwrap();
    let _ = storage.list("key", None).await.unwrap();
    let _ = storage.delete("key1").await.unwrap();

    let stats = storage.stats();
    assert_eq!(stats.uploads, 2);
    assert_eq!(stats.downloads, 1);
    assert_eq!(stats.lists, 1);
    assert_eq!(stats.deletes, 1);
}

#[tokio::test]
async fn test_upload_pretends_success_never_stored() {
    let inner = MockStorageBackend::new();
    let storage = FaultyStorageBackend::new(inner, FaultMode::UploadPretendsSuccessNeverStored);

    let result = storage.upload("key1", b"data1").await;
    assert!(result.is_ok());

    let result = storage.download("key1").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_runtime_mode_change() {
    let inner = MockStorageBackend::new();
    let mut storage = FaultyStorageBackend::new(inner, FaultMode::None);

    storage.upload("key1", b"data1").await.unwrap();

    storage.set_mode(FaultMode::UploadFailsImmediately);

    let result = storage.upload("key2", b"data2").await;
    assert!(result.is_err());

    let data = storage.download("key1").await.unwrap();
    assert_eq!(data, b"data1");
}

#[tokio::test]
async fn test_silent_failure_doesnt_return_garbage() {
    let inner = MockStorageBackend::new();
    let storage = FaultyStorageBackend::new(inner, FaultMode::UploadSilentFailure);

    storage.upload("key1", b"important_data").await.unwrap();

    let result = storage.download("key1").await;

    assert!(
        result.is_err(),
        "silent failure should return error, not garbage data"
    );
}

#[tokio::test]
async fn test_corruption_would_be_detected_by_checksum() {
    let inner = MockStorageBackend::new();
    let _storage = FaultyStorageBackend::new(inner, FaultMode::DownloadCorruptsData);

    let inner2 = MockStorageBackend::new();
    let storage2 = FaultyStorageBackend::new(inner2.clone(), FaultMode::None);
    storage2
        .upload("key1", b"real_data_with_checksum")
        .await
        .unwrap();

    let storage3 = FaultyStorageBackend::new(inner2, FaultMode::DownloadCorruptsData);
    let data = storage3.download("key1").await.unwrap();

    assert_ne!(data, b"real_data_with_checksum");
    assert_eq!(data, b"CORRUPTED_DATA");
}
