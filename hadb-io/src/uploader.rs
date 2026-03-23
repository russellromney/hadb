//! Generic concurrent upload framework.
//!
//! Extracted from walrust's uploader — provides bounded-concurrency JoinSet loop,
//! stats tracking, resume-on-startup, and graceful shutdown drain.
//!
//! Each engine implements `UploadHandler` to define how a single item is uploaded.
//! `ConcurrentUploader<H>` manages the concurrency.
//!
//! ```text
//! Producer Task               ConcurrentUploader (JoinSet, max N concurrent)
//!     |                            |
//!  prepare item                    |
//!     |                            |
//!  tx.send(Upload(id)) -------> rx.recv(id)
//!     |                            |
//!  continue                    spawn handler.upload(id) ──┐
//!                              spawn handler.upload(id) ──┤ (up to max_concurrent)
//!                              spawn handler.upload(id) ──┘
//!                                  |
//!                              reap completed → update stats
//! ```

use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

/// Trait for engine-specific upload logic.
///
/// Implement this for each engine:
/// - walrust: reads LTX from cache, uploads to S3 with retry, marks uploaded
/// - graphstream: reads sealed segment, uploads to S3 with retry
///
/// The handler is responsible for retry logic (via `RetryPolicy`) and
/// error reporting (via `WebhookSender`). `ConcurrentUploader` only
/// manages concurrency and stats.
#[async_trait]
pub trait UploadHandler: Send + Sync + 'static {
    /// The identifier type for items to upload.
    /// e.g., `u64` (TXID) for walrust, `String` (segment name) for graphstream.
    type Id: Clone + Send + std::fmt::Display + std::fmt::Debug + 'static;

    /// Upload a single item. Returns bytes uploaded on success.
    ///
    /// This method should handle retries internally (e.g., via `RetryPolicy::execute`).
    /// Errors returned here are treated as final failures.
    async fn upload(&self, id: Self::Id) -> Result<u64>;

    /// List pending items that need upload (for resume on startup).
    /// Called once when `run()` starts.
    fn pending_items(&self) -> Vec<Self::Id>;

    /// Name for logging (e.g., database name).
    fn name(&self) -> &str;
}

/// Message sent to the uploader task.
#[derive(Debug, Clone)]
pub enum UploadMessage<Id> {
    /// Upload an item with this ID.
    Upload(Id),
    /// Graceful shutdown: complete in-flight uploads, then exit.
    Shutdown,
}

/// Upload statistics.
#[derive(Debug, Clone, Default)]
pub struct UploaderStats {
    pub uploads_attempted: u64,
    pub uploads_succeeded: u64,
    pub uploads_failed: u64,
    pub bytes_uploaded: u64,
}

/// Generic concurrent uploader.
///
/// Manages a `JoinSet` of upload tasks bounded by `max_concurrent`.
/// Resumes pending uploads on startup. Drains in-flight on shutdown.
pub struct ConcurrentUploader<H: UploadHandler> {
    handler: Arc<H>,
    max_concurrent: usize,
    stats: Arc<tokio::sync::Mutex<UploaderStats>>,
}

impl<H: UploadHandler> ConcurrentUploader<H> {
    /// Create a new concurrent uploader.
    pub fn new(handler: Arc<H>, max_concurrent: usize) -> Self {
        Self {
            handler,
            max_concurrent: max_concurrent.max(1),
            stats: Arc::new(tokio::sync::Mutex::new(UploaderStats::default())),
        }
    }

    /// Run the uploader loop. Blocks until Shutdown or channel close.
    pub async fn run(
        &self,
        mut rx: mpsc::Receiver<UploadMessage<H::Id>>,
    ) -> Result<UploaderStats> {
        tracing::info!(
            "[{}] Uploader started (max_concurrent={})",
            self.handler.name(),
            self.max_concurrent
        );

        let mut in_flight: JoinSet<(H::Id, Result<u64>)> = JoinSet::new();

        // Resume pending uploads on startup
        let pending = self.handler.pending_items();
        if !pending.is_empty() {
            tracing::info!(
                "[{}] Resuming {} pending uploads",
                self.handler.name(),
                pending.len()
            );
            for id in pending {
                while in_flight.len() >= self.max_concurrent {
                    if let Some(result) = in_flight.join_next().await {
                        self.handle_join_result(result).await;
                    }
                }
                let handler = self.handler.clone();
                let id_clone = id.clone();
                in_flight.spawn(async move {
                    let result = handler.upload(id_clone.clone()).await;
                    (id_clone, result)
                });
            }
        }

        // Main message loop
        loop {
            tokio::select! {
                // Accept new uploads when under concurrency limit
                msg = rx.recv(), if in_flight.len() < self.max_concurrent => {
                    match msg {
                        Some(UploadMessage::Upload(id)) => {
                            let handler = self.handler.clone();
                            let id_clone = id.clone();
                            in_flight.spawn(async move {
                                let result = handler.upload(id_clone.clone()).await;
                                (id_clone, result)
                            });
                        }
                        Some(UploadMessage::Shutdown) => {
                            tracing::info!(
                                "[{}] Shutdown signal, draining {} in-flight",
                                self.handler.name(),
                                in_flight.len()
                            );
                            while let Some(result) = in_flight.join_next().await {
                                self.handle_join_result(result).await;
                            }
                            break;
                        }
                        None => {
                            tracing::info!(
                                "[{}] Channel closed, draining {} in-flight",
                                self.handler.name(),
                                in_flight.len()
                            );
                            while let Some(result) = in_flight.join_next().await {
                                self.handle_join_result(result).await;
                            }
                            break;
                        }
                    }
                }
                // Reap completed uploads
                Some(result) = in_flight.join_next() => {
                    self.handle_join_result(result).await;
                }
            }
        }

        let stats = self.stats.lock().await.clone();
        tracing::info!(
            "[{}] Uploader stopped. Stats: {:?}",
            self.handler.name(),
            stats
        );
        Ok(stats)
    }

    /// Handle a completed upload task.
    async fn handle_join_result(
        &self,
        result: Result<(H::Id, Result<u64>), tokio::task::JoinError>,
    ) {
        match result {
            Ok((id, Ok(bytes))) => {
                let mut stats = self.stats.lock().await;
                stats.uploads_attempted += 1;
                stats.uploads_succeeded += 1;
                stats.bytes_uploaded += bytes;
                tracing::debug!("[{}] Uploaded {} ({} bytes)", self.handler.name(), id, bytes);
            }
            Ok((id, Err(e))) => {
                let mut stats = self.stats.lock().await;
                stats.uploads_attempted += 1;
                stats.uploads_failed += 1;
                tracing::error!("[{}] Upload failed for {}: {}", self.handler.name(), id, e);
            }
            Err(e) => {
                let mut stats = self.stats.lock().await;
                stats.uploads_attempted += 1;
                stats.uploads_failed += 1;
                tracing::error!("[{}] Upload task panicked: {}", self.handler.name(), e);
            }
        }
    }

    /// Get current upload statistics.
    pub async fn stats(&self) -> UploaderStats {
        self.stats.lock().await.clone()
    }
}

/// Spawn an uploader task and return the channel sender + join handle.
///
/// The JoinHandle allows callers to await completion after sending Shutdown,
/// ensuring in-flight uploads finish before the runtime exits.
///
/// ```ignore
/// let (tx, handle) = spawn_uploader(uploader);
/// // ... send uploads ...
/// tx.send(UploadMessage::Shutdown).await;
/// match tokio::time::timeout(Duration::from_secs(10), handle).await {
///     Ok(Ok(())) => debug!("drained"),
///     Ok(Err(e)) => error!("panicked: {}", e),
///     Err(_) => warn!("drain timed out"),
/// }
/// ```
pub fn spawn_uploader<H: UploadHandler>(
    uploader: Arc<ConcurrentUploader<H>>,
) -> (
    mpsc::Sender<UploadMessage<H::Id>>,
    tokio::task::JoinHandle<()>,
) {
    let (tx, rx) = mpsc::channel(1000);

    let handle = tokio::spawn(async move {
        if let Err(e) = uploader.run(rx).await {
            tracing::error!("Uploader task failed: {}", e);
        }
    });

    (tx, handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::Mutex;
    use tokio::time::{timeout, Duration};

    /// Test upload handler that tracks uploads in memory.
    struct MockHandler {
        name: String,
        pending: Mutex<Vec<u64>>,
        uploaded: Mutex<HashSet<u64>>,
        upload_delay: Option<Duration>,
        fail_ids: Mutex<HashSet<u64>>,
        /// Track peak concurrent uploads
        active: AtomicUsize,
        peak_concurrent: AtomicUsize,
        upload_count: AtomicU64,
    }

    impl MockHandler {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                pending: Mutex::new(vec![]),
                uploaded: Mutex::new(HashSet::new()),
                upload_delay: None,
                fail_ids: Mutex::new(HashSet::new()),
                active: AtomicUsize::new(0),
                peak_concurrent: AtomicUsize::new(0),
                upload_count: AtomicU64::new(0),
            }
        }

        fn with_pending(mut self, pending: Vec<u64>) -> Self {
            self.pending = Mutex::new(pending);
            self
        }

        fn with_delay(mut self, delay: Duration) -> Self {
            self.upload_delay = Some(delay);
            self
        }

        fn with_fail_ids(mut self, ids: HashSet<u64>) -> Self {
            self.fail_ids = Mutex::new(ids);
            self
        }

        fn uploaded_ids(&self) -> HashSet<u64> {
            self.uploaded.lock().unwrap().clone()
        }

        fn peak_concurrent(&self) -> usize {
            self.peak_concurrent.load(Ordering::SeqCst)
        }

        fn upload_count(&self) -> u64 {
            self.upload_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl UploadHandler for MockHandler {
        type Id = u64;

        async fn upload(&self, id: u64) -> Result<u64> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak_concurrent.fetch_max(active, Ordering::SeqCst);

            if let Some(delay) = self.upload_delay {
                tokio::time::sleep(delay).await;
            }

            self.active.fetch_sub(1, Ordering::SeqCst);
            self.upload_count.fetch_add(1, Ordering::SeqCst);

            if self.fail_ids.lock().unwrap().contains(&id) {
                return Err(anyhow::anyhow!("Simulated failure for id {}", id));
            }

            self.uploaded.lock().unwrap().insert(id);
            Ok(100) // 100 bytes per upload
        }

        fn pending_items(&self) -> Vec<u64> {
            self.pending.lock().unwrap().clone()
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    #[tokio::test]
    async fn test_basic_upload() {
        let handler = Arc::new(MockHandler::new("test"));
        let uploader = Arc::new(ConcurrentUploader::new(handler.clone(), 4));

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        tx.send(UploadMessage::Upload(1)).await.unwrap();
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = task.await.unwrap().unwrap();
        assert_eq!(stats.uploads_succeeded, 1);
        assert_eq!(stats.uploads_failed, 0);
        assert_eq!(stats.bytes_uploaded, 100);
        assert!(handler.uploaded_ids().contains(&1));
    }

    #[tokio::test]
    async fn test_multiple_uploads() {
        let handler = Arc::new(MockHandler::new("test"));
        let uploader = Arc::new(ConcurrentUploader::new(handler.clone(), 4));

        let (tx, rx) = mpsc::channel(20);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        for i in 1..=10 {
            tx.send(UploadMessage::Upload(i)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = task.await.unwrap().unwrap();
        assert_eq!(stats.uploads_succeeded, 10);
        assert_eq!(stats.bytes_uploaded, 1000);
        assert_eq!(handler.uploaded_ids().len(), 10);
    }

    #[tokio::test]
    async fn test_resume_pending() {
        let handler = Arc::new(MockHandler::new("test").with_pending(vec![1, 2, 3]));
        let uploader = Arc::new(ConcurrentUploader::new(handler.clone(), 4));

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        // Give time for pending to process
        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = task.await.unwrap().unwrap();
        assert_eq!(stats.uploads_succeeded, 3);
        assert!(handler.uploaded_ids().contains(&1));
        assert!(handler.uploaded_ids().contains(&2));
        assert!(handler.uploaded_ids().contains(&3));
    }

    #[tokio::test]
    async fn test_concurrent_respects_limit() {
        let handler = Arc::new(
            MockHandler::new("test").with_delay(Duration::from_millis(50)),
        );
        let uploader = Arc::new(ConcurrentUploader::new(handler.clone(), 3));

        let (tx, rx) = mpsc::channel(20);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        for i in 1..=10 {
            tx.send(UploadMessage::Upload(i)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = timeout(Duration::from_secs(5), task)
            .await
            .expect("should complete")
            .unwrap()
            .unwrap();

        assert_eq!(stats.uploads_succeeded, 10);
        // Peak concurrent should be at most 3
        assert!(
            handler.peak_concurrent() <= 3,
            "peak concurrent was {}, expected <= 3",
            handler.peak_concurrent()
        );
        // But should actually use concurrency (peak > 1)
        assert!(
            handler.peak_concurrent() > 1,
            "peak concurrent was {}, expected > 1 (should use concurrency)",
            handler.peak_concurrent()
        );
    }

    #[tokio::test]
    async fn test_failure_doesnt_block_others() {
        let mut fail_ids = HashSet::new();
        fail_ids.insert(3);
        fail_ids.insert(7);

        let handler = Arc::new(MockHandler::new("test").with_fail_ids(fail_ids));
        let uploader = Arc::new(ConcurrentUploader::new(handler.clone(), 4));

        let (tx, rx) = mpsc::channel(20);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        for i in 1..=10 {
            tx.send(UploadMessage::Upload(i)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = task.await.unwrap().unwrap();
        assert_eq!(stats.uploads_succeeded, 8);
        assert_eq!(stats.uploads_failed, 2);
        assert!(!handler.uploaded_ids().contains(&3));
        assert!(!handler.uploaded_ids().contains(&7));
        assert!(handler.uploaded_ids().contains(&1));
        assert!(handler.uploaded_ids().contains(&10));
    }

    #[tokio::test]
    async fn test_graceful_shutdown_drains() {
        let handler = Arc::new(
            MockHandler::new("test").with_delay(Duration::from_millis(100)),
        );
        let uploader = Arc::new(ConcurrentUploader::new(handler.clone(), 4));

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        // Send uploads
        for i in 1..=4 {
            tx.send(UploadMessage::Upload(i)).await.unwrap();
        }
        // Small delay to let them start
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Shutdown — should wait for in-flight to complete
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = timeout(Duration::from_secs(5), task)
            .await
            .expect("should complete within timeout")
            .unwrap()
            .unwrap();

        // All 4 should have completed (drained)
        assert_eq!(stats.uploads_succeeded, 4);
    }

    #[tokio::test]
    async fn test_channel_close_drains() {
        let handler = Arc::new(
            MockHandler::new("test").with_delay(Duration::from_millis(50)),
        );
        let uploader = Arc::new(ConcurrentUploader::new(handler.clone(), 4));

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        for i in 1..=3 {
            tx.send(UploadMessage::Upload(i)).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(tx); // Close channel instead of Shutdown

        let stats = timeout(Duration::from_secs(5), task)
            .await
            .expect("should complete")
            .unwrap()
            .unwrap();

        assert_eq!(stats.uploads_succeeded, 3);
    }

    #[tokio::test]
    async fn test_spawn_uploader_helper() {
        let handler = Arc::new(MockHandler::new("test"));
        let uploader = Arc::new(ConcurrentUploader::new(handler.clone(), 4));

        let (tx, handle) = spawn_uploader(uploader);

        tx.send(UploadMessage::Upload(1)).await.unwrap();
        tx.send(UploadMessage::Upload(2)).await.unwrap();
        tx.send(UploadMessage::Shutdown).await.unwrap();

        handle.await.unwrap();
        assert_eq!(handler.upload_count(), 2);
    }

    #[tokio::test]
    async fn test_stats_tracking() {
        let handler = Arc::new(MockHandler::new("test"));
        let uploader = Arc::new(ConcurrentUploader::new(handler.clone(), 4));

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        tokio::spawn(async move { uploader_clone.run(rx).await });

        tx.send(UploadMessage::Upload(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let stats = uploader.stats().await;
        assert_eq!(stats.uploads_attempted, 1);
        assert_eq!(stats.bytes_uploaded, 100);

        tx.send(UploadMessage::Upload(2)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let stats = uploader.stats().await;
        assert_eq!(stats.uploads_attempted, 2);
        assert_eq!(stats.bytes_uploaded, 200);

        tx.send(UploadMessage::Shutdown).await.unwrap();
    }

    #[tokio::test]
    async fn test_max_concurrent_one() {
        // Edge case: max_concurrent=1 should still work (sequential)
        let handler = Arc::new(
            MockHandler::new("test").with_delay(Duration::from_millis(20)),
        );
        let uploader = Arc::new(ConcurrentUploader::new(handler.clone(), 1));

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        for i in 1..=5 {
            tx.send(UploadMessage::Upload(i)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = timeout(Duration::from_secs(5), task)
            .await
            .expect("should complete")
            .unwrap()
            .unwrap();

        assert_eq!(stats.uploads_succeeded, 5);
        assert_eq!(handler.peak_concurrent(), 1);
    }

    #[tokio::test]
    async fn test_zero_concurrency_defaults_to_one() {
        let handler = Arc::new(MockHandler::new("test"));
        let uploader = Arc::new(ConcurrentUploader::new(handler.clone(), 0));

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        tx.send(UploadMessage::Upload(1)).await.unwrap();
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = task.await.unwrap().unwrap();
        assert_eq!(stats.uploads_succeeded, 1);
    }

    #[tokio::test]
    async fn test_empty_shutdown() {
        let handler = Arc::new(MockHandler::new("test"));
        let uploader = Arc::new(ConcurrentUploader::new(handler.clone(), 4));

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = task.await.unwrap().unwrap();
        assert_eq!(stats.uploads_attempted, 0);
    }
}
