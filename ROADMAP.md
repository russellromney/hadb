# hadb Roadmap

## Phase 1: hadb-io — Shared S3/Retry/Upload Infrastructure

### Problem

walrust-core and graphstream independently implement the same S3 operations infrastructure. graphstream's `retry.rs` (679 lines) was literally copy-pasted from walrust-core's `retry.rs` (642 lines) and has already drifted (different rand API, missing webhook support, added `consecutive_failures()` helper). Every improvement (concurrent uploads, shutdown drain, cache cleanup) must be manually ported between them. Future database implementations (haduck, harock) would each copy-paste again.

### What moves to hadb-io

**Retry + Circuit Breaker** (~650 lines, 95% duplicated today)
- `RetryConfig` — max_retries, base_delay_ms, max_delay_ms, circuit breaker thresholds
- `ErrorKind` — Transient, ClientError, AuthError, NotFound, Unknown
- `classify_error()` — maps anyhow/S3 SDK errors to ErrorKind
- `is_retryable()` — convenience wrapper
- `CircuitBreaker` — atomic consecutive failure tracking, cooldown, state (Closed/Open/HalfOpen)
- `RetryPolicy` — exponential backoff with full jitter, circuit breaker integration, `execute()` async retry loop
- Reconcile walrust's webhook-aware CircuitBreaker with graphstream's simpler version: make webhook notification a callback (`on_circuit_open: Option<Box<dyn Fn() + Send + Sync>>`) instead of baking in WebhookSender

Source: `walrust-core/src/retry.rs`, `graphstream/src/retry.rs`

**S3 Client + Helpers** (~470 lines, walrust-only today — graphstream calls SDK directly)
- `parse_bucket(s: &str) -> (String, String)` — parse `s3://bucket/prefix` URLs
- `create_client(endpoint: Option<&str>) -> Client` — HTTP pool tuning, endpoint override
- `upload_bytes()`, `download_bytes()`, `upload_file()`, `download_file()`
- `list_objects()`, `list_objects_after()`
- `exists()` — HeadObject with proper error handling
- `delete_object()`, `delete_objects()` — batch delete in 1000-key chunks
- `upload_bytes_with_checksum()`, `get_checksum()` — integrity metadata

Source: `walrust-core/src/s3.rs`, `walrust/src/s3.rs`

**ObjectStore Trait + S3Backend** (~180 lines)

Three `StorageBackend` traits exist today:
- `hadb::StorageBackend` — minimal (upload, download, list, delete). Used for coordination.
- `walrust_core::StorageBackend` — rich (13 methods: upload_bytes, upload_file, checksums, exists, batch delete). Used for replication data.
- `hadb_s3::S3StorageBackend` — implements the minimal hadb trait.

Reconciliation:
- `hadb::StorageBackend` stays minimal — it's the abstract trait for coordination data. Correct as-is.
- `hadb_io::ObjectStore` — the rich trait from walrust-core, for bulk replication data. Named differently to avoid confusion with hadb's minimal trait.
- `hadb_io::S3Backend` — implements ObjectStore. Replaces `walrust_core::S3Backend`.
- `hadb_s3::S3StorageBackend` gets absorbed into hadb-io (it's a subset of S3Backend).
- graphstream switches from direct `aws_sdk_s3::Client` calls to `hadb_io::ObjectStore` trait — gains testability (MockStorage) for free.

Source: `walrust-core/src/storage.rs`, `hadb-s3/src/storage.rs`

**Split hadb-s3 into hadb-lease-s3 + hadb-io**

`hadb-s3` currently bundles two unrelated concerns:
- `lease_store.rs` — CAS via S3 conditional PUT (ETag). Leader election. Small JSON blobs.
- `storage.rs` — Bulk data upload/download. Replication segments.

These serve different consumers. A `hadb-lease-redis` still needs S3 for bulk storage. And hadb-io's `ObjectStore` is a richer version of what `hadb-s3/storage.rs` does.

After split:
- **`hadb-lease-s3`** — S3LeaseStore only. Implements `hadb::LeaseStore`. ~150 lines. Pure coordination.
- **`hadb-io`** — absorbs `hadb-s3/storage.rs` into its `ObjectStore` + `S3Backend`. Bulk data operations.
- **`hadb-s3`** — deleted. Its two files are split between hadb-lease-s3 and hadb-io.

Future lease store implementations:
- `hadb-lease-redis` — Redis CAS (<1ms vs S3's ~100ms). Fast failover detection.
- `hadb-lease-consul` — Consul sessions. For existing Consul deployments.
- `hadb-lease-etcd` — etcd lease. For Kubernetes-native deployments.

**Concurrent Uploader Framework** (~300 lines, new — based on walrust v0.6.0)

Generic concurrent upload infrastructure. Engine-specific uploaders compose this instead of reimplementing JoinSet boilerplate.

```rust
pub struct ConcurrentUploader<T: UploadItem> {
    max_concurrent: usize,
    // ... JoinSet, stats, shutdown
}

pub trait UploadItem: Send + 'static {
    type Context: Clone + Send + Sync + 'static;
    async fn upload(self, ctx: &Self::Context) -> Result<UploadResult>;
}
```

walrust's `Uploader` becomes: `ConcurrentUploader<LtxUploadItem>` with `LtxUploadItem` implementing `UploadItem`.
graphstream's `spawn_journal_uploader` becomes: `ConcurrentUploader<SegmentUploadItem>`.

Includes:
- Bounded concurrency via `tokio::select!` + `JoinSet` + conditional guard
- `spawn_uploader()` returning `(Sender, JoinHandle)` — shutdown drain discipline
- Resume-on-startup pattern (scan pending items)
- Stats tracking (attempted, succeeded, failed, bytes)

Source: `walrust/src/uploader.rs` (extract generic parts)

**Webhook Notifications** (~290 lines)
- `WebhookConfig` — url, events filter, HMAC secret
- `WebhookSender` — async HTTP POST with HMAC-SHA256 signing
- `WebhookEvent` — enum of event types (generic: circuit_breaker_open, upload_failed, auth_failure)
- Engine-specific events (corruption_detected, sync_failed) stay in each product

Source: `walrust/src/webhook.rs`

**GFS Retention Policy** (~550 lines)
- `RetentionPolicy` — hourly, daily, weekly, monthly tier counts
- `SnapshotEntry` — generic `{ key: String, created_at: DateTime, size: u64 }`
- `select_snapshots_to_delete()` — GFS algorithm, returns keys to remove
- Engine-agnostic: operates on (key, timestamp, size) tuples. Works for LTX snapshots, .graphj segments, Kuzu tarballs, anything.

Source: `walrust/src/retention.rs`

**Shared Config Types** (~100 lines, subset of walrust's config.rs)
- `S3Config` — bucket, endpoint, region
- `RetryConfig` (re-exported from retry module)
- `WebhookConfig` (re-exported from webhook module)
- `CacheConfig` — enabled, retention_duration, max_size, uploader_concurrency
- `parse_duration_string()` — "1h", "30m", "7d" parser

Source: `walrust/src/config.rs` (extract generic parts)

### What stays engine-specific

**walrust-core** keeps:
- `ltx.rs` — LTX format encode/decode, checksums, chain hashing
- `wal.rs` — SQLite WAL header/frame parsing, checkpoint detection
- `shadow.rs` — Shadow WAL manager
- `sync.rs` — WAL sync orchestration (encode frames → LTX → upload)
- `replicator.rs` — hadb::Replicator impl for SQLite

**graphstream** keeps:
- `graphj.rs` — .graphj segment format (128-byte header, compression, encryption)
- `journal.rs` — Journal writer, segment rotation, sealing, entry serialization
- `sync.rs` — Segment download, filename parsing, recovery state
- `uploader.rs` — Becomes thin wrapper: seal trigger + `ConcurrentUploader<SegmentUploadItem>`
- `types.rs` — ParamValue, protobuf types

### Dependency graph after extraction

```
hadb                 — traits only (LeaseStore, Replicator, Executor, StorageBackend)

hadb-io (NEW)        — shared infrastructure
├── retry, circuit breaker
├── S3 client + helpers
├── ObjectStore trait + S3Backend (absorbs hadb-s3/storage.rs)
├── ConcurrentUploader<T>
├── webhook
├── retention
└── shared config types

hadb-lease-s3 (renamed from hadb-s3, lease_store.rs only)
├── S3LeaseStore (implements hadb::LeaseStore)
└── depends on hadb

walrust-core (slimmed)          graphstream (slimmed)
├── ltx.rs                      ├── graphj.rs
├── wal.rs                      ├── journal.rs
├── shadow.rs                   ├── sync.rs (download/recovery)
├── sync.rs                     ├── types.rs
├── replicator.rs               └── depends on hadb-io
└── depends on hadb-io

haqlite                         hakuzu
├── database.rs                 ├── database.rs
├── forwarding.rs               ├── forwarding.rs
├── depends on walrust-core     ├── depends on graphstream
├── depends on hadb             ├── depends on hadb
└── depends on hadb-lease-s3    └── depends on hadb-lease-s3
```

### Workspace changes

`hadb/Cargo.toml` workspace members:
```toml
[workspace]
members = ["hadb", "hadb-io", "hadb-lease-s3"]
```

`hadb-s3` is deleted — `lease_store.rs` moves to `hadb-lease-s3`, `storage.rs` is absorbed into `hadb-io`.

### Migration steps

**Phase 1a: Create hadb-io + split hadb-s3 (hadb workspace)**
1. Create `hadb-io/` crate in hadb workspace
2. Move retry.rs from walrust-core → hadb-io (reconcile walrust/graphstream differences)
3. Move s3.rs from walrust-core → hadb-io
4. Move storage.rs from walrust-core → hadb-io (rename trait to ObjectStore)
5. Absorb `hadb-s3/storage.rs` into hadb-io S3Backend
6. Rename `hadb-s3/` → `hadb-lease-s3/` (keep only lease_store.rs + error.rs)
7. Update workspace Cargo.toml: members = ["hadb", "hadb-io", "hadb-lease-s3"]
8. Move webhook.rs from walrust → hadb-io (generalize events)
9. Move retention.rs from walrust → hadb-io (already generic)
10. Extract shared config types from walrust config.rs → hadb-io
11. Build ConcurrentUploader<T> in hadb-io (generalize from walrust uploader)
12. All hadb workspace tests pass

**Phase 1b: Update walrust-core + walrust CLI**
13. Update walrust-core: `use hadb_io::{RetryPolicy, ObjectStore, S3Backend}`
14. Delete walrust-core's retry.rs, s3.rs, storage.rs
15. Update walrust CLI: use hadb-io webhook/retention
16. Delete walrust's webhook.rs, retention.rs
17. Update walrust uploader to use ConcurrentUploader<LtxUploadItem>
18. All walrust tests pass (unit + S3 integration)

**Phase 1c: Update graphstream**
19. Update graphstream: replace own retry.rs with `use hadb_io::RetryPolicy`
20. Replace direct aws_sdk_s3 calls with `hadb_io::ObjectStore` trait
21. Update uploader to use ConcurrentUploader<SegmentUploadItem>
22. Delete graphstream's retry.rs
23. All graphstream tests pass

**Phase 1d: Update haqlite + hakuzu**
24. Update haqlite: depend on hadb-lease-s3 instead of hadb-s3
25. Update hakuzu: same
26. All integration/e2e tests pass

### Verification

```bash
# hadb workspace (hadb + hadb-io + hadb-lease-s3)
cd ~/Documents/Github/hadb
~/.cargo/bin/cargo check && ~/.cargo/bin/cargo test && ~/.cargo/bin/cargo clippy

# walrust (depends on hadb-io via path)
cd ~/Documents/Github/personal-website/walrust
~/.cargo/bin/cargo check && ~/.cargo/bin/cargo test && ~/.cargo/bin/cargo clippy

# graphstream (depends on hadb-io via path)
cd ~/Documents/Github/graphstream
~/.cargo/bin/cargo check && ~/.cargo/bin/cargo test && ~/.cargo/bin/cargo clippy

# haqlite (transitive dep on hadb-io via walrust-core)
cd ~/Documents/Github/hakuzu
~/.cargo/bin/cargo test

# S3 integration tests (all repos)
~/.soup/bin/soup run -p walrust -e development -- ~/.cargo/bin/cargo test -- s3
~/.soup/bin/soup run -p walrust -e development -- bash -c "cd ~/Documents/Github/hadb && ~/.cargo/bin/cargo test"
```

### New tests for hadb-io

- Retry: all existing tests from both walrust-core and graphstream (merge, deduplicate)
- CircuitBreaker: webhook callback fires on open, resets on success
- ConcurrentUploader: bounded concurrency, shutdown drain, resume pending, failure isolation
- ObjectStore + S3Backend: upload/download/list/exists/delete round-trips
- MockStorage: configurable latency, failure injection, concurrency tracking
- Retention: GFS selection across all tier combinations
- Webhook: HMAC signing, event filtering, timeout handling

---

## Phase 2: haqlite CLI

After hadb-io extraction, haqlite gets a full CLI. See haqlite/ROADMAP.md.

Commands: `serve` (watch + HA + wire protocol), `restore`, `list`, `verify`, `compact`, `replicate`, `explain`.

## Phase 3: hakuzu CLI

Same command surface as haqlite (minus watch — graph DBs require active query interception).

Commands: `serve` (Bolt protocol + HA), `restore`, `list`, `verify`, `compact`, `replicate`.

## Phase 4: Multi-language SDKs

FFI layer (haqlite-ffi, hakuzu-ffi) + language bindings (Python via PyO3, Node via napi-rs, Go via CGO).

Depends on Phase 1 (stable hadb-io) and Phase 2-3 (proven CLI surface).
