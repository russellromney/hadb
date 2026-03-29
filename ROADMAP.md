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

## Future: Separating Replication Concerns

Today, haqlite bundles everything: lease management, WAL sync, snapshots, restore. The future architecture separates these into distinct, composable concerns behind hadb traits.

### LeaseStore (today: S3, future: NATS/Redis/etcd/Consul/DynamoDB/Postgres)

Already abstracted. `LeaseStore` trait with S3 and in-memory implementations. Next implementation: **NATS JetStream KV** (2-5ms CAS vs S3's 50-200ms). Single NATS node first (~$2/month), cluster for HA when needed. S3 charges per request ($17/month at 1000 databases polling every 2s); NATS has zero per-request cost. See `hadb-nats-lease` and other variants in README.

### ReplicationTransport (future: Kafka/Redpanda)

New trait for real-time WAL frame delivery. Today, followers poll S3 every 1-10s (1-10s RPO). With a `ReplicationTransport`, the leader publishes WAL frames to a durable stream (Redpanda topic `wal.{db_name}`), and followers consume and apply immediately (~5ms behind leader). RPO drops to near-zero.

The write path changes: write arrives at engine, engine publishes WAL frame to Redpanda (durable ack across 3 Raft nodes, 2-5ms), then engine applies to local SQLite. The write is durable in Redpanda before SQLite touches it. If the leader crashes after Redpanda ack but before SQLite apply, followers already have the write from the stream.

NATS JetStream works for small scale but doesn't handle thousands of topics well. Redpanda (single binary, Kafka-compatible) is the right choice at scale.

```rust
// Not designed yet, conceptual
#[async_trait]
pub trait ReplicationTransport: Send + Sync {
    async fn publish_wal_frame(&self, db_name: &str, frame: &[u8]) -> Result<u64>; // returns offset
    async fn subscribe(&self, db_name: &str, from_offset: u64) -> Result<FrameStream>;
    async fn latest_offset(&self, db_name: &str) -> Result<u64>;
}
```

### ReplicationCompactor (future: batch writer)

A consumer that reads WAL frames from the hot stream (Redpanda), batches and compacts them, and uploads to S3 as the cold archive. Decouples real-time replication speed from durable archival.

- Redpanda retention stays short (24h). S3 holds the full compacted history.
- Tracks last-compacted Kafka offset per database.
- Runs as a background task in the engine or as a standalone process.
- `StorageBackend` (S3) role changes from "primary replication target" to "cold archive written by the compactor." Leader no longer uploads WAL frames to S3 directly.

### Restore (two-phase)

Restore becomes: fetch latest S3 snapshot (compacted history), then replay Redpanda from the snapshot's Kafka offset forward (recent uncompacted frames). No gaps. If Redpanda retention has expired for older data, S3 has it.

### CDC for free

Any consumer subscribes to `wal.{db_name}` on Redpanda and gets every write in real-time. That's `hadb-stream` with zero additional code. Webhooks, event sourcing, cross-region fanout.

### What this means for the dependency graph

```
hadb                    -- core traits (LeaseStore, Replicator, Executor, StorageBackend)
                           + new: ReplicationTransport trait

hadb-io                 -- shared S3/retry/upload infrastructure (Phase 1)

hadb-lease-s3           -- S3 LeaseStore (today)
hadb-lease-nats         -- NATS JetStream KV LeaseStore (next)
hadb-lease-redis        -- Redis LeaseStore (future)
hadb-lease-etcd         -- etcd LeaseStore (future)
hadb-lease-consul       -- Consul LeaseStore (future)
hadb-lease-dynamo       -- DynamoDB LeaseStore (future)
hadb-lease-pg           -- PostgreSQL LeaseStore (future)

hadb-transport-redpanda -- Redpanda/Kafka ReplicationTransport (future)

hadb-stream             -- CDC consumer on ReplicationTransport (future)
```

### Build order

None of this is being built now. The order when it matters:

1. **NATS lease store** -- biggest bang for buck. Faster failover, zero per-request cost.
2. **Redpanda ReplicationTransport** -- when zero-RPO matters to paying customers.
3. **ReplicationCompactor** -- required alongside #2 to keep S3 as cold archive.
4. **hadb-stream (CDC)** -- falls out for free once #2 exists.
5. **Other lease stores** -- on demand based on deployment environments.

---

## Phase Beacon: Follower Readiness in Coordinator

> After: Phase 1 · Before: Phase 2

**Atomicity: Phase Beacon MUST land simultaneously with hakuzu Phase Parity and haqlite Phase Rampart-e.** Beacon-c changes the `FollowerBehavior` trait signature, which breaks all implementors. All three repos update in one coordinated change.

Every hadb database (haqlite, hakuzu, haduck) needs follower readiness: "has this follower replayed all available data?" Currently hakuzu reimplements this with `Arc<AtomicBool>` + `Arc<AtomicU64>` shared between `KuzuFollowerBehavior` and `HaKuzuInner` (`hakuzu/src/follower_behavior.rs:35-37`, `hakuzu/src/database.rs:354-357`). haqlite has no readiness tracking at all. Pushing this into hadb means every database gets it for free.

### Beacon-a: Add `caught_up` to coordinator per-database state

The coordinator already creates `shared_position: Arc<AtomicU64>` (`coordinator.rs:345`) and passes it to `FollowerBehavior::run_follower_loop()` (`follower.rs:52`). Add `caught_up: Arc<AtomicBool>` alongside it, same pattern.

- Add `caught_up: Arc<AtomicBool>` field next to `follower_position` in `FollowerState` (coordinator.rs)
- Pass to `run_follower_loop()` as new parameter (update `FollowerBehavior` trait in `follower.rs:45-55`)
- Two writers to the same atomic:
  - **Coordinator** sets `caught_up = true` on Promoted, `false` on Demoted/Fenced (extract from `hakuzu/src/database.rs:1030,1060,1067`)
  - **Follower behavior** sets `caught_up` during poll loop (true when no new data, false when new data arrives, true after successful replay). This is the same pattern as `position`, which both coordinator and follower behavior already write to.
- Change `Coordinator::join()` return type from `Result<Role>` to `Result<JoinResult>`:
  ```rust
  pub struct JoinResult {
      pub role: Role,
      pub caught_up: Arc<AtomicBool>,
      pub position: Arc<AtomicU64>,
  }
  ```
  Database layers cache these Arc refs in their inner struct for zero-overhead health checks (single atomic load) instead of locking the coordinator's `HashMap<String, DbEntry>` RwLock on every read. This avoids a performance regression on the health check hot path.

Source: `hakuzu/src/database.rs:682-693` (is_caught_up/replay_position), `hakuzu/src/follower_behavior.rs:112,116,156` (caught_up state transitions)

### Beacon-b: Add readiness gauges to HaMetrics

Extract from `hakuzu/src/database.rs:703-717` (prometheus_metrics readiness section). Add two gauges to `HaMetrics` (`metrics.rs`):

- `follower_caught_up: AtomicU64` (1 = caught up, 0 = behind)
- `follower_replay_position: AtomicU64`
- Add to `MetricsSnapshot` and `to_prometheus()` output
- Coordinator updates these atomics when `caught_up` / `position` change

Caveat: `HaMetrics` is a single global struct per coordinator. With multiple databases on one coordinator, these gauges report last-written values. Per-database prometheus labels would require per-db metrics structs, which is out of scope. This matches the existing limitation on `follower_pulls_succeeded/failed/no_new_data` counters, which are already global.

Source: `hakuzu/src/database.rs:703-717`

### Beacon-c: Update FollowerBehavior trait (BREAKING)

Add `caught_up: Arc<AtomicBool>` parameter to `run_follower_loop()` signature (`follower.rs:45-55`). This is a **breaking trait change** requiring simultaneous updates to all implementors:

- `hakuzu/src/follower_behavior.rs` — delete own `caught_up` field, repoint `self.caught_up.store(...)` calls to use the trait parameter `caught_up.store(...)` instead. The stores are not deleted, they are repointed from a self-owned atomic to the coordinator-owned atomic.
- `haqlite/src/follower_behavior.rs` — add `caught_up.store(true/false)` at the same points as hakuzu (empty poll = true, new data downloaded = false, replay success = true). Currently has no readiness tracking at all.

Source: `hakuzu/src/follower_behavior.rs:109-157` (the complete caught_up state machine to replicate in haqlite and to repoint in hakuzu)

### Beacon-d: Tests

- Coordinator test: follower starts not-caught-up, becomes caught-up after successful pull
- Coordinator test: promoted node is always caught-up
- Coordinator test: demoted/fenced node is not-caught-up
- Coordinator test: `JoinResult` contains valid Arc refs that reflect follower state changes
- Metrics test: readiness gauges appear in prometheus output
- Update existing coordinator_test.rs MockFollowerBehavior to accept new `caught_up` param
- Update all callers of `Coordinator::join()` to destructure `JoinResult` instead of bare `Role`

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
