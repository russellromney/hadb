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

## Phase Volt: NATS JetStream KV Lease Store

> After: Phase 1 · Before: Phase Beacon

NATS JetStream KV as an alternative LeaseStore implementation. S3 leases are 50-200ms per operation and cost $17/month at 1000 databases polling every 2s. NATS KV is 2-5ms per operation with zero per-request cost on a single $2/month Fly machine.

### Volt-a: hadb-lease-nats crate (DONE)

New crate `hadb-lease-nats/` in the hadb workspace. Implements `LeaseStore` trait using NATS JetStream KV.

- `NatsLeaseStore::connect(url, bucket)` convenience constructor (connects + creates/opens KV bucket)
- `NatsLeaseStore::new(store)` for pre-built KV store handle
- `read` -> `store.entry(key)`, revision as etag string. Deleted/purged entries return None.
- `write_if_not_exists` -> `store.create(key, data)`, `CreateErrorKind::AlreadyExists` = CAS conflict
- `write_if_match` -> `store.update(key, data, revision)`, `UpdateErrorKind::WrongLastRevision` = CAS conflict
- `delete` -> `store.purge(key)`, idempotent (hard delete, no history)
- KV bucket config: history=1 (only latest value matters for leases)

Source: `hadb-lease-s3/src/lease_store.rs` (same trait, different backend)

### Volt-b: Tests

10 tests against real NATS (gated by NATS_URL env var):
- read nonexistent, create+read, create conflict, update CAS success/failure, stale revision, delete+read, delete idempotent, delete-then-create, etag is numeric revision

### Volt-c: Engine integration (DONE)

haqlite has `.lease_store(Arc<dyn LeaseStore>)` on HaQLiteBuilder + `WAL_LEASE_NATS_URL` env var behind `nats-lease` feature. Falls back to S3 if NATS unreachable.

---

## Immediate blockers

### walrust 0.3.1: fix crates.io exports

walrust 0.3.0 on crates.io has stale exports (`ReplicationConfig`, `pull_incremental`, `Replicator`, `SyncState` not re-exported from lib.rs). haqlite 0.2.0 can't publish until walrust's public API matches what haqlite imports. Fix: update `walrust-core/src/lib.rs` exports, publish 0.3.1.

### lbug publish: unblock hakuzu + graphstream

`StatementType` was merged upstream (LadybugDB/ladybug-rust#7). Once the ladybug maintainer publishes a new `lbug` version to crates.io, hakuzu can drop the local fork (`personal-website/ladybug-fork`) and publish to crates.io. graphstream has the same blocker via transitive dep.

### NATS deployment

Deploy single NATS server on Fly (~$2/month, shared-cpu-1x-256mb). Wire into cinch engines via `WAL_LEASE_NATS_URL`. The crate and integration exist; just needs a running server.

---

## Phase Signal: ManifestStore Coordinator Integration

> After: Phase Volt . Before: Phase Forge

### Signal-f: Coordinator integration

- `Coordinator` accepts `Arc<dyn ManifestStore>` alongside `Arc<dyn LeaseStore>`
- Writer publishes manifest after each replicator sync
- Nodes poll `manifest_store.meta()` on configurable interval (default 1s)
- If version changed, node calls `manifest_store.get()` then triggers catch-up
- Leader discovery for write forwarding (Dedicated mode) stays in the lease store or service discovery, not the manifest
- `ManifestStore` is optional on Coordinator for backward compat. If None, current S3 file scanning behavior is preserved.

---

## Phase Forge: etcd Lease Store

> After: Phase Volt · Before: Phase 4 (Multi-language SDKs)

etcd is already running in every Kubernetes cluster. `hadb-lease-etcd` means zero new infrastructure for HA SQLite on k8s.

### API mapping

| LeaseStore | etcd |
|---|---|
| `read(key)` | `KV.get(key)`, mod_revision as etag |
| `write_if_not_exists(key, data)` | `Txn.If(CreateRevision == 0).Then(Put)` |
| `write_if_match(key, data, etag)` | `Txn.If(ModRevision == etag).Then(Put)` |
| `delete(key)` | `KV.delete(key)` |

etcd also has native leases with TTL + KeepAlive, which could replace the CAS-based TTL simulation. But for v1, stick with the same CAS pattern as S3/NATS for consistency.

### Forge-a: hadb-lease-etcd crate

New crate `hadb-lease-etcd/` in the hadb workspace. Implements `LeaseStore` via `etcd-client` crate.

- `EtcdLeaseStore::connect(endpoints, prefix)` convenience constructor
- `EtcdLeaseStore::new(client, prefix)` for pre-built client
- CAS via etcd transactions (If/Then/Else)
- Revision as etag (same pattern as NATS)
- Prefix key namespacing to avoid collisions

Source: `hadb-lease-nats/src/lease_store.rs` (same trait, adapt NATS patterns to etcd)

Rust crate: `etcd-client` on crates.io (tonic-based, async)

### Forge-b: Tests

Same test matrix as NATS (gated by ETCD_ENDPOINTS env var):
- read nonexistent, create+read, create conflict, update CAS success/failure, stale revision, delete+read, delete idempotent, delete-then-create, etag is numeric revision

### Forge-c: haqlite integration

haqlite already has `.lease_store()` on the builder and pluggable LeaseStore in serve. Add `WAL_LEASE_ETCD_ENDPOINTS` env var behind `etcd-lease` feature, same pattern as NATS.

### Why etcd before Redis

- Kubernetes users already have etcd. Zero new infra.
- etcd has native CAS transactions (purpose-built for coordination).
- Redis requires Lua scripts for CAS and HA setup (Sentinel/managed) to avoid SPOF.
- etcd is a smaller, more correct implementation.

---

## Future: Separating Replication Concerns

Today, haqlite bundles everything: lease management, WAL sync, snapshots, restore. The future architecture separates these into distinct, composable concerns behind hadb traits.

### LeaseStore (today: S3 + NATS, future: Redis/etcd/Consul/DynamoDB/Postgres)

Already abstracted. `LeaseStore` trait with S3, NATS, and in-memory implementations. NATS JetStream KV is implemented in Phase Volt (2-5ms CAS vs S3's 50-200ms). Single NATS node first (~$2/month), cluster for HA when needed.

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
hadb-lease-nats               -- NATS JetStream KV LeaseStore (Phase Volt, done)
hadb-lease-redis        -- Redis LeaseStore (future)
hadb-lease-etcd         -- etcd LeaseStore (future)
hadb-lease-consul       -- Consul LeaseStore (future)
hadb-lease-dynamo       -- DynamoDB LeaseStore (future)
hadb-lease-pg           -- PostgreSQL LeaseStore (future)

hadb-transport-redpanda -- Redpanda/Kafka ReplicationTransport (future)

hadb-stream             -- CDC consumer on ReplicationTransport (future)
```

### Build order

1. **NATS lease store** -- DONE (Phase Volt). Faster failover, zero per-request cost.
2. **HaNode (engine-level HA)** -- biggest architectural win. Eliminates N leases per process.
3. **Self-organizing replicas** -- engines bid for work, no external orchestrator needed.
4. **Redpanda ReplicationTransport** -- when zero-RPO matters to paying customers.
5. **ReplicationCompactor** -- required alongside #4 to keep S3 as cold archive.
6. **hadb-stream (CDC)** -- falls out for free once #4 exists.
7. **Other lease stores** -- on demand based on deployment environments.

---

## Future: HaNode (Engine-Level HA + Self-Organizing Replicas)

Today, hadb's `Coordinator` manages HA per-database: each database has its own lease, sync loop, and follower poll. This works for the embedded use case (one app, one database). But for multi-tenant platforms (one process, many databases), it doesn't scale. 1000 databases = 1000 independent lease operations.

### HaNode: one lease per process

`HaNode` owns a single lease for the entire process and manages many databases. WAL replication is still per-database (each database has its own WAL). Coordination (who is leader, fencing) is per-process.

```rust
// Per-database (today, still works for embedded use case)
let coordinator = Coordinator::new(replicator, executor, lease_store, storage, config);
coordinator.join("mydb", path).await?;

// Per-process (new, for multi-tenant platforms)
let node = HaNode::new(lease_store, storage, config)
    .with_id("engine-A")
    .with_address("10.0.0.1:6379")
    .await?;

// Add databases. Each gets a Replicator, but no individual lease.
node.add("db-1", path1, replicator1).await?;
node.add("db-2", path2, replicator2).await?;
// ... up to thousands

// One lease renewal, not thousands.
// Role change = all databases transition at once.
```

### Self-organizing replica placement

Nodes discover each other via the lease store's node registry (already exists in hadb-lease-s3). Each node writes its state: id, version, capacity, database list. Nodes read the registry, see which databases need replicas, and bid for work based on spare capacity. No external orchestrator required.

```
Node A starts:
  - Writes to registry: {id: "A", cpus: 8, ram_gb: 32, databases: ["db-1", "db-2", ...]}
  - For each database, checks: "does this have a replica?"

Node B starts:
  - Reads registry. Sees A's databases need replicas. B has spare capacity.
  - Claims via CAS: "I'll replicate db-1, db-7, db-23"
  - Starts pulling WAL for those databases

Node C starts:
  - Sees some claimed by B. Claims unclaimed ones based on its own capacity.
```

**Constraints enforced by the placement algorithm:**
- Never colocate primary + replica on the same node
- Only replicate to nodes running a compatible version
- CAS prevents two nodes from claiming the same replica slot
- Claims have TTL: if a node dies, its claims expire, other nodes bid for the work

**Failover is distributed:** Node A dies. Nodes B, C, D each have some of A's databases. They promote independently. No single node takes all the failover load.

**Custom placement:** Applications can override the default placement with their own logic (e.g., a control plane that assigns replicas based on business rules).

```rust
// Default: hadb's built-in placement (capacity-weighted bidding)
let node = HaNode::new(lease_store, storage, config).await?;

// Custom: application controls placement
let node = HaNode::new(lease_store, storage, config)
    .with_placement(MyControlPlanePlacement::new(cp_client))
    .await?;
```

### Read replicas for free

A follower replicating a database is simultaneously an HA standby and a read replica. The difference is just routing. hadb tracks roles and replication state. The application decides whether to route reads to followers.

This is application-level, not hadb-level. hadb provides:
- Per-database role tracking (`Role::Leader` / `Role::Follower`)
- Replication lag / caught-up state (Phase Beacon)
- `RoleEvent` broadcast for role changes

The application provides:
- Routing logic (proxy, SDK, DNS)
- Read preference configuration
- Stale read tolerance

---

## Phase Beacon: Follower Readiness in Coordinator (DONE)

> After: Phase Volt · Before: Phase 2

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

## Phase Anvil: haduck (HA DuckDB)

> After: Phase Forge · Before: Phase 4

First open-source HA DuckDB. Block-level replication via a custom DuckDB FileSystem extension. No WAL parsing (DuckDB's WAL is internal, undocumented, physical logging with rowids). Instead, intercept block I/O at the filesystem layer and ship dirty 256KB blocks to S3.

### Why block-level, not WAL

DuckDB's WAL uses physical logging (internal rowids, block references). No public API to read it, format changes between versions, nobody has successfully built WAL-based replication. MotherDuck solved this at the filesystem layer (proprietary FUSE). We do the same as a DuckDB extension.

### Architecture

```
Your App
  └── DuckDB (with haduck extension loaded)
        └── HaduckFileSystem (registered via RegisterSubSystem)
              ├── Write path: track dirty 256KB blocks
              ├── Checkpoint: ship dirty blocks to S3 (hadb-io ObjectStore)
              ├── Follower: download block diffs, apply to local copy
              └── hadb Coordinator (leader election, role management)
```

The extension is C++ (DuckDB's FileSystem registration requires the unstable C++ API, same as httpfs). Block-shipping logic delegates to Rust via FFI (hadb-io for S3, hadb for coordination).

### Components

**haduck-ext** (C++ DuckDB extension)
- Subclass `FileSystem`, implement `CanHandleFile()` for `haduck://` scheme
- `Read(handle, buffer, size, location)` -- read from local cache, fetch from S3 on miss
- `Write(handle, buffer, size, location)` -- write to local, mark block dirty in bitmap
- `FileSync(handle)` -- on checkpoint: upload dirty blocks to S3, clear bitmap
- Register via `fs.RegisterSubSystem(make_uniq<HaduckFileSystem>())` in extension load

**haduck-core** (Rust library, called from C++ via FFI)
- `DuckReplicator: hadb::Replicator` -- block-level sync to S3
- `DuckFollowerBehavior: hadb::FollowerBehavior` -- download block diffs, apply
- Block bitmap tracking (which 256KB blocks are dirty since last sync)
- Uses hadb-io ObjectStore for S3 operations
- Uses hadb Coordinator for leader election

**haduck** (Rust binary/library, user-facing API)
- `HaDuck::builder("bucket").open("/data/analytics.duckdb", schema).await?`
- Same pattern as haqlite/hakuzu: builder, coordinator, structured errors
- Loads the C++ extension automatically on open

### Replication flow

**Leader writes:**
1. App writes via DuckDB SQL (INSERT, COPY, etc.)
2. DuckDB writes 256KB blocks through HaduckFileSystem
3. FileSystem marks each written block in a dirty bitmap
4. On checkpoint (controlled, not auto): upload dirty blocks to S3 as `{prefix}/{db}/blocks/{block_id}_{version}`
5. Upload manifest with block list + versions
6. Clear dirty bitmap

**Follower reads:**
1. Poll S3 manifest for new versions
2. Download changed blocks
3. Apply to local file (256KB aligned writes)
4. Local DuckDB reads see updated data

**Cold start:**
1. Download full database from S3 (latest snapshot)
2. Or download manifest + all current block versions
3. Join hadb cluster as follower

### Key design decisions

- **256KB blocks match DuckDB's storage format** -- no translation needed, just pass-through with dirty tracking
- **Checkpoint-controlled, not continuous** -- DuckDB checkpoints are expensive (stop-the-world). Control timing, don't auto-checkpoint.
- **Single-file replication** -- DuckDB is one file + WAL. After checkpoint, WAL is cleared. Ship the main file blocks.
- **Read replicas are natural** -- followers have a full local copy. Analytical queries run locally at full speed.
- **RPO = checkpoint interval** -- data between checkpoints is in the WAL (local only). If leader dies mid-checkpoint, last checkpoint is the recovery point.

### Existing code to reuse

- **Tiered VFS (ladybug-fork)** -- page bitmap, dirty tracking, S3 upload, manifest. Same pattern, different page size (256KB vs 4KB).
- **hadb-io** -- ObjectStore trait, S3Backend, retry, circuit breaker
- **hadb** -- Coordinator, LeaseStore, FollowerBehavior, JoinResult, HaMetrics
- **hadb-lease-nats/etcd** -- fast leader election

### What's different from haqlite/hakuzu

| | haqlite | hakuzu | haduck |
|---|---|---|---|
| DB | SQLite | Kuzu | DuckDB |
| Replication | WAL frames (walrust) | Journal entries (graphstream) | Block diffs (new) |
| Extension lang | Pure Rust | Pure Rust (via lbug) | C++ extension + Rust FFI |
| Block size | 4KB pages | 4KB pages | 256KB blocks |
| Workload | OLTP | Graph queries | OLAP/analytics |
| Checkpoint | SQLite auto | Kuzu manual | DuckDB controlled |

### Phases

**Anvil-a: haduck-ext scaffold**
- DuckDB C++ extension template
- HaduckFileSystem skeleton (pass-through to local filesystem)
- Dirty block bitmap
- Build with CMake, test with DuckDB test framework

**Anvil-b: Block shipping to S3**
- On FileSync: upload dirty blocks via hadb-io (Rust FFI)
- Manifest format: `{block_id: version}` JSON
- Download blocks for follower
- Integration test: write on leader, checkpoint, verify blocks in S3

**Anvil-c: hadb coordination**
- DuckReplicator + DuckFollowerBehavior in Rust
- Leader election via Coordinator
- Follower polls manifest, downloads new blocks
- Write forwarding (DuckDB SQL over HTTP, same pattern as haqlite)

**Anvil-d: haduck user-facing API**
- Rust library wrapping DuckDB + extension loading + hadb coordination
- `HaDuck::builder()` API
- Structured errors (HaDuckError)
- Prometheus metrics

**Anvil-e: Tests**
- Block bitmap unit tests
- Single-node: write + checkpoint + verify S3
- Two-node: leader writes, follower sees data after sync
- Cold start from S3
- Failover: leader dies, follower promotes

### Build dependencies

- DuckDB source (for extension compilation)
- CMake (DuckDB's build system)
- hadb-io (S3, via Rust FFI from C++)
- `duckdb` Rust crate (for the user-facing API)

---

## Phase Solo: Standalone Mode for ha{db} Crates

> After: Phase Anvil

All ha{db} crates (haqlite, hakuzu, haduck) should support a standalone mode
that does replication to S3 without HA coordination. One config flag flips
between standalone (single writer, S3 backup, read replicas) and HA (leader
election, write forwarding, failover). Same codebase, same replication engine.

turbo{db} and ha{db} are independent and composable:

- **turbo{db} only**: tiered storage (S3 page groups, prefetch, NVMe caching). No replication.
- **ha{db} standalone only**: replication to S3, restore, read replicas. No tiering (local disk is source of truth).
- **ha{db} standalone + turbo{db}**: replication + tiered storage. Checkpoint = snapshot.
- **ha{db} HA + turbo{db}**: full stack. Leader election, failover, tiered storage.

The deployment path is incremental. Each layer is one config change:

1. Bare database (local disk)
2. Add turbo{db} (tiered storage, S3 durability, prefetch)
3. Add ha{db} standalone (replication, restore, read replicas)
4. Flip ha{db} to HA mode (leader election, write forwarding, failover)

### a. Coordinator standalone mode
- [ ] Add `ha: bool` config flag to `Coordinator` (default: true for backward compat)
- [ ] When `ha: false`: skip lease acquisition, skip write forwarding HTTP server
- [ ] Still run replicator sync loop (journal/WAL shipping to S3)
- [ ] Still run checkpoint (turbo{db} flushes page groups to S3)
- [ ] Restore from S3 works the same as HA mode
- [ ] Read replicas work: poll S3 for new segments, apply incrementally

### b. haqlite standalone
- [ ] `HaQLite::builder("bucket").ha(false).open(...)` skips lease + forwarding
- [ ] walrust sync loop runs normally, uploads WAL segments to S3
- [ ] turbolite checkpoint flushes page groups to S3
- [ ] CLI: `serve --standalone` flag
- [ ] `restore` command works the same (fetch from S3, replay)

### c. hakuzu standalone
- [ ] `HaKuzu::builder("bucket").ha(false).open(...)` skips lease + forwarding
- [ ] graphstream journal shipping runs normally
- [ ] turbograph checkpoint flushes page groups to S3
- [ ] Restore from manifest + journal segments

### d. haduck standalone
- [ ] `HaDuck::builder("bucket").ha(false).open(...)` skips lease + forwarding
- [ ] duckblock ships dirty blocks to S3 on checkpoint
- [ ] turboduck manages tiered storage
- [ ] Restore from S3 block snapshot

### e. Tests
- [ ] Each ha{db}: standalone write + checkpoint + restore on fresh node
- [ ] Each ha{db}: standalone writer + read replica, replica sees new data
- [ ] Each ha{db}: flip ha=false to ha=true, second node joins as follower
- [ ] Coordinator: standalone mode skips lease, HA mode acquires lease

---

## Phase 2: haqlite CLI (DONE)

See haqlite/ROADMAP.md Phase Meridian. 7 CLI bugs fixed, prefix threading, graceful shutdown, deterministic TXID.

## Phase 3: hakuzu CLI

Same command surface as haqlite (minus watch — graph DBs require active query interception).

Commands: `serve` (Bolt protocol + HA), `restore`, `list`, `verify`, `compact`, `replicate`.

## Replication Format Primitive (design note)

Every hadb database implementation needs a "change batch" format. Today each defines its own:

| Current | Engine | Type | Page/block ID | Page size |
|---------|--------|------|---------------|-----------|
| .ltx (walrust) | SQLite | Physical | u32, 1-indexed | 4KB |
| .seg (duckblock) | DuckDB | Physical | u64, 0-indexed | 256KB |
| .graphj (graphstream) | Kuzu | Logical | N/A | N/A |

These should converge into **two ecosystem-standard formats**:

### `.hadbp` -- hadb physical

Ships changed pages/blocks. Follower writes them at byte offsets. Generic over page ID type and page size.

```
[magic: "HDBP"] [version: u8]
[page_id_size: u8]         // 4 = u32 (SQLite), 8 = u64 (DuckDB, RocksDB)
[max_page_size: u32]       // 4096, 262144, etc.
[seq: u64] [prev_checksum: u64]
[entry_count: u32]
[page_id, data_len: u32, data: bytes]*
[checksum: u64]
```

Replaces .ltx and .seg. Each engine provides only:
- Page ID type (u32 or u64)
- Max page size
- How to read dirty pages from the database

Everything else (encode, decode, checksum chain, S3 key layout, discovery, apply) is shared.

### `.hadbj` -- hadb journal

Ships rewritten queries for logical replication. Follower replays them.

```
[magic: "HDBJ"] [version: u8]
[seq: u64] [prev_checksum: u64]
[entry_count: u32]
[timestamp: u64, query_len: u32, query: bytes, params_len: u32, params: bytes]*
[checksum: u64]
```

Replaces .graphj. Each engine provides only:
- How to rewrite queries deterministically
- How to replay a query entry

### Implementation: `hadb-changeset` crate

```
hadb-changeset/
  src/
    physical.rs   -- .hadbp encode/decode/checksum, generic over PageId trait
    journal.rs    -- .hadbj encode/decode/checksum
    discovery.rs  -- S3 key layout, discover_after, generation management
    apply.rs      -- generic physical apply (pwrite at page_id * page_size)
```

All engines share:
- **S3 key layout**: `{prefix}{db_name}/{generation:04x}/{seq:016x}.hadbp`
- **Discovery**: `list_objects_after` for O(new files) catch-up
- **Generations**: 0000 = incrementals, 0001+ = snapshots
- **Checksum chain**: SHA-256(prev || sorted entries), stale lineage detection

Per-engine code shrinks from ~300 lines to ~50 lines of type definitions.

### Migration path

1. Build `hadb-changeset` with .hadbp and .hadbj
2. duckblock adopts .hadbp first (newest, least migration cost)
3. walrust-core migrates .ltx to .hadbp (backward compat: read both, write .hadbp)
4. graphstream migrates .graphj to .hadbj
5. New engines (harock, haduck, etc.) use .hadbp from day one

### When to build

Trigger: third physical replication engine (harock for RocksDB). Until then, duckblock's .seg and walrust's .ltx work fine independently. The S3 coordination is already shared via `hadb-io::ObjectStore`.

## Phase 4: Multi-language SDKs

FFI layer (haqlite-ffi, hakuzu-ffi) + language bindings (Python via PyO3, Node via napi-rs, Go via CGO).

Depends on Phase 1 (stable hadb-io) and Phase 2-3 (proven CLI surface).
