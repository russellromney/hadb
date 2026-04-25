# Changelog

## 2026-04-03

### Phase Beacon: Follower Readiness in Coordinator (complete)
- Beacon-a: `caught_up: Arc<AtomicBool>` added next to `follower_position` in `FollowerState`. Both coordinator (sets on Promoted/Demoted/Fenced) and follower behavior (sets during poll loop) write to the same atomic.
- Beacon-c: `FollowerBehavior::run_follower_loop()` signature grew a `caught_up` parameter (breaking trait change).
- `Coordinator::join()` now returns `JoinResult { role, caught_up, position }` so callers cache `Arc` refs for zero-overhead health checks (single atomic load) instead of locking the per-db `HashMap` RwLock on every read.
- `HaMetrics` grew `follower_caught_up` and `follower_replay_position` gauges, surfaced via `MetricsSnapshot` and `to_prometheus()`.
- Atomic with hakuzu Phase Parity and haqlite Phase Rampart-e — all three landed in one coordinated breaking change.

### Phase 2: haqlite CLI (complete)
- See haqlite Phase Meridian. 7 CLI bugs fixed, prefix threading, graceful shutdown, deterministic TXID.

### Phase Signal: ManifestStore (complete)
- Signal-a through Signal-e: trait, types, 4 backends (S3, NATS, etcd, Redis)
- `ManifestStore` trait in `hadb/src/manifest.rs` with `get`/`put`/`meta` and CAS semantics
- `HaManifest`, `ManifestMeta`, `StorageManifest` (Turbolite + Walrust variants), `FrameEntry`, `BTreeInfo`, `SubframeOverride` types with msgpack + JSON serialization
- `InMemoryManifestStore` for testing (30 tests: CAS, serialization, boundary values, contract enforcement)
- `hadb-manifest-s3`: S3 conditional PUTs, HeadObject for cheap `meta()` via custom metadata headers (13 tests)
- `hadb-manifest-nats`: NATS JetStream KV with native revision CAS (13 tests, verified against real NATS)
- `hadb-manifest-redis`: Lua scripts for atomic CAS, Redis Cluster compatible via hash tags (13 tests)
- Added `rmp-serde` workspace dependency for msgpack serialization
- Signal-f: Coordinator integration
  - `Coordinator` accepts `Option<Arc<dyn ManifestStore>>` (same optional pattern as LeaseStore)
  - Followers poll `manifest_store.meta()` on configurable interval, emit `ManifestChanged` event on version change
  - `manifest_poll_interval` config field (default 1s)
  - `InMemoryManifestStore` made public for integration testing
  - 8 new tests (accessor, config, join-with-manifest, event variant, polling emits event, no-event-when-unchanged)

### Phase Forge: etcd Lease Store (complete)
- `hadb-lease-etcd` crate with `EtcdLeaseStore` implementing `LeaseStore` via etcd transactions
- CAS via `Txn.If(CreateRevision == 0).Then(Put)` and `Txn.If(ModRevision == etag).Then(Put)`
- Prefixed keys, mod_revision as etag
- 10 tests (gated by ETCD_ENDPOINTS env var)

## 2026-03-23

### Phase Volt: NATS JetStream KV Lease Store (complete)
- `hadb-lease-nats` crate implements `LeaseStore` trait via NATS JetStream KV.
- `NatsLeaseStore::connect(url, bucket)` (creates/opens KV bucket) + `NatsLeaseStore::new(store)` for a pre-built handle.
- `read` → `store.entry(key)`, revision as etag string. Deleted/purged entries return None.
- `write_if_not_exists` → `store.create(key, data)`, `CreateErrorKind::AlreadyExists` = CAS conflict.
- `write_if_match` → `store.update(key, data, revision)`, `UpdateErrorKind::WrongLastRevision` = CAS conflict.
- `delete` → `store.purge(key)`, idempotent (hard delete, no history).
- KV bucket config: `history=1` (only the latest value matters for leases).
- 10 tests against real NATS (gated by `NATS_URL` env var).
- haqlite consumes via `HaQLiteBuilder::lease_store(...)` + `WAL_LEASE_NATS_URL` env var behind the `nats-lease` feature; falls back to S3 if NATS is unreachable.
- Operationally: NATS KV is 2-5ms per op vs S3 lease store's 50-200ms, with zero per-request cost on a single $2/month Fly machine vs ~$17/month at 1000 databases polling every 2s.

### Phase 1a: Create hadb-io + split hadb-s3
- Created `hadb-io` crate: shared S3/retry/upload/webhook/retention infrastructure
- Moved retry.rs, s3.rs, storage.rs from walrust-core → hadb-io (reconciled walrust/graphstream differences)
- Built `ConcurrentUploader<T>` generic framework (extracted from walrust uploader)
- Moved webhook.rs, retention.rs from walrust → hadb-io
- Extracted shared config types (S3Config, WebhookConfig, CacheConfig, parse_duration_string)
- Split `hadb-s3` → `hadb-lease-s3` (coordination) + absorbed storage into hadb-io
- Deleted stale `hadb-s3` directory

### Phase 1b: Migrate walrust to hadb-io
- walrust-core: deleted retry.rs, s3.rs, storage.rs; re-exports from hadb-io
- walrust CLI: replaced webhook.rs, retention.rs, config types with hadb-io re-exports
- ~4,275 lines deleted from walrust (replaced by thin wrappers)
- 303 walrust tests passing, no regressions
- RSS benchmarks confirmed: 23-31 MB across 1-100 databases (no memory regression)
