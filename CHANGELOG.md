# Changelog

## 2026-04-03

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
