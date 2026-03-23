# Changelog

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
