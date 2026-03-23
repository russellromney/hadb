# hadb-io

Shared S3/retry/upload infrastructure for the [hadb](https://github.com/russellromney/hadb) ecosystem. 

- **Retry** — `RetryPolicy` with exponential backoff, full jitter, `CircuitBreaker`, error classification
- **S3** — Client helpers, upload/download, checksums (feature-gated behind `s3`)
- **ObjectStore** — Rich storage trait for bulk replication data + `S3Backend` impl
- **ConcurrentUploader** — Bounded-concurrency upload framework with shutdown drain
- **Webhook** — HTTP POST notifications with HMAC-SHA256 signing, event filtering
- **Retention** — GFS (Grandfather/Father/Son) snapshot rotation
- **Config** — `S3Config`, `WebhookConfig`, `CacheConfig`, duration string parsing

```rust
use hadb_io::{RetryPolicy, S3Backend, ObjectStore, WebhookSender};

// Your Replicator implementation uses these instead of
// writing S3 upload/retry logic from scratch.
```
