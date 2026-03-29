# hadb: cheap HA embedded databases

> **Experimental.** hadb is under active development and not yet stable. APIs will change without notice.

hadb aims to make any embedded database highly available via cloud storage, without having to write custom failover/restore logic.

hadb's only high-level goal is high availability with better economics with multiple databases - not scaling or performance or single-database optimization.

S3 provides impressive primitives:

- **Strong consistency** (since 2020) -- linearizable reads after writes
- **Conditional PUTs** (ETags) -- compare-and-swap for leader election
- **11 nines durability, 99.99% availability**
- **~$0.02/GB/month**

The hadb pattern is: embed your database in your application, use ecosystem primitives to replicate transactional changes to S3, and let hadb coordinate leader-follower roles and failover using leases in S3.

Read the [project manifesto](PROJECT.md) for the full argument — where hadb shines, why embed your database, tradeoffs, comparisons, and directions.

## Architecture

**Embedded mode** — the database lives inside your application process:

```
┌──────────────┐                        ┌──────────────┐
│   Leader     │                        │  Follower    │
│ ┌──────────┐ │                        │ ┌──────────┐ │
│ │ Your App │ │                        │ │ Your App │ │
│ │    +     │ │    ┌──────────────┐    │ │    +     │ │
│ │   DB     │─┼───>│     S3       │───>┼─│   DB     │ │
│ └──────────┘ │    │              │    │ └──────────┘ │
│ ┌──────────┐ │    │  WAL + lease │    │ ┌──────────┐ │
│ │  hadb    │─┼───>│              │<───┼─│  hadb    │ │
│ └──────────┘ │    └──────────────┘    │ └──────────┘ │
└──────────────┘ <───forward writes──── └──────────────┘
```

**Cluster mode** — hadb runs as a standalone server, applications connect over a wire protocol:

```
┌──────────┐          ┌──────────┐
│ Your App │          │ Your App │
└────┬─────┘          └────┬─────┘
     │ wire protocol       │ wire protocol
     ▼                     ▼
┌──────────────┐    ┌──────────────┐
│   Leader     │    │  Follower    │
│ ┌──────────┐ │    │ ┌──────────┐ │
│ │   DB     │─┼───>│ │   DB     │ │
│ └──────────┘ │ S3 │ └──────────┘ │
│ ┌──────────┐ │    │ ┌──────────┐ │
│ │  hadb    │─┼───>│ │  hadb    │ │
│ └──────────┘ │    │ └──────────┘ │
└──────────────┘<───└──────────────┘
          forward writes
```

Same replication and leader election in both modes. The difference is whether the database shares a process with the application or runs separately.

> If you need cluster mode, consider other traditional HA database setups, as hadb is not clearly better, although it is fun.

## Design

hadb includes three crate layers:

- **hadb** — Core coordination. Leader election, role management, follower behavior, metrics. Generic over four traits. Zero cloud dependencies.
- **hadb-io** — Shared IO infrastructure. S3 client, retry with circuit breaker, concurrent uploads, HMAC-signed webhooks, GFS retention.
- **hadb-lease-s3** — S3 leader election via conditional PUTs (ETags) for compare-and-swap.

Database-specific crates (like haqlite for SQLite) compose all three layers to execute commands, replicate to storage, and determine leadership.

### Example: haqlite (SQLite HA)

```rust
let db = HaQLite::builder("my-s3-bucket")
    .open("my.db", "CREATE TABLE IF NOT EXISTS users (name TEXT)")
    .await?;

db.execute(
    "INSERT INTO users (name) VALUES (?1)",
    &[SqlValue::Text("Alice".into())]
).await?;

let name: String = db.query_row(
    "SELECT name FROM users WHERE rowid = 1", &[], |row| row.get(0)
).await?;
```

If this node is the leader, writes go directly to the local SQLite database. If it's a follower, writes are forwarded to the leader over HTTP. Reads are local by default — fast, but eventually consistent (followers are 1-2s behind the leader). For strong consistency, reads can forward to the leader too, at the cost of a network round trip.

### Adding a new database

hadb handles coordination; you tell it how your database replicates and executes queries.

| Trait | What it does | SQLite example |
|-------|-------------|----------------|
| **`Replicator`** | Sync layer — add, pull, remove, sync | [walrust](https://github.com/russellromney/walrust) — WAL replication to S3 |
| **`Executor`** | Query execution — execute, is_mutation | [haqlite](https://github.com/russellromney/haqlite) — rusqlite read/write with role-aware forwarding |
| **`LeaseStore`** | Leader election — read, write_if_match, delete | [hadb-lease-s3](https://github.com/russellromney/hadb/tree/main/hadb-lease-s3) — S3 conditional PUT (ETag CAS) leader claims and node registry |
| **`StorageBackend`** | Coordination blobs — upload, download, list, delete | [hadb-io/s3](https://github.com/russellromney/hadb/tree/main/hadb-io) — S3 for storage |

`LeaseStore` and `StorageBackend` already have S3 implementations in hadb-lease-s3. For most new databases, you only need to implement `Replicator` and `Executor`.

Shared infrastructure (S3 client, retry/circuit breaker, concurrent uploads, webhooks, GFS retention) is provided by [hadb-io](https://github.com/russellromney/hadb/tree/main/hadb-io) — your replicator uses it instead of writing S3 upload logic from scratch.

## Project status

- [**hadb**](https://github.com/russellromney/hadb/tree/main/hadb) — Core coordination. Leader election, role management, follower behavior, metrics.
- [**hadb-io**](https://github.com/russellromney/hadb/tree/main/hadb-io) — Shared S3/retry/upload/webhook/retention infrastructure.
- [**hadb-lease-s3**](https://github.com/russellromney/hadb/tree/main/hadb-lease-s3) — S3 `LeaseStore`, `StorageBackend`, `NodeRegistry`.
- [**haqlite**](https://github.com/russellromney/haqlite) — SQLite HA. `Executor` + `Replicator` via walrust.
- [**walrust**](https://github.com/russellromney/walrust) — SQLite replication to S3.

## Acknowledgments

Built on ideas from [Litestream](https://litestream.io/) and [LiteFS](https://fly.io/blog/introducing-litefs/) by [Ben Johnson](https://github.com/benbjohnson). Litestream proved that S3 replication works for SQLite; LiteFS proved that transparent leader election and read replicas work at the edge. hadb aims to generalize both ideas to any embedded database, as an embedded library.

## Future directions

### Fast lease stores

S3 conditional PUTs work but have ~50-200ms latency per operation and charge per request. At scale (1000 databases, lease check every 2s), that's about $17/month just for lease polling, growing linearly with engines and databases. For faster failover and zero per-request cost, swap the `LeaseStore` implementation. S3 remains the storage backend for durability; these are just the fast path for coordination. Start with a single NATS node (about $2/month), cluster for HA.

**hadb-lease-nats** ([crates.io](https://crates.io/crates/hadb-lease-nats)) -- NATS JetStream KV with CAS for leader election. 2-5ms per operation. Lightweight self-hosted Raft (single binary, ~30MB RAM per node). Recommended fast path. Start with 1 node, add 2 more for HA when needed.

**hadb-redis-lease** -- Redis `SET NX PX` for acquire, Lua scripts for atomic renew/release. 1-5ms per operation. Managed options everywhere (Upstash, ElastiCache, Aiven). Needs Redis HA (Sentinel or managed) to avoid being a SPOF; fall back to S3 lease if Redis is unreachable.

**hadb-etcd-lease** -- etcd native leases with KeepAlive. 2-5ms per operation. Purpose-built for distributed coordination (Kubernetes uses it). Best for teams already running Kubernetes.

**hadb-consul-lease** -- Consul sessions + KV store. Proven pattern (LiteFS used this). Best for HashiCorp ecosystem deployments.

**hadb-dynamo-lease** -- DynamoDB conditional writes (`PutItem` with `attribute_not_exists`). 5-10ms per operation. AWS-managed, zero ops, serverless pricing.

**hadb-pg-lease** -- PostgreSQL advisory locks or `SELECT FOR UPDATE` with conditional writes. 5-20ms per operation. The "you already have Postgres" option.

### WAL streaming (Kafka/Redpanda)

Today, replication is S3 polling (1-10s RPO). For real-time replication, publish WAL frames to Kafka/Redpanda. Followers consume and apply immediately (~5ms behind leader). RPO drops to near-zero. A batch compactor consumer reads Redpanda, compacts WAL frames, and uploads to S3 (cold archive). Restore: fetch S3 snapshot, then replay Redpanda from the snapshot's offset forward. CDC (change data capture) falls out for free: any consumer subscribes to the WAL topic. NATS JetStream works for small scale but doesn't handle thousands of topics well; Redpanda (single binary, Kafka-compatible) is the right choice at scale.

### Other future crates

**hadb-fly-lease** -- Fly.io native HA. Uses `.internal` DNS for node discovery. Tigris for S3. Any hadb database works on Fly with zero config changes.

**hadb-XXX-replicator-transport** -- CDC from the replication log. With Kafka/Redpanda streaming, every write is a subscribable event. Webhooks, event sourcing, cross-region fanout. Also, can do hadb-XXX-replicator-batcher to get per-WAL-write durability with efficient batching to a hadb-replicator-storage

**hadb-proxy** -- Smart read/write routing. Routes writes to leader, reads to nearest replica. For apps that can't embed hadb directly.

## License

Apache-2.0
