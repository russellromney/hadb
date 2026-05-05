# hadb: cheap HA embedded databases

> **Experimental.** hadb is under active development and contains bugs. Be careful.

hadb makes any embedded database highly available via cloud storage, without custom failover/restore logic.

The goal is high availability with better economics for multi-database workloads, not scaling or single-database optimization.

S3 provides impressive primitives:

- **Strong consistency** (since 2020) -- linearizable reads after writes
- **Conditional PUTs** (ETags) -- compare-and-swap for leader election (AWS S3 only; Tigris does not enforce atomic conditional PUTs for concurrent requests)
- **11 nines durability, 99.99% availability**
- **~$0.02/GB/month**

The hadb pattern is: embed your database in your application, use ecosystem primitives to replicate transactional changes to S3, and let hadb coordinate leader-follower roles and failover using leases in S3.

Read the [project manifesto](PROJECT.md) for the full argument -- where hadb shines, why embed your database, tradeoffs, comparisons, and directions.

## Architecture

**Embedded mode** -- the database lives inside your application process:

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

**Cluster mode** -- hadb runs as a standalone server, applications connect over a wire protocol:

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

hadb is a workspace of crates:

**Coordination traits:**
- **hadb** — Core coordination. Leader election via `LeaseStore` trait, role management, follower readiness. Zero cloud dependencies.
- **hadb-lease** — `LeaseStore` trait crate.
- **hadb-storage** — `StorageBackend` trait crate (byte-level storage for WAL replication and coordination blobs).

**Lease store implementations:**
- **hadb-lease-s3** — S3 leader election via conditional PUTs (ETags).
- **hadb-lease-nats** — NATS JetStream KV leader election. 2-5ms operations.
- **hadb-lease-cinch** — HTTP lease store backend.
- **hadb-lease-mem** — In-memory lease store for tests.

**Storage backend implementations:**
- **hadb-storage-s3** — S3 storage backend.
- **hadb-storage-local** — Local filesystem storage backend.
- **hadb-storage-cinch** — HTTP storage backend.
- **hadb-storage-mem** — In-memory storage backend for tests.

**Manifest layer (turbodb spec):**
- **turbodb** — Manifest trait and spec for S3-tiered storage.
- **turbodb-manifest-s3** — S3 manifest store implementation.
- **turbodb-manifest-cinch** — HTTP manifest store backend.
- **turbodb-manifest-nats** — NATS manifest store.
- **turbodb-manifest-mem** — In-memory manifest store for tests.

**Shared infrastructure:**
- **hadb-io** — S3 client, retry with circuit breaker, concurrent uploads.
- **hadb-changeset** — Unified replication formats (`.hadbp`, `.hadbj`).
- **hadb-cli** — Shared CLI framework.

Database-specific crates (haqlite for SQLite, hakuzu for Kuzu) compose these layers. Tiered storage implementations (turbolite, turbograph) follow the turbodb spec.

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

If this node is the leader, writes go directly to the local SQLite database. If it's a follower, writes are forwarded to the leader over HTTP. Reads are local by default -- fast, but eventually consistent (followers are 1-2s behind the leader). For strong consistency, reads can forward to the leader too, at the cost of a network round trip.

### Adding a new database

hadb handles coordination; you tell it how your database replicates and executes queries.

| Trait | What it does | SQLite example |
|-------|-------------|----------------|
| **`Replicator`** | Sync layer -- add, pull, remove, sync | [walrust](https://github.com/russellromney/walrust) -- WAL replication to S3 |
| **`Executor`** | Query execution -- execute, is_mutation | [haqlite](https://github.com/russellromney/haqlite) -- rusqlite read/write with role-aware forwarding |
| **`LeaseStore`** | Leader election -- read, write_if_match, delete | [hadb-lease-s3](https://github.com/russellromney/hadb/tree/main/hadb-lease-s3) -- S3 conditional PUT (ETag CAS) leader claims and node registry |
| **`StorageBackend`** | Coordination blobs -- upload, download, list, delete | [hadb-storage-s3](https://github.com/russellromney/hadb/tree/main/hadb-storage-s3) -- S3 storage backend |

`LeaseStore` and `StorageBackend` already have S3 implementations in hadb-lease-s3. For most new databases, you only need to implement `Replicator` and `Executor`.

Shared infrastructure (S3 client, retry/circuit breaker, concurrent uploads) is provided by [hadb-io](https://github.com/russellromney/hadb/tree/main/hadb-io). Concrete storage backends live in `hadb-storage-*` crates.

## Project status

All crates published to [crates.io](https://crates.io/crates/hadb).

**Core (hadb workspace):**
- [**hadb**](https://github.com/russellromney/hadb/tree/main/hadb) — Coordination framework. Leader election, role management, follower readiness. Stable.
- [**hadb-lease**](https://github.com/russellromney/hadb/tree/main/hadb-lease) — `LeaseStore` trait.
- [**hadb-storage**](https://github.com/russellromney/hadb/tree/main/hadb-storage) — `StorageBackend` trait.
- [**hadb-io**](https://github.com/russellromney/hadb/tree/main/hadb-io) — Shared infrastructure. S3 client, retry/circuit breaker, concurrent uploads. Stable.
- [**hadb-cli**](https://github.com/russellromney/hadb/tree/main/hadb-cli) — Shared CLI framework.

**Lease stores:**
- [**hadb-lease-s3**](https://github.com/russellromney/hadb/tree/main/hadb-lease-s3) — S3 lease store via conditional PUTs. Works on AWS S3. **Not compatible with Tigris**.
- [**hadb-lease-nats**](https://github.com/russellromney/hadb/tree/main/hadb-lease-nats) — NATS JetStream KV lease store. 20-50x faster than S3.
- [**hadb-lease-cinch**](https://github.com/russellromney/hadb/tree/main/hadb-lease-cinch) — HTTP lease store backend.
- [**hadb-lease-mem**](https://github.com/russellromney/hadb/tree/main/hadb-lease-mem) — In-memory lease store for tests.

**Storage backends:**
- [**hadb-storage-s3**](https://github.com/russellromney/hadb/tree/main/hadb-storage-s3) — S3 storage backend.
- [**hadb-storage-local**](https://github.com/russellromney/hadb/tree/main/hadb-storage-local) — Local filesystem storage backend.
- [**hadb-storage-cinch**](https://github.com/russellromney/hadb/tree/main/hadb-storage-cinch) — HTTP storage backend.
- [**hadb-storage-mem**](https://github.com/russellromney/hadb/tree/main/hadb-storage-mem) — In-memory storage backend for tests.

**Manifest layer (turbodb):**
- [**turbodb**](https://github.com/russellromney/hadb/tree/main/turbodb) — Manifest trait and spec.
- [**turbodb-manifest-s3**](https://github.com/russellromney/hadb/tree/main/turbodb-manifest-s3) — S3 manifest store.
- [**turbodb-manifest-cinch**](https://github.com/russellromney/hadb/tree/main/turbodb-manifest-cinch) — HTTP manifest store backend.
- [**turbodb-manifest-nats**](https://github.com/russellromney/hadb/tree/main/turbodb-manifest-nats) — NATS manifest store.

**Database implementations:**
- [**haqlite**](https://github.com/russellromney/haqlite) -- SQLite HA. Structured errors, forwarding retry, read semaphore, graceful shutdown, pluggable LeaseStore (S3 or NATS), CLI with 7 commands. Most complete.
- [**hakuzu**](https://github.com/russellromney/hakuzu) -- Kuzu/LadybugDB HA. Deterministic Cypher rewriter, snapshot bootstrap, ObjectStore-based replication. Functional, less CLI tooling than haqlite.

**Replication engines:**
- [**walrust**](https://github.com/russellromney/walrust) -- SQLite WAL shipping to S3 via HADBP changesets. Deterministic TXID, synchronous flush, point-in-time restore. Mature.
- [**graphstream**](https://github.com/russellromney/graphstream) -- Graph journal shipping to S3 via .graphj segments. Chain-hashed, compressed, encrypted. ObjectStore-based (hadb-io). Mature.
- [**duckblock**](https://github.com/russellromney/duckblock) -- DuckDB block-level replication to S3 via HADBP segments. Checksum-chained, follower apply, pull_incremental.

## Acknowledgments

Built on ideas from [Litestream](https://litestream.io/) and [LiteFS](https://fly.io/blog/introducing-litefs/) by [Ben Johnson](https://github.com/benbjohnson). Litestream proved that S3 replication works for SQLite; LiteFS proved that transparent leader election and read replicas work at the edge. hadb aims to generalize both ideas to any embedded database, as an embedded library.

## Crate naming convention

hadb crates follow the pattern `hadb-{role}-{backend}` where the role maps to a trait:

| Pattern | Trait | What it does | Examples |
|---------|-------|-------------|----------|
| `hadb-lease-{backend}` | `LeaseStore` | Leader election via CAS | hadb-lease-s3, hadb-lease-nats, hadb-lease-etcd |
| `hadb-storage-{backend}` | `StorageBackend` | Coordination blob storage | hadb-storage-s3 (in hadb-lease-s3 today) |
| `hadb-transport-{backend}` | `ReplicationTransport` (future) | Real-time WAL streaming | hadb-transport-redpanda |
| `hadb-stream-{backend}` | (consumer) | CDC from replication log | hadb-stream-redpanda |

Core crates without a backend suffix are trait-only or shared infrastructure:

| Crate | What it does |
|-------|-------------|
| `hadb` | Core traits + coordinator. Zero cloud dependencies. |
| `hadb-io` | Shared S3 client, retry, concurrent uploads, webhooks, retention. |
| `hadb-cli` | Shared CLI framework (args, config, commands). |

Database-specific crates live in their own repos and compose hadb layers:

| Crate | Database | Replicator |
|-------|----------|-----------|
| [haqlite](https://github.com/russellromney/haqlite) | SQLite | [walrust](https://github.com/russellromney/walrust) (WAL shipping) |
| [hakuzu](https://github.com/russellromney/hakuzu) | Kuzu/LadybugDB | [graphstream](https://github.com/russellromney/graphstream) (journal shipping) |
| [haduck](https://github.com/russellromney/haduck) | DuckDB | [duckblock](https://github.com/russellromney/duckblock) (block shipping) |

Each ha{db} crate can optionally use a turbo{db} storage engine for S3-tiered caching. With turbo{db}, local disk becomes a cache instead of the source of truth. Followers cold-start in milliseconds (fetch structural pages from S3, serve queries immediately, warm the rest on demand). Volumes are sized for the working set, not the full database.

| ha{db} | turbo{db} storage engine | Database |
|---|---|---|
| [haqlite](https://github.com/russellromney/haqlite) | [turbolite](https://github.com/russellromney/turbolite) | SQLite |
| [hakuzu](https://github.com/russellromney/hakuzu) | [turbograph](https://github.com/russellromney/turbograph) | Kuzu/LadybugDB |
| [haduck](https://github.com/russellromney/haduck) | [turboduck](https://github.com/russellromney/turboduck) | DuckDB |

turbo{db} implementations follow the [turbodb spec](turbodb/). They work independently of ha{db} (tiered storage without HA) or as ha{db}'s storage engine (tiered + HA, where checkpoint = replication snapshot).

## Future directions

### Fast lease stores

S3 conditional PUTs work on AWS S3 but have ~50-200ms latency per operation and charge per request. **Note:** Tigris S3 does not enforce atomic conditional PUTs for concurrent requests, so hadb-lease-s3 is not safe on Tigris. Use NATS or another lease store instead. At scale (1000 databases, lease check every 2s), that's about $17/month just for lease polling, growing linearly with engines and databases. For faster failover and zero per-request cost, swap the `LeaseStore` implementation. S3 remains the storage backend for durability; these are just the fast path for coordination. Start with a single NATS node (about $2/month), cluster for HA.

**hadb-lease-nats** ([crates.io](https://crates.io/crates/hadb-lease-nats)) -- NATS JetStream KV with CAS for leader election. 2-5ms per operation. Lightweight self-hosted Raft (single binary, ~30MB RAM per node). Recommended fast path. Start with 1 node, add 2 more for HA when needed.

**hadb-lease-redis** (future) -- Redis `SET NX PX` for acquire, Lua scripts for atomic renew/release. 1-5ms per operation. Managed options everywhere (Upstash, ElastiCache, Aiven). Needs Redis HA (Sentinel or managed) to avoid being a SPOF; fall back to S3 lease if Redis is unreachable.

**hadb-lease-etcd** ([crates.io](https://crates.io/crates/hadb-lease-etcd)) -- etcd KV transactions for CAS leader election. 2-5ms per operation. Zero new infra on Kubernetes (uses existing etcd). Purpose-built for distributed coordination.

**hadb-lease-consul** (future) -- Consul sessions + KV store. Proven pattern (LiteFS used this). Best for HashiCorp ecosystem deployments.

**hadb-lease-dynamo** (future) -- DynamoDB conditional writes (`PutItem` with `attribute_not_exists`). 5-10ms per operation. AWS-managed, zero ops, serverless pricing.

**hadb-lease-pg** (future) -- PostgreSQL advisory locks or `SELECT FOR UPDATE` with conditional writes. 5-20ms per operation. The "you already have Postgres" option.

### WAL streaming (Kafka/Redpanda)

Today, replication is S3 polling (1-10s RPO). For real-time replication, publish WAL frames to Kafka/Redpanda. Followers consume and apply immediately (~5ms behind leader). RPO drops to near-zero. A batch compactor consumer reads Redpanda, compacts WAL frames, and uploads to S3 (cold archive). Restore: fetch S3 snapshot, then replay Redpanda from the snapshot's offset forward. CDC (change data capture) falls out for free: any consumer subscribes to the WAL topic. NATS JetStream works for small scale but doesn't handle thousands of topics well; Redpanda (single binary, Kafka-compatible) is the right choice at scale.

### Other future crates

**hadb-lease-fly** (future) -- Fly.io native HA. Uses `.internal` DNS for node discovery. Tigris for S3. Any hadb database works on Fly with zero config changes.

**hadb-transport-redpanda** (future) -- Real-time WAL frame delivery via Redpanda/Kafka topics. Implements `ReplicationTransport` trait.

**hadb-stream-redpanda** (future) -- CDC consumer on the WAL topic. Webhooks, event sourcing, cross-region fanout.

**hadb-proxy** (future) -- Smart read/write routing. Routes writes to leader, reads to nearest replica. For apps that can't embed hadb directly.

## License

Apache-2.0
