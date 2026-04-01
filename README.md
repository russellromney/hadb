# hadb: cheap HA embedded databases

> **Experimental.** hadb is under active development and not yet stable. APIs will change without notice.

hadb makes any embedded database highly available via cloud storage, without custom failover/restore logic.

The goal is high availability with better economics for multi-database workloads, not scaling or single-database optimization.

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

hadb is a workspace of crates:

- **hadb** -- Core coordination. Leader election via `LeaseStore` trait, role management, follower readiness (`JoinResult` with `caught_up` + `position`), `ShardedLeaseStore` for horizontal scaling. Zero cloud dependencies.
- **hadb-io** -- Shared IO infrastructure. `ObjectStore` trait, S3 client, retry with circuit breaker, concurrent uploads, HMAC-signed webhooks, GFS retention.
- **hadb-lease-s3** -- S3 leader election via conditional PUTs (ETags). CAS for compare-and-swap.
- **hadb-lease-nats** -- NATS JetStream KV leader election. 2-5ms operations (vs S3's 50-200ms). Zero per-request cost.
- **hadb-lease-etcd** -- etcd leader election via KV transactions. Zero new infra on Kubernetes.
- **hadb-cli** -- Shared CLI framework for database tools (args, config, commands).
- **[turbodb](turbodb/)** -- Spec for S3-tiered storage (page groups, prefetch, encryption, caching). Checkpoint = snapshot. The turbo layer's manifest is the ha layer's replication cursor.

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

All crates published to [crates.io](https://crates.io/crates/hadb).

**Core (hadb workspace):**
- [**hadb**](https://github.com/russellromney/hadb/tree/main/hadb) -- Coordination framework. Leader election, role management, follower readiness, ShardedLeaseStore. Stable.
- [**hadb-io**](https://github.com/russellromney/hadb/tree/main/hadb-io) -- Shared infrastructure. ObjectStore trait, S3 client, retry/circuit breaker, concurrent uploads, webhooks, GFS retention. Stable.
- [**hadb-lease-s3**](https://github.com/russellromney/hadb/tree/main/hadb-lease-s3) -- S3 lease store via conditional PUTs. Production-ready.
- [**hadb-lease-nats**](https://github.com/russellromney/hadb/tree/main/hadb-lease-nats) -- NATS JetStream KV lease store. 20-50x faster than S3. Tested against real NATS.
- [**hadb-lease-etcd**](https://github.com/russellromney/hadb/tree/main/hadb-lease-etcd) -- etcd lease store via KV transactions. Zero new infra on Kubernetes. Tested against real etcd.
- [**hadb-cli**](https://github.com/russellromney/hadb/tree/main/hadb-cli) -- Shared CLI framework (args, config, commands). Used by haqlite.

**Database implementations:**
- [**haqlite**](https://github.com/russellromney/haqlite) -- SQLite HA. Structured errors, forwarding retry, read semaphore, graceful shutdown, pluggable LeaseStore (S3 or NATS), CLI with 7 commands. Most complete.
- [**hakuzu**](https://github.com/russellromney/hakuzu) -- Kuzu/LadybugDB HA. Deterministic Cypher rewriter, snapshot bootstrap, ObjectStore-based replication. Functional, less CLI tooling than haqlite.

**Replication engines:**
- [**walrust**](https://github.com/russellromney/walrust) -- SQLite WAL shipping to S3 via LTX format. Deterministic TXID, synchronous flush, point-in-time restore, streaming encoding. Mature.
- [**graphstream**](https://github.com/russellromney/graphstream) -- Graph journal shipping to S3 via .graphj segments. Chain-hashed, compressed, encrypted. ObjectStore-based (hadb-io). Mature.

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
| `haqlite` | SQLite | walrust (WAL shipping) |
| `hakuzu` | Kuzu/LadybugDB | graphstream (journal shipping) |

S3-tiered storage implementations follow the [turbodb spec](turbodb/):

| Implementation | Database | Language |
|---|---|---|
| [turbolite](https://github.com/russellromney/turbolite) | SQLite | Rust |
| [turbograph](https://github.com/russellromney/turbograph) | Kuzu/LadybugDB | C++ |

## Future directions

### Fast lease stores

S3 conditional PUTs work but have ~50-200ms latency per operation and charge per request. At scale (1000 databases, lease check every 2s), that's about $17/month just for lease polling, growing linearly with engines and databases. For faster failover and zero per-request cost, swap the `LeaseStore` implementation. S3 remains the storage backend for durability; these are just the fast path for coordination. Start with a single NATS node (about $2/month), cluster for HA.

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
