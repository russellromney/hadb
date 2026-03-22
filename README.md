# hadb: Make Any Embedded DB Highly Available

## Thesis

S3 is the new consensus layer. Any embedded database with a WAL becomes highly available for ~$5/month.

Traditional HA requires Raft or Paxos across 3-5 nodes with constant inter-node chatter. That's expensive and complex. But S3 already solved the hard distributed systems problems:

- **Strong consistency** (since 2020) — linearizable reads after writes
- **Conditional PUTs** (ETags) — compare-and-swap for leader election
- **11 nines durability, 99.99% availability** — more durable and available than any self-managed cluster
- **$0.02/GB/month** — cheaper than any compute

The pattern: **embedded DB + WAL shipping to S3 + hadb coordination = HA**. Cost: 1 primary + 1 warm standby + S3 storage. That's it.

## Inspiration

[LiteFS](https://fly.io/blog/introducing-litefs/) and [Litestream](https://litestream.io/), both by Ben Johnson at Fly.io, proved that SQLite replication via S3 works. [Litestream v0.5.0](https://fly.io/blog/litestream-v050-is-here/) is getting close — LTX format, PITR, S3 conditional writes for distributed leasing, VFS read replicas. It's a serious replication tool.

hadb takes the same core insight — S3 as the coordination and storage layer — but makes it **database-agnostic** and **embeddable**. Litestream is a separate process that replicates SQLite. hadb is a library that adds HA to any embedded database from inside your application. No FUSE, no sidecar, no external processes. Implement four traits and your database gets leader election, automatic failover, write forwarding, and read replicas.

## Before and after

Without hadb:
```rust
let conn = Connection::open("my.db")?;
conn.execute("INSERT INTO users (name) VALUES (?1)", params!["Alice"])?;
// Single node. If it dies, you restore from backup and lose recent writes.
```

With hadb:
```rust
let db = HaQLite::builder("my-s3-bucket")
    .open("my.db", "CREATE TABLE IF NOT EXISTS users (name TEXT)")
    .await?;
db.execute("INSERT INTO users (name) VALUES (?1)", &[SqlValue::Text("Alice".into())]).await?;
// Two nodes. If the leader dies, the follower promotes in <5s.
// Recent writes replicated to S3 within the sync interval.
```

Five lines of setup. Same SQLite underneath. HA for ~$5/month.

## Core traits

hadb is generic over four traits. Implement them for your database and you get HA:

- **`Replicator`** — sync layer (add, pull, remove, sync). Litestream-style WAL shipping for SQLite, journal replay for Kuzu.
- **`Executor`** — query execution (execute, is_mutation). rusqlite for SQLite, Kuzu for graph.
- **`LeaseStore`** — leader election (claim, renew, release, read). S3 conditional PUT is the reference impl.
- **`StorageBackend`** — replication storage (upload, download, list, delete). S3 is the reference impl.

## Architecture

```
hadb/                    # Core crate (zero cloud deps)
├── traits.rs            # LeaseStore, StorageBackend, Replicator, Executor
├── coordinator.rs       # Coordinator<R, E, L, S>
├── client.rs            # HaClient (leader discovery + HTTP forwarding)
└── ...

hadb-s3/                 # S3 reference implementation
├── lease_store.rs       # S3LeaseStore (conditional PUT via ETag)
└── storage.rs           # S3StorageBackend (PutObject/GetObject)
```

## Usage

```rust
use hadb::{Coordinator, Replicator, Executor};
use hadb_s3::{S3LeaseStore, S3StorageBackend};

let s3_config = aws_config::load_from_env().await;
let s3_client = aws_sdk_s3::Client::new(&s3_config);

let coordinator = Coordinator::new(
    my_replicator,      // Implements Replicator
    my_executor,        // Implements Executor
    S3LeaseStore::new(s3_client.clone(), "my-bucket".into()),
    S3StorageBackend::new(s3_client, "my-bucket".into()),
    config,
)?;

coordinator.join("mydb", path).await?;
```

## Status

- **Traits defined** — all 4 core traits with comprehensive docs
- **S3 implementations** — S3LeaseStore and S3StorageBackend, 9 integration tests against Tigris
- **HaClient** — generic leader discovery + HTTP forwarding with retry
- **Coordinator** — coming next (extract from haqlite, make generic)

## Replication

**WAL shipping** (Litestream pattern): capture WAL deltas, upload to S3, followers pull and apply. This is what hadb implements today. Best for databases with well-defined, stable WAL formats (SQLite, RocksDB).

Page-level replication (dirty page tracking, S3 as source of truth) is a theoretical future approach for databases without stable WAL formats. Nothing uses it yet.

## Key comparisons

**vs rqlite**: S3 instead of Raft. Embedded library instead of standalone server. Zero inter-node networking. The tradeoff: rqlite is synchronously replicated (no committed write is ever lost), hadb is asynchronous (writes within the replication lag window can be lost on leader crash). hadb trades a small durability window for 10x simpler architecture and 10x lower cost.

**vs LiteFS**: Library instead of FUSE sidecar. S3 instead of Consul. Works everywhere, not just Fly.io. LiteFS proved the concept — hadb is the idea rebuilt from scratch with the right architecture. hadb-fly is the spiritual successor: no FUSE, uses Fly's `.internal` DNS for discovery.

**vs Litestream**: Litestream is a separate process for SQLite only. hadb is an embedded library for any database. Litestream handles replication ownership (one writer per destination) but not application-level HA — your app still handles promotion and routing. hadb owns the full lifecycle: leader election, follower promotion, write forwarding, read replicas. If you just need SQLite replication, Litestream is excellent. If you need HA as an embedded library, use hadb.

**vs managed databases** (RDS, Neon, PlanetScale): 10-100x cheaper. No vendor lock-in. Keep using your embedded database, add 5 lines of code, get HA. Primary node has zero read latency.

## The ecosystem we're building

### What exists today

**hadb** is the core coordination framework — leader election via S3 leases, role management, follower behavior, metrics. Zero cloud dependencies, ~2K lines. **hadb-s3** is the reference implementation of `LeaseStore` and `StorageBackend` using S3 conditional PUTs.

**haqlite** is the first database implementation. SQLite HA using Litestream-style WAL shipping. Leader election, automatic failover, write forwarding, read replicas. 10 integration tests, battle-tested with real Tigris S3 in an end-to-end experiment with 4 failovers.

### What we envision

**hakuzu** — Kuzu/graph HA. Same pattern as haqlite but for graph databases. The replication layer already exists inside graphd (journal-based logical replication) — it needs to be extracted as a `Replicator` trait impl. ~200 lines of new code on top of hadb. This is the next thing to build.

**hadb-fly** — Fly.io native HA. The spiritual successor to LiteFS, as a library. No FUSE. Uses Fly's `.internal` DNS for node discovery instead of Consul. Leader election still goes through S3 (Tigris is Fly-native). Any hadb database implementation (haqlite, hakuzu) works on Fly with zero config changes — hadb-fly just provides the discovery layer.

**hadb-stream** — Change data capture from the S3 transaction log. Every write that hits S3 becomes a CDC event for free. Webhooks, event sourcing, cross-region fanout — all without touching the primary. This falls out naturally from the architecture (the txn log is already in S3) but isn't urgent until there are users who need it.

**hadb-proxy** — Smart read/write routing. Sits in front of a hadb cluster and routes writes to the leader, reads to the nearest replica. Useful for deployments where the application can't embed hadb directly (e.g. existing apps that speak Postgres or MySQL wire protocol).

**hadb-redis** — Redis as the lease store instead of S3. S3 conditional PUTs have ~100ms latency; Redis CAS is <1ms. For deployments that already run Redis, this cuts leader election and failover detection time by 100x. S3 remains the storage backend for durability — Redis is just the fast path for coordination.

**haduck** — DuckDB HA. Speculative. See notes below.

**harock** — RocksDB HA via WAL + SST shipping. RocksDB has a well-defined WAL format and the `BackupEngine` API for consistent snapshots. Interesting because it would bring HA to the entire RocksDB ecosystem (TiKV, CockroachDB's storage layer, countless key-value stores). Not planned — would build on demand.

## Notes on haduck (DuckDB)

DuckDB's WAL format is undocumented, changes between versions, and has known durability issues (WAL corruption on OOM/crash, WAL size explosion under concurrent load). WAL shipping is risky — a format change between DuckDB versions would break replication silently.

The safer approach is page-level replication (tiered VFS), which doesn't need to understand DuckDB internals. But DuckDB is increasingly designed for S3-native workloads (Iceberg, Parquet) where the data already lives in object storage. haduck may only make sense for specific version-pinned deployments where DuckDB is used as a persistent embedded store, not as an S3 query engine.

Speculative. Build only if there's clear demand.

## Why this matters

Every embedded database that says "not suitable for production without HA" becomes production-ready with hadb. SQLite, DuckDB, RocksDB, Kuzu, Meilisearch, Qdrant — they're all great databases held back by the lack of a simple HA story. hadb fixes that for all of them at once.

## License

Apache-2.0
