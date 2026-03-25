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

## License

Apache-2.0
