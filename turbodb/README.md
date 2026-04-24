# turbodb: S3-Tiered Storage for Embedded Databases

turbodb is a language-agnostic spec for building S3-backed tiered storage for any embedded database. It is the companion to hadb's HA replication. Together they give any embedded DB both high availability and cost-effective S3-tiered storage.

Reference implementations: [turbolite](https://github.com/russellromney/turbolite) (SQLite, Rust), [turbograph](https://github.com/russellromney/turbograph) (Kuzu, C++).

See [SPEC.md](SPEC.md) for the full implementation specification.

## Rust crate family (in this workspace)

The manifest envelope — `Manifest`, `ManifestMeta`, `ManifestStore` trait, and five backend impls — ships as `turbodb*` sibling crates under `hadb/`. The manifest's `payload` is an opaque `Vec<u8>`; its shape belongs to each consumer (turbolite, turbograph, future turboduck), keeping the abstraction open.

| Crate | Purpose |
|---|---|
| `turbodb` | Trait crate. `Manifest` (opaque-payload envelope), `ManifestMeta`, `ManifestStore`. Depends on `hadb-storage` for `CasResult`. No impls. |
| `turbodb-manifest-mem` | `MemManifestStore` — in-memory, test-only. |
| `turbodb-manifest-s3` | `S3ManifestStore` — S3 conditional PUTs (If-Match / If-None-Match). |
| `turbodb-manifest-cinch` | `CinchManifestStore` — Cinch's `/v1/sync/manifest` HTTP wire (Bearer auth + Fence-Token headers). |
| `turbodb-manifest-nats` | `NatsManifestStore` — NATS JetStream KV with revision-based CAS. |
| `turbodb-manifest-redis` | `RedisManifestStore` — Redis Lua-script CAS with hash-tagged version side-keys. |

The turbolite / turbograph / turboduck reference implementations (the actual tiered-storage engines on top of turbodb's commit format) are separate repos.

## The idea

Embedded databases store everything on local disk. That's fast, but local disk is expensive, sized for peak, and lost when the node dies. S3 is durable, cheap ($0.02/GB/month), and infinite, but too slow for random reads.

turbodb bridges these by treating local disk as a cache and S3 as the source of truth. Pages live in S3 as compressed, optionally encrypted page groups. Cache misses fetch from S3 transparently. Writes go to local disk first, then flush to S3 on checkpoint. An adaptive prefetch engine anticipates access patterns and warms the cache ahead of reads.

The result: a database that feels local but costs like S3.

## Integration with hadb

The turbo layer's checkpoint *is* the ha layer's snapshot. When the leader checkpoints dirty pages to S3, those page groups are the replication state. A replica reads the manifest, fetches page groups on demand, and serves queries from its local cache. No full-DB copy needed.

```
Leader:
  write -> dirty pages -> checkpoint -> S3 page groups + manifest
                                          |
Follower:                                 v
  open -> read manifest -> fetch page groups on demand -> local cache -> serve queries
```

The manifest is both the atomic commit point for tiered storage and the replication cursor for HA. hadb's lease determines who writes the manifest. turbodb's prefetch engine makes reads fast regardless of whether the local cache is warm.

## Durability modes

turbodb defines three user-facing durability presets. Each maps to a `(hadb replication, turbodb flush policy)` pair:

| Mode | Pages reach cloud | WAL shipping | Use case |
|------|-------------------|--------------|----------|
| `Checkpoint` | on checkpoint only | none | Dev / single-node / tests |
| `Continuous` | on checkpoint + WAL ships ~1s | yes | Production default |
| `Cloud` | every commit, before ack | none | Multi-writer (Shared mode) |

`Continuous` is the default. `Cloud` is required for `HaMode::Shared` because multiple writers need every write visible immediately. `Checkpoint` is best for local-only workloads where crash recovery from the last checkpoint is acceptable.

## Key concepts

**Page groups.** Pages are grouped into fixed-size chunks (e.g., 2048 pages = 8MB). Each group is one immutable S3 object, zstd-compressed with seekable sub-frames for targeted range GETs.

**Adaptive prefetch.** On consecutive cache misses, progressively fetch more neighboring page groups. Different schedules for different access patterns: aggressive for scans, conservative for point lookups.

**Per-table scheduling.** Parse database metadata on open to build a page-to-table map. Each table gets its own miss counter and schedule. Relationship/edge tables get aggressive prefetch; node/lookup tables get conservative.

**Query plan frontrunning.** Before executing a query, walk the query plan to find which tables will be accessed. Prefetch all their page groups before the first page read.

**Two-layer encryption.** AES-256-CTR on the local cache (zero overhead, deterministic nonce per page). AES-256-GCM on S3 (authenticated, random nonce per frame). Compress then encrypt.

**Beacon.** On cold open, parse page 0 to discover structural pages (catalog, metadata), fetch them eagerly, then parse metadata to build the table map. Database construction is fast even with an empty cache.

## Implementations

| Implementation | Database | Language | Status |
|---|---|---|---|
| [turbolite](https://github.com/russellromney/turbolite) | SQLite | Rust | Production |
| [turbograph](https://github.com/russellromney/turbograph) | Kuzu/LadybugDB | C++ | Benchmarking |
| [turboduck](https://github.com/russellromney/turboduck) | DuckDB | C++ | Planning |

## What's DB-specific

Most of turbodb is generic. Three pieces require DB-specific adapters:

1. **File system hook**: intercept the DB's file I/O (SQLite VFS, Kuzu FileSystem, DuckDB FileSystem)
2. **Metadata parser**: read the DB's catalog/storage manager format to build the page-to-table map
3. **Plan walker**: inspect the DB's query plan to extract table IDs for frontrunning

Everything else (S3 client, page groups, manifest, bitmap, prefetch engine, encryption, cache management) is the same across databases.
