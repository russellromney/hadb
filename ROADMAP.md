# hadb Roadmap

## Phase Decouple: Opaque StorageManifest

> Before: Phase Anvil

`StorageManifest` is currently an enum with variants for each database backend (Turbolite, TurboliteWalrust, Walrust, Turbograph, TurbographGraphstream). This means hadb knows about its implementations; every new backend requires changing hadb's core types.

**Fix:** Make the storage payload opaque. hadb owns coordination (version, writer_id, lease_epoch, CAS). The storage manifest is a tagged blob that hadb passes through without interpreting.

```rust
pub struct HaManifest {
    pub version: u64,
    pub writer_id: String,
    pub lease_epoch: u64,
    pub timestamp_ms: u64,
    pub storage_type: String,   // "turbolite", "turbograph", etc.
    pub storage: Vec<u8>,       // opaque, impl-owned serialized bytes
}
```

Each implementation (haqlite, hakuzu, haduck) deserializes the `storage` bytes into its own manifest type. hadb never parses it.

### Changes
- [ ] Replace `StorageManifest` enum with `storage_type: String` + `storage: Vec<u8>` on `HaManifest`
- [ ] Update `ManifestMeta` (no storage payload, just coordination fields; already correct)
- [ ] Update all ManifestStore implementations (S3, NATS, etcd, Redis) to pass through opaque bytes
- [ ] Update haqlite: serialize/deserialize its own manifest types from the opaque bytes
- [ ] Update hakuzu: same
- [ ] Remove `StorageManifest` enum and all variant-specific types from hadb (FrameEntry, SubframeOverride, BTreeManifestEntry move to turbolite/turbograph)
- [ ] Tests: round-trip opaque bytes through all ManifestStore backends

### Why not generic type parameter
`HaManifest<S>` would make ManifestStore generic, which ripples through Coordinator. More type gymnastics than it's worth. Opaque bytes with a string discriminator is simpler and hadb never needs to interpret the payload.

---

## Phase Forwarding: Write forwarding at hadb level

> After: Phase Decouple

Write forwarding (followers forward writes to the leader via HTTP) is currently implemented in haqlite with SQLite-specific execute/query handlers. This should be a generic hadb concern so any hadb-based database (haqlite, hakuzu) gets it for free.

### Changes
- [ ] Define a generic `WriteForwarder` trait in hadb with `execute(sql, params)` and `query(sql, params)` methods (or opaque `forward(request_bytes) -> response_bytes`)
- [ ] Move forwarding server lifecycle (start on leader, stop on demotion) from haqlite to hadb Coordinator
- [ ] Each implementation (haqlite, hakuzu) registers a handler that deserializes and executes
- [ ] Port for forwarding server should be 0 (OS-assigned) per database, not a fixed default

---


## Immediate blockers

### walrust 0.3.1: fix crates.io exports

walrust 0.3.0 on crates.io has stale exports (`ReplicationConfig`, `pull_incremental`, `Replicator`, `SyncState` not re-exported from lib.rs). haqlite 0.2.0 can't publish until walrust's public API matches what haqlite imports. Fix: update `walrust-core/src/lib.rs` exports, publish 0.3.1.

### lbug publish: unblock hakuzu + graphstream

`StatementType` was merged upstream (LadybugDB/ladybug-rust#7). Once the ladybug maintainer publishes a new `lbug` version to crates.io, hakuzu can drop the local fork (`personal-website/ladybug-fork`) and publish to crates.io. graphstream has the same blocker via transitive dep.

### NATS deployment

Deploy single NATS server on Fly (~$2/month, shared-cpu-1x-256mb). Wire into consumer engines via `WAL_LEASE_NATS_URL`. The crate and integration exist; just needs a running server.

---

---

## Future: Separating Replication Concerns

Today, haqlite bundles everything: lease management, WAL sync, snapshots, restore. The future architecture separates these into distinct, composable concerns behind hadb traits.

### LeaseStore (today: S3 + NATS, future: Redis/etcd/Consul/DynamoDB/Postgres)

Already abstracted. `LeaseStore` trait with S3, NATS, and in-memory implementations. NATS JetStream KV is implemented in Phase Volt (2-5ms CAS vs S3's 50-200ms). Single NATS node first (~$2/month), cluster for HA when needed.

### ReplicationTransport (future: Kafka/Redpanda)

New trait for real-time WAL frame delivery. Today, followers poll S3 every 1-10s (1-10s RPO). With a `ReplicationTransport`, the leader publishes WAL frames to a durable stream (Redpanda topic `wal.{db_name}`), and followers consume and apply immediately (~5ms behind leader). RPO drops to near-zero.

The write path changes: write arrives at engine, engine publishes WAL frame to Redpanda (durable ack across 3 Raft nodes, 2-5ms), then engine applies to local SQLite. The write is durable in Redpanda before SQLite touches it. If the leader crashes after Redpanda ack but before SQLite apply, followers already have the write from the stream.

NATS JetStream works for small scale but doesn't handle thousands of topics well. Redpanda (single binary, Kafka-compatible) is the right choice at scale.

```rust
// Not designed yet, conceptual
#[async_trait]
pub trait ReplicationTransport: Send + Sync {
    async fn publish_wal_frame(&self, db_name: &str, frame: &[u8]) -> Result<u64>; // returns offset
    async fn subscribe(&self, db_name: &str, from_offset: u64) -> Result<FrameStream>;
    async fn latest_offset(&self, db_name: &str) -> Result<u64>;
}
```

### ReplicationCompactor (future: batch writer)

A consumer that reads WAL frames from the hot stream (Redpanda), batches and compacts them, and uploads to S3 as the cold archive. Decouples real-time replication speed from durable archival.

- Redpanda retention stays short (24h). S3 holds the full compacted history.
- Tracks last-compacted Kafka offset per database.
- Runs as a background task in the engine or as a standalone process.
- `StorageBackend` (S3) role changes from "primary replication target" to "cold archive written by the compactor." Leader no longer uploads WAL frames to S3 directly.

### Restore (two-phase)

Restore becomes: fetch latest S3 snapshot (compacted history), then replay Redpanda from the snapshot's Kafka offset forward (recent uncompacted frames). No gaps. If Redpanda retention has expired for older data, S3 has it.

### CDC for free

Any consumer subscribes to `wal.{db_name}` on Redpanda and gets every write in real-time. That's `hadb-stream` with zero additional code. Webhooks, event sourcing, cross-region fanout.

### What this means for the dependency graph

```
hadb                    -- core traits (LeaseStore, Replicator, Executor, StorageBackend)
                           + new: ReplicationTransport trait

hadb-io                 -- shared S3/retry/upload infrastructure (Phase 1)

hadb-lease-s3           -- S3 LeaseStore (today)
hadb-lease-nats               -- NATS JetStream KV LeaseStore (Phase Volt, done)
hadb-lease-redis        -- Redis LeaseStore (future)
hadb-lease-etcd         -- etcd LeaseStore (future)
hadb-lease-consul       -- Consul LeaseStore (future)
hadb-lease-dynamo       -- DynamoDB LeaseStore (future)
hadb-lease-pg           -- PostgreSQL LeaseStore (future)

hadb-transport-redpanda -- Redpanda/Kafka ReplicationTransport (future)

hadb-stream             -- CDC consumer on ReplicationTransport (future)
```

### Build order

1. **NATS lease store** -- DONE (Phase Volt). Faster failover, zero per-request cost.
2. **HaNode (engine-level HA)** -- biggest architectural win. Eliminates N leases per process.
3. **Self-organizing replicas** -- engines bid for work, no external orchestrator needed.
4. **Redpanda ReplicationTransport** -- when zero-RPO matters to paying customers.
5. **ReplicationCompactor** -- required alongside #4 to keep S3 as cold archive.
6. **hadb-stream (CDC)** -- falls out for free once #4 exists.
7. **Other lease stores** -- on demand based on deployment environments.

---

## Future: HaNode (Engine-Level HA + Self-Organizing Replicas)

Today, hadb's `Coordinator` manages HA per-database: each database has its own lease, sync loop, and follower poll. This works for the embedded use case (one app, one database). But for multi-tenant platforms (one process, many databases), it doesn't scale. 1000 databases = 1000 independent lease operations.

### HaNode: one lease per process

`HaNode` owns a single lease for the entire process and manages many databases. WAL replication is still per-database (each database has its own WAL). Coordination (who is leader, fencing) is per-process.

```rust
// Per-database (today, still works for embedded use case)
let coordinator = Coordinator::new(replicator, executor, lease_store, storage, config);
coordinator.join("mydb", path).await?;

// Per-process (new, for multi-tenant platforms)
let node = HaNode::new(lease_store, storage, config)
    .with_id("engine-A")
    .with_address("10.0.0.1:6379")
    .await?;

// Add databases. Each gets a Replicator, but no individual lease.
node.add("db-1", path1, replicator1).await?;
node.add("db-2", path2, replicator2).await?;
// ... up to thousands

// One lease renewal, not thousands.
// Role change = all databases transition at once.
```

### Self-organizing replica placement

Nodes discover each other via the lease store's node registry (already exists in hadb-lease-s3). Each node writes its state: id, version, capacity, database list. Nodes read the registry, see which databases need replicas, and bid for work based on spare capacity. No external orchestrator required.

```
Node A starts:
  - Writes to registry: {id: "A", cpus: 8, ram_gb: 32, databases: ["db-1", "db-2", ...]}
  - For each database, checks: "does this have a replica?"

Node B starts:
  - Reads registry. Sees A's databases need replicas. B has spare capacity.
  - Claims via CAS: "I'll replicate db-1, db-7, db-23"
  - Starts pulling WAL for those databases

Node C starts:
  - Sees some claimed by B. Claims unclaimed ones based on its own capacity.
```

**Constraints enforced by the placement algorithm:**
- Never colocate primary + replica on the same node
- Only replicate to nodes running a compatible version
- CAS prevents two nodes from claiming the same replica slot
- Claims have TTL: if a node dies, its claims expire, other nodes bid for the work

**Failover is distributed:** Node A dies. Nodes B, C, D each have some of A's databases. They promote independently. No single node takes all the failover load.

**Custom placement:** Applications can override the default placement with their own logic (e.g., a control plane that assigns replicas based on business rules).

```rust
// Default: hadb's built-in placement (capacity-weighted bidding)
let node = HaNode::new(lease_store, storage, config).await?;

// Custom: application controls placement
let node = HaNode::new(lease_store, storage, config)
    .with_placement(MyControlPlanePlacement::new(cp_client))
    .await?;
```

### Read replicas for free

A follower replicating a database is simultaneously an HA standby and a read replica. The difference is just routing. hadb tracks roles and replication state. The application decides whether to route reads to followers.

This is application-level, not hadb-level. hadb provides:
- Per-database role tracking (`Role::Leader` / `Role::Follower`)
- Replication lag / caught-up state (Phase Beacon)
- `RoleEvent` broadcast for role changes

The application provides:
- Routing logic (proxy, SDK, DNS)
- Read preference configuration
- Stale read tolerance

---

## Phase Anvil: haduck (HA DuckDB)

> After: Phase Forge · Before: Phase 4

First open-source HA DuckDB. Block-level replication via a custom DuckDB FileSystem extension. No WAL parsing (DuckDB's WAL is internal, undocumented, physical logging with rowids). Instead, intercept block I/O at the filesystem layer and ship dirty 256KB blocks to S3.

### Why block-level, not WAL

DuckDB's WAL uses physical logging (internal rowids, block references). No public API to read it, format changes between versions, nobody has successfully built WAL-based replication. MotherDuck solved this at the filesystem layer (proprietary FUSE). We do the same as a DuckDB extension.

### Architecture

```
Your App
  └── DuckDB (with haduck extension loaded)
        └── HaduckFileSystem (registered via RegisterSubSystem)
              ├── Write path: track dirty 256KB blocks
              ├── Checkpoint: ship dirty blocks to S3 (hadb-io ObjectStore)
              ├── Follower: download block diffs, apply to local copy
              └── hadb Coordinator (leader election, role management)
```

The extension is C++ (DuckDB's FileSystem registration requires the unstable C++ API, same as httpfs). Block-shipping logic delegates to Rust via FFI (hadb-io for S3, hadb for coordination).

### Components

**haduck-ext** (C++ DuckDB extension)
- Subclass `FileSystem`, implement `CanHandleFile()` for `haduck://` scheme
- `Read(handle, buffer, size, location)` -- read from local cache, fetch from S3 on miss
- `Write(handle, buffer, size, location)` -- write to local, mark block dirty in bitmap
- `FileSync(handle)` -- on checkpoint: upload dirty blocks to S3, clear bitmap
- Register via `fs.RegisterSubSystem(make_uniq<HaduckFileSystem>())` in extension load

**haduck-core** (Rust library, called from C++ via FFI)
- `DuckReplicator: hadb::Replicator` -- block-level sync to S3
- `DuckFollowerBehavior: hadb::FollowerBehavior` -- download block diffs, apply
- Block bitmap tracking (which 256KB blocks are dirty since last sync)
- Uses hadb-io ObjectStore for S3 operations
- Uses hadb Coordinator for leader election

**haduck** (Rust binary/library, user-facing API)
- `HaDuck::builder("bucket").open("/data/analytics.duckdb", schema).await?`
- Same pattern as haqlite/hakuzu: builder, coordinator, structured errors
- Loads the C++ extension automatically on open

### Replication flow

**Leader writes:**
1. App writes via DuckDB SQL (INSERT, COPY, etc.)
2. DuckDB writes 256KB blocks through HaduckFileSystem
3. FileSystem marks each written block in a dirty bitmap
4. On checkpoint (controlled, not auto): upload dirty blocks to S3 as `{prefix}/{db}/blocks/{block_id}_{version}`
5. Upload manifest with block list + versions
6. Clear dirty bitmap

**Follower reads:**
1. Poll S3 manifest for new versions
2. Download changed blocks
3. Apply to local file (256KB aligned writes)
4. Local DuckDB reads see updated data

**Cold start:**
1. Download full database from S3 (latest snapshot)
2. Or download manifest + all current block versions
3. Join hadb cluster as follower

### Key design decisions

- **256KB blocks match DuckDB's storage format** -- no translation needed, just pass-through with dirty tracking
- **Checkpoint-controlled, not continuous** -- DuckDB checkpoints are expensive (stop-the-world). Control timing, don't auto-checkpoint.
- **Single-file replication** -- DuckDB is one file + WAL. After checkpoint, WAL is cleared. Ship the main file blocks.
- **Read replicas are natural** -- followers have a full local copy. Analytical queries run locally at full speed.
- **RPO = checkpoint interval** -- data between checkpoints is in the WAL (local only). If leader dies mid-checkpoint, last checkpoint is the recovery point.

### Existing code to reuse

- **Tiered VFS (ladybug-fork)** -- page bitmap, dirty tracking, S3 upload, manifest. Same pattern, different page size (256KB vs 4KB).
- **hadb-io** -- ObjectStore trait, S3Backend, retry, circuit breaker
- **hadb** -- Coordinator, LeaseStore, FollowerBehavior, JoinResult, HaMetrics
- **hadb-lease-nats/etcd** -- fast leader election

### What's different from haqlite/hakuzu

| | haqlite | hakuzu | haduck |
|---|---|---|---|
| DB | SQLite | Kuzu | DuckDB |
| Replication | WAL frames (walrust) | Journal entries (graphstream) | Block diffs (new) |
| Extension lang | Pure Rust | Pure Rust (via lbug) | C++ extension + Rust FFI |
| Block size | 4KB pages | 4KB pages | 256KB blocks |
| Workload | OLTP | Graph queries | OLAP/analytics |
| Checkpoint | SQLite auto | Kuzu manual | DuckDB controlled |

### Phases

**Anvil-a: haduck-ext scaffold**
- DuckDB C++ extension template
- HaduckFileSystem skeleton (pass-through to local filesystem)
- Dirty block bitmap
- Build with CMake, test with DuckDB test framework

**Anvil-b: Block shipping to S3**
- On FileSync: upload dirty blocks via hadb-io (Rust FFI)
- Manifest format: `{block_id: version}` JSON
- Download blocks for follower
- Integration test: write on leader, checkpoint, verify blocks in S3

**Anvil-c: hadb coordination**
- DuckReplicator + DuckFollowerBehavior in Rust
- Leader election via Coordinator
- Follower polls manifest, downloads new blocks
- Write forwarding (DuckDB SQL over HTTP, same pattern as haqlite)

**Anvil-d: haduck user-facing API**
- Rust library wrapping DuckDB + extension loading + hadb coordination
- `HaDuck::builder()` API
- Structured errors (HaDuckError)
- Prometheus metrics

**Anvil-e: Tests**
- Block bitmap unit tests
- Single-node: write + checkpoint + verify S3
- Two-node: leader writes, follower sees data after sync
- Cold start from S3
- Failover: leader dies, follower promotes

### Build dependencies

- DuckDB source (for extension compilation)
- CMake (DuckDB's build system)
- hadb-io (S3, via Rust FFI from C++)
- `duckdb` Rust crate (for the user-facing API)

---

## Phase Solo: Standalone Mode for ha{db} Crates

> After: Phase Anvil

All ha{db} crates (haqlite, hakuzu, haduck) should support a standalone mode
that does replication to S3 without HA coordination. One config flag flips
between standalone (single writer, S3 backup, read replicas) and HA (leader
election, write forwarding, failover). Same codebase, same replication engine.

turbo{db} and ha{db} are independent and composable:

- **turbo{db} only**: tiered storage (S3 page groups, prefetch, NVMe caching). No replication.
- **ha{db} standalone only**: replication to S3, restore, read replicas. No tiering (local disk is source of truth).
- **ha{db} standalone + turbo{db}**: replication + tiered storage. Checkpoint = snapshot.
- **ha{db} HA + turbo{db}**: full stack. Leader election, failover, tiered storage.

The deployment path is incremental. Each layer is one config change:

1. Bare database (local disk)
2. Add turbo{db} (tiered storage, S3 durability, prefetch)
3. Add ha{db} standalone (replication, restore, read replicas)
4. Flip ha{db} to HA mode (leader election, write forwarding, failover)

### a. Coordinator standalone mode
- [ ] Add `ha: bool` config flag to `Coordinator` (default: true for backward compat)
- [ ] When `ha: false`: skip lease acquisition, skip write forwarding HTTP server
- [ ] Still run replicator sync loop (journal/WAL shipping to S3)
- [ ] Still run checkpoint (turbo{db} flushes page groups to S3)
- [ ] Restore from S3 works the same as HA mode
- [ ] Read replicas work: poll S3 for new segments, apply incrementally

### b. haqlite standalone
- [ ] `HaQLite::builder("bucket").ha(false).open(...)` skips lease + forwarding
- [ ] walrust sync loop runs normally, uploads WAL segments to S3
- [ ] turbolite checkpoint flushes page groups to S3
- [ ] CLI: `serve --standalone` flag
- [ ] `restore` command works the same (fetch from S3, replay)

### c. hakuzu standalone
- [ ] `HaKuzu::builder("bucket").ha(false).open(...)` skips lease + forwarding
- [ ] graphstream journal shipping runs normally
- [ ] turbograph checkpoint flushes page groups to S3
- [ ] Restore from manifest + journal segments

### d. haduck standalone
- [ ] `HaDuck::builder("bucket").ha(false).open(...)` skips lease + forwarding
- [ ] duckblock ships dirty blocks to S3 on checkpoint
- [ ] turboduck manages tiered storage
- [ ] Restore from S3 block snapshot

### e. Tests
- [ ] Each ha{db}: standalone write + checkpoint + restore on fresh node
- [ ] Each ha{db}: standalone writer + read replica, replica sees new data
- [ ] Each ha{db}: flip ha=false to ha=true, second node joins as follower
- [ ] Coordinator: standalone mode skips lease, HA mode acquires lease

---

## Phase 3: hakuzu CLI

Same command surface as haqlite (minus watch — graph DBs require active query interception).

Commands: `serve` (Bolt protocol + HA), `restore`, `list`, `verify`, `compact`, `replicate`.

## Replication Format Primitive (design note)

Every hadb database implementation needs a "change batch" format. Today each defines its own:

| Current | Engine | Type | Page/block ID | Page size |
|---------|--------|------|---------------|-----------|
| .ltx (walrust) | SQLite | Physical | u32, 1-indexed | 4KB |
| .seg (duckblock) | DuckDB | Physical | u64, 0-indexed | 256KB |
| .graphj (graphstream) | Kuzu | Logical | N/A | N/A |

These should converge into **two ecosystem-standard formats**:

### `.hadbp` -- hadb physical

Ships changed pages/blocks. Follower writes them at byte offsets. Generic over page ID type and page size.

```
[magic: "HDBP"] [version: u8]
[page_id_size: u8]         // 4 = u32 (SQLite), 8 = u64 (DuckDB, RocksDB)
[max_page_size: u32]       // 4096, 262144, etc.
[seq: u64] [prev_checksum: u64]
[entry_count: u32]
[page_id, data_len: u32, data: bytes]*
[checksum: u64]
```

Replaces .ltx and .seg. Each engine provides only:
- Page ID type (u32 or u64)
- Max page size
- How to read dirty pages from the database

Everything else (encode, decode, checksum chain, S3 key layout, discovery, apply) is shared.

### `.hadbj` -- hadb journal

Ships rewritten queries for logical replication. Follower replays them.

```
[magic: "HDBJ"] [version: u8]
[seq: u64] [prev_checksum: u64]
[entry_count: u32]
[timestamp: u64, query_len: u32, query: bytes, params_len: u32, params: bytes]*
[checksum: u64]
```

Replaces .graphj. Each engine provides only:
- How to rewrite queries deterministically
- How to replay a query entry

### Implementation: `hadb-changeset` crate

```
hadb-changeset/
  src/
    physical.rs   -- .hadbp encode/decode/checksum, generic over PageId trait
    journal.rs    -- .hadbj encode/decode/checksum
    discovery.rs  -- S3 key layout, discover_after, generation management
    apply.rs      -- generic physical apply (pwrite at page_id * page_size)
```

All engines share:
- **S3 key layout**: `{prefix}{db_name}/{generation:04x}/{seq:016x}.hadbp`
- **Discovery**: `list_objects_after` for O(new files) catch-up
- **Generations**: 0000 = incrementals, 0001+ = snapshots
- **Checksum chain**: SHA-256(prev || sorted entries), stale lineage detection

Per-engine code shrinks from ~300 lines to ~50 lines of type definitions.

### Migration path

1. Build `hadb-changeset` with .hadbp and .hadbj
2. duckblock adopts .hadbp first (newest, least migration cost)
3. walrust-core migrates .ltx to .hadbp (backward compat: read both, write .hadbp)
4. graphstream migrates .graphj to .hadbj
5. New engines (harock, haduck, etc.) use .hadbp from day one

### When to build

Trigger: third physical replication engine (harock for RocksDB). Until then, duckblock's .seg and walrust's .ltx work fine independently. The S3 coordination is already shared via `hadb-io::ObjectStore`.

## Phase 4: Multi-language SDKs

FFI layer (haqlite-ffi, hakuzu-ffi) + language bindings (Python via PyO3, Node via napi-rs, Go via CGO).

Depends on Phase 1 (stable hadb-io) and Phase 2-3 (proven CLI surface).
