# turbodb Specification

Full implementation spec for S3-tiered embedded database storage. See [README.md](README.md) for the high-level overview.

## S3 Layout

```
s3://bucket/prefix/
  manifest.json              Atomic commit point (JSON, not encrypted)
  pg/0_v1                    Page group 0, version 1 (compressed, optionally encrypted)
  pg/0_v2                    Page group 0, version 2 (after update)
  pg/1_v1                    Page group 1, version 1
  ...
```

### Manifest schema

```json
{
  "version": 1,
  "pageCount": 102400,
  "pageSize": 4096,
  "pagesPerGroup": 2048,
  "subPagesPerFrame": 4,
  "encrypted": false,
  "pageGroupKeys": ["prefix/pg/0_v1", "prefix/pg/1_v1", ...],
  "frameTables": [
    [{"offset": 0, "len": 16384, "pageCount": 4}, ...],
    ...
  ]
}
```

- `pageGroupKeys[i]` is the immutable S3 key for page group `i`. Never overwritten; new versions get new keys.
- `frameTables[i]` contains byte-offset entries for seekable range GETs within page group `i`. Empty means legacy single-frame format.
- `encrypted` flag signals that page groups are GCM-encrypted. The key is never stored.
- The manifest itself is not encrypted (it contains no page data).

### Versioning and GC

Page groups are immutable. On checkpoint, dirty groups get new versioned keys (`pg/0_v2`). The manifest atomically points to the new keys. Old keys (`pg/0_v1`) become garbage. A background GC pass lists S3 objects and deletes any not referenced by the current manifest.

## Page Group Encoding

### Layout

Pages are grouped into fixed-size chunks. Recommended sizes (uncompressed, typically compress to 20-30% with zstd on columnar data):

| Database | Page size | Pages/group | Group size (raw) |
|---|---|---|---|
| SQLite (turbolite) | 4-64KB | 4096 | 16-256MB |
| Kuzu (turbograph) | 4KB | 4096 | 16MB |
| DuckDB (turboduck) | 256KB | 256 | 64MB |

### Seekable multi-frame encoding

Each page group is encoded as multiple seekable sub-frames for targeted range GETs:

```
S3 object:
  [frame 0: zstd(pages 0-3)]
  [frame 1: zstd(pages 4-7)]
  ...
  [frame N: zstd(pages remaining)]
```

Default: 4 pages per frame (~16KB uncompressed at 4KB page size).

The frame table in the manifest stores byte offsets and lengths for each frame. On a cache miss for a single page, the implementation fetches only the containing frame via an S3 range GET, not the entire group.

### Slingshot prefetch

After fetching a single frame (for the requested page), submit the full page group to the background prefetch pool. The group stays in FETCHING state. Other threads requesting pages from the same group wait on a condition variable until the background fetch completes. Pages from the already-fetched frame hit the bitmap and skip the wait.

## Local Cache

### File layout

Single sparse file on local disk. Pages at natural offsets:

```
offset = page_num * page_size
```

No compression on the local cache. Cache hits are a single `pread()` call.

### Bitmap

A bit-per-page bitmap tracks which pages are present in the local cache file. Updated atomically with `pwrite`. Persisted to disk alongside the cache file for crash recovery.

### Group state machine

Per-page-group atomic state for coordinating sync fetch with async prefetch:

```
NONE  ---(tryClaimGroup CAS)--->  FETCHING  ---(markGroupPresent)--->  PRESENT
  ^                                   |
  +-------(markGroupNone: retry)------+
```

- `NONE`: not fetched, not in progress
- `FETCHING`: a thread owns this group (sync reader or background worker)
- `PRESENT`: all pages from this group are in the local cache

Threads that encounter FETCHING wait on a condition variable. On completion, the owner notifies all waiters.

## Prefetch Engine

### Hop-based adaptive prefetch

On consecutive cache misses, progressively fetch more neighboring page groups. Each miss advances through a schedule:

```
Schedule: [hop_0, hop_1, hop_2, ...]
Implicit final hop: fetch everything remaining
```

Two modes (auto-detected by sum of schedule values):

- **Fraction mode** (sum <= 1.0): each value is a fraction of total page groups
- **Absolute mode** (sum > 1.0): each value is an absolute page group count

Example schedules:
- Scan: `[0.3, 0.3, 0.4]` -- 30%, 30%, 40%, then everything
- Lookup: `[0.0, 0.0, 0.0]` -- three free hops (no prefetch), then everything

### Fan-out pattern

Prefetch groups fan out from the current miss location, alternating forward and backward:

```
miss at group 50: prefetch 51, 49, 52, 48, 53, 47, ...
```

Skip groups that are already PRESENT or FETCHING.

### Named schedules

Support at least three named schedules switchable at runtime:
- `scan`: aggressive, for sequential access patterns
- `lookup`: conservative, for point queries
- `default`: balanced fallback

## Per-Table Prefetch Scheduling

### Page-to-table interval map

Parse the database's metadata (catalog, storage manager state) to build a sorted interval map:

```
[startPage, endPage) -> (tableId, tableType)
```

Construction: after the database opens, read metadata pages and extract per-table column page ranges. Sort intervals by startPage for O(log n) binary search lookup.

### Per-table miss counters

Instead of a single global miss counter, maintain one atomic uint8_t per table ID. On cache miss:

1. Look up page's table via interval map
2. Increment that table's miss counter (saturating at 255)
3. Select schedule based on table type (e.g., relationship tables -> scan, node tables -> lookup)
4. Use that table's miss count for the hop index

On cache hit, reset that table's counter to 0.

Pages not in any table (structural, metadata) fall back to the global miss counter and active schedule.

### DB-specific metadata parsing

Each database has its own metadata format. The turbo implementation provides a parser callback that:

1. Takes raw metadata page bytes
2. Walks the serialized storage manager structure
3. Extracts table IDs and their column page ranges
4. Returns a page-to-table interval map

The parser runs during file open, after structural pages are fetched. The generic tiered core doesn't know the format; the DB-specific adapter provides the parser.

## Query Plan Frontrunning

### Concept

Before executing a query, inspect the query plan to determine which tables will be accessed. Proactively prefetch all page groups for those tables before the first page read.

### Implementation

1. Prepare (but don't execute) the query to get the logical plan
2. Walk the plan tree, collecting table IDs from scan and join operators
3. Look up each table's page ranges in the interval map
4. Convert page ranges to page group IDs
5. Submit all unfetched groups to the background prefetch pool
6. Execute the query (pages arrive from cache or are already in-flight)

### DB-specific plan walking

Each database has its own plan representation. The turbo implementation provides a function that takes a query string and returns the set of table IDs it will access. The generic prefetch logic doesn't know the plan format.

## Encryption

### Overview

Two encryption layers, different ciphers for different threat models:

| | Local cache | S3 storage |
|---|---|---|
| Cipher | AES-256-CTR | AES-256-GCM |
| Nonce | Deterministic (page_num) | Random (12 bytes per frame) |
| Authentication | None | GCM auth tag (16 bytes) |
| Size overhead | 0 bytes | 28 bytes per frame |
| Compression | None (cache is uncompressed) | Compress then encrypt |

### Local cache encryption (CTR)

Encrypt pages before `pwrite`, decrypt after `pread`. IV is the page number as 8-byte little-endian, zero-padded to 16 bytes:

```
IV = [page_num as u64 LE][0x00 * 8]    (16 bytes total)
```

CTR mode produces ciphertext exactly the same size as plaintext. No bitmap or offset changes needed.

Why CTR (not GCM) for cache:
- Zero size overhead (page offsets stay at `page_num * page_size`)
- Cache is ephemeral (repopulated from S3 on eviction), so authentication isn't needed
- CTR is symmetric: encrypt = decrypt, same function for both

### S3 encryption (GCM)

Applied per-frame after compression, before upload:

```
plaintext pages -> zstd compress -> AES-256-GCM encrypt -> S3 upload
```

Each frame gets a random 12-byte nonce. The output format:

```
[12-byte nonce][ciphertext][16-byte auth tag]
```

Frame table entries in the manifest include the 28-byte overhead in their `len` field.

On download: extract nonce (first 12 bytes), GCM decrypt (validates auth tag), then zstd decompress.

### Key management

- Single 256-bit (32-byte) key per database, provided at runtime (env var, config, secret manager)
- Key never stored on disk or in S3
- Manifest records `"encrypted": true` flag (not the key). Opening an encrypted database without a key produces a clear error, not a cryptic decryption failure.
- No key rotation in MVP. Future: re-download all objects, re-encrypt with new key, upload new versions, update manifest.

## Selective Cache Eviction

### Page classification

Classify pages into tiers during database construction and first query:

1. **Structural** (highest priority): catalog, metadata, page 0. Fetched eagerly on cold open.
2. **Index**: hash index data, B-tree interior nodes. Populated during first query execution.
3. **Data** (lowest priority): column chunks, edge data, overflow pages. Everything else.

### Eviction levels

Four benchmark/operational levels:

- **Cold**: evict everything. Measures full S3 fetch cost.
- **Interior**: keep structural, evict index + data. Measures index + data fetch cost.
- **Index**: keep structural + index, evict data. Measures data-only fetch cost.
- **Warm**: keep everything. Measures pure compute cost.

## Beacon: Cold-Start Optimization

On cold open (no local cache):

1. Fetch page 0 to verify database magic bytes
2. Parse database header for catalog and metadata page ranges
3. Compute which page groups contain structural pages
4. Submit all structural page groups to prefetch pool
5. Wait for completion before returning from file open

After Beacon, the database constructor finds all catalog and metadata pages in the local cache. Database initialization is fast even on a cold start.

If a metadata parser callback is registered, Beacon also:

6. Read metadata pages from cache into a contiguous buffer
7. Parse to build the page-to-table interval map
8. Install the map for per-table prefetch scheduling

## Implementation Checklist

For a new turbodb implementation (e.g., turboduck for DuckDB):

### Required (core tiered storage)
- [ ] S3 client (PUT, GET, range GET, DELETE, list objects, manifest GET/PUT)
- [ ] Page group codec (encode/decode with zstd, seekable frames)
- [ ] Manifest (JSON serialize/deserialize, version tracking)
- [ ] Page bitmap (bit-per-page presence tracking, persistence)
- [ ] Cache file (sparse file, pread/pwrite at page offsets)
- [ ] Group state machine (NONE/FETCHING/PRESENT with CAS + condition variables)
- [ ] Prefetch pool (background thread pool, job queue, fan-out scheduling)
- [ ] File system adapter (intercept the DB's file I/O, delegate to tiered logic)

### Required (hadb integration)
- [ ] Checkpoint flushes dirty pages to S3 as page groups
- [ ] Manifest update is the replication commit point
- [ ] Replicas open with manifest, fetch on demand

### Recommended (performance)
- [ ] Seekable multi-frame encoding (range GETs for single pages)
- [ ] Slingshot prefetch (frame fetch triggers background full-group fetch)
- [ ] Beacon cold-start optimization (eager structural page fetch)
- [ ] Named prefetch schedules (scan/lookup/default)
- [ ] Selective cache eviction (structural > index > data)

### Recommended (DB-specific intelligence)
- [ ] Metadata parser (page-to-table interval map)
- [ ] Per-table miss counters and schedule selection
- [ ] Query plan frontrunning (plan walk -> bulk prefetch)

### Optional
- [ ] Encryption (CTR for cache, GCM for S3)
- [ ] Compression dictionary (zstd dictionary training on first checkpoint)
- [ ] Key rotation
- [ ] Per-frame group states (replaces slingshot workaround)
