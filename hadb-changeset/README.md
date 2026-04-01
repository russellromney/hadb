# hadb-changeset

Unified replication formats for the hadb ecosystem. Provides the binary wire format (`.hadbp`) for shipping database pages between leader and followers over S3.

## Formats

### Physical changeset (`.hadbp`)

A sorted list of database pages with a SHA-256 checksum chain. Self-describing: the header declares the page ID width (u32 for SQLite, u64 for DuckDB) so the same format works across databases.

```
Header (40 bytes):
  magic(5) "HADBP" | version(1) | flags(1) | page_id_size(1)
  page_size(4) | seq(8) | prev_checksum(8) | page_count(4) | created_ms(8)

Pages (variable):
  [page_id(4 or 8) | data_len(4) | data(data_len)] x page_count

Trailer (8 bytes):
  checksum(8)  -- SHA-256(prev || pages) truncated to u64
```

Checksums chain: each changeset's `prev_checksum` must equal the previous changeset's `checksum`. This detects gaps, reordering, and stale lineage after leader failover.

### Journal changeset (`.hadbj`)

Planned. Logical (query-level) changesets for databases like Kuzu where page-level replication isn't practical.

## Usage

```rust
use hadb_changeset::physical::*;

// Create a changeset with SQLite pages (u32 IDs, 4KB pages)
let pages = vec![
    PageEntry { page_id: PageId::U32(1), data: page_1_bytes },
    PageEntry { page_id: PageId::U32(5), data: page_5_bytes },
];
let cs = PhysicalChangeset::new(seq, prev_checksum, PageIdSize::U32, 4096, pages);

// Encode to bytes (for S3 upload)
let bytes = encode(&cs);

// Decode from bytes (after S3 download)
let decoded = decode(&bytes)?;

// Verify checksum chain
verify_chain(expected_prev_checksum, &decoded)?;
```

### Storage helpers

S3 key layout and discovery:

```rust
use hadb_changeset::storage::*;

// Key format: {prefix}{db}/{gen:04x}/{seq:016x}.hadbp
let key = format_key("wal/", "mydb", 0, 1, ChangesetKind::Physical);
// -> "wal/mydb/0000/0000000000000001.hadbp"

// Discover incrementals after seq 5
let new = discover_after(&store, "wal/", "mydb", 5, ChangesetKind::Physical).await?;

// Upload/download
upload_physical(&store, "wal/", "mydb", &changeset).await?;
let cs = download_physical(&store, &key).await?;
```

### Apply

Write pages to a local database file with chain verification:

```rust
use hadb_changeset::apply::apply_physical;

// Verifies chain BEFORE writing (fail-fast)
let new_checksum = apply_physical(db_path, &changeset, expected_prev_checksum)?;
```

## Consumers

- [walrust](https://github.com/russellromney/walrust) -- SQLite WAL replication (PageIdSize::U32, 4KB pages)
- [duckblock](https://github.com/russellromney/duckblock) -- DuckDB block replication (PageIdSize::U64, 256KB blocks)

## License

Apache-2.0
