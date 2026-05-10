use anyhow::{anyhow, Result};
use std::collections::HashSet;

use hadb_storage::StorageBackend;

use crate::physical::{self, PhysicalChangeset};

/// Generation 0 = live incremental changesets.
pub const GENERATION_INCREMENTAL: u64 = 0;
/// Generation 1+ = snapshots (full database as pages).
pub const GENERATION_SNAPSHOT: u64 = 1;

/// Which format: physical (.hadbp) or journal (.hadbj).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangesetKind {
    Physical,
    Journal,
}

impl ChangesetKind {
    pub fn extension(self) -> &'static str {
        match self {
            ChangesetKind::Physical => "hadbp",
            ChangesetKind::Journal => "hadbj",
        }
    }
}

/// A discovered changeset from S3 listing.
#[derive(Debug, Clone)]
pub struct DiscoveredChangeset {
    pub key: String,
    pub seq: u64,
    pub kind: ChangesetKind,
}

/// Authoritative base for strict live physical chain discovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrictChainBase {
    pub seq: u64,
    pub checksum: u64,
}

/// Validated head of a strict live physical chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrictChainHead {
    pub seq: u64,
    pub checksum: u64,
    pub count: usize,
}

/// Format an S3 key for a changeset.
///
/// Layout: `{prefix}{db_name}/{generation:04x}/{seq:016x}.{ext}`
pub fn format_key(
    prefix: &str,
    db_name: &str,
    generation: u64,
    seq: u64,
    kind: ChangesetKind,
) -> String {
    format!(
        "{}{}/{:04x}/{:016x}.{}",
        prefix,
        db_name,
        generation,
        seq,
        kind.extension()
    )
}

/// Upload a physical changeset as an incremental.
pub async fn upload_physical(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    changeset: &PhysicalChangeset,
) -> Result<()> {
    let key = format_key(
        prefix,
        db_name,
        GENERATION_INCREMENTAL,
        changeset.header.seq,
        ChangesetKind::Physical,
    );
    let data = physical::encode(changeset);
    storage.put(&key, &data).await
}

/// Upload a physical changeset as a snapshot.
pub async fn upload_physical_snapshot(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    changeset: &PhysicalChangeset,
) -> Result<()> {
    let key = format_key(
        prefix,
        db_name,
        GENERATION_SNAPSHOT,
        changeset.header.seq,
        ChangesetKind::Physical,
    );
    let data = physical::encode(changeset);
    storage.put(&key, &data).await
}

/// Download and decode a physical changeset.
pub async fn download_physical(
    storage: &dyn StorageBackend,
    key: &str,
) -> Result<PhysicalChangeset> {
    let data = storage
        .get(key)
        .await?
        .ok_or_else(|| anyhow::anyhow!("changeset key {} not found", key))?;
    physical::decode(&data)
        .map_err(|e| anyhow::anyhow!("failed to decode changeset at {}: {}", key, e))
}

/// Discover incremental changesets after a given sequence number.
///
/// Uses `list_objects_after` to efficiently skip past already-applied changesets.
/// Returns changesets sorted by seq (ascending).
pub async fn discover_after(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    after_seq: u64,
    kind: ChangesetKind,
) -> Result<Vec<DiscoveredChangeset>> {
    let ext = kind.extension();
    let incr_prefix = format!("{}{}/{:04x}/", prefix, db_name, GENERATION_INCREMENTAL);
    let start_after_key = format!("{}{:016x}.{}", incr_prefix, after_seq, ext);

    let keys = storage.list(&incr_prefix, Some(&start_after_key)).await?;

    let mut changesets = Vec::new();
    for key in &keys {
        let filename = match key.strip_prefix(&incr_prefix) {
            Some(f) => f,
            None => continue,
        };
        if !filename.ends_with(&format!(".{}", ext)) {
            continue;
        }
        let hex_part = &filename[..filename.len() - ext.len() - 1]; // strip ".{ext}"
        let seq = match u64::from_str_radix(hex_part, 16) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if seq <= after_seq {
            continue;
        }
        changesets.push(DiscoveredChangeset {
            key: key.clone(),
            seq,
            kind,
        });
    }

    changesets.sort_by_key(|c| c.seq);
    Ok(changesets)
}

/// Discover and validate the live physical changeset chain after `base`.
///
/// This is the strict protocol helper for consumers whose base state is owned
/// externally (for example Turbolite). It treats the base plus live generation
/// `.hadbp` objects as the authority:
///
/// - only generation `0000` physical changesets are considered,
/// - the first seq after the base must be `base.seq + 1`,
/// - every later seq must be contiguous,
/// - duplicate listed seqs fail closed,
/// - key seq must match header seq,
/// - every changeset must chain from the prior checksum.
pub async fn discover_strict_physical_chain(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    base: StrictChainBase,
) -> Result<StrictChainHead> {
    let files = discover_after(storage, prefix, db_name, base.seq, ChangesetKind::Physical).await?;

    let mut seen = HashSet::new();
    let mut expected_seq = base.seq + 1;
    let mut expected_checksum = base.checksum;
    let mut count = 0usize;

    for file in files {
        if !seen.insert(file.seq) {
            return Err(anyhow!(
                "{}: duplicate live physical changeset seq {} at {}",
                db_name,
                file.seq,
                file.key
            ));
        }
        if file.seq != expected_seq {
            return Err(anyhow!(
                "{}: non-contiguous live physical changeset sequence: expected {}, found {} at {}",
                db_name,
                expected_seq,
                file.seq,
                file.key
            ));
        }

        let changeset = download_physical(storage, &file.key).await?;
        if changeset.header.seq != file.seq {
            return Err(anyhow!(
                "{}: changeset key/header seq mismatch at {}: key {}, header {}",
                db_name,
                file.key,
                file.seq,
                changeset.header.seq
            ));
        }
        physical::verify_chain(expected_checksum, &changeset).map_err(|e| {
            anyhow!(
                "{}: checksum chain break at seq {} ({}): {}",
                db_name,
                file.seq,
                file.key,
                e
            )
        })?;

        expected_checksum = changeset.checksum;
        expected_seq += 1;
        count += 1;
    }

    Ok(StrictChainHead {
        seq: expected_seq - 1,
        checksum: expected_checksum,
        count,
    })
}

/// Discover the latest snapshot changeset (if any).
pub async fn discover_latest_snapshot(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    kind: ChangesetKind,
) -> Result<Option<DiscoveredChangeset>> {
    let ext = kind.extension();
    let snap_prefix = format!("{}{}/{:04x}/", prefix, db_name, GENERATION_SNAPSHOT);
    let keys = storage.list(&snap_prefix, None).await?;

    let mut latest: Option<DiscoveredChangeset> = None;
    for key in &keys {
        let filename = match key.strip_prefix(&snap_prefix) {
            Some(f) => f,
            None => continue,
        };
        if !filename.ends_with(&format!(".{}", ext)) {
            continue;
        }
        let hex_part = &filename[..filename.len() - ext.len() - 1];
        let seq = match u64::from_str_radix(hex_part, 16) {
            Ok(v) => v,
            Err(_) => continue,
        };
        match &latest {
            Some(prev) if prev.seq >= seq => {}
            _ => {
                latest = Some(DiscoveredChangeset {
                    key: key.clone(),
                    seq,
                    kind,
                });
            }
        }
    }
    Ok(latest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::{PageEntry, PageId, PageIdSize, PhysicalChangeset};
    use crate::test_utils::InMemoryObjectStore;

    fn page(id: u64, fill: u8, len: usize) -> PageEntry {
        PageEntry {
            page_id: PageId::U64(id),
            data: vec![fill; len],
        }
    }

    fn make_cs(seq: u64, prev: u64) -> PhysicalChangeset {
        PhysicalChangeset::new(
            seq,
            prev,
            PageIdSize::U64,
            262144,
            vec![page(seq - 1, seq as u8, 32)],
        )
    }

    #[tokio::test]
    async fn test_format_key_physical() {
        assert_eq!(
            format_key("wal/", "mydb", 0, 1, ChangesetKind::Physical),
            "wal/mydb/0000/0000000000000001.hadbp"
        );
    }

    #[tokio::test]
    async fn test_format_key_journal() {
        assert_eq!(
            format_key("wal/", "mydb", 0, 255, ChangesetKind::Journal),
            "wal/mydb/0000/00000000000000ff.hadbj"
        );
    }

    #[tokio::test]
    async fn test_upload_download_roundtrip() {
        let store = InMemoryObjectStore::new();
        let cs = make_cs(1, 0);

        upload_physical(&store, "test/", "mydb", &cs).await.unwrap();

        let key = format_key(
            "test/",
            "mydb",
            GENERATION_INCREMENTAL,
            1,
            ChangesetKind::Physical,
        );
        let downloaded = download_physical(&store, &key).await.unwrap();
        assert_eq!(cs, downloaded);
    }

    #[tokio::test]
    async fn test_upload_snapshot_roundtrip() {
        let store = InMemoryObjectStore::new();
        let cs = make_cs(1, 0);

        upload_physical_snapshot(&store, "test/", "mydb", &cs)
            .await
            .unwrap();

        let key = format_key(
            "test/",
            "mydb",
            GENERATION_SNAPSHOT,
            1,
            ChangesetKind::Physical,
        );
        let downloaded = download_physical(&store, &key).await.unwrap();
        assert_eq!(cs, downloaded);
    }

    #[tokio::test]
    async fn test_discover_empty() {
        let store = InMemoryObjectStore::new();
        let results = discover_after(&store, "test/", "mydb", 0, ChangesetKind::Physical)
            .await
            .unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_discover_after_zero_returns_all() {
        let store = InMemoryObjectStore::new();
        for seq in 1..=5 {
            upload_physical(&store, "test/", "mydb", &make_cs(seq, 0))
                .await
                .unwrap();
        }

        let results = discover_after(&store, "test/", "mydb", 0, ChangesetKind::Physical)
            .await
            .unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].seq, 1);
        assert_eq!(results[4].seq, 5);
    }

    #[tokio::test]
    async fn test_discover_after_partial() {
        let store = InMemoryObjectStore::new();
        for seq in 1..=5 {
            upload_physical(&store, "test/", "mydb", &make_cs(seq, 0))
                .await
                .unwrap();
        }

        let results = discover_after(&store, "test/", "mydb", 3, ChangesetKind::Physical)
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].seq, 4);
        assert_eq!(results[1].seq, 5);
    }

    #[tokio::test]
    async fn strict_physical_chain_empty_returns_base() {
        let store = InMemoryObjectStore::new();
        let head = discover_strict_physical_chain(
            &store,
            "test/",
            "mydb",
            StrictChainBase {
                seq: 7,
                checksum: 42,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            head,
            StrictChainHead {
                seq: 7,
                checksum: 42,
                count: 0,
            }
        );
    }

    #[tokio::test]
    async fn strict_physical_chain_validates_contiguous_checksum_chain() {
        let store = InMemoryObjectStore::new();
        let base = StrictChainBase {
            seq: 10,
            checksum: 111,
        };
        let cs11 = make_cs(11, base.checksum);
        let cs12 = make_cs(12, cs11.checksum);
        upload_physical(&store, "test/", "mydb", &cs11)
            .await
            .unwrap();
        upload_physical(&store, "test/", "mydb", &cs12)
            .await
            .unwrap();

        let head = discover_strict_physical_chain(&store, "test/", "mydb", base)
            .await
            .unwrap();
        assert_eq!(
            head,
            StrictChainHead {
                seq: 12,
                checksum: cs12.checksum,
                count: 2,
            }
        );
    }

    #[tokio::test]
    async fn strict_physical_chain_rejects_gap() {
        let store = InMemoryObjectStore::new();
        let base = StrictChainBase {
            seq: 10,
            checksum: 111,
        };
        upload_physical(&store, "test/", "mydb", &make_cs(12, base.checksum))
            .await
            .unwrap();

        let err = discover_strict_physical_chain(&store, "test/", "mydb", base)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("non-contiguous"), "{err}");
    }

    #[tokio::test]
    async fn strict_physical_chain_rejects_wrong_prev_checksum() {
        let store = InMemoryObjectStore::new();
        let base = StrictChainBase {
            seq: 10,
            checksum: 111,
        };
        upload_physical(&store, "test/", "mydb", &make_cs(11, 999))
            .await
            .unwrap();

        let err = discover_strict_physical_chain(&store, "test/", "mydb", base)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("checksum chain break"), "{err}");
    }

    #[tokio::test]
    async fn strict_physical_chain_rejects_key_header_seq_mismatch() {
        let store = InMemoryObjectStore::new();
        let base = StrictChainBase {
            seq: 10,
            checksum: 111,
        };
        let wrong = make_cs(99, base.checksum);
        let key = format_key(
            "test/",
            "mydb",
            GENERATION_INCREMENTAL,
            11,
            ChangesetKind::Physical,
        );
        store.put(&key, &physical::encode(&wrong)).await.unwrap();

        let err = discover_strict_physical_chain(&store, "test/", "mydb", base)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("key/header seq mismatch"), "{err}");
    }

    struct DuplicateListingStore<'a>(&'a InMemoryObjectStore);

    #[async_trait::async_trait]
    impl<'a> StorageBackend for DuplicateListingStore<'a> {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            self.0.get(key).await
        }

        async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
            self.0.put(key, data).await
        }

        async fn delete(&self, key: &str) -> Result<()> {
            self.0.delete(key).await
        }

        async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
            let mut keys = self.0.list(prefix, after).await?;
            if let Some(first) = keys.first().cloned() {
                keys.insert(0, first);
            }
            Ok(keys)
        }

        async fn exists(&self, key: &str) -> Result<bool> {
            self.0.exists(key).await
        }

        async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<hadb_storage::CasResult> {
            self.0.put_if_absent(key, data).await
        }

        async fn put_if_match(
            &self,
            key: &str,
            data: &[u8],
            etag: &str,
        ) -> Result<hadb_storage::CasResult> {
            self.0.put_if_match(key, data, etag).await
        }
    }

    #[tokio::test]
    async fn strict_physical_chain_rejects_duplicate_listed_seq() {
        let store = InMemoryObjectStore::new();
        let base = StrictChainBase {
            seq: 10,
            checksum: 111,
        };
        upload_physical(&store, "test/", "mydb", &make_cs(11, base.checksum))
            .await
            .unwrap();
        let duplicate = DuplicateListingStore(&store);

        let err = discover_strict_physical_chain(&duplicate, "test/", "mydb", base)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("duplicate"), "{err}");
    }

    struct AfterBlindStore<'a>(&'a InMemoryObjectStore);

    #[async_trait::async_trait]
    impl<'a> StorageBackend for AfterBlindStore<'a> {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            self.0.get(key).await
        }

        async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
            self.0.put(key, data).await
        }

        async fn delete(&self, key: &str) -> Result<()> {
            self.0.delete(key).await
        }

        async fn list(&self, prefix: &str, _after: Option<&str>) -> Result<Vec<String>> {
            self.0.list(prefix, None).await
        }

        async fn exists(&self, key: &str) -> Result<bool> {
            self.0.exists(key).await
        }

        async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<hadb_storage::CasResult> {
            self.0.put_if_absent(key, data).await
        }

        async fn put_if_match(
            &self,
            key: &str,
            data: &[u8],
            etag: &str,
        ) -> Result<hadb_storage::CasResult> {
            self.0.put_if_match(key, data, etag).await
        }
    }

    #[tokio::test]
    async fn test_discover_filters_stale_keys_when_backend_after_is_advisory() {
        let store = InMemoryObjectStore::new();
        for seq in 1..=5 {
            upload_physical(&store, "test/", "mydb", &make_cs(seq, 0))
                .await
                .unwrap();
        }

        let blind = AfterBlindStore(&store);
        let results = discover_after(&blind, "test/", "mydb", 3, ChangesetKind::Physical)
            .await
            .unwrap();
        let seqs: Vec<_> = results.iter().map(|result| result.seq).collect();
        assert_eq!(seqs, vec![4, 5]);
    }

    #[tokio::test]
    async fn test_discover_sorted() {
        let store = InMemoryObjectStore::new();
        for seq in [5, 2, 4, 1, 3] {
            upload_physical(&store, "test/", "mydb", &make_cs(seq, 0))
                .await
                .unwrap();
        }
        let seqs: Vec<u64> = discover_after(&store, "test/", "mydb", 0, ChangesetKind::Physical)
            .await
            .unwrap()
            .iter()
            .map(|r| r.seq)
            .collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_discover_latest_snapshot() {
        let store = InMemoryObjectStore::new();

        assert!(
            discover_latest_snapshot(&store, "test/", "mydb", ChangesetKind::Physical)
                .await
                .unwrap()
                .is_none()
        );

        upload_physical_snapshot(&store, "test/", "mydb", &make_cs(1, 0))
            .await
            .unwrap();
        upload_physical_snapshot(&store, "test/", "mydb", &make_cs(5, 0))
            .await
            .unwrap();

        let found = discover_latest_snapshot(&store, "test/", "mydb", ChangesetKind::Physical)
            .await
            .unwrap();
        assert_eq!(found.unwrap().seq, 5);
    }

    #[tokio::test]
    async fn test_download_nonexistent() {
        let store = InMemoryObjectStore::new();
        assert!(download_physical(&store, "no/such/key.hadbp")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_discover_ignores_junk_keys() {
        let store = InMemoryObjectStore::new();
        upload_physical(&store, "test/", "mydb", &make_cs(1, 0))
            .await
            .unwrap();
        store
            .insert("test/mydb/0000/readme.txt", vec![0u8; 10])
            .await;
        store
            .insert("test/mydb/0000/not-hex.hadbp", vec![0u8; 10])
            .await;

        let results = discover_after(&store, "test/", "mydb", 0, ChangesetKind::Physical)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_discover_100_changesets() {
        let store = InMemoryObjectStore::new();
        for seq in 1..=100 {
            upload_physical(&store, "test/", "mydb", &make_cs(seq, 0))
                .await
                .unwrap();
        }
        let results = discover_after(&store, "test/", "mydb", 50, ChangesetKind::Physical)
            .await
            .unwrap();
        assert_eq!(results.len(), 50);
        assert_eq!(results[0].seq, 51);
    }

    #[tokio::test]
    async fn test_discover_isolates_databases() {
        let store = InMemoryObjectStore::new();
        let cs_a = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page(0, 0xAA, 32)]);
        let cs_b = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page(0, 0xBB, 32)]);
        upload_physical(&store, "test/", "db_a", &cs_a)
            .await
            .unwrap();
        upload_physical(&store, "test/", "db_b", &cs_b)
            .await
            .unwrap();

        let results = discover_after(&store, "test/", "db_a", 0, ChangesetKind::Physical)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        let downloaded = download_physical(&store, &results[0].key).await.unwrap();
        assert_eq!(downloaded.pages[0].data[0], 0xAA);
    }

    #[tokio::test]
    async fn test_discover_isolates_kinds() {
        let store = InMemoryObjectStore::new();
        // Upload a physical changeset
        upload_physical(&store, "test/", "mydb", &make_cs(1, 0))
            .await
            .unwrap();
        // Upload a fake journal key manually
        store
            .insert("test/mydb/0000/0000000000000001.hadbj", vec![0u8; 10])
            .await;

        // Physical discover should not see the journal file
        let results = discover_after(&store, "test/", "mydb", 0, ChangesetKind::Physical)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].kind, ChangesetKind::Physical);
    }

    #[tokio::test]
    async fn test_prefix_with_slashes() {
        let store = InMemoryObjectStore::new();
        upload_physical(&store, "ha/prod/", "my.db", &make_cs(1, 0))
            .await
            .unwrap();
        let results = discover_after(&store, "ha/prod/", "my.db", 0, ChangesetKind::Physical)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
    }
}
