use anyhow::Result;
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
    let key = format_key(prefix, db_name, GENERATION_INCREMENTAL, changeset.header.seq, ChangesetKind::Physical);
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
    let key = format_key(prefix, db_name, GENERATION_SNAPSHOT, changeset.header.seq, ChangesetKind::Physical);
    let data = physical::encode(changeset);
    storage.put(&key, &data).await
}

/// Download and decode a physical changeset.
pub async fn download_physical(storage: &dyn StorageBackend, key: &str) -> Result<PhysicalChangeset> {
    let data = storage
        .get(key)
        .await?
        .ok_or_else(|| anyhow::anyhow!("changeset key {} not found", key))?;
    physical::decode(&data).map_err(|e| anyhow::anyhow!("failed to decode changeset at {}: {}", key, e))
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

    let keys = storage
        .list(&incr_prefix, Some(&start_after_key))
        .await?;

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
        changesets.push(DiscoveredChangeset {
            key: key.clone(),
            seq,
            kind,
        });
    }

    changesets.sort_by_key(|c| c.seq);
    Ok(changesets)
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
        PhysicalChangeset::new(seq, prev, PageIdSize::U64, 262144, vec![page(seq - 1, seq as u8, 32)])
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

        let key = format_key("test/", "mydb", GENERATION_INCREMENTAL, 1, ChangesetKind::Physical);
        let downloaded = download_physical(&store, &key).await.unwrap();
        assert_eq!(cs, downloaded);
    }

    #[tokio::test]
    async fn test_upload_snapshot_roundtrip() {
        let store = InMemoryObjectStore::new();
        let cs = make_cs(1, 0);

        upload_physical_snapshot(&store, "test/", "mydb", &cs).await.unwrap();

        let key = format_key("test/", "mydb", GENERATION_SNAPSHOT, 1, ChangesetKind::Physical);
        let downloaded = download_physical(&store, &key).await.unwrap();
        assert_eq!(cs, downloaded);
    }

    #[tokio::test]
    async fn test_discover_empty() {
        let store = InMemoryObjectStore::new();
        let results = discover_after(&store, "test/", "mydb", 0, ChangesetKind::Physical).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_discover_after_zero_returns_all() {
        let store = InMemoryObjectStore::new();
        for seq in 1..=5 {
            upload_physical(&store, "test/", "mydb", &make_cs(seq, 0)).await.unwrap();
        }

        let results = discover_after(&store, "test/", "mydb", 0, ChangesetKind::Physical).await.unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].seq, 1);
        assert_eq!(results[4].seq, 5);
    }

    #[tokio::test]
    async fn test_discover_after_partial() {
        let store = InMemoryObjectStore::new();
        for seq in 1..=5 {
            upload_physical(&store, "test/", "mydb", &make_cs(seq, 0)).await.unwrap();
        }

        let results = discover_after(&store, "test/", "mydb", 3, ChangesetKind::Physical).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].seq, 4);
        assert_eq!(results[1].seq, 5);
    }

    #[tokio::test]
    async fn test_discover_sorted() {
        let store = InMemoryObjectStore::new();
        for seq in [5, 2, 4, 1, 3] {
            upload_physical(&store, "test/", "mydb", &make_cs(seq, 0)).await.unwrap();
        }
        let seqs: Vec<u64> = discover_after(&store, "test/", "mydb", 0, ChangesetKind::Physical)
            .await.unwrap().iter().map(|r| r.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_discover_latest_snapshot() {
        let store = InMemoryObjectStore::new();

        assert!(discover_latest_snapshot(&store, "test/", "mydb", ChangesetKind::Physical).await.unwrap().is_none());

        upload_physical_snapshot(&store, "test/", "mydb", &make_cs(1, 0)).await.unwrap();
        upload_physical_snapshot(&store, "test/", "mydb", &make_cs(5, 0)).await.unwrap();

        let found = discover_latest_snapshot(&store, "test/", "mydb", ChangesetKind::Physical).await.unwrap();
        assert_eq!(found.unwrap().seq, 5);
    }

    #[tokio::test]
    async fn test_download_nonexistent() {
        let store = InMemoryObjectStore::new();
        assert!(download_physical(&store, "no/such/key.hadbp").await.is_err());
    }

    #[tokio::test]
    async fn test_discover_ignores_junk_keys() {
        let store = InMemoryObjectStore::new();
        upload_physical(&store, "test/", "mydb", &make_cs(1, 0)).await.unwrap();
        store.insert("test/mydb/0000/readme.txt", vec![0u8; 10]).await;
        store.insert("test/mydb/0000/not-hex.hadbp", vec![0u8; 10]).await;

        let results = discover_after(&store, "test/", "mydb", 0, ChangesetKind::Physical).await.unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_discover_100_changesets() {
        let store = InMemoryObjectStore::new();
        for seq in 1..=100 {
            upload_physical(&store, "test/", "mydb", &make_cs(seq, 0)).await.unwrap();
        }
        let results = discover_after(&store, "test/", "mydb", 50, ChangesetKind::Physical).await.unwrap();
        assert_eq!(results.len(), 50);
        assert_eq!(results[0].seq, 51);
    }

    #[tokio::test]
    async fn test_discover_isolates_databases() {
        let store = InMemoryObjectStore::new();
        let cs_a = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page(0, 0xAA, 32)]);
        let cs_b = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page(0, 0xBB, 32)]);
        upload_physical(&store, "test/", "db_a", &cs_a).await.unwrap();
        upload_physical(&store, "test/", "db_b", &cs_b).await.unwrap();

        let results = discover_after(&store, "test/", "db_a", 0, ChangesetKind::Physical).await.unwrap();
        assert_eq!(results.len(), 1);
        let downloaded = download_physical(&store, &results[0].key).await.unwrap();
        assert_eq!(downloaded.pages[0].data[0], 0xAA);
    }

    #[tokio::test]
    async fn test_discover_isolates_kinds() {
        let store = InMemoryObjectStore::new();
        // Upload a physical changeset
        upload_physical(&store, "test/", "mydb", &make_cs(1, 0)).await.unwrap();
        // Upload a fake journal key manually
        store.insert("test/mydb/0000/0000000000000001.hadbj", vec![0u8; 10]).await;

        // Physical discover should not see the journal file
        let results = discover_after(&store, "test/", "mydb", 0, ChangesetKind::Physical).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].kind, ChangesetKind::Physical);
    }

    #[tokio::test]
    async fn test_prefix_with_slashes() {
        let store = InMemoryObjectStore::new();
        upload_physical(&store, "ha/prod/", "my.db", &make_cs(1, 0)).await.unwrap();
        let results = discover_after(&store, "ha/prod/", "my.db", 0, ChangesetKind::Physical).await.unwrap();
        assert_eq!(results.len(), 1);
    }
}
