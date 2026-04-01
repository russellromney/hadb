use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::ChangesetError;
use crate::physical::{self, PhysicalChangeset};

/// Apply a physical changeset to a local database file.
///
/// For each page entry, writes data at `page_id * page_size` via positioned writes.
/// Verifies the checksum chain before writing anything (fail-fast).
///
/// Returns the changeset's checksum (for chaining to the next changeset).
pub fn apply_physical(
    db_path: &Path,
    changeset: &PhysicalChangeset,
    expected_prev_checksum: u64,
) -> Result<u64, ChangesetError> {
    // Verify checksum chain before writing anything
    physical::verify_chain(expected_prev_checksum, changeset)?;

    // Empty changeset (no dirty pages) -- valid, just return checksum
    if changeset.pages.is_empty() {
        return Ok(changeset.checksum);
    }

    let page_size = changeset.header.page_size as u64;

    // Open file for writing. Create if it doesn't exist (initial restore).
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(db_path)
        .map_err(ChangesetError::Io)?;

    for page in &changeset.pages {
        let offset = page.page_id.to_u64() * page_size;
        file.seek(SeekFrom::Start(offset))
            .map_err(ChangesetError::Io)?;
        file.write_all(&page.data)
            .map_err(ChangesetError::Io)?;
    }

    file.sync_all().map_err(ChangesetError::Io)?;

    Ok(changeset.checksum)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::{PageEntry, PageId, PageIdSize, PhysicalChangeset};
    use std::io::Read;
    use tempfile::NamedTempFile;

    fn page_u32(id: u32, fill: u8, len: usize) -> PageEntry {
        PageEntry { page_id: PageId::U32(id), data: vec![fill; len] }
    }

    fn page_u64(id: u64, fill: u8, len: usize) -> PageEntry {
        PageEntry { page_id: PageId::U64(id), data: vec![fill; len] }
    }

    #[test]
    fn test_apply_single_changeset() {
        let tmp = NamedTempFile::new().unwrap();
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![
            page_u64(0, 0xAA, 64),
            page_u64(1, 0xBB, 128),
        ]);
        let checksum = apply_physical(tmp.path(), &cs, 0).unwrap();
        assert_eq!(checksum, cs.checksum);

        let mut contents = Vec::new();
        std::fs::File::open(tmp.path()).unwrap().read_to_end(&mut contents).unwrap();
        assert_eq!(&contents[0..64], &vec![0xAA; 64]);
        assert_eq!(&contents[262144..262144 + 128], &vec![0xBB; 128]);
    }

    #[test]
    fn test_apply_chain() {
        let tmp = NamedTempFile::new().unwrap();
        let cs1 = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(0, 0xAA, 64)]);
        let ck1 = apply_physical(tmp.path(), &cs1, 0).unwrap();

        let cs2 = PhysicalChangeset::new(2, ck1, PageIdSize::U64, 262144, vec![page_u64(1, 0xBB, 64)]);
        let ck2 = apply_physical(tmp.path(), &cs2, ck1).unwrap();
        assert_ne!(ck1, ck2);
    }

    #[test]
    fn test_apply_overwrites_page() {
        let tmp = NamedTempFile::new().unwrap();
        let cs1 = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(0, 0xAA, 64)]);
        let ck1 = apply_physical(tmp.path(), &cs1, 0).unwrap();

        let cs2 = PhysicalChangeset::new(2, ck1, PageIdSize::U64, 262144, vec![page_u64(0, 0xBB, 64)]);
        apply_physical(tmp.path(), &cs2, ck1).unwrap();

        let contents = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&contents[0..64], &vec![0xBB; 64]);
    }

    #[test]
    fn test_apply_empty_changeset() {
        let tmp = NamedTempFile::new().unwrap();
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U32, 4096, vec![]);
        let checksum = apply_physical(tmp.path(), &cs, 0).unwrap();
        assert_eq!(checksum, cs.checksum);
    }

    #[test]
    fn test_apply_bad_checksum_no_write() {
        let tmp = NamedTempFile::new().unwrap();
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(0, 0xAA, 64)]);
        let err = apply_physical(tmp.path(), &cs, 999).unwrap_err();
        assert!(matches!(err, ChangesetError::ChainBroken { .. }));
        assert!(std::fs::read(tmp.path()).unwrap().is_empty());
    }

    #[test]
    fn test_apply_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new.duckdb");
        assert!(!path.exists());
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(0, 0xAA, 32)]);
        apply_physical(&path, &cs, 0).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_apply_extends_file() {
        let tmp = NamedTempFile::new().unwrap();
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(10, 0xFF, 64)]);
        apply_physical(tmp.path(), &cs, 0).unwrap();
        let contents = std::fs::read(tmp.path()).unwrap();
        let offset = 10 * 262144;
        assert!(contents.len() >= offset + 64);
        assert_eq!(&contents[offset..offset + 64], &vec![0xFF; 64]);
    }

    #[test]
    fn test_apply_u32_pages_4kb() {
        let tmp = NamedTempFile::new().unwrap();
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U32, 4096, vec![
            page_u32(1, 0xAA, 4096),
            page_u32(2, 0xBB, 4096),
        ]);
        apply_physical(tmp.path(), &cs, 0).unwrap();
        let contents = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&contents[4096..4096 + 4096], &vec![0xAA; 4096]);
        assert_eq!(&contents[8192..8192 + 4096], &vec![0xBB; 4096]);
    }

    #[test]
    fn test_apply_partial_page() {
        let tmp = NamedTempFile::new().unwrap();
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U32, 4096, vec![page_u32(1, 0xDD, 1000)]);
        apply_physical(tmp.path(), &cs, 0).unwrap();
        let contents = std::fs::read(tmp.path()).unwrap();
        assert_eq!(contents.len(), 4096 + 1000);
    }
}
