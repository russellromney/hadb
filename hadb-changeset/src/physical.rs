use sha2::{Digest, Sha256};

use crate::error::ChangesetError;

pub const HADBP_MAGIC: [u8; 5] = *b"HADBP";
pub const HADBP_VERSION: u8 = 1;
/// Header: magic(5) + version(1) + flags(1) + page_id_size(1) + page_size(4) + seq(8) + prev_checksum(8) + page_count(4) + created_ms(8) = 40
const HEADER_SIZE: usize = 40;
/// Trailer: checksum(8)
const TRAILER_SIZE: usize = 8;
/// Minimum encoded size: header + trailer
const MIN_SIZE: usize = HEADER_SIZE + TRAILER_SIZE;

/// Page ID byte width. Stored in the header so the format is self-describing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageIdSize {
    /// 4 bytes (u32). Used by SQLite.
    U32 = 4,
    /// 8 bytes (u64). Used by DuckDB.
    U64 = 8,
}

impl PageIdSize {
    fn from_byte(b: u8) -> Result<Self, ChangesetError> {
        match b {
            4 => Ok(Self::U32),
            8 => Ok(Self::U64),
            _ => Err(ChangesetError::InvalidPageIdSize(b)),
        }
    }

    fn byte_len(self) -> usize {
        self as usize
    }
}

/// A page ID that can be either u32 or u64.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageId {
    U32(u32),
    U64(u64),
}

impl PageId {
    pub fn to_u64(self) -> u64 {
        match self {
            PageId::U32(v) => v as u64,
            PageId::U64(v) => v,
        }
    }
}

impl PartialOrd for PageId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PageId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.to_u64().cmp(&other.to_u64())
    }
}

/// Header for a physical changeset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalHeader {
    pub flags: u8,
    pub page_id_size: PageIdSize,
    pub page_size: u32,
    pub seq: u64,
    pub prev_checksum: u64,
    pub page_count: u32,
    /// Milliseconds since Unix epoch when this changeset was created.
    /// Used for debugging, retention policies, and diagnostics.
    pub created_ms: i64,
}

/// A single page entry within a changeset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageEntry {
    pub page_id: PageId,
    pub data: Vec<u8>,
}

/// A complete physical changeset: header + pages + checksum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalChangeset {
    pub header: PhysicalHeader,
    pub pages: Vec<PageEntry>,
    pub checksum: u64,
}

impl PhysicalChangeset {
    /// Create a new physical changeset. Pages are sorted by page_id for determinism.
    ///
    /// Panics if any page_id variant doesn't match the declared page_id_size.
    pub fn new(
        seq: u64,
        prev_checksum: u64,
        page_id_size: PageIdSize,
        page_size: u32,
        mut pages: Vec<PageEntry>,
    ) -> Self {
        // Validate all page IDs match the declared size
        for page in &pages {
            match (page_id_size, &page.page_id) {
                (PageIdSize::U32, PageId::U32(_)) | (PageIdSize::U64, PageId::U64(_)) => {}
                (expected, got) => panic!(
                    "page_id variant mismatch: declared {:?} but got {:?}",
                    expected, got
                ),
            }
        }

        pages.sort_by_key(|p| p.page_id);
        let checksum = compute_checksum(prev_checksum, page_id_size, &pages);
        let created_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        Self {
            header: PhysicalHeader {
                flags: 0,
                page_id_size,
                page_size,
                seq,
                prev_checksum,
                page_count: pages.len() as u32,
                created_ms,
            },
            pages,
            checksum,
        }
    }
}

/// Compute checksum for a physical changeset.
/// SHA-256(prev_checksum_be || page_id_be || data_len_be || data ...) truncated to u64.
/// Pages are sorted by page_id for determinism.
pub fn compute_checksum(prev_checksum: u64, page_id_size: PageIdSize, pages: &[PageEntry]) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(prev_checksum.to_be_bytes());

    let mut sorted_indices: Vec<usize> = (0..pages.len()).collect();
    sorted_indices.sort_by_key(|&i| pages[i].page_id);

    for &i in &sorted_indices {
        // Write page_id as the correct width
        match page_id_size {
            PageIdSize::U32 => {
                let id = pages[i].page_id.to_u64() as u32;
                hasher.update(id.to_be_bytes());
            }
            PageIdSize::U64 => {
                hasher.update(pages[i].page_id.to_u64().to_be_bytes());
            }
        }
        hasher.update((pages[i].data.len() as u32).to_be_bytes());
        hasher.update(&pages[i].data);
    }

    let result = hasher.finalize();
    u64::from_be_bytes(result[0..8].try_into().expect("sha256 is 32 bytes"))
}

/// Verify that a changeset's checksum matches the expected chain.
pub fn verify_chain(
    expected_prev_checksum: u64,
    changeset: &PhysicalChangeset,
) -> Result<(), ChangesetError> {
    if changeset.header.prev_checksum != expected_prev_checksum {
        return Err(ChangesetError::ChainBroken {
            expected: expected_prev_checksum,
            changeset_prev: changeset.header.prev_checksum,
        });
    }
    let computed = compute_checksum(
        expected_prev_checksum,
        changeset.header.page_id_size,
        &changeset.pages,
    );
    if computed != changeset.checksum {
        return Err(ChangesetError::ChecksumMismatch {
            expected: changeset.checksum,
            actual: computed,
        });
    }
    Ok(())
}

/// Encode a physical changeset into binary format.
pub fn encode(changeset: &PhysicalChangeset) -> Vec<u8> {
    let pid_len = changeset.header.page_id_size.byte_len();
    let body_size: usize = changeset
        .pages
        .iter()
        .map(|p| pid_len + 4 + p.data.len())
        .sum();
    let mut buf = Vec::with_capacity(HEADER_SIZE + body_size + TRAILER_SIZE);

    // Header
    buf.extend_from_slice(&HADBP_MAGIC);
    buf.push(HADBP_VERSION);
    buf.push(changeset.header.flags);
    buf.push(changeset.header.page_id_size as u8);
    buf.extend_from_slice(&changeset.header.page_size.to_be_bytes());
    buf.extend_from_slice(&changeset.header.seq.to_be_bytes());
    buf.extend_from_slice(&changeset.header.prev_checksum.to_be_bytes());
    buf.extend_from_slice(&changeset.header.page_count.to_be_bytes());
    buf.extend_from_slice(&changeset.header.created_ms.to_be_bytes());

    // Pages (sorted by page_id)
    let mut sorted_indices: Vec<usize> = (0..changeset.pages.len()).collect();
    sorted_indices.sort_by_key(|&i| changeset.pages[i].page_id);

    for &i in &sorted_indices {
        let page = &changeset.pages[i];
        match page.page_id {
            PageId::U32(v) => buf.extend_from_slice(&v.to_be_bytes()),
            PageId::U64(v) => buf.extend_from_slice(&v.to_be_bytes()),
        }
        buf.extend_from_slice(&(page.data.len() as u32).to_be_bytes());
        buf.extend_from_slice(&page.data);
    }

    // Checksum
    buf.extend_from_slice(&changeset.checksum.to_be_bytes());

    buf
}

/// Decode a physical changeset from binary data.
/// Validates magic, version, and recomputes checksum.
pub fn decode(data: &[u8]) -> Result<PhysicalChangeset, ChangesetError> {
    if data.len() < MIN_SIZE {
        return Err(ChangesetError::Truncated {
            needed: MIN_SIZE,
            available: data.len(),
        });
    }

    let mut pos = 0;

    // Magic
    if &data[pos..pos + 5] != &HADBP_MAGIC {
        return Err(ChangesetError::InvalidMagic);
    }
    pos += 5;

    // Version
    let version = data[pos];
    if version != HADBP_VERSION {
        return Err(ChangesetError::UnsupportedVersion(version));
    }
    pos += 1;

    // Flags
    let flags = data[pos];
    pos += 1;

    // Page ID size
    let page_id_size = PageIdSize::from_byte(data[pos])?;
    pos += 1;

    // Page size
    let page_size = u32::from_be_bytes(data[pos..pos + 4].try_into().expect("4 bytes"));
    pos += 4;

    // Seq
    let seq = u64::from_be_bytes(data[pos..pos + 8].try_into().expect("8 bytes"));
    pos += 8;

    // Prev checksum
    let prev_checksum = u64::from_be_bytes(data[pos..pos + 8].try_into().expect("8 bytes"));
    pos += 8;

    // Page count
    let page_count = u32::from_be_bytes(data[pos..pos + 4].try_into().expect("4 bytes"));
    pos += 4;

    // Created timestamp
    let created_ms = i64::from_be_bytes(data[pos..pos + 8].try_into().expect("8 bytes"));
    pos += 8;

    // Pages
    let pid_len = page_id_size.byte_len();
    let mut pages = Vec::with_capacity(page_count as usize);

    for _ in 0..page_count {
        // Need pid_len + 4 (data_len)
        if pos + pid_len + 4 > data.len() {
            return Err(ChangesetError::Truncated {
                needed: pos + pid_len + 4,
                available: data.len(),
            });
        }

        let page_id = match page_id_size {
            PageIdSize::U32 => {
                let v = u32::from_be_bytes(data[pos..pos + 4].try_into().expect("4 bytes"));
                PageId::U32(v)
            }
            PageIdSize::U64 => {
                let v = u64::from_be_bytes(data[pos..pos + 8].try_into().expect("8 bytes"));
                PageId::U64(v)
            }
        };
        pos += pid_len;

        let data_len = u32::from_be_bytes(data[pos..pos + 4].try_into().expect("4 bytes"));
        pos += 4;

        if data_len > page_size {
            return Err(ChangesetError::PageTooLarge {
                data_len,
                page_size,
            });
        }

        if pos + data_len as usize > data.len() {
            return Err(ChangesetError::Truncated {
                needed: pos + data_len as usize,
                available: data.len(),
            });
        }
        let page_data = data[pos..pos + data_len as usize].to_vec();
        pos += data_len as usize;

        pages.push(PageEntry {
            page_id,
            data: page_data,
        });
    }

    // Checksum
    if pos + 8 > data.len() {
        return Err(ChangesetError::Truncated {
            needed: pos + 8,
            available: data.len(),
        });
    }
    let stored_checksum = u64::from_be_bytes(data[pos..pos + 8].try_into().expect("8 bytes"));
    pos += 8;

    // Reject trailing bytes
    if pos != data.len() {
        return Err(ChangesetError::Truncated {
            needed: pos,
            available: data.len(),
        });
    }

    // Verify checksum
    let computed_checksum = compute_checksum(prev_checksum, page_id_size, &pages);
    if computed_checksum != stored_checksum {
        return Err(ChangesetError::ChecksumMismatch {
            expected: stored_checksum,
            actual: computed_checksum,
        });
    }

    Ok(PhysicalChangeset {
        header: PhysicalHeader {
            flags,
            page_id_size,
            page_size,
            seq,
            prev_checksum,
            page_count,
            created_ms,
        },
        pages,
        checksum: stored_checksum,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page_u32(id: u32, fill: u8, len: usize) -> PageEntry {
        PageEntry {
            page_id: PageId::U32(id),
            data: vec![fill; len],
        }
    }

    fn page_u64(id: u64, fill: u8, len: usize) -> PageEntry {
        PageEntry {
            page_id: PageId::U64(id),
            data: vec![fill; len],
        }
    }

    // --- Happy path ---

    #[test]
    fn test_encode_decode_roundtrip_u32() {
        let pages = vec![
            page_u32(1, 0xAA, 256),
            page_u32(2, 0xBB, 512),
            page_u32(5, 0xCC, 128),
        ];
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U32, 4096, pages);
        let encoded = encode(&cs);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(cs, decoded);
        assert_eq!(decoded.header.page_id_size, PageIdSize::U32);
    }

    #[test]
    fn test_encode_decode_roundtrip_u64() {
        let pages = vec![
            page_u64(0, 0xAA, 256),
            page_u64(1, 0xBB, 512),
            page_u64(5, 0xCC, 128),
        ];
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, pages);
        let encoded = encode(&cs);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(cs, decoded);
        assert_eq!(decoded.header.page_id_size, PageIdSize::U64);
    }

    #[test]
    fn test_single_page() {
        let cs = PhysicalChangeset::new(
            42,
            12345,
            PageIdSize::U32,
            4096,
            vec![page_u32(7, 0xFF, 100)],
        );
        let decoded = decode(&encode(&cs)).unwrap();
        assert_eq!(decoded.header.seq, 42);
        assert_eq!(decoded.header.prev_checksum, 12345);
        assert_eq!(decoded.pages.len(), 1);
        assert_eq!(decoded.pages[0].page_id, PageId::U32(7));
    }

    #[test]
    fn test_checksum_chain_valid() {
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(0, 0xAA, 64)]);
        verify_chain(0, &cs).unwrap();
    }

    #[test]
    fn test_sequential_chain() {
        let cs1 =
            PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(0, 0xAA, 64)]);
        verify_chain(0, &cs1).unwrap();

        let cs2 = PhysicalChangeset::new(
            2,
            cs1.checksum,
            PageIdSize::U64,
            262144,
            vec![page_u64(1, 0xBB, 64)],
        );
        verify_chain(cs1.checksum, &cs2).unwrap();
    }

    #[test]
    fn test_three_changeset_chain() {
        let cs1 = PhysicalChangeset::new(1, 0, PageIdSize::U32, 4096, vec![page_u32(1, 0x11, 32)]);
        let cs2 = PhysicalChangeset::new(
            2,
            cs1.checksum,
            PageIdSize::U32,
            4096,
            vec![page_u32(2, 0x22, 32), page_u32(3, 0x33, 32)],
        );
        let cs3 = PhysicalChangeset::new(
            3,
            cs2.checksum,
            PageIdSize::U32,
            4096,
            vec![page_u32(1, 0x44, 32)],
        );

        verify_chain(0, &cs1).unwrap();
        verify_chain(cs1.checksum, &cs2).unwrap();
        verify_chain(cs2.checksum, &cs3).unwrap();
    }

    #[test]
    fn test_page_id_size_preserved() {
        let cs_u32 =
            PhysicalChangeset::new(1, 0, PageIdSize::U32, 4096, vec![page_u32(1, 0xAA, 32)]);
        let cs_u64 =
            PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(1, 0xAA, 32)]);

        assert_eq!(
            decode(&encode(&cs_u32)).unwrap().header.page_id_size,
            PageIdSize::U32
        );
        assert_eq!(
            decode(&encode(&cs_u64)).unwrap().header.page_id_size,
            PageIdSize::U64
        );
    }

    // --- Negative ---

    #[test]
    fn test_bad_magic() {
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(0, 0xAA, 64)]);
        let mut encoded = encode(&cs);
        encoded[0] = b'X';
        assert!(matches!(
            decode(&encoded).unwrap_err(),
            ChangesetError::InvalidMagic
        ));
    }

    #[test]
    fn test_bad_version() {
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(0, 0xAA, 64)]);
        let mut encoded = encode(&cs);
        encoded[5] = 99;
        assert!(matches!(
            decode(&encoded).unwrap_err(),
            ChangesetError::UnsupportedVersion(99)
        ));
    }

    #[test]
    fn test_checksum_mismatch() {
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(0, 0xAA, 64)]);
        let mut encoded = encode(&cs);
        let data_offset = HEADER_SIZE + 8 + 4; // past header + page_id(8) + data_len(4)
        encoded[data_offset] ^= 0xFF;
        assert!(matches!(
            decode(&encoded).unwrap_err(),
            ChangesetError::ChecksumMismatch { .. }
        ));
    }

    #[test]
    fn test_truncated_header() {
        assert!(matches!(
            decode(&[0u8; 10]).unwrap_err(),
            ChangesetError::Truncated { .. }
        ));
    }

    #[test]
    fn test_truncated_page_data() {
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(0, 0xAA, 64)]);
        let encoded = encode(&cs);
        assert!(matches!(
            decode(&encoded[..HEADER_SIZE + 5]).unwrap_err(),
            ChangesetError::Truncated { .. }
        ));
    }

    #[test]
    fn test_chain_broken() {
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(0, 0xAA, 64)]);
        assert!(matches!(
            verify_chain(999, &cs).unwrap_err(),
            ChangesetError::ChainBroken { .. }
        ));
    }

    #[test]
    fn test_invalid_page_id_size() {
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(0, 0xAA, 64)]);
        let mut encoded = encode(&cs);
        encoded[7] = 3; // invalid: not 4 or 8
        assert!(matches!(
            decode(&encoded).unwrap_err(),
            ChangesetError::InvalidPageIdSize(3)
        ));
    }

    #[test]
    fn test_page_too_large() {
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(0, 0xAA, 64)]);
        let mut encoded = encode(&cs);
        // Overwrite data_len to exceed page_size
        let data_len_offset = HEADER_SIZE + 8; // past header + page_id(8)
        let huge: u32 = 262144 + 1;
        encoded[data_len_offset..data_len_offset + 4].copy_from_slice(&huge.to_be_bytes());
        assert!(matches!(
            decode(&encoded).unwrap_err(),
            ChangesetError::PageTooLarge { .. }
        ));
    }

    #[test]
    fn test_trailing_bytes() {
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(0, 0xAA, 64)]);
        let mut encoded = encode(&cs);
        encoded.push(0xFF);
        assert!(matches!(
            decode(&encoded).unwrap_err(),
            ChangesetError::Truncated { .. }
        ));
    }

    // --- Edge cases ---

    #[test]
    fn test_empty_changeset() {
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U32, 4096, vec![]);
        let decoded = decode(&encode(&cs)).unwrap();
        assert_eq!(decoded.pages.len(), 0);
        verify_chain(0, &decoded).unwrap();
    }

    #[test]
    fn test_large_changeset() {
        let pages: Vec<PageEntry> = (0..1000)
            .map(|i| page_u64(i, (i % 256) as u8, 64))
            .collect();
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, pages);
        let decoded = decode(&encode(&cs)).unwrap();
        assert_eq!(decoded.pages.len(), 1000);
        verify_chain(0, &decoded).unwrap();
    }

    #[test]
    fn test_partial_page() {
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U32, 4096, vec![page_u32(1, 0xAA, 1024)]);
        let decoded = decode(&encode(&cs)).unwrap();
        assert_eq!(decoded.pages[0].data.len(), 1024);
    }

    #[test]
    fn test_full_size_page_u32() {
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U32, 4096, vec![page_u32(1, 0xAA, 4096)]);
        let decoded = decode(&encode(&cs)).unwrap();
        assert_eq!(decoded.pages[0].data.len(), 4096);
    }

    #[test]
    fn test_full_size_page_u64() {
        let cs = PhysicalChangeset::new(
            1,
            0,
            PageIdSize::U64,
            262144,
            vec![page_u64(0, 0xBB, 262144)],
        );
        let decoded = decode(&encode(&cs)).unwrap();
        assert_eq!(decoded.pages[0].data.len(), 262144);
    }

    #[test]
    fn test_page_ordering_determinism() {
        let asc = vec![page_u64(0, 0xAA, 32), page_u64(1, 0xBB, 32)];
        let desc = vec![page_u64(1, 0xBB, 32), page_u64(0, 0xAA, 32)];

        let cs1 = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, asc);
        let cs2 = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, desc);
        assert_eq!(encode(&cs1), encode(&cs2));
    }

    #[test]
    fn test_duplicate_page_ids() {
        let pages = vec![page_u64(0, 0xAA, 32), page_u64(0, 0xBB, 32)];
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, pages);
        assert_eq!(cs.pages.len(), 2);
        let decoded = decode(&encode(&cs)).unwrap();
        assert_eq!(decoded.pages.len(), 2);
        verify_chain(0, &decoded).unwrap();
    }

    #[test]
    fn test_zero_length_page_data() {
        let cs = PhysicalChangeset::new(
            1,
            0,
            PageIdSize::U32,
            4096,
            vec![PageEntry {
                page_id: PageId::U32(1),
                data: vec![],
            }],
        );
        let decoded = decode(&encode(&cs)).unwrap();
        assert_eq!(decoded.pages[0].data.len(), 0);
        verify_chain(0, &decoded).unwrap();
    }

    #[test]
    fn test_unsorted_pages_sorted_on_new() {
        let pages = vec![
            page_u64(5, 0xCC, 32),
            page_u64(0, 0xAA, 32),
            page_u64(3, 0xBB, 32),
        ];
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, pages);
        assert_eq!(cs.pages[0].page_id, PageId::U64(0));
        assert_eq!(cs.pages[1].page_id, PageId::U64(3));
        assert_eq!(cs.pages[2].page_id, PageId::U64(5));
        assert_eq!(cs, decode(&encode(&cs)).unwrap());
    }

    #[test]
    fn test_different_data_different_checksum() {
        let cs1 = compute_checksum(0, PageIdSize::U64, &[page_u64(0, 0xAA, 32)]);
        let cs2 = compute_checksum(0, PageIdSize::U64, &[page_u64(0, 0xBB, 32)]);
        assert_ne!(cs1, cs2);
    }

    #[test]
    fn test_different_prev_different_checksum() {
        let pages = vec![page_u64(0, 0xAA, 32)];
        let cs1 = compute_checksum(0, PageIdSize::U64, &pages);
        let cs2 = compute_checksum(1, PageIdSize::U64, &pages);
        assert_ne!(cs1, cs2);
    }

    #[test]
    fn test_u32_max_page_id() {
        let cs = PhysicalChangeset::new(
            1,
            0,
            PageIdSize::U32,
            4096,
            vec![page_u32(u32::MAX, 0xAA, 16)],
        );
        let decoded = decode(&encode(&cs)).unwrap();
        assert_eq!(decoded.pages[0].page_id, PageId::U32(u32::MAX));
    }

    #[test]
    fn test_u64_max_page_id() {
        let cs = PhysicalChangeset::new(
            1,
            0,
            PageIdSize::U64,
            262144,
            vec![page_u64(u64::MAX, 0xBB, 16)],
        );
        let decoded = decode(&encode(&cs)).unwrap();
        assert_eq!(decoded.pages[0].page_id, PageId::U64(u64::MAX));
    }

    #[test]
    fn test_different_page_id_size_different_checksum() {
        // Same numeric page ID and data, different PageIdSize should produce different checksums
        // because the byte width of the page_id in the hash input differs
        let cs_u32 = compute_checksum(0, PageIdSize::U32, &[page_u32(1, 0xAA, 32)]);
        let cs_u64 = compute_checksum(0, PageIdSize::U64, &[page_u64(1, 0xAA, 32)]);
        assert_ne!(cs_u32, cs_u64);
    }

    #[test]
    fn test_page_size_preserved() {
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U32, 8192, vec![page_u32(1, 0xAA, 32)]);
        let decoded = decode(&encode(&cs)).unwrap();
        assert_eq!(decoded.header.page_size, 8192);
    }

    #[test]
    #[should_panic(expected = "page_id variant mismatch")]
    fn test_mixed_page_id_variants_panics() {
        // Mixing U32 and U64 page IDs in a U32 changeset should panic
        let pages = vec![
            PageEntry {
                page_id: PageId::U32(1),
                data: vec![0xAA; 32],
            },
            PageEntry {
                page_id: PageId::U64(2),
                data: vec![0xBB; 32],
            },
        ];
        PhysicalChangeset::new(1, 0, PageIdSize::U32, 4096, pages);
    }

    #[test]
    fn test_flags_roundtrip() {
        // Create changeset with non-zero flags (reserved bits)
        let mut cs =
            PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(0, 0xAA, 32)]);
        cs.header.flags = 0x03; // simulate compression + encryption flags

        let encoded = encode(&cs);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.header.flags, 0x03);
    }

    #[test]
    fn test_timestamp_preserved() {
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(0, 0xAA, 32)]);
        assert!(cs.header.created_ms > 0, "timestamp should be set by new()");

        let decoded = decode(&encode(&cs)).unwrap();
        assert_eq!(decoded.header.created_ms, cs.header.created_ms);
    }
}
