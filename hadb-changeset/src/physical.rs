use sha2::{Digest, Sha256};

use crate::error::ChangesetError;

pub const HADBP_MAGIC: [u8; 5] = *b"HADBP";
/// Version 1: the original live/snapshot changeset format. Unchanged.
pub const HADBP_VERSION: u8 = 1;
/// Version 2: a COMPACTED changeset (a merged range of source changesets that
/// declares its end-of-range chain value). See [`FLAG_COMPACTED`].
///
/// The version byte is the compatibility gate: any decoder that only knows
/// version 1 rejects a version-2 file with [`ChangesetError::UnsupportedVersion`]
/// rather than silently misreading it. (Old decoders ignore unknown *flag* bits
/// but reject unknown *versions*, so the version bump — not the flag — is what
/// makes old readers fail loudly on a compacted file.)
pub const HADBP_VERSION_COMPACTED: u8 = 2;
/// Flag bit set in the header `flags` byte of a version-2 COMPACTED changeset.
///
/// Reserved exclusively for the compacted marker; other flag bits (compression,
/// encryption, ...) must not use it. It is only interpreted for version-2 files;
/// version-1 files leave the flags byte fully opaque, exactly as before.
pub const FLAG_COMPACTED: u8 = 0x80;
/// Header: magic(5) + version(1) + flags(1) + page_id_size(1) + page_size(4) + seq(8) + prev_checksum(8) + page_count(4) + created_ms(8) = 40
const HEADER_SIZE: usize = 40;
/// Trailer: checksum(8)
const TRAILER_SIZE: usize = 8;
/// Extra fixed field present only in COMPACTED (version-2) files, right after
/// the fixed header and before the pages: declared_end_checksum(8).
const COMPACTED_FIELD_SIZE: usize = 8;
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
    /// Content checksum of this changeset's own pages (chained from
    /// `header.prev_checksum`). Always the integrity value of the bytes on disk,
    /// for both normal and compacted changesets.
    pub checksum: u64,
    /// Set only for a COMPACTED changeset (a merged range). It DECLARES the
    /// chain value at the end of the merged range — copied from the last source
    /// changeset's own `checksum` — so linkage stays verifiable end to end even
    /// though a merged file's recomputed content checksum can no longer equal
    /// the original chain value at the range's end.
    ///
    /// - `None`  => normal changeset (encoded as version 1, byte-identical to
    ///   the pre-compaction format).
    /// - `Some(_)` => compacted changeset (encoded as version 2 with
    ///   [`FLAG_COMPACTED`] set and the extra field appended after the header).
    ///
    /// Use [`chain_end`] to get the value a successor must chain from; do NOT
    /// read `checksum` directly for linkage.
    pub declared_end_checksum: Option<u64>,
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
            declared_end_checksum: None,
        }
    }

    /// Create a COMPACTED physical changeset (a merged range).
    ///
    /// - `prev_checksum` is the chain value BEFORE the merged range (the
    ///   `prev_checksum` of the first source changeset).
    /// - `pages` are the merged pages; `checksum` is computed over them exactly
    ///   like a normal changeset (content integrity is unchanged).
    /// - `declared_end_checksum` is the chain value at the END of the merged
    ///   range (the last source changeset's own `checksum`); successors must
    ///   chain from this value, obtained via [`chain_end`].
    ///
    /// The header `flags` byte does not carry [`FLAG_COMPACTED`] in memory; the
    /// flag is derived from `declared_end_checksum` at encode time. Other flag
    /// bits may still be set by the caller afterwards.
    pub fn new_compacted(
        seq: u64,
        prev_checksum: u64,
        page_id_size: PageIdSize,
        page_size: u32,
        pages: Vec<PageEntry>,
        declared_end_checksum: u64,
    ) -> Self {
        let mut cs = Self::new(seq, prev_checksum, page_id_size, page_size, pages);
        cs.declared_end_checksum = Some(declared_end_checksum);
        cs
    }

    /// Whether this changeset is COMPACTED (declares an end-of-range chain value).
    pub fn is_compacted(&self) -> bool {
        self.declared_end_checksum.is_some()
    }
}

/// The chain value a successor changeset must use as its `prev_checksum`.
///
/// For a normal changeset this is its content `checksum`. For a COMPACTED
/// changeset it is the DECLARED end-of-range value. Consumers walking a chain
/// must advance with this instead of reading `checksum` directly, so a merged
/// range links to its successor exactly as the original sequence did.
pub fn chain_end(changeset: &PhysicalChangeset) -> u64 {
    changeset
        .declared_end_checksum
        .unwrap_or(changeset.checksum)
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

    // Compacted files bump the version, set the flag bit, and append the
    // declared end-of-range chain value between the header and the pages. The
    // version bump is the compatibility gate (old decoders reject unknown
    // versions loudly); the flag byte is self-description within version 2.
    let compacted = changeset.declared_end_checksum;
    let (version, extra) = match compacted {
        Some(_) => (HADBP_VERSION_COMPACTED, COMPACTED_FIELD_SIZE),
        None => (HADBP_VERSION, 0),
    };
    let flags_byte = match compacted {
        Some(_) => changeset.header.flags | FLAG_COMPACTED,
        None => changeset.header.flags,
    };
    let mut buf = Vec::with_capacity(HEADER_SIZE + extra + body_size + TRAILER_SIZE);

    // Header
    buf.extend_from_slice(&HADBP_MAGIC);
    buf.push(version);
    buf.push(flags_byte);
    buf.push(changeset.header.page_id_size as u8);
    buf.extend_from_slice(&changeset.header.page_size.to_be_bytes());
    buf.extend_from_slice(&changeset.header.seq.to_be_bytes());
    buf.extend_from_slice(&changeset.header.prev_checksum.to_be_bytes());
    buf.extend_from_slice(&changeset.header.page_count.to_be_bytes());
    buf.extend_from_slice(&changeset.header.created_ms.to_be_bytes());

    // Compacted extra field (only when compacted), between header and pages.
    if let Some(declared_end) = compacted {
        buf.extend_from_slice(&declared_end.to_be_bytes());
    }

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

    // Version. This is the compatibility gate: a version this decoder does not
    // recognize fails loudly here rather than being silently misread. A
    // compacted file (version 2) therefore rejects cleanly on any decoder that
    // only knows version 1.
    let version = data[pos];
    let compacted = match version {
        HADBP_VERSION => false,
        HADBP_VERSION_COMPACTED => true,
        other => return Err(ChangesetError::UnsupportedVersion(other)),
    };
    pos += 1;

    // Flags. For version 1 the flags byte is fully opaque (unchanged behavior).
    // For version 2 the COMPACTED bit MUST be set (it is what the version
    // promises); we then strip it so `header.flags` carries only the other
    // bits, keeping the compacted marker solely in `declared_end_checksum`.
    let mut flags = data[pos];
    pos += 1;
    if compacted {
        if flags & FLAG_COMPACTED == 0 {
            return Err(ChangesetError::InvalidFlags(flags));
        }
        flags &= !FLAG_COMPACTED;
    }

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

    // Compacted extra field (only for version 2): the declared end-of-range
    // chain value, sitting between the header and the pages.
    let declared_end_checksum = if compacted {
        if pos + COMPACTED_FIELD_SIZE > data.len() {
            return Err(ChangesetError::Truncated {
                needed: pos + COMPACTED_FIELD_SIZE,
                available: data.len(),
            });
        }
        let v = u64::from_be_bytes(data[pos..pos + 8].try_into().expect("8 bytes"));
        pos += COMPACTED_FIELD_SIZE;
        Some(v)
    } else {
        None
    };

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
        declared_end_checksum,
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

    // --- COMPACTED (version 2) format ---

    /// Golden bytes for a normal (version 1) changeset. Frozen so a future
    /// change that alters the version-1 wire format cannot pass unnoticed:
    /// old files must always encode/decode byte-identically to today.
    const GOLDEN_V1: &[u8] = &[
        0x48, 0x41, 0x44, 0x42, 0x50, 0x01, 0x00, 0x04, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00,
        0x00, 0x00, 0x04, 0xaa, 0xaa, 0xaa, 0xaa, 0x23, 0x32, 0xa1, 0x2a, 0x64, 0x79, 0xa0, 0x6c,
    ];

    /// Golden bytes for a COMPACTED (version 2) changeset: version byte 0x02,
    /// flags 0x80 (FLAG_COMPACTED), and the 8-byte declared_end_checksum
    /// (0xDEADBEEFCAFEBABE) sitting between the header and the page.
    const GOLDEN_V2: &[u8] = &[
        0x48, 0x41, 0x44, 0x42, 0x50, 0x02, 0x80, 0x04, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x02, 0x23, 0x32, 0xa1, 0x2a, 0x64, 0x79, 0xa0, 0x6c, 0x00, 0x00,
        0x00, 0x01, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0xde, 0xad, 0xbe, 0xef, 0xca,
        0xfe, 0xba, 0xbe, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x04, 0xbb, 0xbb, 0xbb, 0xbb,
        0x3a, 0xaf, 0xd4, 0x9e, 0x3e, 0xc0, 0x51, 0x75,
    ];

    fn golden_v1_changeset() -> PhysicalChangeset {
        let mut cs =
            PhysicalChangeset::new(1, 0, PageIdSize::U32, 4096, vec![page_u32(1, 0xAA, 4)]);
        cs.header.created_ms = 0x0102_0304_0506_0708;
        cs
    }

    fn golden_v2_changeset() -> PhysicalChangeset {
        let mut cs = PhysicalChangeset::new_compacted(
            2,
            0x2332_a12a_6479_a06c, // chains from GOLDEN_V1's checksum
            PageIdSize::U32,
            4096,
            vec![page_u32(2, 0xBB, 4)],
            0xDEAD_BEEF_CAFE_BABE,
        );
        cs.header.created_ms = 0x1112_1314_1516_1718;
        cs
    }

    #[test]
    fn golden_v1_bytes_are_frozen() {
        assert_eq!(encode(&golden_v1_changeset()), GOLDEN_V1);
    }

    #[test]
    fn golden_v1_decodes_to_normal_changeset() {
        let decoded = decode(GOLDEN_V1).unwrap();
        assert_eq!(decoded, golden_v1_changeset());
        assert!(!decoded.is_compacted());
        assert_eq!(decoded.declared_end_checksum, None);
        assert_eq!(decoded.header.flags, 0);
    }

    #[test]
    fn golden_v2_bytes_are_frozen() {
        assert_eq!(encode(&golden_v2_changeset()), GOLDEN_V2);
    }

    #[test]
    fn compacted_roundtrip() {
        let cs = golden_v2_changeset();
        let decoded = decode(&encode(&cs)).unwrap();
        assert_eq!(decoded, cs);
        assert!(decoded.is_compacted());
        assert_eq!(decoded.declared_end_checksum, Some(0xDEAD_BEEF_CAFE_BABE));
        // The on-disk flags byte has the compacted bit; the decoded header
        // strips it so header.flags carries only the other bits.
        assert_eq!(decoded.header.flags, 0);
        assert_eq!(GOLDEN_V2[5], HADBP_VERSION_COMPACTED);
        assert_eq!(GOLDEN_V2[6] & FLAG_COMPACTED, FLAG_COMPACTED);
    }

    #[test]
    fn compacted_preserves_other_flag_bits() {
        // Caller sets a non-compacted flag bit; it survives the encode/decode
        // round trip and is not confused with the compacted marker.
        let mut cs = golden_v2_changeset();
        cs.header.flags = 0x03; // e.g. compression + encryption
        let encoded = encode(&cs);
        assert_eq!(encoded[6], 0x03 | FLAG_COMPACTED);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.header.flags, 0x03);
        assert!(decoded.is_compacted());
    }

    #[test]
    fn chain_end_normal_is_checksum() {
        let cs = PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(0, 0xAA, 32)]);
        assert_eq!(chain_end(&cs), cs.checksum);
    }

    #[test]
    fn chain_end_compacted_is_declared_value() {
        let cs = golden_v2_changeset();
        assert_eq!(chain_end(&cs), 0xDEAD_BEEF_CAFE_BABE);
        assert_ne!(chain_end(&cs), cs.checksum);
    }

    #[test]
    fn compacted_content_checksum_still_validates_on_decode() {
        // Content integrity is unchanged: corrupting a page byte is caught by
        // the trailer checksum even in a compacted file.
        let mut encoded = encode(&golden_v2_changeset());
        // last page data byte sits just before the 8-byte trailer.
        let idx = encoded.len() - TRAILER_SIZE - 1;
        encoded[idx] ^= 0xFF;
        assert!(matches!(
            decode(&encoded).unwrap_err(),
            ChangesetError::ChecksumMismatch { .. }
        ));
    }

    /// Chain linkage flows through a compacted changeset in the middle: the
    /// successor chains from the DECLARED end value, not the recomputed content
    /// checksum.
    #[test]
    fn chain_links_through_compacted_middle() {
        let cs1 =
            PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(0, 0x11, 32)]);
        verify_chain(0, &cs1).unwrap();

        // cs2 is a compacted range. Its declared end differs from its own
        // content checksum (that is the whole point of compaction).
        let declared = 0xABCD_1234_5678_9F00u64;
        let cs2 = PhysicalChangeset::new_compacted(
            2,
            chain_end(&cs1),
            PageIdSize::U64,
            262144,
            vec![page_u64(1, 0x22, 32)],
            declared,
        );
        verify_chain(chain_end(&cs1), &cs2).unwrap();
        assert_eq!(chain_end(&cs2), declared);
        assert_ne!(chain_end(&cs2), cs2.checksum);

        // cs3 chains from cs2's declared end.
        let cs3 = PhysicalChangeset::new(
            3,
            chain_end(&cs2),
            PageIdSize::U64,
            262144,
            vec![page_u64(2, 0x33, 32)],
        );
        verify_chain(chain_end(&cs2), &cs3).unwrap();
    }

    /// A tampered declared_end value passes content decode (it is not part of
    /// the content checksum) but breaks the successor's chain check.
    #[test]
    fn tampered_declared_end_breaks_successor() {
        let cs1 =
            PhysicalChangeset::new(1, 0, PageIdSize::U64, 262144, vec![page_u64(0, 0x11, 32)]);
        let honest_declared = 0x0000_1111_2222_3333u64;
        let cs2 = PhysicalChangeset::new_compacted(
            2,
            chain_end(&cs1),
            PageIdSize::U64,
            262144,
            vec![page_u64(1, 0x22, 32)],
            honest_declared,
        );
        // Successor built against the honest declared end.
        let cs3 = PhysicalChangeset::new(
            3,
            honest_declared,
            PageIdSize::U64,
            262144,
            vec![page_u64(2, 0x33, 32)],
        );
        verify_chain(chain_end(&cs2), &cs3).unwrap();

        // Now tamper cs2's declared end. Content still decodes fine...
        let mut tampered = cs2.clone();
        tampered.declared_end_checksum = Some(honest_declared ^ 0xFFFF);
        let re_decoded = decode(&encode(&tampered)).unwrap();
        assert_eq!(re_decoded, tampered);
        // ...but the successor no longer links.
        assert!(matches!(
            verify_chain(chain_end(&tampered), &cs3).unwrap_err(),
            ChangesetError::ChainBroken { .. }
        ));
    }

    /// OLD readers (that only know version 1) must reject a compacted file
    /// LOUDLY rather than silently misreading it. This replicates the exact
    /// pre-compaction version gate to prove it errors on real compacted bytes.
    #[test]
    fn old_v1_only_decoder_rejects_compacted_file() {
        // The pre-change decoder gate, verbatim: reject any version != 1.
        fn decode_v1_only_version_gate(data: &[u8]) -> Result<(), ChangesetError> {
            assert!(data.len() >= 6);
            if data[0..5] != HADBP_MAGIC {
                return Err(ChangesetError::InvalidMagic);
            }
            let version = data[5];
            if version != HADBP_VERSION {
                return Err(ChangesetError::UnsupportedVersion(version));
            }
            Ok(())
        }

        let compacted = encode(&golden_v2_changeset());
        assert_eq!(compacted[5], HADBP_VERSION_COMPACTED);
        assert!(matches!(
            decode_v1_only_version_gate(&compacted).unwrap_err(),
            ChangesetError::UnsupportedVersion(2)
        ));
    }

    #[test]
    fn version2_without_compacted_flag_is_rejected() {
        let mut encoded = encode(&golden_v2_changeset());
        encoded[6] &= !FLAG_COMPACTED; // clear the flag but keep version 2
        assert!(matches!(
            decode(&encoded).unwrap_err(),
            ChangesetError::InvalidFlags(_)
        ));
    }

    #[test]
    fn compacted_truncated_before_declared_field() {
        let encoded = encode(&golden_v2_changeset());
        // Cut off inside/before the 8-byte declared field (right after header).
        assert!(matches!(
            decode(&encoded[..HEADER_SIZE + 4]).unwrap_err(),
            ChangesetError::Truncated { .. }
        ));
    }

    #[test]
    fn unknown_future_version_rejected() {
        let mut encoded = encode(&golden_v1_changeset());
        encoded[5] = 3; // a version neither 1 nor 2
        assert!(matches!(
            decode(&encoded).unwrap_err(),
            ChangesetError::UnsupportedVersion(3)
        ));
    }

    /// FLAG_COMPACTED (0x80) is meaningful ONLY under version 2. A hand-crafted
    /// version-1 file with 0x80 set in its flags byte is NOT a compacted file:
    /// the version gate keeps it a normal changeset and the bit stays an opaque
    /// flag (exactly like any other unknown flag bit on a v1 file). A compacted
    /// changeset is therefore impossible to represent at version 1 — the flag
    /// alone can never smuggle compaction past the version gate.
    #[test]
    fn flag_compacted_under_version1_is_opaque_not_compacted() {
        // Start from frozen v1 bytes and force the compacted bit in the flags
        // byte while leaving the version byte at 1.
        let mut bytes = GOLDEN_V1.to_vec();
        assert_eq!(bytes[5], HADBP_VERSION);
        bytes[6] |= FLAG_COMPACTED; // set 0x80, keep version = 1

        // The trailer checksum covers only prev_checksum + pages (never the
        // flags byte), so flipping a header flag does not disturb content
        // integrity — the file still decodes cleanly.
        let decoded = decode(&bytes).unwrap();
        assert!(
            !decoded.is_compacted(),
            "a version-1 file is never compacted, regardless of flag bits"
        );
        assert_eq!(decoded.declared_end_checksum, None);
        // Under version 1 the flags byte is fully opaque and preserved verbatim.
        assert_eq!(decoded.header.flags, FLAG_COMPACTED);
    }

    /// The complement of the above: a compacted changeset is impossible to
    /// CONSTRUCT at version 1 through the API — declaring an end-of-range value
    /// forces version 2 (and the flag) at encode time. So `FLAG_COMPACTED with
    /// version == 1` is never produced by this crate.
    #[test]
    fn compacted_encode_always_bumps_version_never_v1_flag() {
        let compacted = golden_v2_changeset();
        let encoded = encode(&compacted);
        assert_eq!(encoded[5], HADBP_VERSION_COMPACTED);
        assert_eq!(encoded[6] & FLAG_COMPACTED, FLAG_COMPACTED);
        // There is no code path that sets the flag without also setting v2:
        // the flag is derived from declared_end_checksum, which is the same
        // thing that selects the version.
        let normal = golden_v1_changeset();
        assert_eq!(encode(&normal)[5], HADBP_VERSION);
        assert_eq!(encode(&normal)[6] & FLAG_COMPACTED, 0);
    }
}
