use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChangesetError {
    #[error("invalid magic bytes")]
    InvalidMagic,

    #[error("unsupported version: {0}")]
    UnsupportedVersion(u8),

    #[error("checksum mismatch: expected {expected:#018x}, got {actual:#018x}")]
    ChecksumMismatch { expected: u64, actual: u64 },

    #[error("chain broken: changeset prev_checksum {changeset_prev:#018x} does not match expected {expected:#018x}")]
    ChainBroken { expected: u64, changeset_prev: u64 },

    #[error("size mismatch: expected {needed} bytes, have {available}")]
    Truncated { needed: usize, available: usize },

    #[error("page data length {data_len} exceeds page size {page_size}")]
    PageTooLarge { data_len: u32, page_size: u32 },

    #[error("invalid page_id_size: {0} (expected 4 or 8)")]
    InvalidPageIdSize(u8),

    #[error("page_size mismatch: header says {header}, expected {expected}")]
    PageSizeMismatch { header: u32, expected: u32 },

    #[error("invalid flags: {0:#04x}")]
    InvalidFlags(u8),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}
