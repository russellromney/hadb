//! Exit status codes and error classification for hadb CLIs.
//!
//! Extracted from walrust's error handling. Each exit code maps to a failure
//! category so operators can script around specific failures.

use std::fmt;
use std::process::ExitCode;

/// Semantic exit codes for hadb CLI tools.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitStatus {
    Success = 0,
    General = 1,
    Config = 2,
    Database = 3,
    S3 = 4,
    Integrity = 5,
    Restore = 6,
}

impl fmt::Display for ExitStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Success => write!(f, "success"),
            Self::General => write!(f, "general error"),
            Self::Config => write!(f, "configuration error"),
            Self::Database => write!(f, "database error"),
            Self::S3 => write!(f, "S3 error"),
            Self::Integrity => write!(f, "integrity error"),
            Self::Restore => write!(f, "restore error"),
        }
    }
}

impl From<ExitStatus> for ExitCode {
    fn from(status: ExitStatus) -> Self {
        ExitCode::from(status as u8)
    }
}

/// Classify an anyhow error into an ExitStatus by pattern-matching the message.
pub fn classify_error(err: &anyhow::Error) -> ExitStatus {
    let msg = format!("{err:#}").to_lowercase();

    if msg.contains("config")
        || msg.contains("toml")
        || msg.contains("missing")
        || msg.contains("invalid")
        || msg.contains("parse")
    {
        return ExitStatus::Config;
    }

    if msg.contains("s3://")
        || msg.contains("s3 ")
        || msg.contains("bucket")
        || msg.contains("credential")
        || msg.contains("access denied")
        || msg.contains("no such key")
        || msg.contains("endpoint")
    {
        return ExitStatus::S3;
    }

    if msg.contains("sqlite")
        || msg.contains("rusqlite")
        || msg.contains("database")
        || msg.contains("wal")
        || msg.contains("journal")
    {
        return ExitStatus::Database;
    }

    if msg.contains("checksum")
        || msg.contains("integrity")
        || msg.contains("corrupt")
        || msg.contains("mismatch")
    {
        return ExitStatus::Integrity;
    }

    if msg.contains("restore") || msg.contains("recovery") {
        return ExitStatus::Restore;
    }

    ExitStatus::General
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exit_status_values() {
        assert_eq!(ExitStatus::Success as u8, 0);
        assert_eq!(ExitStatus::General as u8, 1);
        assert_eq!(ExitStatus::Config as u8, 2);
        assert_eq!(ExitStatus::Database as u8, 3);
        assert_eq!(ExitStatus::S3 as u8, 4);
        assert_eq!(ExitStatus::Integrity as u8, 5);
        assert_eq!(ExitStatus::Restore as u8, 6);
    }

    #[test]
    fn test_classify_config_errors() {
        let err = anyhow::anyhow!("failed to parse TOML config");
        assert_eq!(classify_error(&err), ExitStatus::Config);

        let err = anyhow::anyhow!("missing required field 'bucket'");
        assert_eq!(classify_error(&err), ExitStatus::Config);
    }

    #[test]
    fn test_classify_s3_errors() {
        let err = anyhow::anyhow!("S3 access denied for bucket");
        assert_eq!(classify_error(&err), ExitStatus::S3);

        let err = anyhow::anyhow!("no such key: backups/db/snapshot.ltx");
        assert_eq!(classify_error(&err), ExitStatus::S3);

        let err = anyhow::anyhow!("failed to resolve endpoint");
        assert_eq!(classify_error(&err), ExitStatus::S3);
    }

    #[test]
    fn test_classify_database_errors() {
        let err = anyhow::anyhow!("SQLite error: database is locked");
        assert_eq!(classify_error(&err), ExitStatus::Database);

        let err = anyhow::anyhow!("rusqlite error: unable to open database");
        assert_eq!(classify_error(&err), ExitStatus::Database);
    }

    #[test]
    fn test_classify_integrity_errors() {
        let err = anyhow::anyhow!("checksum mismatch: expected abc got def");
        assert_eq!(classify_error(&err), ExitStatus::Integrity);

        let err = anyhow::anyhow!("file is corrupt: bad header");
        assert_eq!(classify_error(&err), ExitStatus::Integrity);
    }

    #[test]
    fn test_classify_restore_errors() {
        let err = anyhow::anyhow!("restore failed: no snapshots found");
        assert_eq!(classify_error(&err), ExitStatus::Restore);
    }

    #[test]
    fn test_classify_general_errors() {
        let err = anyhow::anyhow!("something unexpected happened");
        assert_eq!(classify_error(&err), ExitStatus::General);
    }

    #[test]
    fn test_exit_code_conversion() {
        let code: ExitCode = ExitStatus::S3.into();
        // ExitCode doesn't expose its value, but we can at least verify it converts
        let _ = code;
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", ExitStatus::Success), "success");
        assert_eq!(format!("{}", ExitStatus::S3), "S3 error");
    }

    #[test]
    fn test_classify_empty_error() {
        let err = anyhow::anyhow!("");
        assert_eq!(classify_error(&err), ExitStatus::General);
    }

    #[test]
    fn test_classify_case_insensitive() {
        // classify_error lowercases — verify uppercase still matches
        let err = anyhow::anyhow!("TOML CONFIG ERROR");
        assert_eq!(classify_error(&err), ExitStatus::Config);

        let err = anyhow::anyhow!("SQLite database locked");
        assert_eq!(classify_error(&err), ExitStatus::Database);
    }

    #[test]
    fn test_classify_precedence_config_before_s3() {
        // "missing bucket" — "missing" matches Config first (it's checked first)
        let err = anyhow::anyhow!("missing bucket");
        assert_eq!(classify_error(&err), ExitStatus::Config);
    }

    #[test]
    fn test_classify_recovery_keyword() {
        let err = anyhow::anyhow!("recovery from backup failed");
        assert_eq!(classify_error(&err), ExitStatus::Restore);
    }

    #[test]
    fn test_classify_wal_as_database() {
        let err = anyhow::anyhow!("WAL file too large");
        assert_eq!(classify_error(&err), ExitStatus::Database);
    }

    #[test]
    fn test_classify_mismatch_as_integrity() {
        let err = anyhow::anyhow!("page count mismatch after apply");
        assert_eq!(classify_error(&err), ExitStatus::Integrity);
    }
}
