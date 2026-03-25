//! hadb-cli: Shared CLI framework for hadb database tools.
//!
//! Provides the building blocks for consistent CLIs across all hadb databases
//! (haqlite, hakuzu, haduck, harock). Each product implements `CliBackend` and
//! gets a full CLI for free: serve, restore, list, verify, compact, replicate,
//! snapshot, explain.

pub mod args;
pub mod config;
pub mod errors;
pub mod runner;
pub mod signals;

pub use args::*;
pub use config::{load_config, SharedConfig};
pub use errors::{ExitStatus, classify_error};
pub use runner::{CliBackend, run_cli};
pub use signals::shutdown_signal;
