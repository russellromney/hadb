# hadb-cli

Shared CLI framework for [hadb](https://github.com/russellromney/hadb) database tools (haqlite, hakuzu, haduck, harock).

Products implement the `CliBackend` trait and get a full CLI with serve, restore, list, verify, compact, replicate, and snapshot commands. Shared config loading, signal handling, and error classification come for free.

## How it works

Each database product (e.g., haqlite) has a thin `main.rs` that calls `run_cli()` with its backend:

```rust
use hadb_cli::{CliBackend, run_cli};

struct MyBackend;

impl CliBackend for MyBackend {
    // Implement only the commands your product supports.
    // Unsupported commands return a clear error.
}

#[tokio::main]
async fn main() {
    run_cli(MyBackend).await;
}
```

## Components

- **`CliBackend` trait**: Products implement this to wire up their database-specific logic for each command
- **Config loading**: TOML config with `[s3]`, `[lease]`, `[retry]`, `[retention]` sections and sensible defaults
- **Shared args**: clap structs for S3, lease, retry, and per-command arguments (with env var support)
- **Signal handling**: `shutdown_signal()` for graceful SIGTERM/SIGINT drain
- **Error classification**: Maps error messages to exit codes (0=success, 2=config, 3=database, 4=S3, 5=integrity, 6=restore)

## License

Apache-2.0
