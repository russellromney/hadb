# hadb

Database-agnostic HA coordination framework.

Pure coordination logic with **zero cloud dependencies**. Abstracts away databases (SQL, graph, document) and storage backends (S3, etcd, Consul).

## Architecture

```
hadb/                    # Core crate (zero cloud deps)
├── traits.rs            # LeaseStore, StorageBackend, Replicator, Executor
├── coordinator.rs       # Coordinator<R, E, L, S> (coming soon)
└── ...

hadb-s3/                 # S3 reference implementation
├── lease_store.rs       # S3LeaseStore (conditional PUT via ETag)
└── storage.rs           # S3StorageBackend (PutObject/GetObject)
```

## Core Traits

**`Replicator`** - Database replication abstraction (walrust for SQLite, graphstream for Kuzu, etc.)

**`Executor`** - Query execution abstraction (rusqlite for SQLite, Kuzu for graphs, etc.)

**`LeaseStore`** - Leader election via CAS (S3, etcd, Consul, Redis, DynamoDB)

**`StorageBackend`** - Replication data storage (S3, etcd, local disk)

## Usage

```rust
use hadb::{Coordinator, Replicator, Executor};
use hadb_s3::{S3LeaseStore, S3StorageBackend};

let s3_config = aws_config::load_from_env().await;
let s3_client = aws_sdk_s3::Client::new(&s3_config);

let coordinator = Coordinator::new(
    my_replicator,      // Implements Replicator
    my_executor,        // Implements Executor
    S3LeaseStore::new(s3_client.clone(), "my-bucket".into()),
    S3StorageBackend::new(s3_client, "my-bucket".into()),
    config,
)?;

coordinator.join("mydb", path).await?;
```

## Status

**✅ Traits defined** - All 4 core traits with comprehensive docs
**✅ S3 implementations** - S3LeaseStore and S3StorageBackend
**⏳ Coordinator** - Coming next (copy from haqlite, make generic)
**⏳ Tests** - Unit tests passing, integration tests coming

## License

Apache-2.0
