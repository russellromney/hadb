# hadb

Core coordination for the [hadb](https://github.com/russellromney/hadb) ecosystem. Leader election, role management, follower behavior, metrics.

Zero cloud dependencies — generic over four traits:

- **`Replicator`** — how your database syncs (WAL shipping, journal replay, etc.)
- **`Executor`** — how your database executes queries
- **`LeaseStore`** — how leader election works (S3, Redis, etc.)
- **`StorageBackend`** — where coordination blobs live (leases, node registry)

```rust
use hadb::{Coordinator, CoordinatorConfig, LeaseConfig};

let coordinator = Coordinator::new(
    replicator,          // Arc<dyn Replicator>
    Some(lease_store),   // Option<Arc<dyn LeaseStore>>
    Some(registry),      // Option<Arc<dyn NodeRegistry>>
    follower_behavior,   // Arc<dyn FollowerBehavior>
    "prefix/",
    config,
);

coordinator.join("mydb", path).await?;
```

See [haqlite](https://github.com/russellromney/haqlite) for a complete SQLite implementation.
