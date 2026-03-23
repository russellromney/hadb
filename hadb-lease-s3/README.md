# hadb-lease-s3

S3 coordination primitives for the [hadb](https://github.com/russellromney/hadb) ecosystem.

- **`S3LeaseStore`** — Leader election via S3 conditional PUTs (ETag CAS). Implements `hadb::LeaseStore`.
- **`S3NodeRegistry`** — Node discovery via S3 object listing. Implements `hadb::NodeRegistry`.
- **`S3StorageBackend`** — Minimal storage for coordination blobs (leases, node registrations). Implements `hadb::StorageBackend`.

```rust
use hadb_lease_s3::{S3LeaseStore, S3StorageBackend, S3NodeRegistry};

let lease_store = S3LeaseStore::new(s3_client.clone(), "my-bucket".into());
let storage = S3StorageBackend::new(s3_client.clone(), "my-bucket".into());
let registry = S3NodeRegistry::new(Arc::new(storage));
```

This is the reference implementation. Alternative lease stores (Redis, etcd, DynamoDB) would implement the same `hadb::LeaseStore` trait.
