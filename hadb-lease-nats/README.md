# hadb-lease-nats

NATS JetStream KV lease store for [hadb](https://github.com/russellromney/hadb).

Implements the `LeaseStore` trait using NATS JetStream KV buckets for leader election. 2-5ms operations (vs S3's 50-200ms), zero per-request cost.

## How it works

NATS KV provides the CAS primitives hadb needs for leader election:

- **`create`** (write-if-not-exists) for initial lease acquisition
- **`update`** (write-if-revision-matches) for lease renewal
- **Revisions** serve as opaque etags for compare-and-swap

A single NATS server (~$2/month on Fly) handles thousands of lease operations per second.

## Usage

```rust
use hadb_lease_nats::NatsLeaseStore;

let client = async_nats::connect("nats://localhost:4222").await?;
let store = NatsLeaseStore::new(client, "hadb-leases").await?;

// Use with hadb Coordinator
let coordinator = Coordinator::builder()
    .lease_store(store)
    .build();
```

## License

Apache-2.0
