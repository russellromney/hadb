# hadb-lease-etcd

etcd lease store for [hadb](https://github.com/russellromney/hadb). Zero-infrastructure HA on Kubernetes.

Implements the `LeaseStore` trait using etcd KV transactions for leader election. Every Kubernetes cluster already has etcd, so this adds HA without deploying additional services.

## How it works

etcd transactions provide atomic CAS for leader election:

- **`If(CreateRevision == 0).Then(Put)`** for initial lease acquisition (write-if-not-exists)
- **`If(ModRevision == etag).Then(Put)`** for lease renewal (write-if-match)
- **mod_revision** serves as opaque etag for compare-and-swap

Keys are prefixed to avoid collisions with other etcd users.

## Usage

```rust
use hadb_lease_etcd::EtcdLeaseStore;

let client = etcd_client::Client::connect(["http://localhost:2379"], None).await?;
let store = EtcdLeaseStore::new(client, "hadb/leases/");

// Use with hadb Coordinator
let coordinator = Coordinator::builder()
    .lease_store(store)
    .build();
```

## License

Apache-2.0
