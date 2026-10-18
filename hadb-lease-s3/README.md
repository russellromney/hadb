# hadb-lease-s3

S3 coordination primitives for the [hadb](https://github.com/russellromney/hadb) ecosystem.

- `S3LeaseStore` — Leader election with persisted numeric revisions and physical S3 ETag CAS. Implements `hadb::LeaseStore`.
- **`S3NodeRegistry`** — Node discovery via S3 object listing. Implements `hadb::NodeRegistry`.
- **`S3StorageBackend`** — Minimal storage for coordination blobs (leases, node registrations). Implements `hadb::StorageBackend`.

```rust
use hadb_lease_s3::{S3LeaseStore, S3StorageBackend, S3NodeRegistry};

let lease_store = S3LeaseStore::new(s3_client.clone(), "my-bucket".into());
let storage = S3StorageBackend::new(s3_client.clone(), "my-bucket".into());
let registry = S3NodeRegistry::new(Arc::new(storage));
```

This is the reference implementation. Alternative lease stores (Redis, etcd, DynamoDB) would implement the same `hadb::LeaseStore` trait.

## Revision and payload contract

The public CAS token is a positive decimal `u64` revision, ordered per lease key.
Every successful mutation advances the persisted revision, including identical-byte
renewals, takeover, release and recreation. Independent clients and processes use
the same sequence. Physical S3 ETags remain opaque and are used only for atomic
`If-Match` and `If-None-Match` requests.

Lease objects use reserved `hadb-lease-format: 1` S3 metadata and a versioned JSON
body containing `version`, `revision` and `payload`. A live payload is encoded as
`{"live": [byte, ...]}`; a tombstone is `"tombstone"`. The revision is in the body,
not just metadata, so unchanged caller bytes still produce changed object content.
`read` returns the caller's original bytes without parsing them as `LeaseData`.
Empty, binary and envelope-looking payloads are supported.

Logical deletion leaves a tombstone. Public `read` returns `None`, but recreation
CAS-writes the next revision instead of resetting to 1. At `u64::MAX`, further
mutations fail rather than wrapping or deleting history. Unknown or incomplete
marked envelopes fail closed.

The service must provide strongly consistent reads and atomic conditional PUTs.
There is no HEAD plus unconditional PUT fallback. Known `ConditionalRequestConflict`
responses and 412 responses are CAS conflicts. Other errors, including unrecognised
409 responses, remain errors and never justify an unconditional write.

## Ownership release

Use `LeaseStore::release_if_match` with the held token to relinquish ownership.
A stale owner cannot release a successor. S3 writes a revisioned tombstone; the
memory adapter conditionally removes its entry. Other adapters inherit a CAS-write
of an immediately expired canonical `LeaseData` sentinel. That default does not
promise deletion or `read == None`, and requires the backend to preserve the
supplied bytes. It clears owner, address and session identity, with `sleeping: false`,
so normal coordinator clients can immediately claim it by CAS.

Administrative `delete` is not ownership proof. S3 implements it with logical
deletion and reports a conflict if the observed object changes during deletion.
Coordinator release paths must not use administrative deletion.

## Upgrade and retention requirements

Stop old clients and drain outstanding writes and retries before upgrading a lease
namespace. Mixed raw-object and revisioned writers, and downgrade after conversion,
are unsupported.

An unmarked legacy object remains readable as unchanged bytes with an opaque
`s3-legacy:` token. Its first token-matched mutation conditionally converts it to
revision 1. Reading alone never migrates it. No revision is inferred from its ETag
or payload. If that namespace already had numeric fence history from another
mechanism, migration needs an explicit external policy; the adapter cannot guess
that history.

Retain lease objects and tombstones indefinitely. Exclude them from lifecycle
expiry, physical DELETE, delete markers and rollback. Bucket versioning alone does
not preserve the current authority. Purging history or reusing its namespace can
make old tokens valid again and is unsupported. GET and PUT permissions are needed
for mutations. Numeric revisions do not themselves enforce S3 WAL writes; retain
consumer-side lease and epoch guards.

## Real S3 contract tests

The service tests are explicitly ignored by default. Set `HADB_S3_SERVICE_FILE` to
a private JSON file containing `endpoint`, `bucket` and an `env` map with
`AWS_REGION`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. The fixture uses
fresh prefixes and never prints credentials. Run against a dedicated service:

```sh
cargo test -p hadb-lease-s3 --test s3_lease_contract -- --ignored --test-threads=1
```

An explicit service-test run fails if configuration is missing. An ordinary
package test run does not exercise these contracts.
