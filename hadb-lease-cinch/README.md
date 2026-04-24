# hadb-lease-http

HTTP lease store for [hadb](https://github.com/russellromney/hadb).

Implements the `LeaseStore` trait over HTTP, using standard conditional headers for CAS semantics. Designed for embedded replicas that coordinate through a proxy server.

## How it works

The HTTP server implements four endpoints for CAS lease operations:

- **POST** (write-if-not-exists) for initial lease acquisition, returns 201 or 409
- **PUT** with `If-Match` header (write-if-etag-matches) for lease renewal, returns 200 or 409
- **GET** for reading the current lease holder
- **DELETE** for releasing the lease

Lease data is base64-encoded in JSON request/response bodies. ETags are opaque strings.

## Usage

```rust
use hadb_lease_http::HttpLeaseStore;

let store = HttpLeaseStore::new("http://proxy:8080", "my-auth-token");

// Use with hadb Coordinator or haqlite builder
let db = HaQLite::builder("my-bucket")
    .lease_store(Arc::new(store))
    .open("/data/my.db", schema)
    .await?;
```

Or via haqlite env var:

```bash
HAQLITE_LEASE_URL=http://proxy:8080?token=my-auth-token
```

## HTTP API contract

```
POST   /v1/lease?key=...  -> 201 { "fence": <u64> }               | 409 (already held)
PUT    /v1/lease?key=...  -> 200 { "fence": <u64> }               | 409 (fence mismatch)
GET    /v1/lease?key=...  -> 200 { "fence": <u64>, "holder": "<base64>" } | 404
DELETE /v1/lease?key=...  -> 204
```

The `fence` field (NATS KV revision) is used as the opaque etag for CAS operations.
The `holder` field is base64-encoded lease data (identity of the lease holder).

## License

Apache-2.0
