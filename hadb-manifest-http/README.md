# hadb-manifest-http

HTTP manifest store for [hadb](https://github.com/russellromney/hadb).

Implements the `ManifestStore` trait over HTTP. Manifests are exchanged as JSON, with CAS versioning on writes and lightweight HEAD polling for followers.

## How it works

Three endpoints:

- **GET** fetches the full manifest (JSON body)
- **POST** publishes a new manifest with CAS version checking, returns 201/200 or 409
- **HEAD** returns version, writer_id, and lease_epoch as response headers (cheap polling for followers)

## Usage

```rust
use hadb_manifest_http::HttpManifestStore;

let store = HttpManifestStore::new("http://proxy:8080", "my-auth-token");

// Use with hadb Coordinator or haqlite builder
let db = HaQLite::builder("my-bucket")
    .manifest_store(Arc::new(store))
    .open("/data/my.db", schema)
    .await?;
```

Or via haqlite env var:

```bash
HAQLITE_MANIFEST_URL=http://proxy:8080?token=my-auth-token
```

## HTTP API contract

```
GET  /v1/manifest?key=...  -> 200 { "manifest": <HaManifest> } | 404
POST /v1/manifest?key=...  -> 201 { "etag": "..." } | 409
                               Body: { "manifest": <HaManifest>, "expected_version": <u64|null> }
HEAD /v1/manifest?key=...  -> 200 with headers: X-Manifest-Version, X-Writer-Id, X-Lease-Epoch
                            | 404
```

## License

Apache-2.0
