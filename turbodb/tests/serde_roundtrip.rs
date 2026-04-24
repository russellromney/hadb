//! Envelope round-trip tests for the opaque-payload `Manifest`.
//!
//! Turbodb no longer decodes the payload — those tests live with each
//! consumer (turbolite's manifest wire, etc.). What we verify here is
//! that the envelope survives the two formats the crate family uses on
//! the wire (msgpack) and at rest (msgpack on S3 / NATS / Redis, JSON
//! only for admin tooling and debug dumps) with arbitrary payload bytes
//! — including empty, tiny, and "large-enough that the msgpack `bin`
//! length prefix switches size class" (10 KiB).

use turbodb::{Manifest, ManifestMeta};

fn envelope(payload: Vec<u8>) -> Manifest {
    Manifest {
        version: 7,
        writer_id: "node-42".to_string(),
        timestamp_ms: 1_700_000_000_000,
        payload,
    }
}

#[test]
fn envelope_roundtrips_through_msgpack_empty_payload() {
    let m = envelope(Vec::new());
    let bytes = rmp_serde::to_vec(&m).expect("msgpack serialize");
    let decoded: Manifest = rmp_serde::from_slice(&bytes).expect("msgpack deserialize");
    assert_eq!(m, decoded);
}

#[test]
fn envelope_roundtrips_through_msgpack_trivial_payload() {
    let m = envelope(b"test-payload".to_vec());
    let bytes = rmp_serde::to_vec(&m).expect("msgpack serialize");
    let decoded: Manifest = rmp_serde::from_slice(&bytes).expect("msgpack deserialize");
    assert_eq!(m, decoded);
}

#[test]
fn envelope_roundtrips_through_msgpack_large_payload() {
    // 10 KiB crosses the msgpack bin16 / bin32 length-prefix boundary
    // (bin8 is ≤255 bytes; bin16 is ≤64 KiB).
    let payload: Vec<u8> = (0u32..10_240).map(|i| (i & 0xff) as u8).collect();
    let m = envelope(payload);
    let bytes = rmp_serde::to_vec(&m).expect("msgpack serialize");
    let decoded: Manifest = rmp_serde::from_slice(&bytes).expect("msgpack deserialize");
    assert_eq!(m, decoded);
}

#[test]
fn envelope_roundtrips_through_json() {
    let m = envelope(b"hello".to_vec());
    let text = serde_json::to_string(&m).expect("json serialize");
    let decoded: Manifest = serde_json::from_str(&text).expect("json deserialize");
    assert_eq!(m, decoded);
}

#[test]
fn msgpack_encodes_payload_as_bin_not_array() {
    // The `serde_bytes` attribute on `payload` is load-bearing for wire
    // size: without it, `Vec<u8>` serializes as an msgpack array (one
    // element per byte, ~2 bytes each). With it, it's a `bin` blob
    // with a small length prefix. Regression-guard by asserting the
    // payload encodes to roughly its own length, not 2× or more.
    let payload = vec![0xAAu8; 256];
    let m = envelope(payload.clone());
    let bytes = rmp_serde::to_vec(&m).expect("msgpack serialize");
    // Envelope overhead is small (few dozen bytes of field names + strings +
    // the bin length prefix). 2× payload would be ~512+; bin16 header is
    // 3 bytes so we expect < payload.len() + 64.
    assert!(
        bytes.len() < payload.len() + 64,
        "expected compact `bin` encoding, got {} bytes for {}-byte payload",
        bytes.len(),
        payload.len()
    );
}

#[test]
fn manifest_meta_from_envelope() {
    let m = envelope(b"whatever".to_vec());
    let meta = ManifestMeta::from(&m);
    assert_eq!(meta.version, 7);
    assert_eq!(meta.writer_id, "node-42");
}
