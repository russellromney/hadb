//! Serde wire-format tests for `Manifest` / `Backend`. Wire-format
//! compatibility with the prior `hadb::HaManifest` / `hadb::StorageManifest`
//! is preserved (only Rust type names changed; variant tags + field names
//! are the same).

use std::collections::BTreeMap;
use turbodb::{
    BTreeManifestEntry, Backend, FrameEntry, Manifest, ManifestMeta, SubframeOverride,
};

fn make_turbolite_manifest(writer: &str, epoch: u64) -> Manifest {
    Manifest {
        version: 0,
        writer_id: writer.to_string(),
        lease_epoch: epoch,
        timestamp_ms: 1000,
        storage: Backend::Turbolite {
            page_count: 100,
            page_size: 4096,
            pages_per_group: 256,
            sub_pages_per_frame: 16,
            strategy: "Positional".to_string(),
            page_group_keys: vec!["pg-0".to_string()],
            frame_tables: vec![vec![FrameEntry {
                offset: 0,
                len: 4096,
                page_count: 0,
            }]],
            group_pages: vec![vec![1, 2, 3]],
            btrees: BTreeMap::from([(
                1,
                BTreeManifestEntry {
                    name: "sqlite_master".to_string(),
                    obj_type: "table".to_string(),
                    group_ids: vec![0, 1],
                },
            )]),
            interior_chunk_keys: BTreeMap::from([(0, "ic-0".to_string())]),
            index_chunk_keys: BTreeMap::from([(0, "idx-0".to_string())]),
            subframe_overrides: vec![BTreeMap::new()],
            turbolite_version: 0,
            db_header: None,
        },
    }
}

fn make_walrust_manifest(writer: &str, epoch: u64) -> Manifest {
    Manifest {
        version: 0,
        writer_id: writer.to_string(),
        lease_epoch: epoch,
        timestamp_ms: 2000,
        storage: Backend::Walrust {
            txid: 42,
            changeset_prefix: "cs/".to_string(),
            latest_changeset_key: "cs/42".to_string(),
            snapshot_key: Some("snap/1".to_string()),
            snapshot_txid: Some(40),
        },
    }
}

fn make_turbograph_manifest(writer: &str, epoch: u64) -> Manifest {
    Manifest {
        version: 0,
        writer_id: writer.to_string(),
        lease_epoch: epoch,
        timestamp_ms: 3000,
        storage: Backend::Turbograph {
            turbograph_version: 5,
            page_count: 200,
            page_size: 4096,
            pages_per_group: 4096,
            sub_pages_per_frame: 4,
            page_group_keys: vec!["pg/0_v5".to_string(), "pg/1_v5".to_string()],
            frame_tables: vec![vec![FrameEntry {
                offset: 0,
                len: 8192,
                page_count: 0,
            }]],
            subframe_overrides: vec![BTreeMap::new()],
            encrypted: false,
            journal_seq: 42,
        },
    }
}

fn make_turbograph_graphstream_manifest(writer: &str, epoch: u64) -> Manifest {
    Manifest {
        version: 0,
        writer_id: writer.to_string(),
        lease_epoch: epoch,
        timestamp_ms: 4000,
        storage: Backend::TurbographGraphstream {
            turbograph_version: 7,
            page_count: 500,
            page_size: 4096,
            pages_per_group: 4096,
            sub_pages_per_frame: 4,
            page_group_keys: vec!["pg/0_v7".to_string()],
            frame_tables: vec![vec![FrameEntry {
                offset: 0,
                len: 16384,
                page_count: 0,
            }]],
            subframe_overrides: vec![BTreeMap::from([(
                0,
                SubframeOverride {
                    key: "pg/0_f0_v7".to_string(),
                    entry: FrameEntry {
                        offset: 0,
                        len: 4096,
                        page_count: 0,
                    },
                },
            )])],
            encrypted: true,
            journal_seq: 100,
            graphstream_segment_prefix: "gs/db1/".to_string(),
        },
    }
}

#[test]
fn manifest_roundtrips_through_msgpack() {
    let m = make_turbolite_manifest("node-1", 3);
    let bytes = rmp_serde::to_vec(&m).expect("msgpack serialize");
    let decoded: Manifest = rmp_serde::from_slice(&bytes).expect("msgpack deserialize");
    assert_eq!(m, decoded);
}

#[test]
fn manifest_roundtrips_through_json() {
    let m = make_walrust_manifest("node-2", 5);
    let json = serde_json::to_string(&m).expect("json serialize");
    let decoded: Manifest = serde_json::from_str(&json).expect("json deserialize");
    assert_eq!(m, decoded);
}

#[test]
fn turbolite_variant_serde_roundtrip() {
    let storage = Backend::Turbolite {
        page_count: 50,
        page_size: 8192,
        pages_per_group: 128,
        sub_pages_per_frame: 8,
        strategy: "BTreeAware".to_string(),
        page_group_keys: vec!["a".into(), "b".into()],
        frame_tables: vec![vec![
            FrameEntry { offset: 0, len: 4096, page_count: 0 },
            FrameEntry { offset: 4096, len: 4096, page_count: 0 },
        ]],
        group_pages: vec![vec![1, 2]],
        btrees: BTreeMap::from([(
            1,
            BTreeManifestEntry {
                name: "idx_foo".to_string(),
                obj_type: "index".to_string(),
                group_ids: vec![0],
            },
        )]),
        interior_chunk_keys: BTreeMap::from([(0, "ic".to_string())]),
        index_chunk_keys: BTreeMap::new(),
        subframe_overrides: vec![],
        turbolite_version: 0,
        db_header: None,
    };
    let bytes = rmp_serde::to_vec(&storage).expect("serialize");
    let decoded: Backend = rmp_serde::from_slice(&bytes).expect("deserialize");
    assert_eq!(storage, decoded);
}

#[test]
fn walrust_variant_serde_roundtrip() {
    let storage = Backend::Walrust {
        txid: 100,
        changeset_prefix: "cs/".into(),
        latest_changeset_key: "cs/100".into(),
        snapshot_key: None,
        snapshot_txid: None,
    };
    let json = serde_json::to_string(&storage).expect("serialize");
    let decoded: Backend = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(storage, decoded);
}

#[test]
fn turbograph_serde_roundtrip_json() {
    let m = make_turbograph_manifest("node-graph", 10);
    let json = serde_json::to_string(&m).expect("json serialize");
    let decoded: Manifest = serde_json::from_str(&json).expect("json deserialize");
    assert_eq!(m, decoded);
}

#[test]
fn turbograph_serde_roundtrip_msgpack() {
    let m = make_turbograph_manifest("node-graph", 10);
    let bytes = rmp_serde::to_vec(&m).expect("msgpack serialize");
    let decoded: Manifest = rmp_serde::from_slice(&bytes).expect("msgpack deserialize");
    assert_eq!(m, decoded);
}

#[test]
fn turbograph_graphstream_serde_roundtrip_json() {
    let m = make_turbograph_graphstream_manifest("node-gs", 20);
    let json = serde_json::to_string(&m).expect("json serialize");
    let decoded: Manifest = serde_json::from_str(&json).expect("json deserialize");
    assert_eq!(m, decoded);
}

#[test]
fn turbograph_graphstream_serde_roundtrip_msgpack() {
    let m = make_turbograph_graphstream_manifest("node-gs", 20);
    let bytes = rmp_serde::to_vec(&m).expect("msgpack serialize");
    let decoded: Manifest = rmp_serde::from_slice(&bytes).expect("msgpack deserialize");
    assert_eq!(m, decoded);
}

#[test]
fn subframe_override_serde_roundtrip() {
    let ovr = SubframeOverride {
        key: "subframe-42".to_string(),
        entry: FrameEntry {
            offset: 8192,
            len: 4096,
            page_count: 0,
        },
    };
    let bytes = rmp_serde::to_vec(&ovr).expect("serialize");
    let decoded: SubframeOverride = rmp_serde::from_slice(&bytes).expect("deserialize");
    assert_eq!(ovr, decoded);

    let json = serde_json::to_string(&ovr).expect("json serialize");
    let decoded2: SubframeOverride = serde_json::from_str(&json).expect("json deserialize");
    assert_eq!(ovr, decoded2);
}

#[test]
fn turbograph_journal_seq_defaults_to_zero() {
    // Old manifest without journal_seq field still deserializes.
    let json = r#"{
        "version": 1,
        "writer_id": "old-node",
        "lease_epoch": 1,
        "timestamp_ms": 1000,
        "storage": {
            "Turbograph": {
                "turbograph_version": 1,
                "page_count": 10,
                "page_size": 4096,
                "pages_per_group": 4096,
                "sub_pages_per_frame": 4,
                "page_group_keys": [],
                "frame_tables": [],
                "subframe_overrides": [],
                "encrypted": false
            }
        }
    }"#;
    let decoded: Manifest = serde_json::from_str(json).expect("deserialize");
    match &decoded.storage {
        Backend::Turbograph { journal_seq, .. } => {
            assert_eq!(*journal_seq, 0, "journal_seq should default to 0");
        }
        _ => panic!("expected Turbograph"),
    }
}

#[test]
fn turbograph_graphstream_journal_seq_defaults_to_zero() {
    let json = r#"{
        "version": 1,
        "writer_id": "old-node",
        "lease_epoch": 1,
        "timestamp_ms": 1000,
        "storage": {
            "TurbographGraphstream": {
                "turbograph_version": 1,
                "page_count": 10,
                "page_size": 4096,
                "pages_per_group": 4096,
                "sub_pages_per_frame": 4,
                "page_group_keys": [],
                "frame_tables": [],
                "subframe_overrides": [],
                "encrypted": false,
                "graphstream_segment_prefix": "gs/old/"
            }
        }
    }"#;
    let decoded: Manifest = serde_json::from_str(json).expect("deserialize");
    match &decoded.storage {
        Backend::TurbographGraphstream { journal_seq, .. } => {
            assert_eq!(*journal_seq, 0, "journal_seq should default to 0");
        }
        _ => panic!("expected TurbographGraphstream"),
    }
}

#[test]
fn turbograph_graphstream_segment_prefix_defaults_to_empty() {
    let json = r#"{
        "version": 1,
        "writer_id": "old-node",
        "lease_epoch": 1,
        "timestamp_ms": 1000,
        "storage": {
            "TurbographGraphstream": {
                "turbograph_version": 1,
                "page_count": 10,
                "page_size": 4096,
                "pages_per_group": 4096,
                "sub_pages_per_frame": 4,
                "page_group_keys": [],
                "frame_tables": [],
                "subframe_overrides": [],
                "encrypted": false,
                "journal_seq": 5
            }
        }
    }"#;
    let decoded: Manifest = serde_json::from_str(json).expect("deserialize");
    match &decoded.storage {
        Backend::TurbographGraphstream {
            graphstream_segment_prefix,
            journal_seq,
            ..
        } => {
            assert_eq!(graphstream_segment_prefix, "");
            assert_eq!(*journal_seq, 5);
        }
        _ => panic!("expected TurbographGraphstream"),
    }
}

#[test]
fn turbograph_graphstream_with_empty_fields() {
    let m = Manifest {
        version: 0,
        writer_id: "node-1".to_string(),
        lease_epoch: 1,
        timestamp_ms: 1000,
        storage: Backend::TurbographGraphstream {
            turbograph_version: 0,
            page_count: 0,
            page_size: 0,
            pages_per_group: 0,
            sub_pages_per_frame: 0,
            page_group_keys: vec![],
            frame_tables: vec![],
            subframe_overrides: vec![],
            encrypted: false,
            journal_seq: 0,
            graphstream_segment_prefix: String::new(),
        },
    };
    let bytes = rmp_serde::to_vec(&m).expect("serialize");
    let decoded: Manifest = rmp_serde::from_slice(&bytes).expect("deserialize");
    assert_eq!(m, decoded);

    let json = serde_json::to_string(&m).expect("json serialize");
    let decoded_json: Manifest = serde_json::from_str(&json).expect("json deserialize");
    assert_eq!(m, decoded_json);
}

#[test]
fn turbograph_with_empty_fields() {
    let m = Manifest {
        version: 0,
        writer_id: "node-1".to_string(),
        lease_epoch: 1,
        timestamp_ms: 1000,
        storage: Backend::Turbograph {
            turbograph_version: 0,
            page_count: 0,
            page_size: 0,
            pages_per_group: 0,
            sub_pages_per_frame: 0,
            page_group_keys: vec![],
            frame_tables: vec![],
            subframe_overrides: vec![],
            encrypted: false,
            journal_seq: 0,
        },
    };
    let bytes = rmp_serde::to_vec(&m).expect("serialize");
    let decoded: Manifest = rmp_serde::from_slice(&bytes).expect("deserialize");
    assert_eq!(m, decoded);
}

#[test]
fn manifest_meta_from_conversion() {
    let m = Manifest {
        version: 7,
        writer_id: "node-42".to_string(),
        lease_epoch: 99,
        timestamp_ms: 1234,
        storage: Backend::Walrust {
            txid: 1,
            changeset_prefix: "cs/".to_string(),
            latest_changeset_key: "cs/1".to_string(),
            snapshot_key: None,
            snapshot_txid: None,
        },
    };
    let meta = ManifestMeta::from(&m);
    assert_eq!(meta.version, 7);
    assert_eq!(meta.writer_id, "node-42");
    assert_eq!(meta.lease_epoch, 99);
}
