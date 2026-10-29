use super::*;
#[test]
fn provider_endpoint_secrets_traversal_and_explicit_tls_refuse() {
    let base = S3Config {
        endpoint: "https://store.example".into(),
        bucket: "private-bucket".into(),
        prefix: "one-job".into(),
        region: "us-east-1".into(),
        dev_allow_http: false,
    };
    base.validate().unwrap();
    for endpoint in [
        "http://store.example",
        "http://127.0.0.1:9000",
        "https://user:secret@store.example",
        "https://store.example/path",
        "https://store.example?token=secret",
        "https://store.example/#secret",
        "file:///tmp/store",
    ] {
        let c = S3Config {
            endpoint: endpoint.into(),
            ..base.clone()
        };
        let e = c.validate().unwrap_err().to_string();
        assert!(!e.contains("secret"));
    }
    S3Config {
        endpoint: "http://127.0.0.1:9000".into(),
        dev_allow_http: true,
        ..base.clone()
    }
    .validate()
    .unwrap();
    for prefix in ["../foreign", "a/b", "%2f", "", "a..b", "a\\b"] {
        assert!(
            S3Config {
                prefix: prefix.into(),
                ..base.clone()
            }
            .validate()
            .is_err()
        );
    }
}
#[test]
fn cost_configured_egress_checked_overflow_and_all_limits() {
    let r = Rates {
        worker_placement: "zone-a".into(),
        store_placement: "zone-b".into(),
        put_nano_usd: 2,
        head_nano_usd: 3,
        get_nano_usd: 5,
        delete_nano_usd: 7,
        write_byte_nano_usd: 11,
        read_byte_nano_usd: 13,
        egress_byte_nano_usd: 17,
        storage_byte_ms_nano_usd: 19,
        retention_ms: 23,
    };
    let c = Counters {
        put: 1,
        head: 2,
        get: 3,
        delete: 4,
        write_bytes: 5,
        read_bytes: 6,
        ..Default::default()
    };
    assert_eq!(
        r.estimate_nano_usd(&c).unwrap(),
        2 + 6 + 15 + 28 + 55 + 78 + 102
    );
    assert_eq!(
        r.estimate_lifecycle_nano_usd(&c, 5).unwrap(),
        2 + 6 + 15 + 28 + 55 + 78 + 102 + 5 * 23 * 19
    );
    assert!(
        r.estimate_nano_usd(&Counters {
            write_bytes: u64::MAX,
            ..c
        })
        .is_err()
    );
    assert!(r.estimate_lifecycle_nano_usd(&c, u64::MAX).is_err());
    for field in [
        "segments",
        "write_bytes",
        "objects",
        "requests",
        "timeout_ms",
        "io_timeout_ms",
    ] {
        let mut value = serde_json::to_value(Limits::default()).unwrap();
        value[field] = 0.into();
        assert!(
            serde_json::from_value::<Limits>(value)
                .unwrap()
                .validate()
                .is_err()
        );
    }
    assert!(
        Limits {
            segments: 257,
            ..Default::default()
        }
        .validate()
        .is_err()
    );
    assert!(
        Limits {
            read_retries: 3,
            ..Default::default()
        }
        .validate()
        .is_err()
    );
}
#[test]
fn old_control_oversized_unknown_and_truncated_remote_frames_refuse() {
    for magic in [b"ADETCP12", b"ADEOPS01", b"ADEOBJ00", b"ADEOBJ01"] {
        assert!(wire::read::<wire::Grant>(&mut &magic[..]).is_err());
    }
    let mut bytes = wire::MAGIC.to_vec();
    bytes.extend_from_slice(&(u32::try_from(MAX_JSON).unwrap() + 1).to_le_bytes());
    assert!(wire::read::<wire::Grant>(&mut &bytes[..]).is_err());
    let mut bytes = wire::MAGIC.to_vec();
    bytes.extend_from_slice(&8u32.to_le_bytes());
    bytes.extend_from_slice(b"{}");
    assert!(wire::read::<wire::Grant>(&mut &bytes[..]).is_err());
}
