use ade_connectors::{
    Catalog, CatalogEntryV1, ConnectorSpecV1, Context, Error, PinnedSplit, Replay,
    ResolvedSnapshotV1, Result, Source,
};
use std::{
    io::{Cursor, Read},
    time::Duration,
};

// Public extension in a separate crate. Its bytes come from an independent
// authority and may be replaced to exercise refusal after resolution.
struct External {
    descriptor: ResolvedSnapshotV1,
    object: Vec<u8>,
}

impl Source for External {
    fn validate(&self, spec: &ConnectorSpecV1) -> Result<()> {
        if spec == &self.descriptor.entry.connector {
            Ok(())
        } else {
            Err(Error::Admission("extension kind"))
        }
    }

    fn resolve(&self, _: &CatalogEntryV1, context: &Context) -> Result<ResolvedSnapshotV1> {
        context.check()?;
        Ok(self.descriptor.clone())
    }

    fn open(&self, _: &PinnedSplit, context: &Context) -> Result<Box<dyn Read + '_>> {
        context.check()?;
        Ok(Box::new(Cursor::new(&self.object)))
    }
}

fn descriptor(bytes: &[u8]) -> ResolvedSnapshotV1 {
    ResolvedSnapshotV1 {
        version: 1,
        entry: CatalogEntryV1 {
            version: 1,
            name: "external".into(),
            connector: ConnectorSpecV1 {
                version: 1,
                kind: "external_sdk".into(),
                namespace: "e".repeat(64),
            },
            schema: vec![ade_physical::Field::new(
                "value",
                ade_physical::Type::I64,
                false,
            )],
        },
        authority: "a".repeat(64),
        splits: vec![PinnedSplit {
            key: "part.parquet".into(),
            bytes: bytes.len() as u64,
            rows: 1,
            blake3: ade_physical::fingerprint(bytes),
        }],
        replay: Replay::Pinned,
    }
}

#[test]
fn extension_binding_checks_content_and_owns_only_its_stage() {
    let original = b"independently-pinned bytes";
    let parent = tempfile::tempdir().unwrap();
    let unrelated = parent.path().join("keep");
    std::fs::write(&unrelated, b"unrelated").unwrap();
    let context = Context::new(Duration::from_secs(5)).unwrap();
    let mut external = External {
        descriptor: descriptor(original),
        object: original.to_vec(),
    };
    let expected = external.descriptor.digest().unwrap();
    let mut catalog = Catalog::default();
    catalog.register(external.descriptor.entry.clone()).unwrap();
    let stage = catalog
        .stage("external", &external, &expected, parent.path(), &context)
        .unwrap();
    assert_eq!(stage.staged_bytes(), original.len() as u64);
    assert_eq!(stage.file_count(), 1);
    drop(stage);

    external.object[0] ^= 1;
    assert!(matches!(
        catalog.stage("external", &external, &expected, parent.path(), &context),
        Err(Error::Identity(_))
    ));
    context.cancel();
    assert!(matches!(
        catalog.stage("external", &external, &expected, parent.path(), &context),
        Err(Error::Cancelled)
    ));
    assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 1);
    assert_eq!(std::fs::read(unrelated).unwrap(), b"unrelated");
}

#[test]
fn strict_grammar_and_native_caps() {
    let snapshot = descriptor(b"independently-pinned bytes");
    let bytes = serde_json::to_vec(&snapshot).unwrap();
    let decoded = ResolvedSnapshotV1::from_json(&bytes).unwrap();
    assert_eq!(decoded.digest().unwrap(), snapshot.digest().unwrap());

    let mut json = serde_json::to_value(&snapshot).unwrap();
    json["entry"]["connector"]["unknown"] = true.into();
    assert!(ResolvedSnapshotV1::from_json(&serde_json::to_vec(&json).unwrap()).is_err());
    let mut bad = snapshot.clone();
    bad.version = 2;
    assert!(bad.digest().is_err());
    bad.version = 1;
    bad.splits[0].key = "../part.parquet".into();
    assert!(bad.digest().is_err());
    bad.splits[0].key = "part.parquet".into();
    bad.splits[0].rows = u64::MAX;
    assert!(bad.digest().is_err());
}

#[cfg(feature = "batch")]
#[tokio::test]
async fn extension_executes_real_parquet_through_native_sql() {
    use arrow::{array::Int64Array, record_batch::RecordBatch};
    use std::sync::Arc;

    let parent = tempfile::tempdir().unwrap();
    let schema = ade_exec::physical::arrow_schema(&vec![ade_physical::Field::new(
        "value",
        ade_physical::Type::I64,
        false,
    )]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![5, -2]))],
    )
    .unwrap();
    let mut object = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut object, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let mut descriptor = descriptor(&object);
    descriptor.splits[0].rows = 2;
    let expected = descriptor.digest().unwrap();
    let mut catalog = Catalog::default();
    catalog.register(descriptor.entry.clone()).unwrap();
    let cx = Context::new(Duration::from_secs(5)).unwrap();
    let staged = catalog
        .stage(
            "external",
            &External { descriptor, object },
            &expected,
            parent.path(),
            &cx,
        )
        .unwrap();
    let output = staged.query("SELECT value FROM input", &cx).await.unwrap();
    assert_eq!(
        output.diagnostics["runtime"]["native_entrypoint"],
        "ade_exec::physical::execute"
    );
    let values = output.batches()[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values().as_ref(), &[5, -2]);
}
