use ade_ext_sdk::{
    Capability, Error, Extension, ExtensionDescriptorV1, Limits, Request, Row, RowKernel,
    SourcePin, Value, digest, executable_digest, execute, negotiate,
};
use arrow::{
    array::{Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use phase24_external_consumer::Calibration;
use std::{
    fs::{self, File},
    sync::Arc,
    time::{Duration, Instant},
};

#[test]
fn calibration_and_admission_use_public_api() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory
        .path()
        .canonicalize()
        .unwrap()
        .join("external.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::UInt64, false),
        Field::new("value_i64", DataType::Int64, false),
        Field::new("sensor", DataType::UInt64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![11, 12, 13])),
            Arc::new(Int64Array::from(vec![-80, 20, 10])),
            Arc::new(UInt64Array::from(vec![1, 0, 7])),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let bytes = fs::read(&path).unwrap();
    let source = SourcePin {
        digest: digest(&bytes),
        bytes: bytes.len() as u64,
        rows: 3,
    };
    let model = 7_i64.to_le_bytes();
    let descriptor = Calibration.descriptor();
    let begin = || Instant::now() + Duration::from_secs(10);
    let request = Request {
        descriptor: &descriptor,
        executable: executable_digest(begin()).unwrap(),
        model_bytes: &model,
        model_pin: digest(&model),
        source,
        path: &path,
        projection: &[0, 1],
        limits: Limits::default(),
        deadline: begin(),
    };
    let output = execute(&request, &Calibration, &Default::default()).unwrap();
    let duplicate_contract = execute(&request, &NoopDuplicate, &Default::default()).unwrap();
    assert_eq!(output.receipt.executable, duplicate_contract.receipt.executable);
    assert_ne!(output.receipt.implementation, duplicate_contract.receipt.implementation);
    assert_ne!(output.receipt.logical, duplicate_contract.receipt.logical);
    assert_ne!(output.receipt.result, duplicate_contract.receipt.result);
    assert_eq!(output.native.batches[0].num_columns(), 2);
    assert_eq!(
        output.native.batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[-100, 47, 13]
    );
    assert!(
        output
            .native
            .metrics
            .operators
            .values()
            .any(|metric| metric.name == "map-batches")
    );
    assert!(
        output
            .native
            .metrics
            .operators
            .values()
            .any(|metric| metric.name == "schema-adapter")
    );

    let mut wrong = descriptor.clone();
    wrong.abi = 2;
    assert!(matches!(
        negotiate(&wrong, &descriptor),
        Err(Error::Version)
    ));
    wrong.abi = 1;
    wrong.capability = Capability::StreamingStateful;
    assert!(matches!(
        negotiate(&wrong, &descriptor),
        Err(Error::Capability)
    ));
    wrong.capability = descriptor.capability;
    wrong.schema.swap(1, 2);
    assert!(matches!(
        negotiate(&wrong, &descriptor),
        Err(Error::Descriptor)
    ));

    let mut request = request;
    request.deadline = begin();
    request.limits.batch_bytes = output.receipt.peak_map_array_bytes;
    assert!(execute(&request, &Calibration, &Default::default()).is_ok());
    request.deadline = begin();
    request.limits.batch_bytes -= 1;
    assert!(execute(&request, &Calibration, &Default::default()).is_err());
    request.limits = Limits::default();
    request.deadline = begin();
    assert!(matches!(
        execute(&request, &IdentityForger, &Default::default()),
        Err(Error::Native(_))
    ));

    request.executable = digest(b"uninstalled");
    request.deadline = begin();
    assert!(matches!(
        execute(&request, &Calibration, &Default::default()),
        Err(Error::Executable)
    ));
}

struct IdentityForger;
impl Extension for IdentityForger {
    type Kernel = Self;

    fn descriptor(&self) -> ExtensionDescriptorV1 {
        Calibration.descriptor()
    }

    fn prepare(&self, _: &[u8]) -> ade_ext_sdk::Result<Self> {
        Ok(Self)
    }
}

impl RowKernel for IdentityForger {
    fn transform(&self, row: &mut Row) -> ade_ext_sdk::Result<()> {
        row.set(0, Value::U64(100))
    }
}

struct NoopDuplicate;
impl Extension for NoopDuplicate {
    type Kernel = Self;

    fn descriptor(&self) -> ExtensionDescriptorV1 {
        Calibration.descriptor()
    }

    fn prepare(&self, _: &[u8]) -> ade_ext_sdk::Result<Self> {
        Ok(Self)
    }
}

impl RowKernel for NoopDuplicate {
    fn transform(&self, _: &mut Row) -> ade_ext_sdk::Result<()> {
        Ok(())
    }
}
