use super::*;
use parquet::arrow::ArrowWriter;

fn fixture(values: &[i64]) -> (tempfile::TempDir, LocalPlan) {
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("secondary", DataType::UInt64, true),
        Field::new("value", DataType::Int64, false),
    ]));
    let mut files = Vec::new();
    for (i, chunk) in values.chunks(3).enumerate() {
        let path = dir.path().join(format!("{i}.parquet"));
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), schema.clone(), None).unwrap();
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from_iter_values(
                (0..chunk.len()).map(|i| (i % 2) as u64),
            )),
            Arc::new(UInt64Array::from_iter(
                (0..chunk.len()).map(|i| if i == 2 { None } else { Some(i as u64) }),
            )),
            Arc::new(Int64Array::from(chunk.to_vec())),
        ];
        writer
            .write(&RecordBatch::try_new(schema.clone(), arrays).unwrap())
            .unwrap();
        writer.close().unwrap();
        files.push(path);
    }
    (
        dir,
        LocalPlan::ParquetScan {
            files,
            batch_size: 2,
        },
    )
}
fn run(plan: LocalPlan) -> ExecutionOutput {
    execute(plan, CancellationToken::default()).unwrap()
}
fn ints(output: &ExecutionOutput, col: usize) -> Vec<i64> {
    output
        .batches
        .iter()
        .flat_map(|b| {
            b.column(col)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}
#[test]
fn core_memory_and_cancellation() {
    let mut m = MemoryTracker::default();
    m.reserve(10).unwrap();
    m.reserve(4).unwrap();
    m.release(10).unwrap();
    assert_eq!((m.current, m.peak), (4, 14));
    assert!(m.release(5).is_err());
    assert_eq!(m.current, 4);
    assert!(m.reserve(usize::MAX).is_err());
    assert_eq!(m.peak, 14);
    let (_dir, plan) = fixture(&[1, 2, 3]);
    let token = CancellationToken::default();
    token.clone().cancel();
    assert!(matches!(
        execute(plan.clone(), token),
        Err(Error::Cancelled)
    ));
    let mut cx = ExecContext::default();
    let mut node = Node::build(plan, &mut cx).unwrap();
    assert!(node.next_batch(&mut cx).unwrap().is_some());
    cx.cancellation.cancel();
    assert!(matches!(node.next_batch(&mut cx), Err(Error::Cancelled)));
}
#[test]
fn parquet_filter_project_and_metrics() {
    let (_dir, scan) = fixture(&[10, 20, 30, 40, 50]);
    let output = run(scan.clone());
    assert_eq!(ints(&output, 2), [10, 20, 30, 40, 50]);
    assert_eq!(output.metrics.output_batches, 3);
    let plan = LocalPlan::Project {
        input: Box::new(LocalPlan::Filter {
            input: Box::new(scan.clone()),
            column: "secondary".into(),
            threshold: 1,
        }),
        columns: vec!["value".into(), "key".into()],
    };
    let output = run(plan.clone());
    assert_eq!(ints(&output, 0), [10, 40]);
    assert_eq!(output.batches[0].schema().field(0).name(), "value");
    let repeat = run(plan);
    assert_eq!(
        output.metrics.operators.keys().collect::<Vec<_>>(),
        repeat.metrics.operators.keys().collect::<Vec<_>>()
    );
    let filter = &output.metrics.operators[&OperatorId(1)];
    assert_eq!((filter.input_rows, filter.output_rows), (5, 2));
    assert!(output.metrics.memory.peak >= output.metrics.memory.current);
    assert!(output.metrics.memory.current > 0);
    assert!(
        output
            .metrics
            .operators
            .values()
            .all(|m| m.memory.current == 0)
    );
    let empty = run(LocalPlan::Filter {
        input: Box::new(scan),
        column: "secondary".into(),
        threshold: 0,
    });
    assert_eq!(empty.metrics.output_rows, 0);
}
#[test]
fn aggregate_empty_overflow_and_order() {
    let (_dir, scan) = fixture(&[10, 20, 30, 40, 50]);
    let aggregate = |input| LocalPlan::HashAggregate {
        input: Box::new(input),
        key: "key".into(),
        value: "value".into(),
    };
    let output = run(aggregate(scan.clone()));
    assert_eq!(ints(&output, 1), [80, 70]);
    let b = &output.batches[0];
    assert_eq!(
        b.column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[0, 1]
    );
    assert_eq!(
        b.column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[3, 2]
    );
    assert!(output.metrics.operators[&OperatorId(1)].memory.peak >= 128);
    let empty = run(aggregate(LocalPlan::Filter {
        input: Box::new(scan),
        column: "secondary".into(),
        threshold: 0,
    }));
    assert_eq!(empty.metrics.output_rows, 0);
    let (_dir, scan) = fixture(&[i64::MAX, 0, 1]);
    assert!(matches!(
        execute(aggregate(scan), CancellationToken::default()),
        Err(Error::Overflow("aggregate Int64 output"))
    ));
}
#[test]
fn limit_stops_upstream_and_schema_errors_are_structured() {
    let (_dir, scan) = fixture(&[1, 2, 3, 4, 5]);
    let output = run(LocalPlan::Limit {
        input: Box::new(scan.clone()),
        rows: 3,
    });
    assert_eq!(ints(&output, 2), [1, 2, 3]);
    assert_eq!(output.metrics.operators[&OperatorId(0)].output_rows, 3);
    let zero = run(LocalPlan::Limit {
        input: Box::new(scan.clone()),
        rows: 0,
    });
    assert_eq!(zero.metrics.operators[&OperatorId(0)].output_batches, 0);
    assert!(
        execute(
            LocalPlan::Filter {
                input: Box::new(scan.clone()),
                column: "value".into(),
                threshold: 1
            },
            CancellationToken::default()
        )
        .is_err()
    );
    assert!(
        execute(
            LocalPlan::Project {
                input: Box::new(scan),
                columns: vec!["missing".into()]
            },
            CancellationToken::default()
        )
        .is_err()
    );
}
