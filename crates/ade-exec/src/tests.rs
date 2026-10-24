use super::*;
use arrow::array::{ArrayRef, Int64Array};
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
    let mut cx = ExecContext::new(
        ExecutionConfig::default(),
        CancellationToken::default(),
        None,
    )
    .unwrap();
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

#[test]
fn managed_reservations_are_atomic_and_raii() {
    let manager = MemoryManager::new(100).unwrap();
    let mut a = manager.reservation(OperatorId(1));
    a.shrink(0).unwrap();
    assert!(a.shrink(1).is_err());
    a.grow(100).unwrap();
    assert!(matches!(a.grow(1), Err(Error::MemoryLimit { .. })));
    assert_eq!(manager.snapshot().current, 100);
    assert!(a.shrink(101).is_err());
    a.shrink(40).unwrap();
    let mut b = manager.reservation(OperatorId(1));
    b.grow(40).unwrap();
    assert_eq!(manager.snapshot().owner_peak[&OperatorId(1)], 100);
    drop(a);
    drop(b);
    assert_eq!(
        (
            manager.snapshot().current,
            manager.snapshot().peak,
            manager.snapshot().denials
        ),
        (0, 100, 1)
    );
    let manager = MemoryManager::new(usize::MAX).unwrap();
    let mut a = manager.reservation(OperatorId(0));
    a.grow(usize::MAX).unwrap();
    assert!(a.grow(1).is_err());
    drop(a);
    assert_eq!(manager.snapshot().current, 0);
}
fn phase2_fixture(rows: u64, cardinality: u64) -> (tempfile::TempDir, LocalPlan) {
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("row_id", DataType::UInt64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let path = dir.path().join("data.parquet");
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from_iter_values(
            (0..rows).map(|i| (i * 37) % cardinality),
        )),
        Arc::new(UInt64Array::from_iter_values((0..rows).rev())),
        Arc::new(Int64Array::from_iter_values(
            (0..rows).map(|i| i64::try_from(i % 97).unwrap() - 48),
        )),
    ];
    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), schema.clone(), None).unwrap();
    writer
        .write(&RecordBatch::try_new(schema, arrays).unwrap())
        .unwrap();
    writer.close().unwrap();
    (
        dir,
        LocalPlan::ParquetScan {
            files: vec![path],
            batch_size: 64,
        },
    )
}
fn triples(output: &ExecutionOutput) -> Vec<Vec<i128>> {
    output
        .batches
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(|i| {
                b.columns()
                    .iter()
                    .map(|a| {
                        if let Some(a) = a.as_any().downcast_ref::<UInt64Array>() {
                            i128::from(a.value(i))
                        } else {
                            i128::from(a.as_any().downcast_ref::<Int64Array>().unwrap().value(i))
                        }
                    })
                    .collect()
            })
        })
        .collect()
}
fn aggregate_plan(scan: LocalPlan) -> LocalPlan {
    LocalPlan::HashAggregate {
        input: Box::new(scan),
        key: "key".into(),
        value: "value".into(),
    }
}
fn sort_plan(scan: LocalPlan) -> LocalPlan {
    LocalPlan::Sort {
        input: Box::new(scan),
        key: "key".into(),
        row_id: "row_id".into(),
        value: "value".into(),
    }
}
fn constrained() -> ExecutionConfig {
    ExecutionConfig {
        memory_limit_bytes: 262_144,
        output_batch_rows: 127,
        ..Default::default()
    }
}
#[test]
fn phase2_spill_aggregate_and_external_sort_are_bounded() {
    let (_data, scan) = phase2_fixture(5000, 701);
    let parent = tempfile::tempdir().unwrap();
    for plan in [aggregate_plan(scan.clone()), sort_plan(scan)] {
        let mut expected = triples(&run(plan.clone()));
        expected.sort();
        let output = execute_in(
            plan.clone(),
            constrained(),
            CancellationToken::default(),
            Some(parent.path()),
        )
        .unwrap();
        let mut actual = triples(&output);
        if matches!(plan, LocalPlan::Sort { .. }) {
            assert!(
                actual
                    .windows(2)
                    .all(|w| (w[0][0], w[0][1]) <= (w[1][0], w[1][1]))
            );
            // More than two initial runs requires online compaction and multiple passes.
            assert!(output.metrics.spill.events > 4);
        }
        actual.sort();
        assert_eq!(actual, expected);
        assert!(output.batches.iter().all(|b| b.num_rows() <= 127));
        assert!(
            output.metrics.spill.files > 1
                && output.metrics.spill.write_bytes > 0
                && output.metrics.spill.read_bytes > 0
        );
        assert!(output.metrics.managed.current > 0 && output.metrics.managed.peak <= 262_144);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
        let config = ExecutionConfig {
            spill_enabled: false,
            ..constrained()
        };
        assert!(matches!(
            execute_in(
                plan,
                config,
                CancellationToken::default(),
                Some(parent.path())
            ),
            Err(Error::MemoryLimit { .. } | Error::SpillDisabled)
        ));
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }
}
#[test]
fn phase2_prefetch_order_error_and_early_drop() {
    let (_data, scan) = phase2_fixture(1000, 71);
    let baseline = triples(&run(scan.clone()));
    for depth in [1, 3] {
        let config = ExecutionConfig {
            prefetch_batches: depth,
            ..Default::default()
        };
        let output =
            execute_with_config(scan.clone(), config.clone(), CancellationToken::default())
                .unwrap();
        assert_eq!(triples(&output), baseline);
        assert_eq!(
            output.metrics.prefetch.produced,
            output.metrics.prefetch.consumed
        );
        assert!(
            output.metrics.prefetch.queue_peak > 0 && output.metrics.prefetch.queue_peak <= depth
        );
        let limited = execute_with_config(
            LocalPlan::Limit {
                input: Box::new(scan.clone()),
                rows: 1,
            },
            config.clone(),
            CancellationToken::default(),
        )
        .unwrap();
        assert_eq!(limited.metrics.output_rows, 1);
        assert!(limited.metrics.prefetch.produced >= limited.metrics.prefetch.consumed);
        assert!(limited.metrics.prefetch.queue_peak <= depth);

        let mut cx = ExecContext::new(config.clone(), CancellationToken::default(), None).unwrap();
        let memory = cx.memory.clone();
        let mut limit = Node::build(
            LocalPlan::Limit {
                input: Box::new(scan.clone()),
                rows: 1,
            },
            &mut cx,
        )
        .unwrap();
        let terminal = limit.next_batch(&mut cx).unwrap().unwrap();
        assert_eq!(terminal.num_rows(), 1);
        assert_eq!(memory.snapshot().current, 0);
        let final_metrics = cx.metrics.prefetch.clone();
        assert!(final_metrics.produced >= final_metrics.consumed);
        assert!(final_metrics.queue_peak <= depth);
        assert!(limit.next_batch(&mut cx).unwrap().is_none());
        assert_eq!(cx.metrics.prefetch.produced, final_metrics.produced);
        assert_eq!(cx.metrics.prefetch.consumed, final_metrics.consumed);
        assert_eq!(cx.metrics.prefetch.queue_peak, final_metrics.queue_peak);
        drop(limit);
        drop(cx);
        assert_eq!(memory.snapshot().current, 0);

        let mut cx = ExecContext::new(config.clone(), CancellationToken::default(), None).unwrap();
        let memory = cx.memory.clone();
        let mut node = Node::build(scan.clone(), &mut cx).unwrap();
        assert!(node.next_batch(&mut cx).unwrap().is_some());
        cx.cancellation.cancel();
        assert!(matches!(node.next_batch(&mut cx), Err(Error::Cancelled)));
        drop(node);
        drop(cx);
        assert_eq!(memory.snapshot().current, 0);
        let mut bad = scan.clone();
        if let LocalPlan::ParquetScan { files, .. } = &mut bad {
            files.push(PathBuf::from("/missing/ade-test.parquet"));
        }
        assert!(matches!(
            execute_with_config(bad, config, CancellationToken::default()),
            Err(Error::Io(_))
        ));
    }
}

#[test]
fn limit_releases_blocking_aggregate_before_terminal_batch_returns() {
    let (_data, scan) = phase2_fixture(100, 31);
    let mut direct_cx = ExecContext::new(
        ExecutionConfig {
            memory_limit_bytes: 32_768,
            output_batch_rows: 128,
            ..Default::default()
        },
        CancellationToken::default(),
        None,
    )
    .unwrap();
    let direct_memory = direct_cx.memory.clone();
    let mut aggregate = Node::build(aggregate_plan(scan.clone()), &mut direct_cx).unwrap();
    assert_eq!(
        aggregate
            .next_batch(&mut direct_cx)
            .unwrap()
            .unwrap()
            .num_rows(),
        31
    );
    assert_eq!(direct_memory.snapshot().current, 0);
    drop(aggregate);
    drop(direct_cx);

    let config = ExecutionConfig {
        memory_limit_bytes: 32_768,
        output_batch_rows: 1,
        ..Default::default()
    };
    let plan = LocalPlan::Limit {
        input: Box::new(aggregate_plan(scan)),
        rows: 1,
    };
    let mut cx = ExecContext::new(config.clone(), CancellationToken::default(), None).unwrap();
    let memory = cx.memory.clone();
    let mut node = Node::build(plan.clone(), &mut cx).unwrap();
    let terminal = node.next_batch(&mut cx).unwrap().unwrap();
    assert_eq!(terminal.num_rows(), 1);
    assert_eq!(memory.snapshot().current, 0);
    drop(node);
    drop(cx);
    assert_eq!(memory.snapshot().current, 0);

    let output = execute_with_config(plan, config, CancellationToken::default()).unwrap();
    assert_eq!(output.metrics.output_rows, 1);
}
#[test]
fn phase2_error_cancellation_and_collection_release_everything() {
    let (_data, scan) = phase2_fixture(5000, 701);
    let parent = tempfile::tempdir().unwrap();
    for plan in [aggregate_plan(scan.clone()), sort_plan(scan.clone())] {
        let mut cx = ExecContext::new(
            ExecutionConfig {
                prefetch_batches: 2,
                ..constrained()
            },
            CancellationToken::default(),
            Some(parent.path()),
        )
        .unwrap();
        let memory = cx.memory.clone();
        let mut node = Node::build(plan, &mut cx).unwrap();
        assert!(node.next_batch(&mut cx).unwrap().is_some());
        assert!(cx.spill.metrics.events > 0);
        cx.cancellation.cancel();
        assert!(matches!(node.next_batch(&mut cx), Err(Error::Cancelled)));
        drop(node);
        drop(cx);
        assert_eq!(memory.snapshot().current, 0);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }
    // Failure after spilling: a later file is missing.
    let mut bad = scan.clone();
    if let LocalPlan::ParquetScan { files, .. } = &mut bad {
        files.push(PathBuf::from("/missing/ade-test.parquet"));
    }
    assert!(
        execute_in(
            sort_plan(bad),
            constrained(),
            CancellationToken::default(),
            Some(parent.path())
        )
        .is_err()
    );
    assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    // Retained output is a hard consumer, even for a plain scan.
    assert!(matches!(
        execute_with_config(
            scan,
            ExecutionConfig {
                memory_limit_bytes: 10000,
                ..Default::default()
            },
            CancellationToken::default()
        ),
        Err(Error::MemoryLimit { .. })
    ));
    // One hot key can repeat indefinitely; partials retain checked sums/counts.
    let (_data, scan) = phase2_fixture(5000, 1);
    assert_eq!(
        execute_with_config(
            aggregate_plan(scan),
            constrained(),
            CancellationToken::default()
        )
        .unwrap()
        .metrics
        .output_rows,
        1
    );
    let (_data, scan) = phase2_fixture(5000, 5000);
    assert!(
        execute_with_config(
            aggregate_plan(scan),
            ExecutionConfig {
                memory_limit_bytes: 1024,
                ..Default::default()
            },
            CancellationToken::default()
        )
        .is_err()
    );
}

#[test]
fn phase2_partition_no_progress_and_empty_sort() {
    let (_data, scan) = phase2_fixture(5000, 5000);
    let parent = tempfile::tempdir().unwrap();
    let mut cx = ExecContext::new(
        ExecutionConfig {
            memory_limit_bytes: 65_536,
            ..Default::default()
        },
        CancellationToken::default(),
        Some(parent.path()),
    )
    .unwrap();
    let memory = cx.memory.clone();
    let mut node = Node::build(aggregate_plan(scan.clone()), &mut cx).unwrap();
    assert!(matches!(
        node.next_batch(&mut cx),
        Err(Error::SpillNoProgress(_))
    ));
    assert!(cx.spill.metrics.events > 0);
    drop(node);
    drop(cx);
    assert_eq!(memory.snapshot().current, 0);
    assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    let empty = run(sort_plan(LocalPlan::Filter {
        input: Box::new(scan),
        column: "row_id".into(),
        threshold: 0,
    }));
    assert_eq!(empty.metrics.output_rows, 0);
}

#[test]
fn phase3_ipc_merge_and_checked_counts() {
    use arrow::ipc::writer::StreamWriter;
    let dir = tempfile::tempdir().unwrap();
    let write = |name: &str, sum: i64, count: u64| {
        let path = dir.path().join(name);
        let mut writer =
            StreamWriter::try_new(File::create(&path).unwrap(), &aggregate_schema()).unwrap();
        let batch = RecordBatch::try_new(
            aggregate_schema(),
            vec![
                Arc::new(UInt64Array::from(vec![7])),
                Arc::new(Int64Array::from(vec![sum])),
                Arc::new(UInt64Array::from(vec![count])),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        path
    };
    let a = write("a.arrow", 11, 3);
    let b = write("b.arrow", -4, 2);
    let plan = |files| LocalPlan::MergeAggregate {
        input: Box::new(LocalPlan::IpcScan { files }),
    };
    let result = run(plan(vec![a.clone(), b.clone()]));
    assert_eq!(ints(&result, 1), vec![7]);
    assert_eq!(
        result.batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        5
    );
    assert!(matches!(
        execute(
            plan(vec![a.clone(), write("overflow.arrow", 0, u64::MAX)]),
            CancellationToken::default()
        ),
        Err(Error::Overflow("aggregate count"))
    ));
    let file = std::fs::OpenOptions::new().write(true).open(&b).unwrap();
    file.set_len(file.metadata().unwrap().len() - 5).unwrap();
    assert!(execute(plan(vec![a, b]), CancellationToken::default()).is_err());
}

#[test]
fn phase3_sink_failure_tears_down_spill_and_prefetch() {
    let (_dir, scan) = phase2_fixture(5000, 1000);
    let parent = tempfile::tempdir().unwrap();
    let config = ExecutionConfig {
        memory_limit_bytes: 65536,
        prefetch_batches: 1,
        output_batch_rows: 1,
        ..Default::default()
    };
    let mut cx =
        ExecContext::new(config, CancellationToken::default(), Some(parent.path())).unwrap();
    let manager = cx.memory.clone();
    let plan = aggregate_plan(scan);
    let mut calls = 0;
    let result = drive(plan, &mut cx, |batch, _| {
        calls += 1;
        assert_eq!(batch.num_rows(), 1);
        Err(Error::Invalid("sink rejected".into()))
    });
    assert!(result.is_err());
    assert_eq!(calls, 1);
    assert_eq!(manager.snapshot().current, 0);
    assert!(cx.spill.snapshot().events > 0);
    drop(cx);
    assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    // Public sink returns post-shutdown metrics without a collection reservation.
    let (_dir, scan) = fixture(&[1, 2, 3]);
    let metrics = execute_to_sink(
        scan,
        ExecutionConfig::default(),
        CancellationToken::default(),
        |_| Ok(()),
    )
    .unwrap();
    assert_eq!(metrics.output_rows, 3);
    assert_eq!(metrics.managed.current, 0);
}

#[derive(Debug)]
struct LiveTestSource {
    batches: std::collections::VecDeque<RecordBatch>,
    fail: bool,
    wait: bool,
    closed: Arc<std::sync::atomic::AtomicBool>,
}
impl BatchSource for LiveTestSource {
    fn schema(&self) -> SchemaRef {
        aggregate_schema()
    }
    fn next_batch(&mut self, token: &CancellationToken) -> Result<Option<RecordBatch>> {
        if self.wait {
            loop {
                token.check()?;
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        if self.fail {
            return Err(Error::Invalid("injected live source failure".into()));
        }
        Ok(self.batches.pop_front())
    }
    fn shutdown(&mut self) {
        self.closed.store(true, Ordering::Release);
    }
}
#[test]
fn live_source_reuses_merge_and_shuts_down_on_error_and_cancel() {
    let batch = RecordBatch::try_new(
        aggregate_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![7, 7])),
            Arc::new(arrow::array::Int64Array::from(vec![3, 4])),
            Arc::new(UInt64Array::from(vec![1, 2])),
        ],
    )
    .unwrap();
    for (fail, wait) in [(false, false), (true, false), (false, true)] {
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let source = LiveTestSource {
            batches: [batch.clone()].into(),
            fail,
            wait,
            closed: closed.clone(),
        };
        let token = CancellationToken::default();
        let cancel = token.clone();
        let handle = std::thread::spawn(move || {
            let mut out = Vec::new();
            let result = execute_merge_source_to_sink(
                Box::new(source),
                ExecutionConfig::default(),
                token,
                |b| {
                    out.push(b);
                    Ok(())
                },
            );
            (result, out)
        });
        if wait {
            std::thread::sleep(Duration::from_millis(20));
            cancel.cancel();
        }
        let (result, out) = handle.join().unwrap();
        assert!(closed.load(Ordering::Acquire));
        if fail || wait {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap().output_rows, 1);
            assert_eq!(
                out[0]
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .value(0),
                7
            );
            assert_eq!(
                out[0]
                    .column(2)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0),
                3
            );
        }
    }
}

#[test]
fn prepared_prefix_resumes_once_with_identical_spill_frames_and_releases_on_failure() {
    let (_data, scan) = phase2_fixture(10000, 2048);
    let plan = aggregate_plan(scan);
    for prefetch in [0, 1] {
        let config = ExecutionConfig {
            prefetch_batches: prefetch,
            ..constrained()
        };
        let mut expected = Vec::new();
        let baseline = execute_to_sink(
            plan.clone(),
            config.clone(),
            CancellationToken::default(),
            |batch| {
                expected.push(batch);
                Ok(())
            },
        )
        .unwrap();
        let prepared =
            PreparedAggregate::new(plan.clone(), config.clone(), CancellationToken::default())
                .unwrap();
        assert!(prepared.prefix_rows() > 0 && prepared.prefix_rows() < 10000);
        let mut actual = Vec::new();
        let metrics = prepared
            .execute(|batch| {
                actual.push(batch);
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(metrics.output_rows, baseline.output_rows);
        assert_eq!(metrics.spill.write_bytes, baseline.spill.write_bytes);
        assert_eq!(metrics.managed.current, 0);
        assert_eq!(metrics.operators.values().last().unwrap().input_rows, 10000);
        for cancel in [false, true] {
            let token = CancellationToken::default();
            let prepared =
                PreparedAggregate::new(plan.clone(), config.clone(), token.clone()).unwrap();
            let memory = prepared.task.as_ref().unwrap().1.memory.clone();
            assert!(memory.snapshot().current > 0);
            if cancel {
                token.cancel();
            }
            assert!(
                prepared
                    .execute(|_| Err(Error::Invalid("sink failure".into())))
                    .is_err()
            );
            assert_eq!(memory.snapshot().current, 0);
            let prepared =
                PreparedAggregate::new(plan.clone(), config.clone(), CancellationToken::default())
                    .unwrap();
            let memory = prepared.task.as_ref().unwrap().1.memory.clone();
            drop(prepared);
            assert_eq!(memory.snapshot().current, 0);
        }
    }
}

fn contribution_map(batches: &[RecordBatch]) -> BTreeMap<u64, (i128, u64)> {
    let mut result = BTreeMap::new();
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let entry = result.entry(keys.value(row)).or_insert((0i128, 0u64));
            entry.0 = entry.0.checked_add(i128::from(sums.value(row))).unwrap();
            entry.1 = entry.1.checked_add(counts.value(row)).unwrap();
        }
    }
    result
}
#[test]
fn adaptive_partial_sample_once_mid_batch_pressure_and_raw_conservation() {
    for cardinality in [4, 5000] {
        let (_data, scan) = phase2_fixture(5000, cardinality);
        let mut expected = BTreeMap::new();
        for i in 0..5000u64 {
            let e = expected
                .entry((i * 37) % cardinality)
                .or_insert((0i128, 0u64));
            e.0 += i128::from(i % 97) - 48;
            e.1 += 1;
        }
        for mode in [partial::Mode::AdaptiveV1, partial::Mode::RawPartial] {
            for sample_rows in [1, 63, 64, 65, 1024, 4096] {
                let plan = LocalPlan::PartialAggregate {
                    input: Box::new(scan.clone()),
                    key: "key".into(),
                    value: "value".into(),
                    config: partial::Config {
                        mode,
                        sample_rows,
                        ..Default::default()
                    },
                };
                let mut batches = Vec::new();
                let metrics =
                    execute_to_sink(plan, constrained(), CancellationToken::default(), |batch| {
                        batches.push(batch);
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(contribution_map(&batches), expected);
                let p = metrics.partial.unwrap();
                if mode == partial::Mode::RawPartial {
                    assert!(!p.hash_to_raw);
                    assert_eq!(p.raw_rows, 5000);
                } else if cardinality == 5000 {
                    assert!(p.hash_to_raw && p.state_released_before_tail);
                    assert_eq!(p.flushed_count, sample_rows as u64);
                    assert_eq!(p.raw_rows, 5000 - sample_rows as u64);
                } else if sample_rows >= 64 {
                    assert!(!p.hash_to_raw);
                    assert_eq!(p.raw_rows, 0);
                }
                assert_eq!(metrics.managed.current, 0);
                assert!(metrics.managed.peak <= constrained().memory_limit_bytes);
            }
        }
    }
}
#[test]
fn adaptive_partial_extreme_prefix_sum_is_split_without_losing_count() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("extreme.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![0; 4])),
            Arc::new(Int64Array::from(vec![
                i64::MIN,
                i64::MIN,
                i64::MAX,
                i64::MAX,
            ])),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    for mode in [
        partial::Mode::AdaptiveV1,
        partial::Mode::RawPartial,
        partial::Mode::FixedHash,
    ] {
        let plan = LocalPlan::PartialAggregate {
            input: Box::new(LocalPlan::ParquetScan {
                files: vec![path.clone()],
                batch_size: 3,
            }),
            key: "key".into(),
            value: "value".into(),
            config: partial::Config {
                mode,
                sample_rows: 2,
                min_reduction_percent: 80,
            },
        };
        let mut batches = Vec::new();
        let metrics = execute_to_sink(
            plan,
            ExecutionConfig {
                output_batch_rows: 1,
                ..Default::default()
            },
            CancellationToken::default(),
            |batch| {
                batches.push(batch);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(contribution_map(&batches), BTreeMap::from([(0, (-2, 4))]));
        if mode == partial::Mode::AdaptiveV1 {
            let p = metrics.partial.unwrap();
            assert!(p.hash_to_raw);
            assert_eq!(p.flushed_count, 2);
            assert_eq!(p.raw_rows, 2);
        }
    }
}
#[test]
fn adaptive_partial_releases_hash_before_tail_and_cleans_on_cancel() {
    let (_data, scan) = phase2_fixture(5000, 5000);
    let parent = tempfile::tempdir().unwrap();
    let token = CancellationToken::default();
    let mut cx = ExecContext::new(
        ExecutionConfig::default(),
        token.clone(),
        Some(parent.path()),
    )
    .unwrap();
    let memory = cx.memory.clone();
    let plan = LocalPlan::PartialAggregate {
        input: Box::new(scan),
        key: "key".into(),
        value: "value".into(),
        config: partial::Config {
            mode: partial::Mode::AdaptiveV1,
            sample_rows: 65,
            ..Default::default()
        },
    };
    let mut node = Node::build(plan, &mut cx).unwrap();
    while !cx
        .metrics
        .partial
        .as_ref()
        .is_some_and(|p| p.state_released_before_tail && p.raw_rows > 0)
    {
        assert!(node.next_batch(&mut cx).unwrap().is_some());
    }
    assert!(memory.snapshot().peak > 1_000_000);
    assert!(memory.snapshot().current < 65536);
    token.cancel();
    assert!(matches!(node.next_batch(&mut cx), Err(Error::Cancelled)));
    node.shutdown(&mut cx).unwrap();
    drop(node);
    drop(cx);
    assert_eq!(memory.snapshot().current, 0);
    assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
}
