use super::*;
use crate::{ExecutionConfig, LocalPlan, execute_with_filter_kernel};
use arrow::{
    array::{Int64Array, StringArray, UInt64Array},
    compute::take,
    datatypes::{DataType, Field, Schema},
};
use std::{path::PathBuf, sync::Arc};

fn input(rows: usize, distribution: usize) -> RecordBatch {
    let mut state = 1307u64;
    let predicates = (0..rows)
        .map(|i| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let turns = [
                0,
                1,
                (1u64 << 63) - 1,
                1u64 << 63,
                (1u64 << 63) + 1,
                u64::MAX - 1,
                u64::MAX,
            ];
            let value = match distribution {
                0 => turns[i % turns.len()],
                1 if i % 8 != 7 => 0,
                _ => state,
            };
            let valid = match distribution {
                0 => true,
                1 => !matches!(i % 17, 0 | 7 | 8 | 15),
                2 => (state >> 28).is_multiple_of(2),
                _ => false,
            };
            valid.then_some(value)
        })
        .collect::<UInt64Array>();
    let signed =
        Int64Array::from_iter_values((0..rows).map(|i| i64::try_from(i % 73).unwrap() - 36));
    let text = StringArray::from_iter((0..rows).map(|i| match i % 5 {
        0 => None,
        1 | 2 => Some("duplicate"),
        _ => Some(""),
    }));
    let schema = Arc::new(Schema::new(vec![
        Field::new("u", DataType::UInt64, true),
        Field::new("i", DataType::Int64, false),
        Field::new("s", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(predicates), Arc::new(signed), Arc::new(text)],
    )
    .unwrap()
}

#[test]
fn exact_order_unsigned_nulls_offsets_and_tails() {
    let rows = [
        0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 31, 32, 33, 255, 256, 257, 8191, 8192, 65_539,
    ];
    let mut requests = vec![Request::Scalar, Request::Arrow];
    if neon_available() {
        requests.push(Request::Force(Isa::Neon));
    }
    let token = CancellationToken::default();
    let memory = MemoryManager::new(MAX_INPUT_BYTES).unwrap();
    for distribution in 0..4 {
        let source = input(rows[rows.len() - 1] + 15, distribution);
        for len in rows {
            for offset in [0, 1, 3, 7, 8, 15] {
                let batch = source.slice(offset, len);
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                for threshold in [0, 1, 1 << 63, u64::MAX] {
                    // Read original rows via Arrow's logical iterator. The oracle
                    // uses indices and take, bypassing comparison mask and filter.
                    let indices =
                        UInt64Array::from_iter_values(values.iter().enumerate().filter_map(
                            |(i, v)| v.filter(|value| *value < threshold).map(|_| i as u64),
                        ));
                    let expected = RecordBatch::try_new(
                        batch.schema(),
                        batch
                            .columns()
                            .iter()
                            .map(|column| take(column.as_ref(), &indices, None).unwrap())
                            .collect(),
                    )
                    .unwrap();
                    for request in &requests {
                        let mut filter = Filter::new(request.clone()).unwrap();
                        let actual = filter
                            .apply(&batch, 0, threshold, &memory, OperatorId(0), &token)
                            .unwrap();
                        assert_eq!(
                            actual, expected,
                            "{len}/{offset}/{threshold}/{distribution}/{request:?}"
                        );
                        assert_eq!(memory.snapshot().current, 0);
                    }
                }
            }
        }
    }
}

#[test]
fn limits_refusal_and_retry() {
    let token = CancellationToken::default();
    let memory = MemoryManager::new(8).unwrap();
    let mut filter = Filter::new(Request::Scalar).unwrap();
    assert!(matches!(
        filter.apply(&input(2, 0), 1, 1, &memory, OperatorId(0), &token),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        filter.apply(&input(8192, 0), 0, 1, &memory, OperatorId(0), &token),
        Err(Error::MemoryLimit { .. })
    ));
    assert_eq!(memory.snapshot().current, 0);
    assert_eq!(memory.snapshot().peak, 0);
    let memory = MemoryManager::new(4096).unwrap();
    assert!(matches!(
        filter.apply(
            &input(MAX_ROWS + 1, 0),
            0,
            1,
            &memory,
            OperatorId(0),
            &token
        ),
        Err(Error::FilterInputLimit)
    ));
    token.cancel();
    let batch = input(32, 0);
    assert!(matches!(
        filter.apply(&batch, 0, 1, &memory, OperatorId(0), &token),
        Err(Error::Cancelled)
    ));
    let new_token = CancellationToken::default();
    assert_eq!(
        filter
            .apply(&batch, 0, 1, &memory, OperatorId(0), &new_token)
            .unwrap()
            .num_rows(),
        5,
    );
    assert_eq!(memory.snapshot().current, 0);
    let nonexistent = LocalPlan::ParquetScan {
        files: vec![PathBuf::from("/nonexistent/phase25")],
        batch_size: 8192,
    };
    assert!(matches!(
        execute_with_filter_kernel(
            nonexistent,
            ExecutionConfig::default(),
            new_token,
            Request::Force(Isa::Avx2)
        ),
        Err(Error::UnsupportedFilterIsa(Isa::Avx2))
    ));
}

#[test]
fn native_plan_preserves_order_across_parquet_files_and_batches() {
    use parquet::arrow::ArrowWriter;
    use std::{fs::File, path::PathBuf};

    let directory = tempfile::tempdir().unwrap();
    let source = input(41, 1);
    let paths = ["part-0.parquet", "part-1.parquet"]
        .into_iter()
        .map(|name| directory.path().join(name))
        .collect::<Vec<PathBuf>>();
    for (file_index, path) in paths.iter().enumerate() {
        let mut writer =
            ArrowWriter::try_new(File::create(path).unwrap(), source.schema(), None).unwrap();
        let start = file_index * 20;
        let end = if file_index == 0 { 20 } else { 41 };
        for offset in (start..end).step_by(5) {
            writer
                .write(&source.slice(offset, (end - offset).min(5)))
                .unwrap();
        }
        writer.close().unwrap();
    }

    let threshold = 1u64 << 63;
    let token = CancellationToken::default();
    let (actual, _) = execute_with_filter_kernel(
        LocalPlan::Filter {
            input: Box::new(LocalPlan::ParquetScan {
                files: paths,
                batch_size: 7,
            }),
            column: "u".into(),
            threshold,
        },
        ExecutionConfig::default(),
        token,
        Request::Scalar,
    )
    .unwrap();

    let expected = source
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .iter()
        .enumerate()
        .filter_map(|(index, value)| value.filter(|value| *value < threshold).map(|_| index))
        .collect::<Vec<_>>();
    let output_u = actual
        .batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .iter()
        })
        .collect::<Vec<_>>();
    let output_i = actual
        .batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
        })
        .collect::<Vec<_>>();
    let output_s = actual
        .batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
        })
        .collect::<Vec<_>>();
    let expected_u = expected
        .iter()
        .map(|&index| {
            source
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(index)
        })
        .collect::<Vec<_>>();
    let expected_i = expected
        .iter()
        .map(|&index| {
            source
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(index)
        })
        .collect::<Vec<_>>();
    let expected_s = expected
        .iter()
        .map(|&index| {
            source
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .nth(index)
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        output_u,
        expected_u.into_iter().map(Some).collect::<Vec<_>>()
    );
    assert_eq!(
        output_i,
        expected_i.into_iter().map(Some).collect::<Vec<_>>()
    );
    assert_eq!(output_s, expected_s);
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[test]
fn cancel_after_first_vector_chunk() {
    if !neon_available() {
        return;
    }
    let token = CancellationToken::default();
    let values = UInt64Array::from(vec![0; 2 * POLL_ROWS + 1]);
    let mut polls = 0;
    let result = packed_mask(&values, 1, || {
        polls += 1;
        if polls == 2 {
            token.cancel();
        }
        token.check()
    });
    assert!(matches!(result, Err(Error::Cancelled)));
    assert_eq!(polls, 2);
}

#[test]
fn policy_requires_paired_gain_and_exact_shape() {
    let samples = |ns| Measurements {
        filter_ns: [ns; 7],
        query_ns: [ns * 2; 7],
        process_ns: [ns * 3; 7],
        rss_bytes: [100; 7],
    };
    let shape = Shape {
        rows: 8192,
        nulls: 0,
        threshold: 250,
    };
    let slow = samples(100);
    let winner = samples(95);
    let weak = samples(96);
    let admitted = AutoSelection::from_verified_samples(shape, 1, &slow, &slow, &winner).unwrap();
    assert_eq!(admitted.select(shape), Reason::MeasuredGain);
    assert_eq!(
        admitted.select(Shape { nulls: 1, ..shape }),
        Reason::UnknownShape
    );
    let unproven = AutoSelection::from_verified_samples(shape, 1, &slow, &slow, &weak).unwrap();
    assert_eq!(unproven.select(shape), Reason::InsufficientGain);
    let invalid = samples(u64::MAX / 4);
    assert!(AutoSelection::from_verified_samples(shape, 1, &slow, &slow, &invalid).is_err());
}
