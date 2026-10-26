//! Native IR entrypoint. All admission completes before operator side effects.
use crate::{CancellationToken, Error, ExecutionConfig, ExecutionOutput, LocalPlan, Result};
use ade_physical::{Operator, PlanV1, Schema, Source, Target, Type};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{
    collections::BTreeSet,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
pub struct Binding {
    pub source: Source,
    pub files: Vec<PathBuf>,
}
#[derive(Debug, Clone, Default)]
pub struct Catalog {
    pub bindings: Vec<Binding>,
}
fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}
#[must_use]
pub fn arrow_type(t: Type) -> DataType {
    match t {
        Type::U64 => DataType::UInt64,
        Type::I64 => DataType::Int64,
        Type::F64 => DataType::Float64,
        Type::U32 => DataType::UInt32,
        Type::Utf8 => DataType::Utf8,
        Type::TimestampMs => DataType::Timestamp(TimeUnit::Millisecond, None),
    }
}
#[must_use]
pub fn arrow_schema(s: &Schema) -> SchemaRef {
    Arc::new(ArrowSchema::new(
        s.iter()
            .map(|f| Field::new(&f.name, arrow_type(f.data_type), f.nullable))
            .collect::<Vec<_>>(),
    ))
}
fn check(token: &CancellationToken, deadline: Instant) -> Result<()> {
    token.check()?;
    if Instant::now() >= deadline {
        return Err(Error::Deadline("physical admission/execution"));
    }
    Ok(())
}
impl Catalog {
    /// Verify exact graph bindings and immutable bytes. Preflight additionally checks
    /// bounded decoding, exact physical schema/rows/nulls and sort tie-breaker uniqueness.
    /// # Errors
    /// Refuses missing/forged/drifting/oversized sources before any returned output.
    pub fn verify(
        &self,
        plan: &PlanV1,
        token: &CancellationToken,
        deadline: Instant,
        preflight: bool,
    ) -> Result<()> {
        plan.validate().map_err(|e| invalid(e.to_string()))?;
        if self.bindings.len() != plan.sources.len() {
            return Err(invalid("catalog coverage"));
        }
        let sorted = plan
            .nodes
            .iter()
            .any(|n| matches!(n.operator, Operator::Sort { .. }));
        let mut last_row_id = None;
        for (binding, source) in self.bindings.iter().zip(&plan.sources) {
            if &binding.source != source || binding.files.len() != source.files.len() {
                return Err(invalid("catalog/source identity mismatch"));
            }
            let mut paths = BTreeSet::new();
            for (path, expected) in binding.files.iter().zip(&source.files) {
                check(token, deadline)?;
                if !paths.insert(path.canonicalize()?) {
                    return Err(invalid("duplicate bound file"));
                }
                let mut file = File::open(path)?;
                if !file.metadata()?.is_file()
                    || file.metadata()?.len() != expected.bytes
                    || expected.bytes < 12
                {
                    return Err(invalid("source byte drift"));
                }
                let mut hasher = blake3::Hasher::new();
                let mut buf = [0; 8192];
                let mut read_bytes = 0u64;
                loop {
                    check(token, deadline)?;
                    let n = file.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    read_bytes = read_bytes
                        .checked_add(n as u64)
                        .ok_or(Error::Overflow("source read bytes"))?;
                    if read_bytes > expected.bytes {
                        return Err(invalid("source grew during verification"));
                    }
                    hasher.update(&buf[..n]);
                }
                if hasher.finalize().to_hex().as_str() != expected.fingerprint {
                    return Err(invalid("source fingerprint drift"));
                }
                if !preflight {
                    continue;
                }
                preflight_file(
                    file,
                    &source.schema,
                    expected.rows,
                    token,
                    deadline,
                    sorted,
                    &mut last_row_id,
                )?;
            }
        }
        Ok(())
    }
}
fn preflight_file(
    mut file: File,
    schema: &Schema,
    expected_rows: u64,
    token: &CancellationToken,
    deadline: Instant,
    sorted: bool,
    last_row_id: &mut Option<u64>,
) -> Result<()> {
    file.seek(SeekFrom::End(-8))?;
    let mut footer = [0; 8];
    file.read_exact(&mut footer)?;
    if &footer[4..] != b"PAR1"
        || u32::from_le_bytes(footer[..4].try_into().map_err(|_| invalid("footer"))?) > 1024 * 1024
    {
        return Err(invalid("Parquet metadata cap"));
    }
    file.seek(SeekFrom::Start(0))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?;
    if reader.schema().as_ref() != arrow_schema(schema).as_ref()
        || u64::try_from(reader.metadata().file_metadata().num_rows()).ok() != Some(expected_rows)
    {
        return Err(invalid("source schema/row mismatch"));
    }
    for rg in reader.metadata().row_groups() {
        let mut decoded = 0u64;
        for c in rg.columns() {
            let bytes = u64::try_from(c.uncompressed_size())
                .map_err(|_| invalid("negative decoded size"))?;
            decoded = decoded
                .checked_add(bytes)
                .ok_or(Error::Overflow("decoded row group"))?;
        }
        if decoded > 16 * 1024 * 1024 {
            return Err(invalid("Parquet decoded row group cap 16MiB"));
        }
    }
    let mut rows = 0u64;
    for batch in reader.with_batch_size(1024).build()? {
        check(token, deadline)?;
        let batch = batch?;
        if batch.columns().iter().any(|a| a.null_count() != 0) {
            return Err(invalid("nullable data unsupported"));
        }
        rows = rows
            .checked_add(batch.num_rows() as u64)
            .ok_or(Error::Overflow("decoded rows"))?;
        if rows > expected_rows {
            return Err(invalid("decoded row count exceeds manifest"));
        }
        if sorted {
            let ids = batch
                .column_by_name("row_id")
                .and_then(|a| a.as_any().downcast_ref::<arrow::array::UInt64Array>())
                .ok_or_else(|| invalid("sort row_id"))?;
            for id in ids.values() {
                if last_row_id.is_some_and(|previous| previous >= *id) {
                    return Err(invalid("sort requires strictly increasing source row_id"));
                }
                *last_row_id = Some(*id);
            }
        }
    }
    if rows != expected_rows {
        return Err(invalid("decoded row count drift"));
    }
    Ok(())
}
/// Lower only validated local operators. Native callers need no frontend dependency.
/// # Errors
/// Refuses nonlocal targets and invalid graphs/catalog coverage.
pub fn lower(plan: &PlanV1, catalog: &Catalog, batch_size: usize) -> Result<LocalPlan> {
    plan.validate().map_err(|e| invalid(e.to_string()))?;
    if plan.target != Target::Local || batch_size == 0 || batch_size > 8192 {
        return Err(invalid("local target and batch_size 1..8192 required"));
    }
    lower_node(plan, catalog, plan.output, batch_size)
}
fn lower_node(plan: &PlanV1, catalog: &Catalog, id: u32, batch: usize) -> Result<LocalPlan> {
    let n = plan.node(id).ok_or_else(|| invalid("missing node"))?;
    let name = |i: u32, c: usize| plan.nodes[i as usize].schema[c].name.clone();
    let child = |i| lower_node(plan, catalog, i, batch).map(Box::new);
    Ok(match &n.operator {
        Operator::Scan { source } => LocalPlan::ParquetScan {
            files: catalog
                .bindings
                .get(*source as usize)
                .ok_or_else(|| invalid("missing binding"))?
                .files
                .clone(),
            batch_size: batch,
        },
        Operator::Filter {
            input,
            column,
            less_than,
        } => LocalPlan::Filter {
            input: child(*input)?,
            column: name(*input, *column),
            threshold: *less_than,
        },
        Operator::Project { input, columns } => LocalPlan::ProjectSchema {
            input: child(*input)?,
            columns: columns.iter().map(|c| name(*input, c.column)).collect(),
            schema: arrow_schema(&n.schema),
        },
        Operator::GroupSumCount { input, key, value } => LocalPlan::ProjectSchema {
            input: Box::new(LocalPlan::HashAggregate {
                input: child(*input)?,
                key: name(*input, *key),
                value: name(*input, *value),
            }),
            columns: vec!["key".into(), "sum".into(), "count".into()],
            schema: arrow_schema(&n.schema),
        },
        Operator::Sort {
            input,
            key,
            row_id,
            value,
        } => LocalPlan::Sort {
            input: child(*input)?,
            key: name(*input, *key),
            row_id: name(*input, *row_id),
            value: name(*input, *value),
        },
        Operator::Limit { input, rows } => LocalPlan::Limit {
            input: child(*input)?,
            rows: usize::try_from(*rows).map_err(|_| invalid("limit range"))?,
        },
        Operator::InnerJoin { .. } => {
            return Err(invalid("join requires distributed_join target"));
        }
    })
}

/// Execute the actual validated IR, retaining the native output reservation.
/// # Errors
/// Admission, native overflow/resource/cancellation or source-drift failure returns no output.
pub fn execute(
    plan: &PlanV1,
    catalog: &Catalog,
    config: ExecutionConfig,
    batch_size: usize,
    token: CancellationToken,
    timeout: Duration,
) -> Result<ExecutionOutput> {
    if timeout.is_zero() || timeout > Duration::from_secs(120) {
        return Err(invalid("timeout must be 1ns..120s"));
    }
    let deadline = Instant::now() + timeout;
    config.validate()?;
    if config.output_batch_rows > 8192 {
        return Err(invalid("physical output batch cap 8192"));
    }
    let local = lower(plan, catalog, batch_size)?;
    catalog.verify(plan, &token, deadline, true)?;
    std::thread::scope(|scope| {
        let (tx, rx) = std::sync::mpsc::channel();
        let timer_token = token.clone();
        scope.spawn(move || {
            if rx
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .is_err()
            {
                timer_token.cancel();
            }
        });
        let postflight_token = token.clone();
        let output = crate::execute_in_bounded(
            local,
            config,
            token,
            None,
            Some((plan.max_output_rows, plan.max_output_bytes)),
        );
        let _ = tx.send(());
        let output = output?;
        check(&postflight_token, deadline)?;
        catalog.verify(plan, &postflight_token, deadline, false)?;
        Ok(output)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ade_physical::{Field as IrField, SourceFile, fingerprint};
    use arrow::{
        array::{Int64Array, UInt64Array},
        record_batch::RecordBatch,
    };
    #[test]
    fn frontend_independent_ir_empty_signed_and_overflow() {
        for values in [vec![], vec![-9, 4], vec![i64::MAX, 1]] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("input.parquet");
            let fields = vec![
                IrField::new("key", Type::U64, false),
                IrField::new("value_i64", Type::I64, false),
            ];
            let schema = arrow_schema(&fields);
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(vec![0; values.len()])),
                    Arc::new(Int64Array::from(values.clone())),
                ],
            )
            .unwrap();
            let mut writer =
                parquet::arrow::ArrowWriter::try_new(File::create(&path).unwrap(), schema, None)
                    .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            let bytes = std::fs::read(&path).unwrap();
            let source = Source {
                id: 0,
                table: "input".into(),
                manifest: fingerprint(b"test manifest"),
                logical: fingerprint(b"test rows"),
                schema_identity: fingerprint(b"schema"),
                schema: fields,
                files: vec![SourceFile {
                    name: "input.parquet".into(),
                    bytes: bytes.len() as u64,
                    rows: values.len() as u64,
                    fingerprint: fingerprint(&bytes),
                }],
                replayable: true,
            };
            let catalog = Catalog {
                bindings: vec![Binding {
                    source: source.clone(),
                    files: vec![path],
                }],
            };
            let mut plan = PlanV1::new(Target::Local, vec![source]);
            plan.push(Operator::Scan { source: 0 }).unwrap();
            plan.push(Operator::GroupSumCount {
                input: 0,
                key: 0,
                value: 1,
            })
            .unwrap();
            let result = execute(
                &plan,
                &catalog,
                ExecutionConfig::default(),
                1024,
                CancellationToken::default(),
                Duration::from_secs(5),
            );
            if values == [i64::MAX, 1] {
                assert!(matches!(result, Err(Error::Overflow(_))));
            } else {
                let output = result.unwrap();
                assert!(
                    output
                        .metrics
                        .operators
                        .values()
                        .any(|m| m.name == "hash-aggregate")
                );
                if values.is_empty() {
                    assert!(output.batches.iter().all(|b| b.num_rows() == 0));
                } else {
                    assert_eq!(
                        output.batches[0]
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(0),
                        -5
                    );
                    assert_eq!(output.batches[0].column(2).data_type(), &DataType::Int64);
                }
            }
        }
    }
}
