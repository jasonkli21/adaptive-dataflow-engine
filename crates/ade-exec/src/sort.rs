//! Arrow-sorted runs, IPC streams, and online binary compaction (fan-in two).
use crate::{Error, ExecContext, Node, OperatorId, Result, memory::Reservation};
use arrow::{
    array::{Array, ArrayRef, Int64Array, UInt64Array},
    compute::{SortColumn, lexsort_to_indices},
    datatypes::SchemaRef,
    ipc::{reader::StreamReader, writer::StreamWriter},
    record_batch::RecordBatch,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
const CHUNK: usize = 128;
type Row = (u64, u64, i64);
fn batch(rows: &[Row], schema: &SchemaRef) -> Result<RecordBatch> {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.0))),
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.1))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))),
    ];
    Ok(RecordBatch::try_new(schema.clone(), arrays)?)
}
fn row(batch: &RecordBatch, i: usize, columns: [usize; 3]) -> Result<Row> {
    let key = batch
        .column(columns[0])
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| Error::Invalid("sort key type".into()))?;
    let id = batch
        .column(columns[1])
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| Error::Invalid("sort row-id type".into()))?;
    let value = batch
        .column(columns[2])
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::Invalid("sort value type".into()))?;
    if key.is_null(i) || id.is_null(i) || value.is_null(i) {
        return Err(Error::Invalid("sort nulls unsupported".into()));
    }
    Ok((key.value(i), id.value(i), value.value(i)))
}
fn ordered(rows: &[Row], schema: &SchemaRef) -> Result<Vec<Row>> {
    let batch = batch(rows, schema)?;
    let columns = [0, 1].map(|i| SortColumn {
        values: batch.column(i).clone(),
        options: None,
    });
    let indices = lexsort_to_indices(&columns, None)?;
    Ok(indices.values().iter().map(|i| rows[*i as usize]).collect())
}
struct Cursor {
    reader: StreamReader<crate::spill::SpillReader>,
    batch: Option<RecordBatch>,
    index: usize,
}
impl Cursor {
    fn open(path: &Path, schema: &SchemaRef, cx: &mut ExecContext) -> Result<Self> {
        let reader = StreamReader::try_new(cx.spill.reading(path)?, None)?;
        if reader.schema().as_ref() != schema.as_ref() {
            return Err(Error::CorruptSpill("sort schema"));
        }
        Ok(Self {
            reader,
            batch: None,
            index: 0,
        })
    }
    fn peek(&mut self) -> Result<Option<Row>> {
        if self
            .batch
            .as_ref()
            .is_none_or(|b| self.index == b.num_rows())
        {
            self.batch = self.reader.next().transpose()?;
            self.index = 0;
            if let Some(b) = &self.batch
                && (b.num_rows() == 0 || b.num_rows() > CHUNK)
            {
                return Err(Error::CorruptSpill("sort chunk bound"));
            }
        }
        self.batch
            .as_ref()
            .map(|b| row(b, self.index, [0, 1, 2]))
            .transpose()
    }
    fn pop(&mut self) -> Result<Option<Row>> {
        let r = self.peek()?;
        if r.is_some() {
            self.index += 1;
        }
        Ok(r)
    }
}
fn write_run(
    rows: &[Row],
    schema: &SchemaRef,
    id: OperatorId,
    cx: &mut ExecContext,
) -> Result<PathBuf> {
    let (path, file) = cx.spill.create(id)?;
    let mut writer = StreamWriter::try_new(file, schema)?;
    for chunk in rows.chunks(CHUNK) {
        cx.cancellation.check()?;
        writer.write(&batch(chunk, schema)?)?;
    }
    writer.finish()?;
    drop(writer);
    cx.spill.wrote(&path)?;
    cx.spill.event(id);
    cx.spill.metrics.sort_runs += 1;
    Ok(path)
}
fn merge(
    left: PathBuf,
    right: PathBuf,
    schema: &SchemaRef,
    id: OperatorId,
    cx: &mut ExecContext,
) -> Result<PathBuf> {
    let mut reservation = cx.memory.reservation(id);
    reservation.grow(32768)?;
    let mut a = Cursor::open(&left, schema, cx)?;
    let mut b = Cursor::open(&right, schema, cx)?;
    let (path, file) = cx.spill.create(id)?;
    let mut writer = StreamWriter::try_new(file, schema)?;
    let mut rows = Vec::with_capacity(CHUNK);
    loop {
        cx.cancellation.check()?;
        let next = match (a.peek()?, b.peek()?) {
            (None, None) => break,
            (Some(_), None) => a.pop()?,
            (None, Some(_)) => b.pop()?,
            (Some(x), Some(y)) => {
                if (x.0, x.1) <= (y.0, y.1) {
                    a.pop()?
                } else {
                    b.pop()?
                }
            }
        };
        rows.push(next.expect("nonempty"));
        if rows.len() == CHUNK {
            writer.write(&batch(&rows, schema)?)?;
            rows.clear();
        }
    }
    if !rows.is_empty() {
        writer.write(&batch(&rows, schema)?)?;
    }
    writer.finish()?;
    drop(writer);
    drop(a);
    drop(b);
    cx.spill.wrote(&path)?;
    cx.spill.event(id);
    cx.spill.metrics.sort_merges += 1;
    std::fs::remove_file(left)?;
    std::fs::remove_file(right)?;
    Ok(path)
}
pub(crate) struct Sort {
    rows: Vec<Row>,
    cursor: Option<Cursor>,
    index: usize,
    reservation: Option<Reservation>,
}
impl Sort {
    pub fn build(
        input: &mut Node,
        columns: [usize; 3],
        schema: &SchemaRef,
        id: OperatorId,
        cx: &mut ExecContext,
    ) -> Result<Self> {
        let mut metadata = cx.memory.reservation(id);
        metadata.grow(16384)?;
        let capacity = (cx.config.memory_limit_bytes / 4 / 128).clamp(1, 65536);
        let mut reservation = cx.memory.reservation(id);
        reservation.grow(capacity * 128)?;
        let mut rows = Vec::with_capacity(capacity);
        // At most one run per level; never retain metadata proportional to input size.
        let mut levels: [Option<PathBuf>; 64] = std::array::from_fn(|_| None);
        let mut external = false;
        while let Some(batch) = input.next_batch(cx)? {
            cx.input(id, &batch);
            let start = Instant::now();
            for i in 0..batch.num_rows() {
                if i % 1024 == 0 {
                    cx.cancellation.check()?;
                }
                if rows.len() == capacity {
                    cx.memory.pressure();
                    if !cx.config.spill_enabled {
                        return Err(Error::SpillDisabled);
                    }
                    let sorted = ordered(&rows, schema)?;
                    let path = write_run(&sorted, schema, id, cx)?;
                    Self::compact(path, &mut levels, schema, id, cx)?;
                    rows.clear();
                    external = true;
                }
                rows.push(row(&batch, i, columns)?);
            }
            cx.work(id, start);
        }
        input.shutdown(cx)?;
        let start = Instant::now();
        let sorted = ordered(&rows, schema)?;
        drop(rows); // release the original allocation before transferring its reservation
        let rows = sorted;
        if !external {
            cx.work(id, start);
            return Ok(Self {
                rows,
                cursor: None,
                index: 0,
                reservation: Some(reservation),
            });
        }
        if !rows.is_empty() {
            let path = write_run(&rows, schema, id, cx)?;
            Self::compact(path, &mut levels, schema, id, cx)?;
        }
        drop(rows);
        drop(reservation);
        let mut final_run = None;
        for path in levels.into_iter().flatten() {
            final_run = Some(if let Some(previous) = final_run {
                merge(previous, path, schema, id, cx)?
            } else {
                path
            });
        }
        let mut reservation = cx.memory.reservation(id);
        reservation.grow(32768)?;
        let cursor = Cursor::open(&final_run.expect("external runs"), schema, cx)?;
        cx.work(id, start);
        Ok(Self {
            rows: Vec::new(),
            cursor: Some(cursor),
            index: 0,
            reservation: Some(reservation),
        })
    }
    fn compact(
        mut path: PathBuf,
        levels: &mut [Option<PathBuf>; 64],
        schema: &SchemaRef,
        id: OperatorId,
        cx: &mut ExecContext,
    ) -> Result<()> {
        for level in levels {
            if let Some(previous) = level.take() {
                path = merge(previous, path, schema, id, cx)?;
            } else {
                *level = Some(path);
                return Ok(());
            }
        }
        Err(Error::SpillNoProgress("sort run count exceeds 64 levels"))
    }
    pub fn next(
        &mut self,
        schema: &SchemaRef,
        id: OperatorId,
        cx: &mut ExecContext,
    ) -> Result<Option<RecordBatch>> {
        if self.cursor.is_some() {
            let exhausted = self
                .cursor
                .as_mut()
                .expect("checked above")
                .peek()?
                .is_none();
            if exhausted {
                self.cursor.take();
                self.reservation.take();
                return Ok(None);
            }
        } else if self.index >= self.rows.len() {
            self.rows = Vec::new();
            self.reservation.take();
            return Ok(None);
        }
        let size = cx.config.output_batch_rows.min(CHUNK);
        let mut reservation = cx.memory.reservation(id);
        reservation.grow(size * 48 + 1024)?;
        if self.cursor.is_some() {
            let (rows, exhausted) = {
                let cursor = self.cursor.as_mut().expect("checked above");
                let mut rows = Vec::with_capacity(size);
                for _ in 0..size {
                    cx.cancellation.check()?;
                    if let Some(r) = cursor.pop()? {
                        rows.push(r);
                    } else {
                        break;
                    }
                }
                let exhausted = cursor.peek()?.is_none();
                (rows, exhausted)
            };
            if rows.is_empty() {
                self.cursor.take();
                self.reservation.take();
                return Ok(None);
            }
            let result = batch(&rows, schema)?;
            if exhausted {
                self.cursor.take();
                self.reservation.take();
            }
            return Ok(Some(result));
        }
        let end = (self.index + size).min(self.rows.len());
        let result = batch(&self.rows[self.index..end], schema)?;
        self.index = end;
        if end == self.rows.len() {
            self.rows = Vec::new();
            self.reservation.take();
        }
        Ok(Some(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("key", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new("row_id", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
        ]))
    }

    #[test]
    fn exhausted_sort_does_not_reserve_output_memory() {
        let mut cx = ExecContext::new(
            crate::ExecutionConfig {
                memory_limit_bytes: 10_000,
                ..Default::default()
            },
            crate::CancellationToken::default(),
            None,
        )
        .unwrap();
        let id = OperatorId(0);
        let mut competing = cx.memory.reservation(OperatorId(1));
        competing.grow(3_000).unwrap();
        let mut state = Sort {
            rows: Vec::new(),
            cursor: None,
            index: 0,
            reservation: None,
        };
        let attempts = cx.memory.snapshot().attempts;
        assert!(state.next(&schema(), id, &mut cx).unwrap().is_none());
        assert_eq!(cx.memory.snapshot().attempts, attempts);
    }

    #[test]
    fn terminal_in_memory_batch_releases_sort_reservation() {
        let mut cx = ExecContext::new(
            crate::ExecutionConfig::default(),
            crate::CancellationToken::default(),
            None,
        )
        .unwrap();
        let id = OperatorId(0);
        let mut retained = cx.memory.reservation(id);
        retained.grow(128).unwrap();
        let mut state = Sort {
            rows: vec![(1, 2, 3)],
            cursor: None,
            index: 0,
            reservation: Some(retained),
        };
        assert!(state.next(&schema(), id, &mut cx).unwrap().is_some());
        assert_eq!(cx.memory.snapshot().current, 0);
    }

    #[test]
    fn cancelled_merge_releases_reservation_and_spill_root() {
        let parent = tempfile::tempdir().unwrap();
        let token = crate::CancellationToken::default();
        let mut cx = ExecContext::new(
            crate::ExecutionConfig::default(),
            token.clone(),
            Some(parent.path()),
        )
        .unwrap();
        let id = OperatorId(0);
        let schema = schema();
        let left = write_run(&[(1, 1, 10)], &schema, id, &mut cx).unwrap();
        let right = write_run(&[(2, 2, 20)], &schema, id, &mut cx).unwrap();
        token.cancel();
        assert!(matches!(
            merge(left, right, &schema, id, &mut cx),
            Err(Error::Cancelled)
        ));
        assert_eq!(cx.memory.snapshot().current, 0);
        drop(cx);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }
}
