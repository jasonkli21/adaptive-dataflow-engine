use crate::{Error, MAX_COLUMNS, Request, Result, RowKernel};
use ade_exec::{BatchMap, CancellationToken, physical::arrow_schema};
use arrow::{
    array::{Array, ArrayRef, Float64Array, Int64Array, UInt64Array},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

#[derive(Debug, Clone, Copy)]
pub enum Value {
    U64(u64),
    I64(i64),
    F64(f64),
}

impl Value {
    pub(crate) fn bits(self) -> u64 {
        match self {
            Self::U64(v) => v,
            Self::I64(v) => u64::from_le_bytes(v.to_le_bytes()),
            Self::F64(v) => v.to_bits(),
        }
    }

    fn finite(self) -> bool {
        !matches!(self, Self::F64(v) if !v.is_finite())
    }
}

/// One borrowed row's scalar values. Replacing a protected field fails the run.
pub struct Row {
    values: [Value; MAX_COLUMNS],
    len: usize,
}

impl Row {
    #[must_use]
    pub fn get(&self, index: usize) -> Option<Value> {
        self.values[..self.len].get(index).copied()
    }

    /// # Errors
    /// Rejects out-of-range cells. The adapter subsequently checks type and write permissions.
    pub fn set(&mut self, index: usize, value: Value) -> Result<()> {
        *self.values[..self.len]
            .get_mut(index)
            .ok_or(Error::Kernel("cell index"))? = value;
        Ok(())
    }
}

pub(crate) enum Column<'a> {
    U64(&'a UInt64Array),
    I64(&'a Int64Array),
    F64(&'a Float64Array),
}

impl Column<'_> {
    pub(crate) fn get(&self, index: usize) -> Value {
        match self {
            Self::U64(array) => Value::U64(array.value(index)),
            Self::I64(array) => Value::I64(array.value(index)),
            Self::F64(array) => Value::F64(array.value(index)),
        }
    }
}

pub(crate) fn columns(batch: &RecordBatch) -> Result<Vec<Column<'_>>> {
    batch
        .columns()
        .iter()
        .map(|array| {
            let any = array.as_any();
            if let Some(array) = any.downcast_ref::<UInt64Array>() {
                Ok(Column::U64(array))
            } else if let Some(array) = any.downcast_ref::<Int64Array>() {
                Ok(Column::I64(array))
            } else if let Some(array) = any.downcast_ref::<Float64Array>() {
                Ok(Column::F64(array))
            } else {
                Err(Error::Schema)
            }
        })
        .collect()
}

enum Buffer {
    Protected,
    Signed(Vec<i64>),
    Float(Vec<f64>),
}

impl Buffer {
    fn push(&mut self, original: Value, mapped: Value) -> ade_exec::Result<()> {
        if !mapped.finite() {
            return Err(ade_exec::Error::BatchMap("nonfinite output"));
        }
        match (self, original, mapped) {
            (Self::Signed(values), Value::I64(_), Value::I64(value)) => values.push(value),
            (Self::Float(values), Value::F64(_), Value::F64(value)) => values.push(value),
            (Self::Protected, a, b)
                if std::mem::discriminant(&a) == std::mem::discriminant(&b)
                    && a.bits() == b.bits() => {}
            _ => {
                return Err(ade_exec::Error::BatchMap(
                    "protected field or mapped type changed",
                ));
            }
        }
        Ok(())
    }

    fn finish(self, original: &ArrayRef) -> ArrayRef {
        match self {
            Self::Protected => original.clone(),
            Self::Signed(values) => Arc::new(Int64Array::from(values)),
            Self::Float(values) => Arc::new(Float64Array::from(values)),
        }
    }
}

pub(crate) struct Guarded<K> {
    kernel: K,
    schema: SchemaRef,
    writable: [bool; MAX_COLUMNS],
    max_rows: usize,
    max_bytes: usize,
    deadline: Instant,
    last_id: Mutex<Option<u64>>,
    pub peak: AtomicUsize,
}

impl<K> Guarded<K> {
    pub(crate) fn new(kernel: K, request: &Request<'_>) -> Self {
        let mut writable = [false; MAX_COLUMNS];
        for &index in &request.descriptor.mutable {
            writable[index] = true;
        }
        Self {
            kernel,
            schema: arrow_schema(&request.descriptor.schema),
            writable,
            max_rows: request.limits.batch_rows,
            max_bytes: request.limits.batch_bytes,
            deadline: request.deadline,
            last_id: Mutex::new(None),
            peak: AtomicUsize::new(0),
        }
    }
}

impl<K> fmt::Debug for Guarded<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GuardedRowMap").finish_non_exhaustive()
    }
}

impl<K: RowKernel> BatchMap for Guarded<K> {
    fn map(
        &self,
        batch: &RecordBatch,
        cancel: &CancellationToken,
    ) -> ade_exec::Result<RecordBatch> {
        let error = ade_exec::Error::BatchMap;
        let rows = batch.num_rows();
        if batch.schema() != self.schema
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(error("invalid input schema or null"));
        }
        if rows > self.max_rows {
            return Err(error("batch row cap"));
        }
        let input = batch.get_array_memory_size();
        // Charge both input and output array claims, even when protected arrays alias.
        // Charge writable scalar buffers and bounded row/column bookkeeping before allocation.
        let workspace =
            rows * self.writable.iter().filter(|&&writable| writable).count() * 8 + 1024;
        if input.saturating_mul(2).saturating_add(workspace) > self.max_bytes {
            return Err(error("extension batch byte cap"));
        }
        let source = columns(batch).map_err(|_| error("invalid column type"))?;
        let mut buffers = source
            .iter()
            .enumerate()
            .map(|(i, column)| match (self.writable[i], column) {
                (true, Column::I64(_)) => Buffer::Signed(Vec::with_capacity(rows)),
                (true, Column::F64(_)) => Buffer::Float(Vec::with_capacity(rows)),
                _ => Buffer::Protected,
            })
            .collect::<Vec<_>>();
        let mut last_id = self.last_id.lock().map_err(|_| error("poisoned map"))?;
        for i in 0..rows {
            cancel.check()?;
            if Instant::now() >= self.deadline {
                return Err(ade_exec::Error::Deadline("extension mapping"));
            }
            let mut row = Row {
                len: source.len(),
                values: [Value::U64(0); MAX_COLUMNS],
            };
            for (value, column) in row.values.iter_mut().zip(&source) {
                *value = column.get(i);
                if !value.finite() {
                    return Err(error("nonfinite input"));
                }
            }
            let Value::U64(id) = row.values[0] else {
                return Err(error("UInt64 row identity required"));
            };
            if last_id.is_some_and(|previous| previous >= id) {
                return Err(error("row IDs must strictly increase"));
            }
            *last_id = Some(id);
            self.kernel.transform(&mut row).map_err(|cause| {
                if let Error::Kernel(reason) = cause {
                    error(reason)
                } else {
                    error("kernel refused")
                }
            })?;
            for ((buffer, column), value) in buffers.iter_mut().zip(&source).zip(row.values) {
                buffer.push(column.get(i), value)?;
            }
        }
        let arrays = buffers
            .into_iter()
            .zip(batch.columns())
            .map(|(buffer, original)| buffer.finish(original))
            .collect();
        let result = RecordBatch::try_new(self.schema.clone(), arrays)?;
        let charged = input + result.get_array_memory_size() + workspace;
        if charged > self.max_bytes {
            return Err(error("extension batch byte cap"));
        }
        self.peak.fetch_max(charged, Ordering::Relaxed);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Capability, ExtensionDescriptorV1, Field, Limits, SourcePin, Type, digest};
    use std::{path::Path, time::Duration};

    struct Twice;

    impl RowKernel for Twice {
        fn transform(&self, row: &mut Row) -> Result<()> {
            let Some(Value::I64(value)) = row.get(1) else {
                return Err(Error::Kernel("test schema"));
            };
            row.set(1, Value::I64(value * 2))
        }
    }

    struct CancelOnFirst(CancellationToken);

    impl RowKernel for CancelOnFirst {
        fn transform(&self, _: &mut Row) -> Result<()> {
            self.0.cancel();
            Ok(())
        }
    }

    #[test]
    fn every_batch_split_preserves_order_and_cancellation_aborts() {
        let descriptor = ExtensionDescriptorV1 {
            abi: 1,
            id: "test.twice".into(),
            version: 1,
            capability: Capability::LocalReplayableRowMap,
            schema: vec![
                Field::new("row_id", Type::U64, false),
                Field::new("value_i64", Type::I64, false),
            ],
            mutable: vec![1],
        };
        let ids = (0..257).collect::<Vec<u64>>();
        let values = (0..257).collect::<Vec<i64>>();
        let batch = RecordBatch::try_new(
            arrow_schema(&descriptor.schema),
            vec![
                Arc::new(UInt64Array::from(ids.clone())),
                Arc::new(Int64Array::from(values.clone())),
            ],
        )
        .unwrap();
        let no_pin = digest(b"");
        let mut request = Request {
            descriptor: &descriptor,
            executable: no_pin,
            model_bytes: &[],
            model_pin: no_pin,
            source: SourcePin {
                digest: no_pin,
                bytes: 0,
                rows: 257,
            },
            path: Path::new("/unused"),
            projection: &[0, 1],
            limits: Limits::default(),
            deadline: Instant::now() + Duration::from_secs(10),
        };
        for split in 1..=256 {
            request.limits.batch_rows = split;
            let guarded = Guarded::new(Twice, &request);
            for offset in (0..ids.len()).step_by(split) {
                let len = split.min(ids.len() - offset);
                let output = guarded
                    .map(&batch.slice(offset, len), &CancellationToken::default())
                    .unwrap();
                let fields = columns(&output).unwrap();
                for index in 0..len {
                    assert!(
                        matches!(fields[0].get(index), Value::U64(id) if id == ids[offset + index])
                    );
                    assert!(
                        matches!(fields[1].get(index), Value::I64(value) if value == 2 * values[offset + index])
                    );
                }
            }
        }
        let token = CancellationToken::default();
        let guarded = Guarded::new(CancelOnFirst(token.clone()), &request);
        assert!(matches!(
            guarded.map(&batch.slice(0, 2), &token),
            Err(ade_exec::Error::Cancelled)
        ));
    }
}
