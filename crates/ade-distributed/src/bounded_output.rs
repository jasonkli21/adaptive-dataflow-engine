use crate::{Error, Result};
use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone, Copy)]
pub(crate) struct OutputLimits {
    pub rows: u64,
    pub bytes: u64,
}
impl OutputLimits {
    pub fn validate(self) -> Result<()> {
        if self.rows == 0 || self.bytes == 0 {
            return Err(Error::Invalid(
                "positive physical output limits required".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) struct OutputBudget {
    limits: OutputLimits,
    rows: u64,
    bytes: u64,
}

impl OutputBudget {
    pub fn new(limits: OutputLimits) -> Result<Self> {
        limits.validate()?;
        Ok(Self {
            limits,
            rows: 0,
            bytes: 0,
        })
    }

    /// Account a decoded batch before retaining it in the collected result.
    pub fn include(&mut self, batch: &RecordBatch) -> Result<()> {
        let rows = u64::try_from(batch.num_rows())
            .map_err(|_| Error::Invalid("physical output row overflow".into()))?;
        let bytes = batch
            .get_array_memory_size()
            .checked_add(128)
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(|| Error::Invalid("physical output byte overflow".into()))?;
        self.rows = self
            .rows
            .checked_add(rows)
            .ok_or_else(|| Error::Invalid("physical output row overflow".into()))?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| Error::Invalid("physical output byte overflow".into()))?;
        if self.rows > self.limits.rows || self.bytes > self.limits.bytes {
            return Err(Error::Invalid(
                "physical output cap; no committed output".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::UInt64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

    fn batch(rows: usize) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "key",
                DataType::UInt64,
                false,
            )])),
            vec![Arc::new(UInt64Array::from_iter_values(0..rows as u64))],
        )
        .unwrap()
    }

    fn accounted_bytes(batch: &RecordBatch) -> u64 {
        u64::try_from(batch.get_array_memory_size() + 128).unwrap()
    }

    #[test]
    fn one_batch_at_exact_row_and_byte_boundary_is_admitted() {
        let batch = batch(3);
        let mut budget = OutputBudget::new(OutputLimits {
            rows: 3,
            bytes: accounted_bytes(&batch),
        })
        .unwrap();
        budget.include(&batch).unwrap();
    }

    #[test]
    fn multi_batch_overflow_is_refused_before_the_next_batch_is_retained() {
        let first = batch(1);
        let second = batch(1);
        let mut budget = OutputBudget::new(OutputLimits {
            rows: 2,
            bytes: accounted_bytes(&first),
        })
        .unwrap();
        budget.include(&first).unwrap();
        assert!(budget.include(&second).is_err());
    }

    #[test]
    fn empty_collection_does_not_consume_output_budget() {
        let _budget = OutputBudget::new(OutputLimits { rows: 1, bytes: 1 }).unwrap();
    }
}
