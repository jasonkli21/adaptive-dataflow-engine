use crate::{CancellationToken, Error, Result, physical::arrow_schema};
use ade_physical::cycle::Role;
use arrow::array::{Array, UInt64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    time::Instant,
};

pub const MAX_BATCH_ROWS: usize = 1024;
pub const MAX_PARQUET_METADATA: u32 = 1024 * 1024;
pub const MAX_DECODED_GROUP_BYTES: u64 = 16 * 1024 * 1024;

/// Decode the exact typed graph scan from an already attested descriptor.
/// The owner must keep and re-attest that descriptor and its path after decoding.
/// Returns maximum observed Arrow batch buffer bytes.
///
/// # Errors
/// Refuses mismatched Parquet shape, rows, nulls, metadata and decode bounds.
pub fn scan_graph_file(
    mut file: File,
    role: Role,
    rows: usize,
    batch_rows: usize,
    cancel: &CancellationToken,
    deadline: Instant,
    mut emit: impl FnMut(u64, Option<u64>),
) -> Result<usize> {
    let invalid = |why: &str| Error::Invalid(why.into());
    if !(1..=MAX_BATCH_ROWS).contains(&batch_rows) {
        return Err(invalid("graph scan batch rows"));
    }
    super::check(cancel, deadline)?;
    file.seek(SeekFrom::End(-8))?;
    let mut footer = [0u8; 8];
    file.read_exact(&mut footer)?;
    if &footer[4..] != b"PAR1"
        || u32::from_le_bytes([footer[0], footer[1], footer[2], footer[3]]) > MAX_PARQUET_METADATA
    {
        return Err(invalid("graph Parquet metadata cap or invalid footer"));
    }
    file.seek(SeekFrom::Start(0))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?;
    if reader.schema().as_ref() != arrow_schema(&role.schema()).as_ref()
        || usize::try_from(reader.metadata().file_metadata().num_rows()).ok() != Some(rows)
    {
        return Err(invalid("graph Parquet schema or declared rows"));
    }
    for group in reader.metadata().row_groups() {
        let bytes = u64::try_from(group.total_byte_size())
            .map_err(|_| invalid("negative Parquet decode bytes"))?;
        let columns = group.columns().iter().try_fold(0u64, |sum, column| {
            let bytes = u64::try_from(column.uncompressed_size())
                .map_err(|_| invalid("negative Parquet column bytes"))?;
            sum.checked_add(bytes)
                .ok_or(Error::Overflow("Parquet decoded columns"))
        })?;
        if bytes.max(columns) > MAX_DECODED_GROUP_BYTES {
            return Err(invalid("graph Parquet decoded row group cap"));
        }
    }
    let (mut seen, mut peak) = (0usize, 0usize);
    for batch in reader.with_batch_size(batch_rows).build()? {
        super::check(cancel, deadline)?;
        let batch = batch?;
        seen = seen
            .checked_add(batch.num_rows())
            .ok_or(Error::Overflow("graph decoded rows"))?;
        if seen > rows
            || batch.num_rows() > batch_rows
            || batch.columns().iter().any(|a| a.null_count() != 0)
        {
            return Err(invalid("graph Parquet decoded rows or nulls"));
        }
        peak = peak.max(batch.get_array_memory_size());
        let column = |i| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| invalid("graph Parquet decoded column type"))
        };
        let left = column(0)?;
        match role {
            Role::Vertices => left.values().iter().for_each(|&v| emit(v, None)),
            Role::Edges => {
                for (&src, &dst) in left.values().iter().zip(column(1)?.values()) {
                    emit(src, Some(dst));
                }
            }
        }
    }
    if seen != rows {
        return Err(invalid("graph Parquet decoded row count drift"));
    }
    Ok(peak)
}
