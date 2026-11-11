use crate::{Error, Result};
use parquet::{arrow::arrow_reader::ParquetRecordBatchReaderBuilder, basic::Compression};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

const MAX_DECODED_GROUP: u64 = 16 * 1024 * 1024;

pub(crate) fn open(
    path: &Path,
    uncompressed: bool,
) -> Result<ParquetRecordBatchReaderBuilder<File>> {
    let mut file = File::open(path)?;
    let bytes = file.metadata()?.len();
    file.seek(SeekFrom::End(-8))?;
    let mut footer = [0; 8];
    file.read_exact(&mut footer)?;
    let footer_bytes = u64::from(u32::from_le_bytes([
        footer[0], footer[1], footer[2], footer[3],
    ]));
    let data_end = bytes
        .checked_sub(8 + footer_bytes)
        .filter(|&n| n >= 4)
        .ok_or(Error::Protocol("Parquet metadata range"))?;
    if &footer[4..] != b"PAR1" || footer_bytes > 1024 * 1024 {
        return Err(Error::Admission("Parquet footer or encryption"));
    }
    file.rewind()?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|_| Error::Protocol("Parquet metadata"))?;
    for group in reader.metadata().row_groups() {
        let mut decoded = 0;
        for column in group.columns() {
            match column.compression() {
                Compression::UNCOMPRESSED => {}
                Compression::SNAPPY if !uncompressed => {}
                _ => return Err(Error::Admission("connector Parquet compression")),
            }
            let n = u64::try_from(column.uncompressed_size())
                .map_err(|_| Error::Protocol("Parquet decoded size"))?;
            if n > MAX_DECODED_GROUP - decoded {
                return Err(Error::Limit("Parquet decoded row group"));
            }
            decoded += n;
            let start = column
                .dictionary_page_offset()
                .unwrap_or_else(|| column.data_page_offset());
            let end = start
                .checked_add(column.compressed_size())
                .and_then(|n| u64::try_from(n).ok());
            if start < 4 || column.compressed_size() < 0 || end.is_none_or(|n| n > data_end) {
                return Err(Error::Protocol("Parquet column byte range"));
            }
        }
    }
    Ok(reader)
}
