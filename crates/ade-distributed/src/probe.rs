//! Bounded deterministic source-prefix observation. No exchange data is emitted.
use crate::{
    Error, Result, require,
    topology::{Observation, SHARDS, TopologyConfig, shard},
};
use arrow::array::{Array, UInt64Array};
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::file::metadata::ParquetMetaData;
use std::{collections::BTreeSet, fs::File, path::PathBuf, time::Instant};

const MAX_KEY_CHUNK_DECODE_BYTES: i64 = 2 * 1024 * 1024;

fn source_row_count(metadata: &ParquetMetaData) -> Result<u64> {
    metadata.row_groups().iter().try_fold(0u64, |sum, group| {
        let rows = u64::try_from(group.num_rows())
            .map_err(|_| Error::Invalid("negative probe row group".into()))?;
        sum.checked_add(rows)
            .ok_or_else(|| Error::Invalid("probe source row count overflow".into()))
    })
}

fn complete_source_coverage(
    files: &[PathBuf],
    max_files: usize,
    processed_files: usize,
    expected_rows: u64,
    sampled_rows: usize,
) -> bool {
    files.len() <= max_files
        && processed_files == files.len()
        && expected_rows == sampled_rows as u64
}

struct FileProbe {
    rows: usize,
    row_groups: usize,
    expected_rows: u64,
    opened_file_bytes: u64,
}

struct ProbeContext<'a> {
    config: &'a TopologyConfig,
    token: &'a ade_exec::CancellationToken,
    deadline: Instant,
    bytes: std::sync::Arc<std::sync::atomic::AtomicU64>,
    distinct: BTreeSet<u64>,
    counts: Vec<u64>,
}

fn probe_file(
    path: &std::path::Path,
    row_limit: usize,
    context: &mut ProbeContext<'_>,
) -> Result<FileProbe> {
    let file = File::open(path)?;
    let opened_file_bytes = file.metadata()?.len();
    let builder = ParquetRecordBatchReaderBuilder::try_new(MeteredFile {
        file,
        bytes: context.bytes.clone(),
        cap: context.config.sample_bytes as u64,
        token: context.token.clone(),
        deadline: context.deadline,
    })
    .map_err(|e| Error::Invalid(format!("probe parquet: {e}")))?;
    let expected_rows = source_row_count(builder.metadata())?;
    let key = builder.schema().index_of("key")?;
    require(
        builder.schema().field(key).data_type() == &arrow::datatypes::DataType::UInt64,
        "probe requires primitive UInt64 key",
    )?;
    let leaf = builder
        .parquet_schema()
        .columns()
        .iter()
        .position(|c| c.path().parts() == ["key"])
        .ok_or_else(|| Error::Invalid("probe key leaf missing".into()))?;
    let mask = ProjectionMask::roots(builder.parquet_schema(), [key]);
    let limit = context.config.rows_per_file.min(row_limit);
    let mut remaining = limit as u64;
    let mut groups = Vec::new();
    for (index, group) in builder.metadata().row_groups().iter().enumerate() {
        if remaining == 0 {
            break;
        }
        let rows = u64::try_from(group.num_rows())
            .map_err(|_| Error::Invalid("negative probe row group".into()))?;
        if rows > 0 {
            let size = group.column(leaf).uncompressed_size();
            require(
                (0..=MAX_KEY_CHUNK_DECODE_BYTES).contains(&size),
                "probe key decode chunk exceeds cap",
            )?;
            groups.push(index);
            remaining = remaining.saturating_sub(rows);
        }
    }
    let row_groups = groups.len();
    let reader = builder
        .with_row_groups(groups)
        .with_projection(mask)
        .with_batch_size(256)
        .with_limit(limit)
        .build()
        .map_err(|e| Error::Invalid(format!("probe parquet: {e}")))?;
    let mut rows = 0usize;
    for batch in reader {
        context.token.check()?;
        require(Instant::now() < context.deadline, "probe deadline exceeded")?;
        let batch = batch?;
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| Error::Invalid("probe key type".into()))?;
        require(
            keys.null_count() == 0 && rows + keys.len() <= limit,
            "probe key nulls/row limit",
        )?;
        rows += keys.len();
        for key in keys.values() {
            if context.distinct.insert(*key) {
                context.counts[shard(*key)] += 1;
            }
        }
    }
    Ok(FileProbe {
        rows,
        row_groups,
        expected_rows,
        opened_file_bytes,
    })
}

pub(crate) fn probe(
    files: &[PathBuf],
    producer: u64,
    config: &TopologyConfig,
    token: &ade_exec::CancellationToken,
    deadline: Instant,
) -> Result<Observation> {
    config.validate()?;
    let started = Instant::now();
    let mut out = Observation {
        producer,
        counts: vec![0; SHARDS],
        ..Default::default()
    };
    let mut context = ProbeContext {
        config,
        token,
        deadline,
        bytes: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        distinct: BTreeSet::new(), // <= sample_rows keys across the entire probe
        counts: vec![0; SHARDS],
    };
    let mut expected_rows = 0u64;
    let mut processed_files = 0usize;
    for path in files.iter().take(config.max_files) {
        token.check()?;
        require(Instant::now() < deadline, "probe deadline exceeded")?;
        if out.rows == config.sample_rows {
            break;
        }
        let file_probe = probe_file(path, config.sample_rows - out.rows, &mut context)?;
        out.opened_file_bytes = out
            .opened_file_bytes
            .checked_add(file_probe.opened_file_bytes)
            .ok_or_else(|| Error::Invalid("probe source size overflow".into()))?;
        expected_rows = expected_rows
            .checked_add(file_probe.expected_rows)
            .ok_or_else(|| Error::Invalid("probe source row count overflow".into()))?;
        out.rows += file_probe.rows;
        out.files += 1;
        out.row_groups_selected += file_probe.row_groups;
        processed_files += 1;
    }
    out.source_complete = complete_source_coverage(
        files,
        config.max_files,
        processed_files,
        expected_rows,
        out.rows,
    );
    out.counts = context.counts;
    out.read_bytes = context.bytes.load(std::sync::atomic::Ordering::Relaxed);
    out.elapsed_ns = crate::materialization::ns(started);
    Ok(out)
}

struct MeteredFile {
    file: File,
    bytes: std::sync::Arc<std::sync::atomic::AtomicU64>,
    cap: u64,
    token: ade_exec::CancellationToken,
    deadline: Instant,
}
impl std::io::Read for MeteredFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.token.check().map_err(std::io::Error::other)?;
        if Instant::now() >= self.deadline {
            return Err(std::io::Error::other("probe deadline"));
        }
        let remaining = self.cap - self.bytes.load(std::sync::atomic::Ordering::Relaxed);
        if remaining == 0 {
            return Err(std::io::Error::other("probe read byte cap exceeded"));
        }
        let limit = buf
            .len()
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let read = std::io::Read::read(&mut self.file, &mut buf[..limit])?;
        self.bytes
            .fetch_add(read as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(read)
    }
}
impl parquet::file::reader::Length for MeteredFile {
    fn len(&self) -> u64 {
        parquet::file::reader::Length::len(&self.file)
    }
}
impl parquet::file::reader::ChunkReader for MeteredFile {
    type T = Self;
    fn get_read(&self, start: u64) -> parquet::errors::Result<Self> {
        let mut file = self.file.try_clone()?;
        std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(start))?;
        Ok(Self {
            file,
            bytes: self.bytes.clone(),
            cap: self.cap,
            token: self.token.clone(),
            deadline: self.deadline,
        })
    }
    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        if length as u64 > self.cap - self.bytes.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(parquet::errors::ParquetError::General(
                "probe read byte cap exceeded".into(),
            ));
        }
        let mut reader = self.get_read(start)?;
        let mut bytes = vec![0; length];
        std::io::Read::read_exact(&mut reader, &mut bytes)?;
        Ok(bytes.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compressed_large_key_chunk_is_refused_before_one_row_probe_decode() {
        use arrow::{
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use std::sync::Arc;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("large.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            DataType::UInt64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from_iter_values(0..350_000))],
        )
        .unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .set_max_row_group_row_count(Some(350_000))
            .build();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(File::create(&path).unwrap(), schema, Some(props))
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let config = TopologyConfig {
            sample_rows: 1,
            rows_per_file: 1,
            ..Default::default()
        };
        let error = probe(
            &[path],
            0,
            &config,
            &ade_exec::CancellationToken::default(),
            Instant::now() + std::time::Duration::from_secs(5),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("decode chunk exceeds cap"),
            "{error}"
        );
    }
}
