//! Projected primitive Parquet source with independent API/metadata/decode bounds.
//! Prefix mode is ONLY an observation source; full mode refuses truncation.
use crate::{BatchSource, CancellationToken, Error, Result, join};
use arrow::{
    array::Array,
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
    },
    file::reader::{ChunkReader, Length},
};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Full,
    Prefix,
}
#[derive(Debug, Clone)]
pub struct Config {
    pub mode: Mode,
    pub max_files: usize,
    pub max_rows: usize,
    pub max_read_bytes: u64,
    pub max_metadata_bytes: u64,
    pub max_decode_chunk_bytes: u64,
    pub batch_rows: usize,
    pub timeout: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            mode: Mode::Full,
            max_files: 64,
            max_rows: 1_000_000,
            max_read_bytes: 64 * 1024 * 1024,
            max_metadata_bytes: 1024 * 1024,
            max_decode_chunk_bytes: 1024 * 1024,
            batch_rows: 1024,
            timeout: Duration::from_secs(120),
        }
    }
}
impl Config {
    /// # Errors
    /// Refuses unbounded/unsupported source parameters.
    pub fn validate(&self) -> Result<()> {
        if !(1..=64).contains(&self.max_files)
            || !(1..=1_000_000).contains(&self.max_rows)
            || !(1..=1024).contains(&self.batch_rows)
            || self.max_read_bytes == 0
            || self.max_read_bytes > 1024 * 1024 * 1024
            || self.max_metadata_bytes == 0
            || self.max_metadata_bytes > 1024 * 1024
            || self.max_decode_chunk_bytes == 0
            || self.max_decode_chunk_bytes > 2 * 1024 * 1024
            || self.timeout.is_zero()
            || self.timeout > Duration::from_secs(120)
        {
            return Err(Error::Invalid("row source bounds".into()));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stats {
    pub rows: u64,
    pub peak_batch_bytes: usize,
    pub opened_files: usize,
    pub completed_files: usize,
    pub expected_opened_rows: u64,
    pub read_bytes: u64,
    pub metadata_bytes: u64,
    pub decode_chunk_peak_bytes: u64,
    pub selected_row_groups: usize,
    pub complete: bool,
    pub limited: bool,
}
#[derive(Debug)]
pub struct ParquetRows {
    files: std::vec::IntoIter<PathBuf>,
    assigned: usize,
    config: Config,
    deadline: Instant,
    reader: Option<ParquetRecordBatchReader>,
    order: [usize; 2],
    file_rows: u64,
    expected_file_rows: u64,
    bytes: Arc<AtomicU64>,
    stats: Stats,
}
impl ParquetRows {
    /// # Errors
    /// Rejects invalid bounds or an excessive file list, before opening sources.
    pub fn new(files: Vec<PathBuf>, config: Config) -> Result<Self> {
        config.validate()?;
        if files.len() > config.max_files {
            return Err(Error::Invalid("row source file cap".into()));
        }
        Ok(Self {
            assigned: files.len(),
            files: files.into_iter(),
            deadline: Instant::now() + config.timeout,
            config,
            reader: None,
            order: [0, 1],
            file_rows: 0,
            expected_file_rows: 0,
            bytes: Arc::default(),
            stats: Stats::default(),
        })
    }
    #[must_use]
    pub fn stats(&self) -> Stats {
        let mut stats = self.stats.clone();
        stats.read_bytes = self.bytes.load(Ordering::Relaxed);
        stats
    }
    fn limited(&mut self) -> Result<Option<RecordBatch>> {
        self.stats.limited = true;
        if self.config.mode == Mode::Full {
            Err(Error::Invalid(
                "full row source exceeds configured row bound".into(),
            ))
        } else {
            Ok(None)
        }
    }
    #[allow(
        clippy::too_many_lines,
        reason = "Metadata and decode guards must all precede construction of this reader."
    )]
    fn open(&mut self, path: PathBuf, token: &CancellationToken) -> Result<()> {
        let mut file = File::open(&path)?;
        let length = file.metadata()?.len();
        if length < 8 {
            return Err(Error::Invalid("Parquet footer length".into()));
        }
        file.seek(SeekFrom::End(-8))?;
        let mut footer = [0; 8];
        let mut metered = Metered {
            path,
            file,
            length,
            bytes: self.bytes.clone(),
            cap: self.config.max_read_bytes,
            token: token.clone(),
            deadline: self.deadline,
        };
        metered.read_exact(&mut footer)?;
        if &footer[4..] != b"PAR1" {
            return Err(Error::Invalid("Parquet footer magic".into()));
        }
        let metadata = u64::from(u32::from_le_bytes(
            footer[..4]
                .try_into()
                .map_err(|_| Error::Invalid("Parquet footer".into()))?,
        ));
        self.stats.metadata_bytes = self
            .stats
            .metadata_bytes
            .checked_add(metadata)
            .ok_or(Error::Overflow("source metadata"))?;
        if metadata + 8 > length || self.stats.metadata_bytes > self.config.max_metadata_bytes {
            return Err(Error::Invalid(
                "Parquet metadata cap before allocation".into(),
            ));
        }
        let builder = ParquetRecordBatchReaderBuilder::try_new(metered)?;
        if builder.schema().fields().len() > 16 || builder.metadata().num_row_groups() > 4096 {
            return Err(Error::Invalid(
                "source schema/row-group metadata cap".into(),
            ));
        }
        let key = builder.schema().index_of("key")?;
        let value = builder.schema().index_of("value_i64")?;
        for (index, kind) in [(key, DataType::UInt64), (value, DataType::Int64)] {
            if builder.schema().field(index).data_type() != &kind
                || builder.schema().field(index).is_nullable()
            {
                return Err(Error::Invalid(
                    "source requires non-null UInt64 key/Int64 value_i64".into(),
                ));
            }
        }
        let expected = builder
            .metadata()
            .row_groups()
            .iter()
            .try_fold(0u64, |sum, g| {
                let rows = u64::try_from(g.num_rows())
                    .map_err(|_| Error::Invalid("negative source rows".into()))?;
                sum.checked_add(rows).ok_or(Error::Overflow("source rows"))
            })?;
        self.stats.expected_opened_rows = self
            .stats
            .expected_opened_rows
            .checked_add(expected)
            .ok_or(Error::Overflow("source rows"))?;
        self.stats.opened_files += 1;
        if self.config.mode == Mode::Full
            && self.stats.expected_opened_rows > self.config.max_rows as u64
        {
            return Err(Error::Invalid(
                "full source metadata exceeds row bound".into(),
            ));
        }
        let mask = ProjectionMask::roots(builder.parquet_schema(), [key, value]);
        let selected: Vec<_> = builder
            .parquet_schema()
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(i, c)| {
                if c.path().parts() == ["key"] || c.path().parts() == ["value_i64"] {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();
        if selected.len() != 2 {
            return Err(Error::Invalid("source primitive column paths".into()));
        }
        let limit = self.config.max_rows
            - usize::try_from(self.stats.rows).map_err(|_| Error::Overflow("source row index"))?;
        let mut remaining = limit as u64;
        let mut groups = Vec::new();
        for (index, group) in builder.metadata().row_groups().iter().enumerate() {
            if remaining == 0 {
                break;
            }
            let rows = u64::try_from(group.num_rows())
                .map_err(|_| Error::Invalid("negative source group".into()))?;
            if rows == 0 {
                continue;
            }
            let decoded = selected.iter().try_fold(0u64, |sum, c| {
                let bytes = u64::try_from(group.column(*c).uncompressed_size())
                    .map_err(|_| Error::Invalid("negative source decode bytes".into()))?;
                sum.checked_add(bytes)
                    .ok_or(Error::Overflow("source decode bytes"))
            })?;
            if decoded > self.config.max_decode_chunk_bytes {
                return Err(Error::Invalid(
                    "source selected decode chunk exceeds cap before decoding".into(),
                ));
            }
            self.stats.decode_chunk_peak_bytes = self.stats.decode_chunk_peak_bytes.max(decoded);
            groups.push(index);
            remaining = remaining.saturating_sub(rows);
        }
        self.stats.selected_row_groups += groups.len();
        self.order = if key < value { [0, 1] } else { [1, 0] };
        self.expected_file_rows = expected;
        self.file_rows = 0;
        self.reader = Some(
            builder
                .with_projection(mask)
                .with_row_groups(groups)
                .with_batch_size(self.config.batch_rows)
                .with_limit(limit)
                .build()?,
        );
        Ok(())
    }
}
impl BatchSource for ParquetRows {
    fn schema(&self) -> SchemaRef {
        join::input_schema()
    }
    fn next_batch(&mut self, token: &CancellationToken) -> Result<Option<RecordBatch>> {
        if self.stats.limited {
            return self.limited();
        }
        loop {
            token.check()?;
            if Instant::now() >= self.deadline {
                return Err(Error::Deadline("projected source"));
            }
            if let Some(reader) = &mut self.reader {
                if let Some(batch) = reader.next() {
                    let batch = batch?;
                    if batch.columns().iter().any(|c| c.null_count() > 0) {
                        return Err(Error::Invalid("row source nulls".into()));
                    }
                    let rows = batch.num_rows() as u64;
                    self.file_rows += rows;
                    self.stats.rows += rows;
                    if self.stats.rows > self.config.max_rows as u64
                        || self.file_rows > self.expected_file_rows
                    {
                        return Err(Error::Invalid("source row-count drift".into()));
                    }
                    let projected = RecordBatch::try_new(
                        join::input_schema(),
                        self.order
                            .iter()
                            .map(|i| batch.column(*i).clone())
                            .collect(),
                    )?;
                    if projected.get_array_memory_size() > join::MAX_BATCH_BYTES {
                        return Err(Error::Invalid("source batch allocation exceeds cap".into()));
                    }
                    self.stats.peak_batch_bytes = self
                        .stats
                        .peak_batch_bytes
                        .max(projected.get_array_memory_size());
                    return Ok(Some(projected));
                }
                self.reader.take();
                if self.file_rows != self.expected_file_rows {
                    if self.config.mode == Mode::Prefix
                        && self.stats.rows == self.config.max_rows as u64
                    {
                        return self.limited();
                    }
                    return Err(Error::Invalid(
                        "source row-count mismatch before EOF".into(),
                    ));
                }
                self.stats.completed_files += 1;
            }
            if self.files.len() == 0 {
                self.stats.complete = self.stats.completed_files == self.assigned;
                return Ok(None);
            }
            if self.stats.rows == self.config.max_rows as u64 && self.config.mode == Mode::Prefix {
                return self.limited();
            }
            let path = self
                .files
                .next()
                .ok_or_else(|| Error::Invalid("source cursor".into()))?;
            self.open(path, token)?;
        }
    }
    fn shutdown(&mut self) {
        self.reader.take();
        self.files = Vec::new().into_iter();
    }
}
#[derive(Debug)]
struct Metered {
    path: PathBuf,
    file: File,
    length: u64,
    bytes: Arc<AtomicU64>,
    cap: u64,
    token: CancellationToken,
    deadline: Instant,
}
impl Read for Metered {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.token.check().map_err(std::io::Error::other)?;
        if Instant::now() >= self.deadline {
            return Err(std::io::Error::other("source read deadline"));
        }
        let available = self.cap.saturating_sub(self.bytes.load(Ordering::Relaxed));
        if available == 0 {
            return Err(std::io::Error::other("source API read cap"));
        }
        let size = buffer
            .len()
            .min(usize::try_from(available).unwrap_or(usize::MAX));
        let read = self.file.read(&mut buffer[..size])?;
        self.bytes.fetch_add(read as u64, Ordering::Relaxed);
        Ok(read)
    }
}
impl Length for Metered {
    fn len(&self) -> u64 {
        self.length
    }
}
impl ChunkReader for Metered {
    type T = Self;
    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(start))?;
        Ok(Self {
            path: self.path.clone(),
            file,
            length: self.length,
            bytes: self.bytes.clone(),
            cap: self.cap,
            token: self.token.clone(),
            deadline: self.deadline,
        })
    }
    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        if length as u64 > self.cap.saturating_sub(self.bytes.load(Ordering::Relaxed)) {
            return Err(parquet::errors::ParquetError::General(
                "source range read cap before allocation".into(),
            ));
        }
        let mut reader = self.get_read(start)?;
        let mut bytes = vec![0; length];
        reader.read_exact(&mut bytes)?;
        Ok(bytes.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Int64Array, UInt64Array},
        record_batch::RecordBatch,
    };
    use parquet::arrow::ArrowWriter;
    fn fixture(root: &std::path::Path, name: &str, rows: u64) -> PathBuf {
        let path = root.join(name);
        let schema = join::input_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values(0..rows)),
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|r| if r % 2 == 0 { i64::MIN } else { i64::MAX }),
                )),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }
    #[test]
    fn complete_prefix_and_full_refusal_never_confuse_coverage() {
        let root = tempfile::tempdir().unwrap();
        let files = vec![
            fixture(root.path(), "a.parquet", 32),
            fixture(root.path(), "b.parquet", 32),
        ];
        let token = CancellationToken::default();
        let mut full = ParquetRows::new(
            files.clone(),
            Config {
                max_rows: 64,
                batch_rows: 7,
                ..Default::default()
            },
        )
        .unwrap();
        let mut rows = 0;
        while let Some(batch) = full.next_batch(&token).unwrap() {
            assert_eq!(batch.schema(), join::input_schema());
            rows += batch.num_rows();
        }
        assert_eq!(rows, 64);
        let stats = full.stats();
        assert!(stats.complete && !stats.limited);
        assert_eq!(stats.completed_files, 2);
        assert_eq!(stats.expected_opened_rows, 64);
        assert!(stats.read_bytes > 0 && stats.decode_chunk_peak_bytes > 0);
        let mut prefix = ParquetRows::new(
            files.clone(),
            Config {
                mode: Mode::Prefix,
                max_rows: 8,
                batch_rows: 7,
                ..Default::default()
            },
        )
        .unwrap();
        let mut rows = 0;
        while let Some(batch) = prefix.next_batch(&token).unwrap() {
            rows += batch.num_rows();
        }
        assert_eq!(rows, 8);
        let stats = prefix.stats();
        assert!(!stats.complete && stats.limited);
        assert!(stats.read_bytes <= 8 * 1024 * 1024);
        let mut refused = ParquetRows::new(
            files,
            Config {
                max_rows: 8,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(refused.next_batch(&token).is_err());
        assert_eq!(refused.stats().rows, 0);
        // Reaching the full-mode row bound must still permit trailing empty files.
        let files = vec![
            fixture(root.path(), "one.parquet", 1),
            fixture(root.path(), "empty.parquet", 0),
        ];
        let mut full = ParquetRows::new(
            files,
            Config {
                max_rows: 1,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(full.next_batch(&token).unwrap().unwrap().num_rows(), 1);
        assert!(full.next_batch(&token).unwrap().is_none());
        assert!(full.stats().complete);
        let mut empty = ParquetRows::new(Vec::new(), Config::default()).unwrap();
        assert!(empty.next_batch(&token).unwrap().is_none());
        assert!(empty.stats().complete);
    }
    #[test]
    fn metadata_api_and_selected_decode_caps_precede_large_allocations() {
        let root = tempfile::tempdir().unwrap();
        let path = fixture(root.path(), "huge.parquet", 350_000);
        let token = CancellationToken::default();
        let mut source = ParquetRows::new(
            vec![path.clone()],
            Config {
                mode: Mode::Prefix,
                max_rows: 1,
                max_decode_chunk_bytes: 65536,
                ..Default::default()
            },
        )
        .unwrap();
        let error = source.next_batch(&token).unwrap_err();
        assert!(error.to_string().contains("decode chunk"));
        assert_eq!(source.stats().rows, 0);
        let mut source = ParquetRows::new(
            vec![path],
            Config {
                max_read_bytes: 1,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(source.next_batch(&token).is_err());
        assert_eq!(source.stats().read_bytes, 1);
        let path = root.path().join("forged.parquet");
        let mut footer = Vec::from(u32::MAX.to_le_bytes());
        footer.extend_from_slice(b"PAR1");
        std::fs::write(&path, footer).unwrap();
        let mut source = ParquetRows::new(vec![path], Config::default()).unwrap();
        assert!(
            source
                .next_batch(&token)
                .unwrap_err()
                .to_string()
                .contains("metadata cap")
        );
        assert_eq!(source.stats().rows, 0);
        let mut source = ParquetRows::new(Vec::new(), Config::default()).unwrap();
        token.cancel();
        assert!(matches!(source.next_batch(&token), Err(Error::Cancelled)));
    }
}
