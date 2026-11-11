use crate::{
    CatalogEntryV1, Context, Error, PinnedSplit, Replay, ResolvedSnapshotV1, Result, Stage,
    contract::hash,
};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
    time::Instant,
};

const MAX_BYTES: u64 = 16 * 1024 * 1024;
const MAX_ROWS: u64 = 65_536;

pub(crate) struct Capture {
    dir: tempfile::TempDir,
    entry: CatalogEntryV1,
    schema: SchemaRef,
    writer: ArrowWriter<Capped>,
    rows: u64,
    decoded: u64,
    started: Instant,
}

struct Capped(File, u64);

impl Write for Capped {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() as u64 > MAX_BYTES - self.1 {
            return Err(std::io::Error::other("captured Parquet cap"));
        }
        let n = self.0.write(bytes)?;
        self.1 += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

impl Capture {
    pub(crate) fn new(entry: CatalogEntryV1, parent: &Path) -> Result<Self> {
        entry.validate()?;
        ResolvedSnapshotV1 {
            version: 1,
            entry: entry.clone(),
            authority: "0".repeat(64),
            splits: vec![PinnedSplit {
                key: "capture.parquet".into(),
                bytes: 12,
                rows: 0,
                blake3: "0".repeat(64),
            }],
            replay: Replay::OneShot,
        }
        .physical()?;
        let dir = tempfile::Builder::new()
            .prefix(".ade-stage-")
            .tempdir_in(parent)?;
        let schema = ade_exec::physical::arrow_schema(&entry.schema);
        let file = File::create_new(dir.path().join("capture.parquet"))?;
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1024))
            .set_dictionary_enabled(false)
            .build();
        let writer = ArrowWriter::try_new(Capped(file, 0), schema.clone(), Some(props))
            .map_err(|_| Error::Protocol("captured Parquet writer"))?;
        Ok(Self {
            dir,
            entry,
            schema,
            writer,
            rows: 0,
            decoded: 0,
            started: Instant::now(),
        })
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(crate) fn push(&mut self, batch: &RecordBatch, context: &Context) -> Result<()> {
        context.check()?;
        if batch.schema() != self.schema || batch.columns().iter().any(|c| c.null_count() != 0) {
            return Err(Error::Identity("captured batch schema or null"));
        }
        self.rows = self
            .rows
            .checked_add(batch.num_rows() as u64)
            .filter(|&n| n <= MAX_ROWS)
            .ok_or(Error::Limit("captured rows"))?;
        self.decoded = self
            .decoded
            .checked_add(batch.get_array_memory_size() as u64)
            .filter(|&n| n <= MAX_BYTES)
            .ok_or(Error::Limit("captured decoded bytes"))?;
        self.writer
            .write(batch)
            .map_err(|_| Error::Protocol("captured Parquet write"))
    }

    pub(crate) fn finish(self, authority: String, context: &Context) -> Result<Stage> {
        context.check()?;
        if !hash(&authority) {
            return Err(Error::Admission("capture authority"));
        }
        self.writer
            .close()
            .map_err(|_| Error::Protocol("captured Parquet close"))?;
        let path = self.dir.path().join("capture.parquet");
        let mut file = File::open(&path)?;
        let bytes = file.metadata()?.len();
        let mut hasher = blake3::Hasher::new();
        let mut buffer = [0; 8192];
        loop {
            context.check()?;
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }
        let snapshot = ResolvedSnapshotV1 {
            version: 1,
            entry: self.entry,
            authority,
            splits: vec![PinnedSplit {
                key: "capture.parquet".into(),
                bytes,
                rows: self.rows,
                blake3: hasher.finalize().to_hex().to_string(),
            }],
            replay: Replay::OneShot,
        };
        let physical = snapshot.physical()?;
        Ok(Stage {
            normalized: true,
            elapsed: self.started.elapsed(),
            _dir: self.dir,
            snapshot,
            physical,
            files: vec![path],
        })
    }
}
