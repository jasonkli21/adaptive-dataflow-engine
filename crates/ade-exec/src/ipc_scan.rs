//! One Arrow IPC file and record batch at a time; files remain caller-owned.
use crate::{Error, Result};
use arrow::{datatypes::SchemaRef, ipc::reader::StreamReader, record_batch::RecordBatch};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
};
fn open(path: &std::path::Path) -> Result<StreamReader<File>> {
    let mut file = File::open(path)?;
    // This narrow source consumes finalized IPC streams, never live append streams.
    let mut end = [0; 8];
    file.seek(SeekFrom::End(-8))?;
    file.read_exact(&mut end)?;
    if end != [255, 255, 255, 255, 0, 0, 0, 0] {
        return Err(Error::Invalid("IPC stream lacks completion marker".into()));
    }
    file.rewind()?;
    Ok(StreamReader::try_new(file, None)?)
}
pub(crate) struct Scan {
    files: std::vec::IntoIter<PathBuf>,
    reader: Option<StreamReader<File>>,
    pub schema: SchemaRef,
}
impl Scan {
    pub fn new(files: Vec<PathBuf>) -> Result<Self> {
        let first = files
            .first()
            .ok_or_else(|| Error::Invalid("IPC scan needs files".into()))?;
        let reader = open(first)?;
        let schema = reader.schema();
        Ok(Self {
            files: files.into_iter(),
            reader: None,
            schema,
        })
    }
    pub fn next(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if let Some(reader) = &mut self.reader
                && let Some(batch) = reader.next()
            {
                return Ok(Some(batch?));
            }
            let Some(file) = self.files.next() else {
                self.reader = None;
                return Ok(None);
            };
            let reader = open(&file)?;
            if reader.schema() != self.schema {
                return Err(Error::Invalid("IPC scan schema mismatch".into()));
            }
            self.reader = Some(reader);
        }
    }
    pub fn shutdown(&mut self) {
        self.reader = None;
    }
}
