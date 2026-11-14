use crate::{CancellationToken, Error, MAX_ROWS, MAX_SOURCE_BYTES, Request, Result, check};
use ade_exec::physical::arrow_schema;
use ade_physical::{Source, SourceFile};
use parquet::{
    arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
    basic::{Compression, Encoding},
};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Component,
    time::Instant,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Digest([u8; 32]);

impl Digest {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(crate) fn from_hash(hash: blake3::Hash) -> Self {
        Self(*hash.as_bytes())
    }

    #[must_use]
    pub fn hex(self) -> String {
        blake3::Hash::from_bytes(self.0).to_hex().to_string()
    }
}

#[must_use]
pub fn digest(bytes: &[u8]) -> Digest {
    Digest::from_hash(blake3::hash(bytes))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourcePin {
    pub digest: Digest,
    pub bytes: u64,
    pub rows: u64,
}

pub(crate) fn binding(request: &Request<'_>) -> Result<Source> {
    let pin = request.source;
    if pin.rows > MAX_ROWS || !(12..=MAX_SOURCE_BYTES).contains(&pin.bytes) {
        return Err(Error::Limit("source limits"));
    }
    let schema = request.descriptor.schema.clone();
    let schema_identity = digest(&serde_json::to_vec(&schema)?).hex();
    let snapshot = digest(&serde_json::to_vec(&(
        "ade-extension-source-v1",
        pin,
        &schema_identity,
    ))?)
    .hex();
    Ok(Source {
        id: 0,
        table: "input".into(),
        manifest: snapshot.clone(),
        logical: snapshot,
        schema_identity,
        schema,
        files: vec![SourceFile {
            name: "input.parquet".into(),
            bytes: pin.bytes,
            rows: pin.rows,
            fingerprint: pin.digest.hex(),
        }],
        replayable: true,
    })
}

fn attest(
    file: &mut File,
    bytes: u64,
    cancel: &CancellationToken,
    deadline: Instant,
) -> Result<Digest> {
    let mut hasher = blake3::Hasher::new();
    let mut remaining = bytes;
    let mut buffer = [0; 8192];
    loop {
        check(cancel, deadline)?;
        let size = file.read(&mut buffer)?;
        if size == 0 {
            break;
        }
        remaining = remaining.checked_sub(size as u64).ok_or(Error::Source)?;
        hasher.update(&buffer[..size]);
    }
    if remaining != 0 {
        return Err(Error::Source);
    }
    Ok(Digest::from_hash(hasher.finalize()))
}

pub(crate) fn verify(request: &Request<'_>, cancel: &CancellationToken) -> Result<()> {
    let path = request.path;
    if path.as_os_str().len() > 4096
        || !path.is_absolute()
        || path.components().any(|c| matches!(c, Component::ParentDir))
        || path.canonicalize()? != path
    {
        return Err(Error::Source);
    }
    let meta = path.symlink_metadata()?;
    if !meta.is_file() || meta.len() != request.source.bytes {
        return Err(Error::Source);
    }
    let mut file = File::open(path)?;
    if attest(&mut file, request.source.bytes, cancel, request.deadline)? != request.source.digest {
        return Err(Error::Source);
    }
    check(cancel, request.deadline)?;
    file.seek(SeekFrom::End(-8))?;
    let mut trailer = [0; 8];
    file.read_exact(&mut trailer)?;
    let footer = u64::from(u32::from_le_bytes(
        trailer[..4].try_into().map_err(|_| Error::Schema)?,
    ));
    if &trailer[4..] != b"PAR1" || footer > 64 * 1024 || footer > request.source.bytes - 12 {
        return Err(Error::Schema);
    }
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = reader.metadata();
    if *reader.schema() != arrow_schema(&request.descriptor.schema)
        || u64::try_from(metadata.file_metadata().num_rows()).ok() != Some(request.source.rows)
        || metadata.num_row_groups() > 128
    {
        return Err(Error::Schema);
    }
    let mut rows = 0u64;
    for group in metadata.row_groups() {
        check(cancel, request.deadline)?;
        rows = rows
            .checked_add(u64::try_from(group.num_rows()).map_err(|_| Error::Schema)?)
            .ok_or(Error::Schema)?;
        for column in group.columns() {
            if column.num_values() != group.num_rows()
                || column.file_path().is_some()
                || column.compression() != Compression::UNCOMPRESSED
                || column.dictionary_page_offset().is_some()
                || column.uncompressed_size() != column.compressed_size()
                || column
                    .encodings()
                    .any(|encoding| !matches!(encoding, Encoding::PLAIN | Encoding::RLE))
            {
                return Err(Error::Schema);
            }
            let offset = u64::try_from(column.data_page_offset()).map_err(|_| Error::Schema)?;
            let size = u64::try_from(column.uncompressed_size()).map_err(|_| Error::Schema)?;
            if offset == 0 && size == 0 && group.num_rows() == 0 {
                continue;
            }
            if offset < 4
                || offset
                    .checked_add(size)
                    .is_none_or(|end| end > request.source.bytes - footer - 8)
            {
                return Err(Error::Schema);
            }
        }
    }
    if rows != request.source.rows {
        return Err(Error::Schema);
    }
    Ok(())
}

/// Hash the operator-installed executable using a bounded streaming read.
///
/// # Errors
/// Returns I/O or deadline/budget errors when the running image cannot be measured.
pub fn executable_digest(deadline: Instant) -> Result<Digest> {
    measure_executable(&CancellationToken::default(), deadline)
}

pub(crate) fn measure_executable(cancel: &CancellationToken, deadline: Instant) -> Result<Digest> {
    let path = if cfg!(target_os = "linux") {
        "/proc/self/exe".into()
    } else {
        std::env::current_exe()?
    };
    let mut file = File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > 512 * 1024 * 1024 {
        return Err(Error::Limit("executable size"));
    }
    attest(&mut file, metadata.len(), cancel, deadline)
}
