use super::{Iceberg, Metadata};
use crate::{Catalog, Context, Error, Result, Stage};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use parquet::arrow::ArrowWriter;
use std::{
    fs::{self, File},
    io::Write,
    path::Path,
    time::Instant,
};

impl Iceberg {
    /// Stage the pinned data and normalize verified Iceberg field IDs into the
    /// canonical Arrow schema expected by the unchanged native v1 verifier.
    ///
    /// # Errors
    /// Refuses unsupported or inconsistent Parquet schema, field IDs, metadata,
    /// resource limits and snapshot drift.
    pub fn stage(
        &self,
        catalog: &Catalog,
        expected: &str,
        parent: &Path,
        context: &Context,
    ) -> Result<Stage> {
        let started = Instant::now();
        let mut staged = catalog.stage(&self.entry.name, self, expected, parent, context)?;
        let bytes = self.read(&self.pin.metadata, context)?;
        let metadata: Metadata =
            serde_json::from_slice(&bytes).map_err(|_| Error::Protocol("Iceberg table schema"))?;
        let ids = metadata
            .schema
            .fields
            .iter()
            .map(|f| f.id)
            .collect::<Vec<_>>();
        let schema = ade_exec::physical::arrow_schema(&self.entry.schema);
        let mut remaining = ade_physical::MAX_SOURCE_BYTES;
        let mut owned_bytes = staged.download_bytes;
        let mut normalization_write_bytes = 0u64;
        let mut peak_owned_stage_bytes = staged.peak_owned_stage_bytes;
        for (index, (path, physical)) in staged
            .files
            .iter_mut()
            .zip(&mut staged.physical.files)
            .enumerate()
        {
            context.check()?;
            let name = format!("normalized-{index:04}.parquet");
            let destination = path.with_file_name(&name);
            let original_bytes = physical.bytes;
            let (bytes, fingerprint) = normalize(
                path,
                &destination,
                &schema,
                &ids,
                physical.rows,
                remaining,
                context,
            )?;
            remaining = remaining
                .checked_sub(bytes)
                .ok_or(Error::Limit("Iceberg normalized bytes"))?;
            peak_owned_stage_bytes = peak_owned_stage_bytes.max(
                owned_bytes
                    .checked_add(bytes)
                    .ok_or(Error::Limit("Iceberg peak stage bytes"))?,
            );
            fs::remove_file(&*path)?;
            owned_bytes = owned_bytes
                .checked_sub(original_bytes)
                .and_then(|n| n.checked_add(bytes))
                .ok_or(Error::Limit("Iceberg stage byte accounting"))?;
            normalization_write_bytes = normalization_write_bytes
                .checked_add(bytes)
                .ok_or(Error::Limit("Iceberg normalization byte accounting"))?;
            *path = destination;
            physical.name = name;
            physical.bytes = bytes;
            physical.fingerprint = fingerprint;
        }
        staged
            .physical
            .validate()
            .map_err(|_| Error::Admission("normalized Iceberg physical source"))?;
        staged.normalized = true;
        staged.normalization_write_bytes = normalization_write_bytes;
        staged.peak_owned_stage_bytes = peak_owned_stage_bytes;
        staged.elapsed = started.elapsed();
        Ok(staged)
    }
}

struct Output {
    file: File,
    remaining: u64,
    hash: blake3::Hasher,
}

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() as u64 > self.remaining {
            return Err(std::io::Error::other("Iceberg normalized byte cap"));
        }
        let n = self.file.write(bytes)?;
        self.remaining -= n as u64;
        self.hash.update(&bytes[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

fn normalize(
    path: &Path,
    destination: &Path,
    schema: &SchemaRef,
    ids: &[i32],
    rows: u64,
    cap: u64,
    context: &Context,
) -> Result<(u64, String)> {
    let reader = crate::batch::parquet_stage::open(path, true)?;
    let parquet = reader.metadata().file_metadata();
    let columns = parquet.schema_descr().columns();
    if columns.len() != ids.len()
        || u64::try_from(parquet.num_rows()).ok() != Some(rows)
        || !columns.iter().zip(ids).all(|(c, &id)| {
            let info = c.self_type().get_basic_info();
            info.has_id() && info.id() == id
        })
        || !reader.schema().metadata().is_empty()
        || reader.schema().fields().len() != schema.fields().len()
        || !reader
            .schema()
            .fields()
            .iter()
            .zip(schema.fields())
            .all(|(a, b)| {
                a.name() == b.name()
                    && a.data_type() == b.data_type()
                    && !a.is_nullable()
                    && a.metadata().keys().all(|key| key == "PARQUET:field_id")
            })
    {
        return Err(Error::Identity("Iceberg Parquet field IDs, types or rows"));
    }
    let mut output = Output {
        file: File::create_new(destination)?,
        remaining: cap,
        hash: blake3::Hasher::new(),
    };
    let props = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(1024))
        .build();
    let mut writer = ArrowWriter::try_new(&mut output, schema.clone(), Some(props))
        .map_err(|_| Error::Protocol("Iceberg normalized writer"))?;
    let mut decoded_rows = 0;
    for batch in reader
        .with_batch_size(1024)
        .build()
        .map_err(|_| Error::Protocol("Iceberg Parquet scan"))?
    {
        context.check()?;
        let batch = batch.map_err(|_| Error::Protocol("Iceberg Parquet decode"))?;
        decoded_rows += batch.num_rows() as u64;
        if decoded_rows > rows || batch.columns().iter().any(|c| c.null_count() != 0) {
            return Err(Error::Identity("Iceberg Parquet rows or nulls"));
        }
        let normalized = RecordBatch::try_new(schema.clone(), batch.columns().to_vec())
            .map_err(|_| Error::Identity("Iceberg normalized schema"))?;
        writer
            .write(&normalized)
            .map_err(|_| Error::Protocol("Iceberg normalized write"))?;
    }
    if decoded_rows != rows {
        return Err(Error::Identity("Iceberg decoded rows"));
    }
    writer
        .close()
        .map_err(|_| Error::Protocol("Iceberg normalized footer"))?;
    Ok((
        cap - output.remaining,
        output.hash.finalize().to_hex().to_string(),
    ))
}
