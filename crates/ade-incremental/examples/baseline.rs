//! Fixture encoder. The independent campaign supplies the row ledger on stdin.
use arrow::{
    array::{Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, Encoding},
    file::properties::{WriterProperties, WriterVersion},
};
use std::{
    fs::{self, File},
    io::{self, BufRead},
    path::Path,
    sync::Arc,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let path = args.next().ok_or("baseline PARQUET [nullable]")?;
    let path = Path::new(&path);
    let nullable = args.next().is_some_and(|v| v == "nullable");
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("value", DataType::Int64, nullable),
    ]));
    let config = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(32_768))
        .build();
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema.clone(), Some(config))?;
    let mut keys = Vec::with_capacity(4096);
    let mut values = Vec::with_capacity(4096);
    let mut rows = 0u64;
    for line in io::stdin().lock().lines() {
        let line = line?;
        let (key, value) = line.split_once(',').ok_or("expected key,value")?;
        keys.push(key.parse()?);
        values.push(value.parse()?);
        rows += 1;
        if keys.len() == 4096 {
            write_batch(&mut writer, &schema, &mut keys, &mut values)?;
        }
    }
    if !keys.is_empty() {
        write_batch(&mut writer, &schema, &mut keys, &mut values)?;
    }
    writer.close()?;
    let bytes = fs::read(path)?;
    let file = ade_physical::SourceFile {
        name: path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or("file name")?
            .into(),
        bytes: bytes.len() as u64,
        rows,
        fingerprint: ade_physical::fingerprint(&bytes),
    };
    println!("{}", serde_json::to_string(&file)?);
    Ok(())
}

fn write_batch(
    writer: &mut ArrowWriter<File>,
    schema: &Arc<Schema>,
    keys: &mut Vec<u64>,
    values: &mut Vec<i64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(std::mem::take(keys))),
            Arc::new(Int64Array::from(std::mem::take(values))),
        ],
    )?;
    writer.write(&batch)?;
    Ok(())
}
