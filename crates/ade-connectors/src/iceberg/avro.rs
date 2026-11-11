//! Preflight uncompressed Avro object containers before calling the datum decoder.
use crate::{Context, Error, Result};
use apache_avro::{Schema, types::Value};
use std::collections::BTreeMap;

const MAX_RECORDS: usize = 128;

type Metadata = BTreeMap<String, Vec<u8>>;

pub(super) fn read(bytes: &[u8], context: &Context) -> Result<(Metadata, Vec<Value>)> {
    let mut input = bytes
        .strip_prefix(b"Obj\x01")
        .ok_or(Error::Protocol("Avro magic"))?;
    let mut metadata = BTreeMap::new();
    blocks(&mut input, 32, |input| {
        let key = std::str::from_utf8(blob(input, 128)?)
            .map_err(|_| Error::Protocol("Avro metadata key"))?;
        let bytes = blob(input, 64 * 1024)?;
        if metadata.insert(key.to_owned(), bytes.to_vec()).is_some() {
            return Err(Error::Protocol("duplicate Avro metadata key"));
        }
        Ok(())
    })?;
    if metadata
        .get("avro.codec")
        .is_some_and(|codec| codec != b"null")
        || metadata
            .keys()
            .any(|key| key.starts_with("avro.") && key != "avro.schema" && key != "avro.codec")
    {
        return Err(Error::Admission("Avro codec or extension"));
    }
    let raw = metadata
        .remove("avro.schema")
        .ok_or(Error::Protocol("Avro writer schema"))?;
    let text = std::str::from_utf8(&raw).map_err(|_| Error::Protocol("Avro schema text"))?;
    // serde_json imposes a recursion limit before the Avro schema parser runs.
    let _: serde_json::Value =
        serde_json::from_str(text).map_err(|_| Error::Protocol("Avro schema JSON"))?;
    let schema = Schema::parse_str(text).map_err(|_| Error::Protocol("Avro schema"))?;
    let sync = take(&mut input, 16)?;
    let mut records = Vec::new();
    let mut values = 32_768;
    while !input.is_empty() {
        context.check()?;
        let count = size(&mut input, MAX_RECORDS - records.len())?;
        if count == 0 {
            return Err(Error::Protocol("empty Avro block"));
        }
        let mut block = blob(&mut input, 1024 * 1024)?;
        for _ in 0..count {
            context.check()?;
            let mut remainder = block;
            walk(&schema, &mut remainder, 0, &mut values)?;
            let len = block.len() - remainder.len();
            let mut datum = take(&mut block, len)?;
            let record = apache_avro::from_avro_datum(&schema, &mut datum, None)
                .map_err(|_| Error::Protocol("Avro datum"))?;
            if !datum.is_empty() {
                return Err(Error::Protocol("Avro datum trailing bytes"));
            }
            records.push(record);
        }
        if !block.is_empty() || take(&mut input, 16)? != sync {
            return Err(Error::Protocol("Avro block length or sync marker"));
        }
    }
    Ok((metadata, records))
}

fn walk(schema: &Schema, input: &mut &[u8], depth: usize, values: &mut usize) -> Result<()> {
    *values = values
        .checked_sub(1)
        .ok_or(Error::Limit("Avro decoded values"))?;
    if depth == 12 {
        return Err(Error::Limit("Avro datum nesting"));
    }
    match schema {
        Schema::Null => {}
        Schema::Boolean => {
            if take(input, 1)?[0] > 1 {
                return Err(Error::Protocol("Avro boolean"));
            }
        }
        Schema::Int | Schema::Long => {
            number(input)?;
        }
        Schema::Float => {
            take(input, 4)?;
        }
        Schema::Double => {
            take(input, 8)?;
        }
        Schema::Bytes | Schema::String => {
            blob(input, 64 * 1024)?;
        }
        Schema::Record(record) if record.fields.len() <= 32 => {
            for field in &record.fields {
                walk(&field.schema, input, depth + 1, values)?;
            }
        }
        Schema::Union(union) if !union.variants().is_empty() => {
            let index = size(input, union.variants().len() - 1)?;
            walk(&union.variants()[index], input, depth + 1, values)?;
        }
        Schema::Array(array) => blocks(input, 64, |input| {
            walk(&array.items, input, depth + 1, values)
        })?,
        Schema::Map(map) => blocks(input, 64, |input| {
            blob(input, 4096)?;
            walk(&map.types, input, depth + 1, values)
        })?,
        _ => return Err(Error::Admission("Avro schema type")),
    }
    Ok(())
}

fn blocks(
    input: &mut &[u8],
    mut max: usize,
    mut decode: impl FnMut(&mut &[u8]) -> Result<()>,
) -> Result<()> {
    loop {
        let count = number(input)?;
        if count == 0 {
            return Ok(());
        }
        let n =
            usize::try_from(count.unsigned_abs()).map_err(|_| Error::Limit("Avro item count"))?;
        max = max.checked_sub(n).ok_or(Error::Limit("Avro item count"))?;
        if count < 0 {
            let mut block = blob(input, 1024 * 1024)?;
            for _ in 0..n {
                decode(&mut block)?;
            }
            if !block.is_empty() {
                return Err(Error::Protocol("Avro collection block length"));
            }
        } else {
            for _ in 0..n {
                decode(input)?;
            }
        }
    }
}

fn number(input: &mut &[u8]) -> Result<i64> {
    let mut n = 0u64;
    for shift in (0..=63).step_by(7) {
        let byte = take(input, 1)?[0];
        if shift == 63 && byte > 1 {
            return Err(Error::Protocol("Avro integer overflow"));
        }
        n |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 {
            let half = i64::try_from(n >> 1).map_err(|_| Error::Protocol("Avro integer"))?;
            return Ok(if n & 1 == 0 { half } else { !half });
        }
    }
    Err(Error::Protocol("Avro integer"))
}

fn size(input: &mut &[u8], max: usize) -> Result<usize> {
    usize::try_from(number(input)?)
        .ok()
        .filter(|&n| n <= max)
        .ok_or(Error::Limit("Avro length"))
}

fn blob<'a>(input: &mut &'a [u8], max: usize) -> Result<&'a [u8]> {
    let n = size(input, max)?;
    take(input, n)
}

fn take<'a>(input: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    let (value, rest) = input
        .split_at_checked(n)
        .ok_or(Error::Protocol("truncated Avro container"))?;
    *input = rest;
    Ok(value)
}
