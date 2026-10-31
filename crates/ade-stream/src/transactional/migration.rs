//! Actual compatible codec transformation. V2 adds validated end and format tag.
use crate::{
    Error, Result,
    state::{Entry, check, store},
};
use std::io::{Read, Write};
#[must_use]
pub fn width(codec: u32) -> u64 {
    if codec == 1 { 64 } else { 80 }
}
/// # Errors
/// Unsupported codec, arithmetic or I/O errors.
pub fn write(file: &mut impl Write, e: Entry, codec: u32, window: i64) -> Result<()> {
    check([1, 2].contains(&codec) && window > 0, "state codec/window")?;
    if codec == 1 {
        return store::io(file.write_all(&store::encode(e)));
    }
    let mut b = [0u8; 80];
    b[..32].copy_from_slice(&store::encode(e)[..32]);
    b[32..40].copy_from_slice(
        &e.start
            .checked_add(window)
            .ok_or(Error::Overflow("codec window end"))?
            .to_le_bytes(),
    );
    b[40..48].copy_from_slice(b"ADE3REC2");
    let digest = blake3::hash(&b[..48]);
    b[48..].copy_from_slice(digest.as_bytes());
    store::io(file.write_all(&b))
}
/// # Errors
/// Rejects truncated, incompatible or semantically contradictory records.
/// # Panics
/// Fixed-length slices are converted to arrays with statically checked lengths.
pub fn read(file: &mut impl Read, codec: u32, window: i64) -> Result<Option<Entry>> {
    check([1, 2].contains(&codec) && window > 0, "state codec/window")?;
    let n = usize::try_from(width(codec)).map_err(|_| Error::State("record width"))?;
    let mut b = [0u8; 80];
    if store::io(file.read(&mut b[..1]))? == 0 {
        return Ok(None);
    }
    store::io(file.read_exact(&mut b[1..n]))?;
    let content = n - 32;
    check(
        blake3::hash(&b[..content]).as_bytes() == &b[content..n],
        "v3 record checksum",
    )?;
    let e = Entry {
        start: i64::from_le_bytes(b[0..8].try_into().expect("fixed")),
        key: u64::from_le_bytes(b[8..16].try_into().expect("fixed")),
        sum: i64::from_le_bytes(b[16..24].try_into().expect("fixed")),
        count: u64::from_le_bytes(b[24..32].try_into().expect("fixed")),
    };
    check(
        e.count > 0 && e.start.rem_euclid(window) == 0,
        "record count/alignment",
    )?;
    let end = e
        .start
        .checked_add(window)
        .ok_or(Error::Overflow("codec window end"))?;
    if codec == 2 {
        check(
            &b[40..48] == b"ADE3REC2"
                && i64::from_le_bytes(b[32..40].try_into().expect("fixed")) == end,
            "v2 record end/tag",
        )?;
    }
    Ok(Some(e))
}
