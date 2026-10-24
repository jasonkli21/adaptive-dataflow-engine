//! Fixed-capacity, reservation-backed hash state and 16 deterministic spill partitions.
use crate::{Error, ExecContext, Node, OperatorId, Result, memory::Reservation};
use arrow::{
    array::{Array, ArrayRef, Int64Array, UInt64Array},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use std::{
    fs::OpenOptions,
    io::{BufReader, BufWriter, Read, Write},
    path::PathBuf,
    sync::Arc,
    time::Instant,
};
const PARTITIONS: usize = 16;
const MAGIC: &[u8; 8] = b"ADEAGG01";
type Partial = (u64, i128, u64);
fn hash(key: u64) -> u64 {
    let mut x = key.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}
struct Table {
    slots: Vec<Option<Partial>>,
    len: usize,
    _reservation: Reservation,
}
impl Table {
    fn new(id: OperatorId, cx: &ExecContext) -> Result<Self> {
        let count = (cx.config.memory_limit_bytes / 4 / std::mem::size_of::<Option<Partial>>())
            .clamp(2, 65536);
        let count = 1usize << count.ilog2();
        let mut reservation = cx.memory.reservation(id);
        reservation.grow(count * std::mem::size_of::<Option<Partial>>())?;
        Ok(Self {
            slots: vec![None; count],
            len: 0,
            _reservation: reservation,
        })
    }
    fn add(&mut self, row: Partial) -> Result<bool> {
        let mask = self.slots.len() - 1;
        let mut index =
            usize::try_from(hash(row.0) & u64::try_from(mask).expect("mask")).expect("masked");
        loop {
            match &mut self.slots[index] {
                Some((key, sum, count)) if *key == row.0 => {
                    let s = sum
                        .checked_add(row.1)
                        .ok_or(Error::Overflow("aggregate sum"))?;
                    let n = count
                        .checked_add(row.2)
                        .ok_or(Error::Overflow("aggregate count"))?;
                    *sum = s;
                    *count = n;
                    return Ok(true);
                }
                None => {
                    if self.len >= self.slots.len() / 2 {
                        return Ok(false);
                    }
                    self.slots[index] = Some(row);
                    self.len += 1;
                    return Ok(true);
                }
                _ => index = (index + 1) & mask,
            }
        }
    }
    fn clear(&mut self) {
        self.slots.fill(None);
        self.len = 0;
    }
    fn sort(&mut self) {
        self.slots
            .sort_unstable_by_key(|row| row.as_ref().map(|r| r.0));
    }
}
pub(crate) struct Aggregate {
    pub built: bool,
    table: Option<Table>,
    paths: [Option<PathBuf>; PARTITIONS],
    partition: usize,
    cursor: usize,
    spilled: bool,
    metadata: Option<Reservation>,
}
impl Aggregate {
    pub(crate) fn new(id: OperatorId, cx: &ExecContext) -> Result<Self> {
        let mut metadata = cx.memory.reservation(id);
        metadata.grow(8192)?;
        Ok(Self {
            built: false,
            table: Some(Table::new(id, cx)?),
            paths: std::array::from_fn(|_| None),
            partition: 0,
            cursor: 0,
            spilled: false,
            metadata: Some(metadata),
        })
    }
    pub fn prime(
        input: &mut Node,
        columns: [usize; 2],
        id: OperatorId,
        cx: &mut ExecContext,
    ) -> Result<Self> {
        let mut state = Self::new(id, cx)?;
        if let Some(batch) = input.next_batch(cx)? {
            state.ingest(&batch, columns[0], columns[1], None, id, cx)?;
        }
        Ok(state)
    }
    pub(crate) fn ingest(
        &mut self,
        batch: &RecordBatch,
        key: usize,
        value: usize,
        count: Option<usize>,
        id: OperatorId,
        cx: &mut ExecContext,
    ) -> Result<()> {
        cx.input(id, batch);
        let start = Instant::now();
        let keys = batch
            .column(key)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| Error::Invalid("aggregate key type".into()))?;
        let values = batch
            .column(value)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::Invalid("aggregate value type".into()))?;
        let counts = count
            .map(|c| {
                batch
                    .column(c)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| Error::Invalid("aggregate count type".into()))
            })
            .transpose()?;
        if counts.is_some_and(|c| c.null_count() != 0) {
            return Err(Error::Invalid(
                "aggregate count nulls are unsupported".into(),
            ));
        }
        if keys.null_count() != 0 || values.null_count() != 0 {
            return Err(Error::Invalid("aggregate nulls are unsupported".into()));
        }
        for i in 0..batch.num_rows() {
            if i % 1024 == 0 {
                cx.cancellation.check()?;
            }
            let count = counts.map_or(1, |c| c.value(i));
            if count == 0 {
                return Err(Error::Invalid("partial count must be positive".into()));
            }
            let row = (keys.value(i), i128::from(values.value(i)), count);
            if !self.table.as_mut().expect("table").add(row)? {
                cx.memory.pressure();
                if !cx.config.spill_enabled {
                    return Err(Error::SpillDisabled);
                }
                self.flush(id, cx)?;
                if !self.table.as_mut().expect("table").add(row)? {
                    cx.memory.pressure();
                    return Err(Error::SpillNoProgress("aggregate table too small"));
                }
            }
        }
        cx.work(id, start);
        Ok(())
    }
    pub fn build(
        input: &mut Node,
        columns: [usize; 2],
        count: Option<usize>,
        id: OperatorId,
        cx: &mut ExecContext,
        initial: Option<Self>,
    ) -> Result<Self> {
        let mut state = match initial {
            Some(state) => state,
            None => Self::new(id, cx)?,
        };
        while let Some(batch) = input.next_batch(cx)? {
            state.ingest(&batch, columns[0], columns[1], count, id, cx)?;
        }
        input.shutdown(cx)?;
        let start = Instant::now();
        if state.spilled {
            state.flush(id, cx)?;
            state.table.take();
        } else {
            state.table.as_mut().expect("table").sort();
        }
        cx.work(id, start);
        state.built = true;
        Ok(state)
    }
    pub(crate) fn observation(&self) -> (usize, bool) {
        (
            self.table.as_ref().map_or(0, |table| table.len),
            self.spilled,
        )
    }
    pub(crate) fn into_raw(self) -> RawPartials {
        RawPartials {
            state: self,
            slot: 0,
            partition: 0,
            reader: None,
            path: None,
            remaining: 0,
            pending: None,
        }
    }
    fn flush(&mut self, id: OperatorId, cx: &mut ExecContext) -> Result<()> {
        let table = self.table.as_ref().expect("table");
        if table.len == 0 {
            return Ok(());
        }
        cx.spill.event(id);
        for p in 0..PARTITIONS {
            cx.cancellation.check()?;
            if !table
                .slots
                .iter()
                .flatten()
                .any(|r| hash(r.0) & 15 == p as u64)
            {
                continue;
            }
            let (file, fresh) = if let Some(path) = &self.paths[p] {
                (OpenOptions::new().append(true).open(path)?, false)
            } else {
                let (path, file) = cx.spill.create(id)?;
                self.paths[p] = Some(path);
                (file, true)
            };
            let before = file.metadata()?.len();
            let mut writer = BufWriter::with_capacity(1024, file);
            if fresh {
                writer.write_all(MAGIC)?;
            }
            for &(key, sum, count) in table
                .slots
                .iter()
                .flatten()
                .filter(|r| hash(r.0) & 15 == p as u64)
            {
                writer.write_all(&key.to_le_bytes())?;
                writer.write_all(&sum.to_le_bytes())?;
                writer.write_all(&count.to_le_bytes())?;
            }
            writer.flush()?;
            cx.spill.metrics.write_bytes += writer.get_ref().metadata()?.len() - before;
        }
        self.table.as_mut().expect("table").clear();
        self.spilled = true;
        Ok(())
    }
    fn load(&mut self, id: OperatorId, cx: &mut ExecContext) -> Result<bool> {
        while self.partition < PARTITIONS {
            let path = self.paths[self.partition].take();
            self.partition += 1;
            let Some(path) = path else {
                continue;
            };
            let mut table = Table::new(id, cx)?;
            let file = cx.spill.reading(&path)?;
            let length = file.metadata()?.len();
            if length < 8 || (length - 8) % 32 != 0 {
                return Err(Error::CorruptSpill("aggregate length"));
            }
            let mut reader = BufReader::with_capacity(1024, file);
            let mut magic = [0; 8];
            reader.read_exact(&mut magic)?;
            if &magic != MAGIC {
                return Err(Error::CorruptSpill("aggregate version"));
            }
            for n in 0..(length - 8) / 32 {
                if n % 1024 == 0 {
                    cx.cancellation.check()?;
                }
                let mut record = [0; 32];
                reader.read_exact(&mut record)?;
                let row = (
                    u64::from_le_bytes(record[..8].try_into().expect("key")),
                    i128::from_le_bytes(record[8..24].try_into().expect("sum")),
                    u64::from_le_bytes(record[24..].try_into().expect("count")),
                );
                if row.2 == 0 {
                    return Err(Error::CorruptSpill("zero partial count"));
                }
                if !table.add(row)? {
                    return Err(Error::SpillNoProgress(
                        "aggregate partition exceeds managed table; increase budget",
                    ));
                }
            }
            drop(reader);
            std::fs::remove_file(path)?;
            table.sort();
            self.table = Some(table);
            self.cursor = 0;
            return Ok(true);
        }
        Ok(false)
    }
    pub fn next(
        &mut self,
        schema: &SchemaRef,
        id: OperatorId,
        cx: &mut ExecContext,
    ) -> Result<Option<RecordBatch>> {
        loop {
            if self.table.is_none() && (!self.spilled || !self.load(id, cx)?) {
                return Ok(None);
            }
            let no_more_partitions = self.paths[self.partition..].iter().all(Option::is_none);
            let (result, exhausted) = {
                let table = self.table.as_mut().expect("loaded");
                while self.cursor < table.slots.len() && table.slots[self.cursor].is_none() {
                    self.cursor += 1;
                }
                if self.cursor == table.slots.len() {
                    self.table.take();
                    continue;
                }
                let rows = cx
                    .config
                    .output_batch_rows
                    .min(table.slots.len() - self.cursor);
                let mut output = cx.memory.reservation(id);
                output.grow(
                    rows.checked_mul(24)
                        .and_then(|n| n.checked_add(1024))
                        .ok_or(Error::Overflow("aggregate output"))?,
                )?;
                let mut keys = Vec::with_capacity(rows);
                let mut sums = Vec::with_capacity(rows);
                let mut counts = Vec::with_capacity(rows);
                for slot in &table.slots[self.cursor..self.cursor + rows] {
                    let (key, sum, count) = slot.expect("sorted suffix");
                    keys.push(key);
                    sums.push(
                        i64::try_from(sum)
                            .map_err(|_| Error::Overflow("aggregate Int64 output"))?,
                    );
                    counts.push(count);
                }
                self.cursor += rows;
                let exhausted = self.cursor == table.slots.len() && no_more_partitions;
                let arrays: Vec<ArrayRef> = vec![
                    Arc::new(UInt64Array::from(keys)),
                    Arc::new(Int64Array::from(sums)),
                    Arc::new(UInt64Array::from(counts)),
                ];
                (RecordBatch::try_new(schema.clone(), arrays)?, exhausted)
            };
            if exhausted {
                self.table.take();
                self.metadata.take();
            }
            return Ok(Some(result));
        }
    }
}

/// Drains exact buffered contributions without reaggregating a spill partition.
/// This supports pressure fallback even when a prefix exceeds a legacy spill bucket.
pub(crate) struct RawPartials {
    state: Aggregate,
    slot: usize,
    partition: usize,
    reader: Option<BufReader<crate::spill::SpillReader>>,
    path: Option<PathBuf>,
    remaining: u64,
    pending: Option<Partial>,
}
impl RawPartials {
    fn partial(&mut self, cx: &mut ExecContext) -> Result<Option<Partial>> {
        if let Some(table) = &self.state.table {
            while self.slot < table.slots.len() {
                let row = table.slots[self.slot];
                self.slot += 1;
                if row.is_some() {
                    return Ok(row);
                }
            }
            self.state.table.take();
        }
        loop {
            cx.cancellation.check()?;
            if self.remaining > 0 {
                let mut row = [0; 32];
                self.reader
                    .as_mut()
                    .ok_or(Error::CorruptSpill("partial reader"))?
                    .read_exact(&mut row)?;
                self.remaining -= 1;
                return Ok(Some((
                    u64::from_le_bytes(
                        row[..8]
                            .try_into()
                            .map_err(|_| Error::CorruptSpill("partial key"))?,
                    ),
                    i128::from_le_bytes(
                        row[8..24]
                            .try_into()
                            .map_err(|_| Error::CorruptSpill("partial sum"))?,
                    ),
                    u64::from_le_bytes(
                        row[24..]
                            .try_into()
                            .map_err(|_| Error::CorruptSpill("partial count"))?,
                    ),
                )));
            }
            self.reader.take();
            if let Some(path) = self.path.take() {
                std::fs::remove_file(path)?;
            }
            if self.partition == PARTITIONS {
                return Ok(None);
            }
            let path = self.state.paths[self.partition].take();
            self.partition += 1;
            if let Some(path) = path {
                let mut reader = BufReader::with_capacity(4096, cx.spill.reading(&path)?);
                let length = reader.get_ref().metadata()?.len();
                if length < 8 || (length - 8) % 32 != 0 {
                    return Err(Error::CorruptSpill("partial spill length"));
                }
                let mut magic = [0; 8];
                reader.read_exact(&mut magic)?;
                if &magic != MAGIC {
                    return Err(Error::CorruptSpill("partial spill version"));
                }
                self.remaining = (length - 8) / 32;
                self.reader = Some(reader);
                self.path = Some(path);
            }
        }
    }
    pub(crate) fn next(&mut self, cx: &mut ExecContext) -> Result<Option<(u64, i64, u64)>> {
        if self.pending.is_none() {
            self.pending = self.partial(cx)?;
        }
        let Some((key, sum, count)) = self.pending else {
            return Ok(None);
        };
        if count == 0
            || sum < i128::from(i64::MIN) * i128::from(count)
            || sum > i128::from(i64::MAX) * i128::from(count)
        {
            return Err(Error::CorruptSpill("partial sum/count range"));
        }
        if let Ok(value) = i64::try_from(sum) {
            self.pending = None;
            return Ok(Some((key, value, count)));
        }
        // Split a large exact prefix sum into representable contributions. Each
        // emitted piece consumes one original count; the last piece keeps the rest.
        let value = if sum < 0 { i64::MIN } else { i64::MAX };
        self.pending = Some((
            key,
            sum.checked_sub(i128::from(value))
                .ok_or(Error::Overflow("partial split"))?,
            count - 1,
        ));
        Ok(Some((key, value, 1)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partial_codec_and_checked_merge_reject_corruption() {
        let mut cx = ExecContext::new(
            crate::ExecutionConfig::default(),
            crate::CancellationToken::default(),
            None,
        )
        .unwrap();
        let id = OperatorId(0);
        for bytes in [b"ADEAGG02".to_vec(), [MAGIC.as_slice(), &[1]].concat()] {
            let (path, mut file) = cx.spill.create(id).unwrap();
            file.write_all(&bytes).unwrap();
            drop(file);
            let mut state = Aggregate {
                built: true,
                table: None,
                paths: std::array::from_fn(|_| None),
                partition: 0,
                cursor: 0,
                spilled: true,
                metadata: None,
            };
            state.paths[0] = Some(path);
            assert!(matches!(
                state.load(id, &mut cx),
                Err(Error::CorruptSpill(_))
            ));
        }
        let mut table = Table::new(id, &cx).unwrap();
        assert!(table.add((1, i128::MAX, u64::MAX)).unwrap());
        assert!(matches!(
            table.add((1, 1, 0)),
            Err(Error::Overflow("aggregate sum"))
        ));
        assert!(matches!(
            table.add((1, 0, 1)),
            Err(Error::Overflow("aggregate count"))
        ));
    }

    #[test]
    fn cancelled_partition_load_releases_state_and_spill_root() {
        let parent = tempfile::tempdir().unwrap();
        let token = crate::CancellationToken::default();
        let mut cx = ExecContext::new(
            crate::ExecutionConfig::default(),
            token.clone(),
            Some(parent.path()),
        )
        .unwrap();
        let id = OperatorId(0);
        let (path, mut file) = cx.spill.create(id).unwrap();
        file.write_all(MAGIC).unwrap();
        file.write_all(&1_u64.to_le_bytes()).unwrap();
        file.write_all(&1_i128.to_le_bytes()).unwrap();
        file.write_all(&1_u64.to_le_bytes()).unwrap();
        drop(file);
        let mut state = Aggregate {
            built: true,
            table: None,
            paths: std::array::from_fn(|_| None),
            partition: 0,
            cursor: 0,
            spilled: true,
            metadata: None,
        };
        state.paths[0] = Some(path);
        token.cancel();
        assert!(matches!(state.load(id, &mut cx), Err(Error::Cancelled)));
        assert_eq!(cx.memory.snapshot().current, 0);
        drop(state);
        drop(cx);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }
}
