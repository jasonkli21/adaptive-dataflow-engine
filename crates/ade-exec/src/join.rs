//! Bounded bag equijoin. Distribution and observation policy belong to callers.
use crate::{
    BatchSource, CancellationToken, Error, OperatorId, Result,
    memory::{MemoryManager, Reservation},
};
use arrow::{
    array::{Array, Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

const PARTITIONS: usize = 32;
const MAGIC: &[u8; 8] = b"ADEJOIN1";
const NONE: usize = usize::MAX;
pub const MAX_BATCH_ROWS: usize = 1024;
pub const MAX_BATCH_BYTES: usize = 64 * 1024;

/// Physical build input; output orientation always remains left, right.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Left,
    Right,
}
impl Side {
    #[must_use]
    pub const fn index(self) -> usize {
        match self {
            Self::Left => 0,
            Self::Right => 1,
        }
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Algorithm {
    #[default]
    Auto,
    HashOnly,
    Grace,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub build: Side,
    pub algorithm: Algorithm,
    pub hash_rows: usize,
    pub memory_limit_bytes: usize,
    pub output_batch_rows: usize,
    pub max_output_rows: u64,
    pub max_input_rows_per_side: u64,
    pub max_spill_bytes: u64,
    pub spill_enabled: bool,
    pub timeout: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            build: Side::Right,
            algorithm: Algorithm::Auto,
            hash_rows: 65536,
            memory_limit_bytes: 32 * 1024 * 1024,
            output_batch_rows: 1024,
            max_output_rows: 1_000_000,
            max_input_rows_per_side: 1_000_000,
            max_spill_bytes: 64 * 1024 * 1024,
            spill_enabled: true,
            timeout: Duration::from_secs(120),
        }
    }
}
impl Config {
    /// # Errors
    /// Rejects unsupported bounds and forced spilling when spill is disabled.
    pub fn validate(&self) -> Result<()> {
        if !(1..=1_048_576).contains(&self.hash_rows)
            || !(1..=1024).contains(&self.output_batch_rows)
            || self.max_output_rows == 0
            || self.max_output_rows > 1_000_000
            || self.max_input_rows_per_side == 0
            || self.max_input_rows_per_side > 1_000_000
            || self.max_spill_bytes == 0
            || self.max_spill_bytes > 1024 * 1024 * 1024
            || self.timeout.is_zero()
            || self.timeout > Duration::from_secs(120)
        {
            return Err(Error::Invalid(
                "join bounds outside supported domain".into(),
            ));
        }
        if self.algorithm == Algorithm::Grace && !self.spill_enabled {
            return Err(Error::SpillDisabled);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metrics {
    pub input_rows: [u64; 2],
    pub output_rows: u64,
    pub hash_rows_peak: usize,
    pub hash_capacity_rows: usize,
    pub transitioned: bool,
    pub switch_build_rows: u64,
    pub used_grace: bool,
    pub managed_peak: usize,
    pub managed_current: usize,
    pub input_batch_peak_bytes: usize,
    pub output_batch_peak_bytes: usize,
    pub spill_files: usize,
    pub spill_fd_peak: usize,
    pub spill_write_bytes: u64,
    pub spill_read_bytes: u64,
    pub source_pull_ns: u64,
    pub sink_ns: u64,
    pub operator_work_ns: u64,
    pub cleaned: bool,
}
#[must_use]
pub fn input_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("value_i64", DataType::Int64, false),
    ]))
}
#[must_use]
pub fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("left_value", DataType::Int64, false),
        Field::new("right_value", DataType::Int64, false),
    ]))
}
fn columns(batch: &RecordBatch) -> Result<(&UInt64Array, &Int64Array)> {
    if batch.schema() != input_schema()
        || batch.num_rows() > MAX_BATCH_ROWS
        || batch.get_array_memory_size() > MAX_BATCH_BYTES
        || batch.columns().iter().any(|c| c.null_count() != 0)
    {
        return Err(Error::Invalid("join input schema/null/batch bound".into()));
    }
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| Error::Invalid("join key".into()))?;
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::Invalid("join value".into()))?;
    Ok((keys, values))
}
fn hash(key: u64) -> u64 {
    let mut x = key.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}
fn partition(key: u64) -> usize {
    ((hash(key) >> 16) & 31) as usize
}
#[derive(Clone, Copy)]
struct Row {
    key: u64,
    value: i64,
    next: usize,
}
struct Table {
    rows: Vec<Row>,
    heads: Vec<usize>,
    capacity: usize,
    _reservation: Reservation,
}
impl Table {
    fn new(capacity: usize, memory: &MemoryManager) -> Result<Self> {
        let slots = capacity
            .checked_next_power_of_two()
            .and_then(|n| n.checked_mul(2))
            .ok_or(Error::Overflow("join slots"))?;
        let bytes = capacity
            .checked_mul(std::mem::size_of::<Row>())
            .and_then(|n| {
                slots
                    .checked_mul(std::mem::size_of::<usize>())
                    .and_then(|s| n.checked_add(s))
            })
            .ok_or(Error::Overflow("join state"))?;
        let mut reservation = memory.reservation(OperatorId(0));
        reservation.grow(bytes)?;
        Ok(Self {
            rows: Vec::with_capacity(capacity),
            heads: vec![NONE; slots],
            capacity,
            _reservation: reservation,
        })
    }
    fn head(&self, key: u64) -> usize {
        self.heads[((hash(key) >> 32) as usize) & (self.heads.len() - 1)]
    }
    fn add(&mut self, key: u64, value: i64) -> bool {
        if self.rows.len() == self.capacity {
            return false;
        }
        let slot = ((hash(key) >> 32) as usize) & (self.heads.len() - 1);
        let next = self.heads[slot];
        self.heads[slot] = self.rows.len();
        self.rows.push(Row { key, value, next });
        true
    }
}
struct Context<'a> {
    config: &'a Config,
    token: &'a CancellationToken,
    deadline: Instant,
    metrics: Metrics,
    memory: MemoryManager,
}
impl Context<'_> {
    fn check(&self) -> Result<()> {
        self.token.check()?;
        if Instant::now() >= self.deadline {
            return Err(Error::Deadline("join"));
        }
        Ok(())
    }
    fn pull(&mut self, source: &mut dyn BatchSource, side: usize) -> Result<Option<RecordBatch>> {
        self.check()?;
        let start = Instant::now();
        let batch = source.next_batch(self.token);
        self.metrics.source_pull_ns += ns(start);
        if let Some(batch) = &batch? {
            columns(batch)?;
            self.metrics.input_rows[side] = self.metrics.input_rows[side]
                .checked_add(batch.num_rows() as u64)
                .ok_or(Error::Overflow("join input rows"))?;
            if self.metrics.input_rows[side] > self.config.max_input_rows_per_side {
                return Err(Error::Invalid("join input row bound exceeded".into()));
            }
            self.metrics.input_batch_peak_bytes = self
                .metrics
                .input_batch_peak_bytes
                .max(batch.get_array_memory_size());
            return Ok(Some(batch.clone()));
        }
        Ok(None)
    }
}
fn ns(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}
struct Output {
    keys: Vec<u64>,
    left: Vec<i64>,
    right: Vec<i64>,
    rows: usize,
}
impl Output {
    fn new(rows: usize) -> Self {
        Self {
            keys: Vec::with_capacity(rows),
            left: Vec::with_capacity(rows),
            right: Vec::with_capacity(rows),
            rows,
        }
    }
    fn push(
        &mut self,
        key: u64,
        build: i64,
        probe: i64,
        cx: &mut Context<'_>,
        sink: &mut impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        if cx.metrics.output_rows >= cx.config.max_output_rows {
            return Err(Error::JoinOutputLimit {
                limit: cx.config.max_output_rows,
            });
        }
        cx.metrics.output_rows += 1;
        self.keys.push(key);
        let (left, right) = if cx.config.build == Side::Left {
            (build, probe)
        } else {
            (probe, build)
        };
        self.left.push(left);
        self.right.push(right);
        if self.keys.len() == self.rows {
            self.flush(cx, sink)?;
        }
        Ok(())
    }
    fn flush(
        &mut self,
        cx: &mut Context<'_>,
        sink: &mut impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        if self.keys.is_empty() {
            return Ok(());
        }
        cx.check()?;
        let batch = RecordBatch::try_new(
            output_schema(),
            vec![
                Arc::new(UInt64Array::from(std::mem::take(&mut self.keys))),
                Arc::new(Int64Array::from(std::mem::take(&mut self.left))),
                Arc::new(Int64Array::from(std::mem::take(&mut self.right))),
            ],
        )?;
        cx.metrics.output_batch_peak_bytes = cx
            .metrics
            .output_batch_peak_bytes
            .max(batch.get_array_memory_size());
        let start = Instant::now();
        let result = sink(batch);
        cx.metrics.sink_ns += ns(start);
        result?;
        self.keys = Vec::with_capacity(self.rows);
        self.left = Vec::with_capacity(self.rows);
        self.right = Vec::with_capacity(self.rows);
        Ok(())
    }
}
fn probe(
    table: &Table,
    key: u64,
    value: i64,
    out: &mut Output,
    cx: &mut Context<'_>,
    sink: &mut impl FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    cx.check()?;
    let mut cursor = table.head(key);
    let mut steps = 0;
    while cursor != NONE {
        if steps % 1024 == 0 {
            cx.check()?;
        }
        let row = table.rows[cursor];
        cursor = row.next;
        steps += 1;
        if row.key == key {
            out.push(key, row.value, value, cx, sink)?;
        }
    }
    Ok(())
}
struct Spool {
    root: Option<tempfile::TempDir>,
    paths: [[Option<PathBuf>; PARTITIONS]; 2],
    rows: [[u64; PARTITIONS]; 2],
    writes: u64,
    reads: Arc<AtomicU64>,
    files: usize,
    _reservation: Reservation,
}
impl Spool {
    fn new(parent: Option<&Path>, memory: &MemoryManager) -> Result<Self> {
        let root = if let Some(parent) = parent {
            tempfile::tempdir_in(parent)?
        } else {
            tempfile::tempdir()?
        };
        let path_bytes = root
            .path()
            .as_os_str()
            .len()
            .checked_add(32)
            .and_then(|n| n.checked_mul(2 * 2 * PARTITIONS))
            .and_then(|n| n.checked_add(16384))
            .ok_or(Error::Overflow("join spill metadata"))?;
        let mut reservation = memory.reservation(OperatorId(2));
        reservation.grow(path_bytes)?;
        Ok(Self {
            root: Some(root),
            paths: std::array::from_fn(|_| std::array::from_fn(|_| None)),
            rows: [[0; PARTITIONS]; 2],
            writes: 0,
            reads: Arc::default(),
            files: 0,
            _reservation: reservation,
        })
    }
    fn charge(&mut self, bytes: u64, cx: &Context<'_>) -> Result<()> {
        self.writes = self
            .writes
            .checked_add(bytes)
            .ok_or(Error::Overflow("join spill bytes"))?;
        if self.writes > cx.config.max_spill_bytes {
            return Err(Error::JoinSpillLimit {
                limit: cx.config.max_spill_bytes,
            });
        }
        Ok(())
    }
    fn append(
        &mut self,
        side: usize,
        p: usize,
        rows: impl Iterator<Item = (u64, i64)>,
        cx: &Context<'_>,
    ) -> Result<()> {
        let mut rows = rows.peekable();
        if rows.peek().is_none() {
            return Ok(());
        }
        cx.check()?;
        let fresh = self.paths[side][p].is_none();
        if fresh {
            self.paths[side][p] = Some(
                self.root
                    .as_ref()
                    .ok_or_else(|| Error::Invalid("closed join spill".into()))?
                    .path()
                    .join(format!("{side}-{p}.join")),
            );
            self.files += 1;
        }
        let path = self.paths[side][p]
            .as_ref()
            .ok_or_else(|| Error::Invalid("join spill path".into()))?;
        let mut writer = BufWriter::with_capacity(
            4096,
            OpenOptions::new()
                .create_new(fresh)
                .append(true)
                .open(path)?,
        );
        if fresh {
            self.charge(16, cx)?;
            writer.write_all(MAGIC)?;
            writer.write_all(
                &u32::try_from(side)
                    .map_err(|_| Error::Overflow("spill side"))?
                    .to_le_bytes(),
            )?;
            writer.write_all(
                &u32::try_from(p)
                    .map_err(|_| Error::Overflow("spill partition"))?
                    .to_le_bytes(),
            )?;
        }
        for (index, (key, value)) in rows.enumerate() {
            if index % 1024 == 0 {
                cx.check()?;
            }
            self.charge(16, cx)?;
            writer.write_all(&key.to_le_bytes())?;
            writer.write_all(&value.to_le_bytes())?;
            self.rows[side][p] = self.rows[side][p]
                .checked_add(1)
                .ok_or(Error::Overflow("join spill rows"))?;
        }
        writer.flush()?;
        Ok(())
    }
    fn batch(&mut self, side: usize, batch: &RecordBatch, cx: &Context<'_>) -> Result<()> {
        let (keys, values) = columns(batch)?;
        for p in 0..PARTITIONS {
            self.append(
                side,
                p,
                (0..batch.num_rows())
                    .filter(|i| partition(keys.value(*i)) == p)
                    .map(|i| (keys.value(i), values.value(i))),
                cx,
            )?;
        }
        Ok(())
    }
    fn seal(&mut self, cx: &Context<'_>) -> Result<()> {
        for side in 0..2 {
            for p in 0..PARTITIONS {
                cx.check()?;
                if let Some(path) = self.paths[side][p].clone() {
                    let mut file = OpenOptions::new().read(true).append(true).open(path)?;
                    let hash = digest(&mut file, 16 + self.rows[side][p] * 16, &self.reads, cx)?;
                    self.charge(40, cx)?;
                    file.write_all(&self.rows[side][p].to_le_bytes())?;
                    file.write_all(hash.as_bytes())?;
                    file.flush()?;
                }
            }
        }
        Ok(())
    }
    fn validate(&self, side: usize, p: usize, cx: &Context<'_>) -> Result<()> {
        let Some(path) = &self.paths[side][p] else {
            return Ok(());
        };
        let mut file = File::open(path)?;
        let prefix = 16 + self.rows[side][p] * 16;
        if file.metadata()?.len() != prefix + 40 {
            return Err(Error::CorruptSpill("join file length"));
        }
        let hash = digest(&mut file, prefix, &self.reads, cx)?;
        let mut footer = [0; 40];
        file.read_exact(&mut footer)
            .map_err(|_| Error::CorruptSpill("join footer"))?;
        self.reads.fetch_add(40, Ordering::Relaxed);
        if u64::from_le_bytes(
            footer[..8]
                .try_into()
                .map_err(|_| Error::CorruptSpill("join count"))?,
        ) != self.rows[side][p]
            || &footer[8..] != hash.as_bytes()
        {
            return Err(Error::CorruptSpill("join checksum/count"));
        }
        file.seek(SeekFrom::Start(0))?;
        let mut header = [0; 16];
        file.read_exact(&mut header)?;
        self.reads.fetch_add(16, Ordering::Relaxed);
        if &header[..8] != MAGIC
            || u32::from_le_bytes(
                header[8..12]
                    .try_into()
                    .map_err(|_| Error::CorruptSpill("join side"))?,
            ) != u32::try_from(side).map_err(|_| Error::Overflow("spill side"))?
            || u32::from_le_bytes(
                header[12..]
                    .try_into()
                    .map_err(|_| Error::CorruptSpill("join partition"))?,
            ) != u32::try_from(p).map_err(|_| Error::Overflow("spill partition"))?
        {
            return Err(Error::CorruptSpill("join header/side/partition"));
        }
        Ok(())
    }
    fn reader(&self, side: usize, p: usize) -> Result<Rows> {
        let path = self.paths[side][p]
            .as_ref()
            .ok_or(Error::CorruptSpill("missing join partition"))?;
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(16))?;
        Ok(Rows {
            reader: BufReader::with_capacity(4096, file),
            left: self.rows[side][p],
            reads: self.reads.clone(),
        })
    }
    fn close(&mut self) -> Result<()> {
        if let Some(root) = self.root.take() {
            root.close()?;
        }
        Ok(())
    }
}
fn digest(
    file: &mut File,
    mut bytes: u64,
    reads: &AtomicU64,
    cx: &Context<'_>,
) -> Result<blake3::Hash> {
    let mut hash = blake3::Hasher::new();
    let mut buffer = [0; 4096];
    while bytes > 0 {
        cx.check()?;
        let size =
            usize::try_from(bytes.min(4096)).map_err(|_| Error::Overflow("join checksum bytes"))?;
        file.read_exact(&mut buffer[..size])
            .map_err(|_| Error::CorruptSpill("join truncated data"))?;
        reads.fetch_add(size as u64, Ordering::Relaxed);
        hash.update(&buffer[..size]);
        bytes -= size as u64;
    }
    Ok(hash.finalize())
}
struct Rows {
    reader: BufReader<File>,
    left: u64,
    reads: Arc<AtomicU64>,
}
impl Rows {
    fn next(&mut self) -> Result<Option<(u64, i64)>> {
        if self.left == 0 {
            return Ok(None);
        }
        let mut row = [0; 16];
        self.reader
            .read_exact(&mut row)
            .map_err(|_| Error::CorruptSpill("join truncated row"))?;
        self.reads.fetch_add(16, Ordering::Relaxed);
        self.left -= 1;
        Ok(Some((
            u64::from_le_bytes(
                row[..8]
                    .try_into()
                    .map_err(|_| Error::CorruptSpill("join key"))?,
            ),
            i64::from_le_bytes(
                row[8..]
                    .try_into()
                    .map_err(|_| Error::CorruptSpill("join value"))?,
            ),
        )))
    }
}

/// Execute a bounded inner join and stream attempt-local output. The caller owns
/// any retained sink data and must commit it only after success. No rows are returned
/// on failure. Sources shut down on every path; spill/state are execution-local.
/// # Errors
/// Returns typed schema/bound/pressure/skew/spill/cancellation or sink errors.
pub fn execute(
    left: Box<dyn BatchSource>,
    right: Box<dyn BatchSource>,
    config: &Config,
    token: &CancellationToken,
    parent: Option<&Path>,
    mut sink: impl FnMut(RecordBatch) -> Result<()>,
) -> Result<Metrics> {
    let start = Instant::now();
    let mut sources = [left, right];
    let result = (|| {
        config.validate()?;
        if sources.iter().any(|s| s.schema() != input_schema()) {
            return Err(Error::Invalid("join source schema".into()));
        }
        let memory = MemoryManager::new(config.memory_limit_bytes)?;
        let mut transient = memory.reservation(OperatorId(1));
        transient.grow(2 * MAX_BATCH_BYTES + config.output_batch_rows * 24 + 1024 + 8192 + 4096)?;
        let mut cx = Context {
            config,
            token,
            deadline: Instant::now() + config.timeout,
            metrics: Metrics {
                hash_capacity_rows: config.hash_rows,
                ..Default::default()
            },
            memory,
        };
        cx.check()?;
        let mut output = Output::new(config.output_batch_rows);
        run(&mut sources, &mut cx, parent, &mut output, &mut sink)?;
        output.flush(&mut cx, &mut sink)?;
        drop(output);
        drop(transient);
        let managed = cx.memory.snapshot();
        cx.metrics.managed_peak = managed.peak;
        cx.metrics.managed_current = managed.current;
        cx.metrics.operator_work_ns = ns(start)
            .saturating_sub(cx.metrics.source_pull_ns)
            .saturating_sub(cx.metrics.sink_ns);
        cx.metrics.cleaned = true;
        Ok(cx.metrics)
    })();
    for source in &mut sources {
        source.shutdown();
    }
    result
}
#[allow(
    clippy::too_many_lines,
    reason = "The pre-output transition and partition cleanup share one ownership scope."
)]
fn run(
    sources: &mut [Box<dyn BatchSource>; 2],
    cx: &mut Context<'_>,
    parent: Option<&Path>,
    output: &mut Output,
    sink: &mut impl FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    let build = cx.config.build.index();
    let other = 1 - build;
    let mut table = if cx.config.algorithm == Algorithm::Grace {
        None
    } else {
        Some(Table::new(cx.config.hash_rows, &cx.memory)?)
    };
    let mut spill = if cx.config.algorithm == Algorithm::Grace {
        Some(Spool::new(parent, &cx.memory)?)
    } else {
        None
    };
    while let Some(batch) = cx.pull(sources[build].as_mut(), build)? {
        if let Some(spill) = &mut spill {
            spill.batch(build, &batch, cx)?;
            continue;
        }
        let (keys, values) = columns(&batch)?;
        for i in 0..batch.num_rows() {
            cx.check()?;
            let state = table
                .as_mut()
                .ok_or_else(|| Error::Invalid("missing join hash state".into()))?;
            if state.add(keys.value(i), values.value(i)) {
                cx.metrics.hash_rows_peak = cx.metrics.hash_rows_peak.max(state.rows.len());
                continue;
            }
            cx.memory.pressure();
            if !cx.config.spill_enabled || cx.config.algorithm == Algorithm::HashOnly {
                return Err(Error::SpillDisabled);
            }
            cx.metrics.transitioned = true;
            cx.metrics.switch_build_rows = state.rows.len() as u64;
            let mut sp = Spool::new(parent, &cx.memory)?;
            for p in 0..PARTITIONS {
                sp.append(
                    build,
                    p,
                    state
                        .rows
                        .iter()
                        .filter(|r| partition(r.key) == p)
                        .map(|r| (r.key, r.value)),
                    cx,
                )?;
            }
            table.take();
            // Preserve the exact unconsumed suffix of the triggering batch once.
            let tail = RecordBatch::try_new(
                input_schema(),
                vec![
                    Arc::new(UInt64Array::from_iter_values(
                        (i..batch.num_rows()).map(|r| keys.value(r)),
                    )),
                    Arc::new(Int64Array::from_iter_values(
                        (i..batch.num_rows()).map(|r| values.value(r)),
                    )),
                ],
            )?;
            sp.batch(build, &tail, cx)?;
            spill = Some(sp);
            break;
        }
    }
    sources[build].shutdown();
    if let Some(mut spill) = spill {
        cx.metrics.used_grace = true;
        while let Some(batch) = cx.pull(sources[other].as_mut(), other)? {
            spill.batch(other, &batch, cx)?;
        }
        sources[other].shutdown();
        spill.seal(cx)?;
        for side in 0..2 {
            for p in 0..PARTITIONS {
                spill.validate(side, p, cx)?;
            }
        }
        // Refuse all known oversized matching partitions before the first result.
        for p in 0..PARTITIONS {
            if spill.rows[other][p] > 0 && spill.rows[build][p] > cx.config.hash_rows as u64 {
                return Err(Error::JoinSkew {
                    partition: p,
                    build_rows: spill.rows[build][p],
                    capacity_rows: cx.config.hash_rows,
                });
            }
        }
        for p in 0..PARTITIONS {
            cx.check()?;
            if spill.rows[build][p] == 0 || spill.rows[other][p] == 0 {
                continue;
            }
            let mut state = Table::new(cx.config.hash_rows, &cx.memory)?;
            {
                let mut rows = spill.reader(build, p)?;
                while let Some((key, value)) = rows.next()? {
                    cx.check()?;
                    if !state.add(key, value) {
                        return Err(Error::JoinSkew {
                            partition: p,
                            build_rows: spill.rows[build][p],
                            capacity_rows: cx.config.hash_rows,
                        });
                    }
                    cx.metrics.hash_rows_peak = cx.metrics.hash_rows_peak.max(state.rows.len());
                }
            }
            let mut rows = spill.reader(other, p)?;
            while let Some((key, value)) = rows.next()? {
                probe(&state, key, value, output, cx, sink)?;
            }
        }
        cx.metrics.spill_files = spill.files;
        cx.metrics.spill_fd_peak = usize::from(spill.files > 0);
        cx.metrics.spill_write_bytes = spill.writes;
        cx.metrics.spill_read_bytes = spill.reads.load(Ordering::Relaxed);
        spill.close()?;
    } else {
        let table = table.ok_or_else(|| Error::Invalid("missing built join state".into()))?;
        while let Some(batch) = cx.pull(sources[other].as_mut(), other)? {
            let (keys, values) = columns(&batch)?;
            for i in 0..batch.num_rows() {
                probe(&table, keys.value(i), values.value(i), output, cx, sink)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    #[derive(Debug)]
    struct Input {
        batches: VecDeque<RecordBatch>,
        stopped: Arc<AtomicU64>,
    }
    impl BatchSource for Input {
        fn schema(&self) -> SchemaRef {
            input_schema()
        }
        fn next_batch(&mut self, token: &CancellationToken) -> Result<Option<RecordBatch>> {
            token.check()?;
            Ok(self.batches.pop_front())
        }
        fn shutdown(&mut self) {
            self.batches.clear();
            self.stopped.fetch_add(1, Ordering::Relaxed);
        }
    }
    #[derive(Debug)]
    struct Cancelling {
        inner: Input,
        token: CancellationToken,
        pulls: usize,
    }
    impl BatchSource for Cancelling {
        fn schema(&self) -> SchemaRef {
            input_schema()
        }
        fn next_batch(&mut self, t: &CancellationToken) -> Result<Option<RecordBatch>> {
            self.pulls += 1;
            if self.pulls == 2 {
                self.token.cancel();
            }
            self.inner.next_batch(t)
        }
        fn shutdown(&mut self) {
            self.inner.shutdown();
        }
    }
    fn batch(rows: &[(u64, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            input_schema(),
            vec![
                Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
            ],
        )
        .unwrap()
    }
    fn input(rows: &[(u64, i64)], size: usize, stopped: &Arc<AtomicU64>) -> Box<dyn BatchSource> {
        Box::new(Input {
            batches: rows.chunks(size).map(batch).collect(),
            stopped: stopped.clone(),
        })
    }
    fn run_rows(
        left: &[(u64, i64)],
        right: &[(u64, i64)],
        config: &Config,
    ) -> (Vec<(u64, i64, i64)>, Metrics) {
        let stopped: Arc<AtomicU64> = Arc::default();
        let parent = tempfile::tempdir().unwrap();
        let mut rows = Vec::new();
        let metrics = execute(
            input(left, 7, &stopped),
            input(right, 3, &stopped),
            config,
            &CancellationToken::default(),
            Some(parent.path()),
            |b| {
                let keys = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
                let left = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                let right = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
                for r in 0..b.num_rows() {
                    rows.push((keys.value(r), left.value(r), right.value(r)));
                }
                Ok(())
            },
        )
        .unwrap();
        rows.sort_unstable();
        assert_eq!(metrics.input_rows, [left.len() as u64, right.len() as u64]);
        assert_eq!(metrics.managed_current, 0);
        assert!(metrics.managed_peak <= config.memory_limit_bytes);
        assert!(metrics.cleaned);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
        assert!(stopped.load(Ordering::Relaxed) >= 2);
        (rows, metrics)
    }
    #[test]
    fn bags_orientation_boundaries_and_actual_hash_to_grace() {
        let mut left = vec![(0, i64::MIN), (0, i64::MIN), (u64::MAX, 7)];
        let mut right = vec![(0, i64::MAX), (0, i64::MAX), (0, i64::MAX), (u64::MAX, -7)];
        // Independent partitions avoid accidentally turning a correctness fixture
        // into the separately tested oversized-partition refusal.
        let mut used = std::collections::BTreeSet::from([partition(0), partition(u64::MAX)]);
        for key in 1..1000 {
            if used.insert(partition(key)) {
                left.push((key, i64::try_from(key).unwrap()));
                right.push((key, -(i64::try_from(key).unwrap())));
                if left.len() == 15 {
                    break;
                }
            }
        }
        let mut expected = Vec::new();
        for l in &left {
            for r in &right {
                if l.0 == r.0 {
                    expected.push((l.0, l.1, r.1));
                }
            }
        }
        expected.sort_unstable();
        assert_eq!(expected.iter().filter(|r| r.0 == 0).count(), 6);
        for build in [Side::Left, Side::Right] {
            for algorithm in [Algorithm::Auto, Algorithm::HashOnly, Algorithm::Grace] {
                let config = Config {
                    build,
                    algorithm,
                    hash_rows: if algorithm == Algorithm::HashOnly {
                        32
                    } else {
                        4
                    },
                    output_batch_rows: 2,
                    ..Default::default()
                };
                let (actual, m) = run_rows(&left, &right, &config);
                assert_eq!(actual, expected);
                if algorithm == Algorithm::Auto {
                    assert!(m.transitioned && m.used_grace);
                    assert_eq!(m.switch_build_rows, 4);
                    assert!(m.spill_write_bytes > 0 && m.spill_read_bytes > 0);
                }
                if algorithm == Algorithm::Grace {
                    assert!(m.used_grace && !m.transitioned);
                }
                assert!(m.spill_files <= 64 && m.spill_fd_peak <= 1);
            }
        }
        for (l, r) in [
            (&[][..], right.as_slice()),
            (left.as_slice(), &[][..]),
            (&[(1, 9)][..], &[(2, 8)][..]),
        ] {
            for build in [Side::Left, Side::Right] {
                assert_eq!(
                    run_rows(
                        l,
                        r,
                        &Config {
                            build,
                            hash_rows: 4,
                            ..Default::default()
                        }
                    )
                    .0,
                    Vec::<(u64, i64, i64)>::new()
                );
            }
        }
    }
    #[test]
    fn typed_bounds_cancellation_sink_failure_and_cleanup() {
        let parent = tempfile::tempdir().unwrap();
        let rows = vec![(1, 1); 20];
        for variant in 0..6 {
            let stopped: Arc<AtomicU64> = Arc::default();
            let token = CancellationToken::default();
            let config = Config {
                hash_rows: 4,
                output_batch_rows: 2,
                spill_enabled: variant != 0,
                max_output_rows: if variant == 2 { 3 } else { 1000 },
                max_spill_bytes: if variant == 5 { 16 } else { 1_000_000 },
                ..Default::default()
            };
            if variant == 3 {
                token.cancel();
            }
            let right = if variant == 2 || variant == 4 {
                vec![(1, 1); 2]
            } else {
                rows.clone()
            };
            let result = execute(
                input(&rows, 7, &stopped),
                input(&right, 3, &stopped),
                &config,
                &token,
                Some(parent.path()),
                |_| {
                    if variant == 4 {
                        token.cancel();
                        Err(Error::Invalid("sink failed".into()))
                    } else {
                        Ok(())
                    }
                },
            );
            match variant {
                0 => assert!(matches!(result, Err(Error::SpillDisabled))),
                1 => assert!(matches!(result, Err(Error::JoinSkew { .. }))),
                2 => assert!(matches!(result, Err(Error::JoinOutputLimit { limit: 3 }))),
                3 => assert!(matches!(result, Err(Error::Cancelled))),
                4 => assert!(result.is_err()),
                5 => assert!(matches!(result, Err(Error::JoinSpillLimit { limit: 16 }))),
                _ => unreachable!(),
            }
            assert!(stopped.load(Ordering::Relaxed) >= 2);
            assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
        }
        // Cancellation while partitioning, after the hash pressure transition.
        let stopped: Arc<AtomicU64> = Arc::default();
        let token = CancellationToken::default();
        let right = Box::new(Cancelling {
            inner: Input {
                batches: rows.chunks(7).map(batch).collect(),
                stopped: stopped.clone(),
            },
            token: token.clone(),
            pulls: 0,
        });
        assert!(matches!(
            execute(
                input(&rows, 7, &stopped),
                right,
                &Config {
                    hash_rows: 4,
                    ..Default::default()
                },
                &token,
                Some(parent.path()),
                |_| Ok(())
            ),
            Err(Error::Cancelled)
        ));
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }
    #[test]
    fn corrupt_and_truncated_internal_files_are_rejected_before_reading_pairs() {
        let parent = tempfile::tempdir().unwrap();
        let config = Config::default();
        let token = CancellationToken::default();
        let cx = Context {
            config: &config,
            token: &token,
            deadline: Instant::now() + config.timeout,
            metrics: Metrics::default(),
            memory: MemoryManager::new(config.memory_limit_bytes).unwrap(),
        };
        for truncated in [false, true] {
            let mut spool = Spool::new(Some(parent.path()), &cx.memory).unwrap();
            spool
                .append(0, 0, [(0, i64::MIN)].into_iter(), &cx)
                .unwrap();
            spool.seal(&cx).unwrap();
            let path = spool.paths[0][0].as_ref().unwrap();
            if truncated {
                OpenOptions::new()
                    .write(true)
                    .open(path)
                    .unwrap()
                    .set_len(17)
                    .unwrap();
            } else {
                let mut f = OpenOptions::new().write(true).open(path).unwrap();
                f.seek(SeekFrom::Start(16)).unwrap();
                f.write_all(&[77]).unwrap();
            }
            assert!(matches!(
                spool.validate(0, 0, &cx),
                Err(Error::CorruptSpill(_))
            ));
        }
        assert_eq!(cx.memory.snapshot().current, 0);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }
}
