//! Phase15 opt-in stateful composition; local working state is never a checkpoint.
pub mod checkpoint;
pub mod store;
use crate::{Engine, Error, Progress, Receipt, Result, StreamConfigV1};
use ade_exec::CancellationToken;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

pub(crate) fn check(ok: bool, why: &'static str) -> Result<()> {
    if ok { Ok(()) } else { Err(Error::State(why)) }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u32,
    pub job_id: Uuid,
    pub source: StreamConfigV1,
    pub topic_partitions: Vec<i32>,
    pub operator: String,
    pub window_ms: i64,
    pub late_policy: String,
    pub hash_version: u32,
    pub owners: usize,
    pub hot_bytes: usize,
    pub cold_bytes: u64,
    pub checkpoint_bytes: u64,
    pub max_generations: usize,
    pub checkpoint_every_records: u64,
    pub checkpoint_every_ms: u64,
    pub working_root: PathBuf,
    pub checkpoint_root: PathBuf,
}
impl Config {
    /// # Errors
    /// Refuses unknown fields, incompatible semantics and unbounded resources.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        check(bytes.len() <= 16384, "stateful config byte cap")?;
        let c: Self =
            serde_json::from_slice(bytes).map_err(|_| Error::State("strict stateful config"))?;
        c.validate()?;
        Ok(c)
    }
    /// # Errors
    /// All static constraints are checked before file/network/output effects.
    pub fn validate(&self) -> Result<()> {
        self.source.validate()?;
        check(
            self.version == 1
                && !self.job_id.is_nil()
                && self.operator == "tumbling_sum_count_v1"
                && self.late_policy == "receipt_drop_closed_v1"
                && self.hash_version == 1,
            "stateful version/operator/window/late/hash",
        )?;
        check(
            !self.topic_partitions.is_empty()
                && self.topic_partitions.len() <= 4
                && self
                    .topic_partitions
                    .iter()
                    .copied()
                    .eq(0..i32::try_from(self.topic_partitions.len()).unwrap_or(0))
                && self
                    .source
                    .starts
                    .iter()
                    .all(|s| self.topic_partitions.contains(&s.partition)),
            "full topic partition inventory",
        )?;
        check(
            self.window_ms > 0 && self.source.timers.is_empty(),
            "positive window/no transient timer registrations",
        )?;
        check([1, 2, 4].contains(&self.owners), "logical owners 1/2/4")?;
        check(
            (store::CHARGE * self.owners..=16 * 1024 * 1024).contains(&self.hot_bytes)
                && (128..=128 * 1024 * 1024).contains(&self.cold_bytes),
            "hot/cold caps",
        )?;
        check(
            (65536..=256 * 1024 * 1024).contains(&self.checkpoint_bytes)
                && (2..=64).contains(&self.max_generations),
            "checkpoint quotas",
        )?;
        check(
            (1..=1_000_000).contains(&self.checkpoint_every_records)
                && (100..=3_600_000).contains(&self.checkpoint_every_ms),
            "fixed checkpoint cadence",
        )?;
        for p in [&self.working_root, &self.checkpoint_root] {
            check(
                p.is_absolute()
                    && p.as_os_str().len() <= 1024
                    && p.components().all(|c| {
                        matches!(
                            c,
                            std::path::Component::RootDir | std::path::Component::Normal(_)
                        )
                    }),
                "absolute canonical roots",
            )?;
        }
        check(
            !self.working_root.starts_with(&self.checkpoint_root)
                && !self.checkpoint_root.starts_with(&self.working_root),
            "separate working/checkpoint roots",
        )
    }
    /// Immutable semantic/source fingerprint. Resume offsets, owners, resource knobs,
    /// endpoint/credentials and session UUID are deliberately excluded.
    /// # Errors
    /// Returns serialization errors without echoing sensitive configuration.
    pub fn fingerprint(&self) -> Result<String> {
        let v = serde_json::json!({"version":1,"job":self.job_id,"schema":"EventV1/u64/i64/i64","operator":self.operator,"width":self.window_ms,"late":self.late_policy,"hash":self.hash_version,
            "cluster":self.source.expected_cluster_id,"topic":self.source.topic,"partitions":self.source.starts.iter().map(|s|s.partition).collect::<Vec<_>>(),"topic_partitions":self.topic_partitions,"key_less_than":self.source.key_less_than,"allowance":self.source.allowance_ms,"history":"immutable-uncompressed-32768-v1"});
        Ok(
            blake3::hash(&serde_json::to_vec(&v).map_err(|_| Error::State("fingerprint"))?)
                .to_hex()
                .to_string(),
        )
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    pub start: i64,
    pub key: u64,
    pub sum: i64,
    pub count: u64,
}
impl Entry {
    #[must_use]
    pub fn id(self) -> (i64, u64) {
        (self.start, self.key)
    }
}
/// Stable hash contract: low five bits of BLAKE3(u64 little endian).
#[must_use]
pub fn shard(key: u64) -> usize {
    usize::from(blake3::hash(&key.to_le_bytes()).as_bytes()[0] & 31)
}
/// # Errors
/// Refuses alignment/end outside signed milliseconds.
pub fn window(time: i64, width: i64) -> Result<(i64, i64)> {
    check(width > 0, "window width")?;
    let start = time
        .div_euclid(width)
        .checked_mul(width)
        .ok_or(Error::Overflow("window start"))?;
    Ok((start, end(start, width)?))
}
pub(crate) fn end(start: i64, width: i64) -> Result<i64> {
    start
        .checked_add(width)
        .ok_or(Error::Overflow("window end"))
}
#[derive(Debug, Clone, Serialize)]
pub struct WindowOutput {
    pub r#type: &'static str,
    pub output_version: u32,
    pub job_id: Uuid,
    pub key: u64,
    pub window_start: i64,
    pub window_end: i64,
    pub sum_i64: i64,
    pub count_u64: u64,
    pub output_id: String,
}
pub struct State {
    pub config: Config,
    pub store: store::Store,
    pub closed_window_late: u64,
    pub last_closed_late: bool,
    input_pending: bool,
    pending_close: Option<Entry>,
    pub generation: Option<checkpoint::Pointer>,
    last_records: u64,
    last_ms: u64,
    pub test_pause_at: Option<String>,
    failed: bool,
    _lock: checkpoint::Lock,
}
impl State {
    /// # Errors
    /// Refuses unsafe/occupied roots; restore is fully verified before rebuilding.
    pub fn open(
        config: Config,
        session: Uuid,
        token: CancellationToken,
        restore: bool,
    ) -> Result<(Self, Option<Progress>)> {
        config.validate()?;
        let lock = checkpoint::Lock::acquire(&config)?;
        let loaded = if restore {
            Some(checkpoint::load(&config, &token)?)
        } else {
            check(
                !config.checkpoint_root.join("CURRENT").exists(),
                "existing checkpoint requires explicit restore",
            )?;
            None
        };
        let mut store = store::Store::new(&config, session, token)?;
        let mut progress = None;
        let mut generation = None;
        let mut closed_window_late = 0;
        let mut last_records = 0;
        if let Some((pointer, manifest)) = loaded {
            // Validation above completed for every shard before any state is adopted.
            for s in 0..store::SHARDS {
                let path = config
                    .checkpoint_root
                    .join(pointer.generation.to_string())
                    .join(format!("shard-{s:02}.bin"));
                let mut f = store::regular(&path, config.checkpoint_bytes)?;
                while let Some(e) = store::read(&mut f)? {
                    store.restore_entry(e)?;
                }
            }
            let mut p = manifest.progress;
            p.session = session;
            for c in p.partitions.values_mut() {
                c.start = c.next_offset_suggestion;
                c.seen = c.forwarded;
            }
            last_records = p.forwarded_records;
            closed_window_late = manifest.closed_window_late;
            generation = Some(pointer);
            progress = Some(p);
        }
        Ok((
            Self {
                config,
                store,
                closed_window_late,
                last_closed_late: false,
                input_pending: false,
                pending_close: None,
                generation,
                last_records,
                last_ms: 0,
                test_pause_at: None,
                failed: false,
                _lock: lock,
            },
            progress,
        ))
    }
    pub(crate) fn apply(&mut self, r: &Receipt, frontier: Option<i64>) -> Result<()> {
        check(
            !self.failed && !self.input_pending && self.pending_close.is_none(),
            "state input/close pending",
        )?;
        self.input_pending = true;
        self.last_closed_late = false;
        if let Some(e) = &r.projected {
            let (start, end) = window(e.event_time_ms, self.config.window_ms)?;
            if frontier.is_some_and(|f| end <= f) {
                self.closed_window_late = self
                    .closed_window_late
                    .checked_add(1)
                    .ok_or(Error::Overflow("closed late"))?;
                self.last_closed_late = true;
            } else {
                let old = self.store.get((start, e.key))?;
                let mut next = old.unwrap_or(Entry {
                    start,
                    key: e.key,
                    sum: 0,
                    count: 0,
                });
                next.sum = next
                    .sum
                    .checked_add(e.value_i64)
                    .ok_or(Error::Overflow("window sum"))?;
                next.count = next
                    .count
                    .checked_add(1)
                    .ok_or(Error::Overflow("window count"))?;
                self.store.put(next, old.is_none())?;
            }
        }
        Ok(())
    }
    pub(crate) fn poison(&mut self) {
        self.failed = true;
    }
    pub(crate) fn confirm_input(&mut self) {
        self.input_pending = false;
    }
    /// One outstanding close record; scanning uses fixed scratch and no output queue.
    /// # Errors
    /// Refuses corrupt tiers, arithmetic overflow or an unmatched input.
    pub fn next_close(&mut self, frontier: Option<i64>) -> Result<Option<WindowOutput>> {
        check(
            !self.failed && !self.input_pending,
            "failed attempt/input receipt pending",
        )?;
        if self.pending_close.is_none() {
            self.pending_close = self.store.first_closed(frontier, self.config.window_ms)?;
        }
        let Some(e) = self.pending_close else {
            return Ok(None);
        };
        let end = end(e.start, self.config.window_ms)?;
        Ok(Some(WindowOutput {
            r#type: "window",
            output_version: 1,
            job_id: self.config.job_id,
            key: e.key,
            window_start: e.start,
            window_end: end,
            sum_i64: e.sum,
            count_u64: e.count,
            output_id: format!("{}:{}:{}:{}:1", self.config.job_id, e.key, e.start, end),
        }))
    }
    /// # Errors
    /// Only called after the whole close frame reaches the sink.
    pub fn confirm_close(&mut self) -> Result<()> {
        let e = self.pending_close.ok_or(Error::State("no pending close"))?;
        self.store.remove(e)?;
        self.pending_close = None;
        Ok(())
    }
    #[must_use]
    pub fn due(&self, p: &Progress, ms: u64) -> bool {
        p.forwarded_records.saturating_sub(self.last_records)
            >= self.config.checkpoint_every_records
            || ms.saturating_sub(self.last_ms) >= self.config.checkpoint_every_ms
    }
    /// # Errors
    /// Refuses any incomplete cut; source demand must already be paused by caller.
    pub fn checkpoint(
        &mut self,
        p: &Progress,
        ms: u64,
        hook: &mut dyn FnMut(&str) -> Result<()>,
    ) -> Result<checkpoint::Pointer> {
        check(
            !self.failed
                && !self.input_pending
                && self.pending_close.is_none()
                && self
                    .store
                    .first_closed(p.frontier, self.config.window_ms)?
                    .is_none(),
            "checkpoint requires quiescent cut",
        )?;
        self.failed = true;
        let pointer = checkpoint::publish(self, p, hook)?;
        self.failed = false;
        self.last_records = p.forwarded_records;
        self.last_ms = ms;
        self.generation = Some(pointer.clone());
        Ok(pointer)
    }
}
impl Engine {
    /// # Errors
    /// Opens fresh/verified restored state before Kafka assignment/output.
    pub fn stateful(
        mut config: Config,
        session: Uuid,
        token: CancellationToken,
        restore: bool,
    ) -> Result<Self> {
        let (state, progress) = State::open(config.clone(), session, token, restore)?;
        if let Some(p) = &progress {
            for start in &mut config.source.starts {
                start.offset = p.partitions[&start.partition].next_offset_suggestion;
            }
        }
        let mut engine = Self::new(config.source, session)?;
        if let Some(p) = progress {
            engine.progress = p;
        }
        engine.state = Some(state);
        Ok(engine)
    }
}
