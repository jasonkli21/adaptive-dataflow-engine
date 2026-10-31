//! Opt-in unbounded source ownership. Idle is Pending; offsets are not timestamps.
use arrow::{
    array::{Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc, time::Instant};
use uuid::Uuid;

#[cfg(feature = "adaptive-checkpoint")]
pub mod adaptive;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "kafka")]
pub mod runtime;
#[cfg(feature = "stateful")]
pub mod state;
pub mod timers;
#[cfg(feature = "transactional-state")]
pub mod transactional;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("stream admission: {0}")]
    Admission(&'static str),
    #[error("source identity/offset: {0}")]
    Source(&'static str),
    #[error("invalid event (payload redacted)")]
    Event,
    #[error("stream arithmetic overflow: {0}")]
    Overflow(&'static str),
    #[error("timer capacity/identity: {0}")]
    Timer(&'static str),
    #[cfg(feature = "stateful")]
    #[error("state/checkpoint: {0}")]
    State(&'static str),
    #[error("recovery unavailable (replay: {replay}, restore: {restore})")]
    RecoveryUnavailable {
        replay: &'static str,
        restore: &'static str,
    },
    #[error("broker failure (details redacted)")]
    Broker,
    #[error("sink failure (details redacted)")]
    Sink,
    #[error("operator test limit interrupted a partially written frame ({written}/{total} bytes)")]
    PartialFrame { written: usize, total: usize },
    #[error("sink stall deadline")]
    SinkDeadline,
    #[error("cancelled")]
    Cancelled,
    #[error("native operator: {0}")]
    Native(#[from] ade_exec::Error),
}
impl Error {
    #[must_use]
    pub fn class(&self) -> &'static str {
        match self {
            Self::Admission(_) => "admission",
            Self::Source(_) => "source_identity_or_offset",
            Self::Event => "event_schema_or_size",
            Self::Overflow(_) => "arithmetic_overflow",
            Self::Timer(_) => "timer_capacity_or_identity",
            #[cfg(feature = "stateful")]
            Self::State(_) => "state_or_checkpoint",
            Self::RecoveryUnavailable { .. } => "recovery_unavailable",
            Self::Broker => "broker",
            Self::Sink => "sink_or_diagnostics",
            Self::PartialFrame { .. } => "partial_output_frame",
            Self::SinkDeadline => "sink_deadline",
            Self::Cancelled => "cancelled",
            Self::Native(_) => "native_operator",
        }
    }

    /// Byte counts for a frame interrupted by an operator test deadline.
    #[must_use]
    pub fn partial_output(&self) -> Option<(usize, usize)> {
        match self {
            Self::PartialFrame { written, total } => Some((*written, *total)),
            _ => None,
        }
    }
}
pub type Result<T> = std::result::Result<T, Error>;
/// # Errors
/// Returns a structured admission error when the predicate is false.
pub fn require(ok: bool, message: &'static str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(Error::Admission(message))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Start {
    pub partition: i32,
    pub offset: i64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum Security {
    LoopbackDev {},
    SaslSsl {
        username_env: String,
        password_env: String,
        ca_path: String,
    },
}
// Deliberately no Debug for security or config: no credential/path echo.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamConfigV1 {
    pub version: u32,
    pub bootstrap: String,
    pub expected_cluster_id: String,
    pub topic: String,
    pub starts: Vec<Start>,
    pub security: Security,
    pub key_less_than: u64,
    pub allowance_ms: i64,
    pub payload_bytes: usize,
    pub client_queue_kib: u32,
    pub sink_stall_ms: u64,
    pub timers: Vec<timers::Registration>,
}
impl StreamConfigV1 {
    /// # Errors
    /// Refuses unsupported/oversized configurations before broker/output effects.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        require(bytes.len() <= 16384, "config byte cap")?;
        let config: Self =
            serde_json::from_slice(bytes).map_err(|_| Error::Admission("strict config JSON"))?;
        config.validate()?;
        Ok(config)
    }
    /// # Errors
    /// Enforces v1 bounds; all checks precede network connection.
    pub fn validate(&self) -> Result<()> {
        require(self.version == 1, "version")?;
        require(
            !self.topic.is_empty()
                && self.topic.len() <= 128
                && self
                    .topic
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
                && self.topic != "."
                && self.topic != "..",
            "topic",
        )?;
        require(
            !self.expected_cluster_id.is_empty()
                && self.expected_cluster_id.len() <= 128
                && self
                    .expected_cluster_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b)),
            "cluster identity",
        )?;
        require(
            (1..=4).contains(&self.starts.len()),
            "1..4 fixed partitions",
        )?;
        let mut previous = -1;
        for start in &self.starts {
            require(
                start.partition > previous
                    && start.partition < 4
                    && start.offset >= 0
                    && start.offset < i64::MAX,
                "sorted unique partitions and absolute offsets",
            )?;
            previous = start.partition;
        }
        require(self.allowance_ms >= 0, "nonnegative allowance")?;
        require(
            (128..=4096).contains(&self.payload_bytes),
            "payload cap 128..4096",
        )?;
        require(
            (1..=1024).contains(&self.client_queue_kib),
            "client queue 1..1024KiB",
        )?;
        require(
            (100..=120_000).contains(&self.sink_stall_ms),
            "sink stall 100..120000ms",
        )?;
        // One explicit bootstrap endpoint; metadata endpoints are checked too.
        let addr: std::net::SocketAddr = self
            .bootstrap
            .parse()
            .map_err(|_| Error::Admission("bootstrap must be numeric IP:port"))?;
        require(addr.port() != 0, "port")?;
        match &self.security {
            Security::LoopbackDev {} => require(
                addr.ip().is_loopback(),
                "plaintext only explicit loopback development",
            )?,
            Security::SaslSsl {
                username_env,
                password_env,
                ca_path,
            } => {
                for name in [username_env, password_env] {
                    require(
                        !name.is_empty()
                            && name.len() <= 128
                            && name
                                .bytes()
                                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'),
                        "credential environment name",
                    )?;
                }
                require(
                    !ca_path.is_empty()
                        && ca_path.len() <= 1024
                        && std::path::Path::new(ca_path).is_absolute(),
                    "CA path",
                )?;
            }
        }
        let mut queue = timers::Timers::new(64, 8192, 1024)?;
        for timer in &self.timers {
            queue.register(timer.clone())?;
        }
        Ok(())
    }
    /// Stable public plan identity; secret environment values are absent.
    /// # Errors
    /// Returns an admission error if serialization fails.
    pub fn identity(&self) -> Result<String> {
        let bytes = serde_json::to_vec(self).map_err(|_| Error::Admission("config identity"))?;
        Ok(blake3::hash(&bytes).to_hex().to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventV1 {
    pub version: u32,
    pub key: u64,
    pub value_i64: i64,
    pub event_time_ms: i64,
}
#[derive(Debug)]
pub struct RawRecord {
    pub session: Uuid,
    pub partition: i32,
    pub offset: i64,
    pub payload: Vec<u8>,
}
#[derive(Debug)]
pub enum Poll {
    Data(RawRecord),
    Pending,
    Stopped,
}
/// Separate from bounded `BatchSource`. No empty-poll EOF.
pub trait Source {
    /// # Errors
    /// Returns broker, identity or payload errors; never infers EOF from idle.
    fn poll(&mut self) -> Result<Poll>;
    /// # Errors
    /// Returns an error if ADE demand cannot be paused; bounded client prefetch may continue.
    fn pause(&mut self) -> Result<()>;
    /// # Errors
    /// Returns a broker error if demand cannot be resumed.
    fn resume(&mut self) -> Result<()>;
    /// # Errors
    /// Returns metadata/identity/retention errors.
    fn verify(&mut self, progress: &Progress) -> Result<()>;
    fn snapshot(&self, _: &Progress) -> serde_json::Value {
        serde_json::Value::Null
    }
}
/// Injectable elapsed monotonic time, never wall-clock event time.
pub trait Clock {
    fn now_ms(&self) -> u64;
}
pub struct MonotonicClock(Instant);
impl Default for MonotonicClock {
    fn default() -> Self {
        Self(Instant::now())
    }
}
impl Clock for MonotonicClock {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.0.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cursor {
    pub start: i64,
    pub seen: Option<i64>,
    pub forwarded: Option<i64>,
    pub next_offset_suggestion: i64,
    pub max_forwarded_time: Option<i64>,
    pub watermark: Option<i64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Progress {
    pub session: Uuid,
    pub partitions: BTreeMap<i32, Cursor>,
    pub frontier: Option<i64>,
    pub forwarded_records: u64,
    pub late_records: u64,
}
#[derive(Debug, Clone, Serialize)]
pub struct Receipt {
    pub version: u32,
    pub session: Uuid,
    pub partition: i32,
    pub offset: i64,
    pub event_time_ms: i64,
    pub late: bool,
    /// None is an explicit filtered receipt, never a silent skip.
    pub projected: Option<Projected>,
}
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Projected {
    pub value_i64: i64,
    pub key: u64,
    pub event_time_ms: i64,
}

pub struct Engine {
    pub progress: Progress,
    pub timers: timers::Timers,
    config: StreamConfigV1,
    pending: Option<Receipt>,
    #[cfg(feature = "stateful")]
    pub state: Option<state::State>,
    pub decoded_peak_bytes: usize,
    pub arrow_peak_bytes: usize,
    pub managed_in_flight_peak_bytes: usize,
}
impl Engine {
    /// # Errors
    /// Validates config and timer inventory without external effects.
    pub fn new(config: StreamConfigV1, session: Uuid) -> Result<Self> {
        config.validate()?;
        let mut timers = timers::Timers::new(64, 8192, 1024)?;
        for timer in &config.timers {
            timers.register(timer.clone())?;
        }
        let partitions = config
            .starts
            .iter()
            .map(|s| {
                (
                    s.partition,
                    Cursor {
                        start: s.offset,
                        seen: None,
                        forwarded: None,
                        next_offset_suggestion: s.offset,
                        max_forwarded_time: None,
                        watermark: None,
                    },
                )
            })
            .collect();
        Ok(Self {
            progress: Progress {
                session,
                partitions,
                frontier: None,
                forwarded_records: 0,
                late_records: 0,
            },
            timers,
            config,
            pending: None,
            #[cfg(feature = "stateful")]
            state: None,
            decoded_peak_bytes: 0,
            arrow_peak_bytes: 0,
            managed_in_flight_peak_bytes: 0,
        })
    }
    /// # Errors
    /// Rejects foreign/backward records, malformed data and arithmetic before output.
    #[allow(
        clippy::too_many_lines,
        clippy::needless_pass_by_value,
        reason = "Single admission transition takes ownership of the bounded source payload"
    )]
    pub fn prepare(&mut self, record: RawRecord) -> Result<Receipt> {
        if self.pending.is_some() {
            return Err(Error::Source("one in-flight receipt"));
        }
        if record.session != self.progress.session {
            return Err(Error::Source("session"));
        }
        let cursor = self
            .progress
            .partitions
            .get_mut(&record.partition)
            .ok_or(Error::Source("partition"))?;
        if record.offset < cursor.start || cursor.seen.is_some_and(|old| record.offset <= old) {
            return Err(Error::Source("backward offset/start"));
        }
        record
            .offset
            .checked_add(1)
            .ok_or(Error::Overflow("offset"))?;
        cursor.seen = Some(record.offset);
        if record.payload.len() > self.config.payload_bytes {
            return Err(Error::Event);
        }
        self.decoded_peak_bytes = self.decoded_peak_bytes.max(record.payload.len());
        let event: EventV1 = serde_json::from_slice(&record.payload).map_err(|_| Error::Event)?;
        if event.version != 1 {
            return Err(Error::Event);
        }
        cursor
            .max_forwarded_time
            .unwrap_or(event.event_time_ms)
            .max(event.event_time_ms)
            .checked_sub(self.config.allowance_ms)
            .ok_or(Error::Overflow("watermark"))?;
        self.progress
            .forwarded_records
            .checked_add(1)
            .ok_or(Error::Overflow("records"))?;
        let late = cursor
            .watermark
            .is_some_and(|watermark| event.event_time_ms < watermark);
        if late {
            self.progress
                .late_records
                .checked_add(1)
                .ok_or(Error::Overflow("late records"))?;
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt64, false),
            Field::new("value_i64", DataType::Int64, false),
            Field::new("event_time_ms", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![event.key])),
                Arc::new(Int64Array::from(vec![event.value_i64])),
                Arc::new(Int64Array::from(vec![event.event_time_ms])),
            ],
        )
        .map_err(|_| Error::Event)?;
        let filtered = ade_exec::stateless::filter_u64_lt(&batch, 0, self.config.key_less_than)?;
        let projected = ade_exec::stateless::project(&filtered, &[1, 0, 2])?;
        self.arrow_peak_bytes = self.arrow_peak_bytes.max(
            batch.get_array_memory_size()
                + filtered.get_array_memory_size()
                + projected.get_array_memory_size(),
        );
        // Conservative simultaneous managed retention: payload, three Arrow views,
        // one bounded sink frame and two receipt values. Client/kernel buffers separate.
        let in_flight = record.payload.len()
            + batch.get_array_memory_size()
            + filtered.get_array_memory_size()
            + projected.get_array_memory_size()
            + 2048
            + 2 * std::mem::size_of::<Receipt>();
        if self.arrow_peak_bytes > 8192 || in_flight > 16384 {
            return Err(Error::Admission("Arrow/in-flight byte cap"));
        }
        self.managed_in_flight_peak_bytes = self.managed_in_flight_peak_bytes.max(in_flight);
        let output = if projected.num_rows() == 0 {
            None
        } else {
            let value = projected
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or(Error::Event)?
                .value(0);
            let key = projected
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or(Error::Event)?
                .value(0);
            let time = projected
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or(Error::Event)?
                .value(0);
            Some(Projected {
                value_i64: value,
                key,
                event_time_ms: time,
            })
        };
        let receipt = Receipt {
            version: 1,
            session: record.session,
            partition: record.partition,
            offset: record.offset,
            event_time_ms: event.event_time_ms,
            late,
            projected: output,
        };
        #[cfg(feature = "stateful")]
        if let Some(state) = &mut self.state {
            state.apply(&receipt, self.progress.frontier)?;
        }
        self.pending = Some(receipt.clone());
        Ok(receipt)
    }
    /// Only the owner invokes this after a complete sink frame acknowledgment.
    /// # Errors
    /// Refuses acknowledgment without prepared input. No external receipt can be forged.
    pub fn acknowledge(&mut self) -> Result<()> {
        let r = self
            .pending
            .take()
            .ok_or(Error::Source("ack without input"))?;
        let cursor = self
            .progress
            .partitions
            .get_mut(&r.partition)
            .ok_or(Error::Source("ack partition"))?;
        cursor.forwarded = Some(r.offset);
        cursor.next_offset_suggestion = r.offset.checked_add(1).ok_or(Error::Overflow("offset"))?;
        let max = cursor
            .max_forwarded_time
            .unwrap_or(r.event_time_ms)
            .max(r.event_time_ms);
        cursor.max_forwarded_time = Some(max);
        cursor.watermark = Some(
            max.checked_sub(self.config.allowance_ms)
                .ok_or(Error::Overflow("watermark"))?,
        );
        self.progress.frontier = self
            .progress
            .partitions
            .values()
            .map(|c| c.watermark)
            .collect::<Option<Vec<_>>>()
            .and_then(|v| v.into_iter().min());
        self.progress.forwarded_records += 1;
        self.progress.late_records += u64::from(r.late);
        #[cfg(feature = "stateful")]
        if let Some(state) = &mut self.state {
            state.confirm_input();
        }
        Ok(())
    }
    #[must_use]
    pub fn has_unconfirmed_input(&self) -> bool {
        self.pending.is_some()
    }
    pub fn cancel(&mut self) {
        self.pending = None;
        self.timers.clear();
    }
}
