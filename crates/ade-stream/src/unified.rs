//! Compile shared logical semantics into the existing native stream machinery.
use crate::{
    Error, NativeExecutionV1, Result, StreamConfigV1, require,
    timers::{Kind, Registration},
};
use ade_logical::{
    Choice, Computation, Controls, Decision, Kafka, KafkaMode, LogicalPlanV1, Sink, TimerKind,
};
use serde::Serialize;
use std::io::Read;
use uuid::Uuid;

/// Read-only, one-way representation of an admitted Kafka program. It has no
/// deserializer or public constructor, so untrusted bytes must always pass
/// through `Admission::compile` before becoming executable.
/// ```compile_fail
/// let _: ade_stream::unified::NativeStreamSpecV1 =
///     serde_json::from_str(r#"{}"#).unwrap();
/// ```
#[derive(Clone, Debug, Serialize)]
pub struct NativeStreamSpecV1 {
    version: u32,
    source: Kafka,
    logical_job: String,
    key_less_than: u64,
    allowance_ms: i64,
    projection: Vec<String>,
    state: Option<NativeStateV1>,
    timers: Vec<Registration>,
    delivery: NativeDeliveryV1,
    semantic_digest: String,
    native_job_id: Uuid,
}

#[derive(Clone, Debug, Serialize)]
struct NativeStateV1 {
    operator: &'static str,
    window_ms: i64,
    late_policy: &'static str,
    hash_version: u32,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum NativeDeliveryV1 {
    Receipts,
    CommittedLocalV3,
}

/// A compiled authority cannot be modified through the read-only decision view.
/// ```compile_fail
/// # fn tamper(admitted: ade_stream::unified::Admission) {
/// admitted.decision().properties.stateful = false;
/// # }
/// ```
#[derive(Clone)]
pub struct Admission {
    plan: LogicalPlanV1,
    decision: Decision,
    spec: NativeStreamSpecV1,
    spec_hash: String,
}
impl Admission {
    #[must_use]
    pub fn decision(&self) -> &Decision {
        &self.decision
    }

    #[must_use]
    pub fn native_spec(&self) -> &NativeStreamSpecV1 {
        &self.spec
    }

    #[must_use]
    pub fn native_spec_hash(&self) -> &str {
        &self.spec_hash
    }

    /// # Errors
    /// Compiles strict shared semantics without I/O or native effects.
    pub fn compile(bytes: &[u8]) -> Result<Self> {
        let plan =
            LogicalPlanV1::from_json(bytes).map_err(|_| Error::Admission("strict logical plan"))?;
        let decision = Self::optimize(&plan, true)?;
        let spec = NativeStreamSpecV1::lower(&plan, &decision)?;
        let spec_bytes =
            serde_json::to_vec(&spec).map_err(|_| Error::Admission("native spec encode"))?;
        let spec_hash = blake3::hash(&spec_bytes).to_hex().to_string();
        Ok(Self {
            plan,
            decision,
            spec,
            spec_hash,
        })
    }

    fn optimize(plan: &LogicalPlanV1, enabled: bool) -> Result<Decision> {
        let decision = ade_logical::optimize(
            plan,
            &Controls {
                enabled,
                static_choice: Choice::NativeKafka,
                ..Controls::default()
            },
        )
        .map_err(|_| Error::Admission("no legal streaming target"))?;
        require(
            decision.chosen == Choice::NativeKafka,
            "stream semantic plan required",
        )?;
        Ok(decision)
    }

    #[must_use]
    pub fn source(&self) -> &Kafka {
        &self.spec.source
    }

    pub(crate) fn native_execution(&self) -> NativeExecutionV1 {
        let projection = self
            .spec
            .projection
            .iter()
            .map(|column| match column.as_str() {
                "value_i64" => Some(1),
                "key" => Some(0),
                "event_time_ms" => Some(2),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .and_then(|columns| columns.try_into().ok())
            .expect("logical admission fixed the native projection contract");
        NativeExecutionV1 {
            key_less_than: self.spec.key_less_than,
            allowance_ms: self.spec.allowance_ms,
            projection,
            timers: self.spec.timers.clone(),
        }
    }

    /// Lower semantic source/operator fields into the existing runtime config.
    /// The caller's copy is only a compatibility assertion; connection, security,
    /// quota, and mutable recovery-cursor settings remain operational context.
    /// # Errors
    /// Rejects stale semantic assertions before the source can connect.
    pub fn bind_source(&self, c: &StreamConfigV1) -> Result<StreamConfigV1> {
        c.validate()?;
        let source = &self.spec.source;
        require(
            c.expected_cluster_id == source.cluster
                && c.topic == source.topic
                && c.key_less_than == self.spec.key_less_than
                && c.allowance_ms == self.spec.allowance_ms
                && c.timers == self.spec.timers,
            "logical/native source/transform/timers mismatch",
        )?;
        require(
            c.starts.len() == source.original_starts.len()
                && c.starts
                    .iter()
                    .zip(&source.original_starts)
                    .all(|(actual, origin)| {
                        actual.partition == origin.partition && actual.offset >= origin.offset
                    }),
            "logical original start/cursor mismatch",
        )?;
        let mut bound = c.clone();
        bound.expected_cluster_id.clone_from(&source.cluster);
        bound.topic.clone_from(&source.topic);
        bound.key_less_than = self.spec.key_less_than;
        bound.allowance_ms = self.spec.allowance_ms;
        bound.timers.clone_from(&self.spec.timers);
        bound.validate()?;
        Ok(bound)
    }

    fn require_original_starts(&self, c: &StreamConfigV1) -> Result<()> {
        require(
            c.starts.len() == self.spec.source.original_starts.len()
                && c.starts.iter().zip(&self.spec.source.original_starts).all(
                    |(actual, origin)| {
                        actual.partition == origin.partition && actual.offset == origin.offset
                    },
                ),
            "logical original starts mismatch",
        )
    }

    /// # Errors
    /// Binds the optional state operator and delivery identity before opening roots.
    #[cfg(feature = "stateful")]
    pub fn bind_state(
        &self,
        c: &crate::state::Config,
        committed: bool,
    ) -> Result<crate::state::Config> {
        self.require_original_starts(&c.source)?;
        let source = self.bind_source(&c.source)?;
        let state = self
            .spec
            .state
            .as_ref()
            .ok_or(Error::Admission("logical state operator required"))?;
        let delivery = if committed {
            NativeDeliveryV1::CommittedLocalV3
        } else {
            NativeDeliveryV1::Receipts
        };
        require(
            self.spec.delivery == delivery,
            "logical/native delivery mismatch",
        )?;
        require(
            c.topic_partitions == self.spec.source.topic_partitions
                && c.operator == state.operator
                && c.window_ms == state.window_ms
                && c.late_policy == state.late_policy
                && c.hash_version == state.hash_version
                && c.job_id == self.spec.native_job_id,
            "logical/native state identity mismatch",
        )?;
        let mut bound = c.clone();
        bound.source = source;
        bound
            .topic_partitions
            .clone_from(&self.spec.source.topic_partitions);
        bound.job_id = self.spec.native_job_id;
        state.operator.clone_into(&mut bound.operator);
        bound.window_ms = state.window_ms;
        state.late_policy.clone_into(&mut bound.late_policy);
        bound.hash_version = state.hash_version;
        bound.validate()?;
        Ok(bound)
    }

    /// # Errors
    /// Requires the exact committed-local v3 contract and history attestation.
    #[cfg(feature = "transactional-state")]
    pub fn bind_transactional(
        &self,
        c: &crate::transactional::Config,
    ) -> Result<crate::transactional::Config> {
        c.validate()?;
        require(
            self.spec.source.immutable_history == c.trusted_history,
            "logical v3 history mismatch",
        )?;
        let mut bound = c.clone();
        bound.state = self.bind_state(&c.state, true)?;
        bound
            .trusted_history
            .clone_from(&self.spec.source.immutable_history);
        bound.validate()?;
        Ok(bound)
    }

    /// # Errors
    /// Retained compatibility validator; execution uses the returned lowered config.
    pub fn validate_source(&self, c: &StreamConfigV1) -> Result<()> {
        self.bind_source(c).map(|_| ())
    }
    /// # Errors
    /// Stateful identity, source/window and delivery must match before root effects.
    #[cfg(feature = "stateful")]
    pub fn validate_state(&self, c: &crate::state::Config, committed: bool) -> Result<()> {
        self.bind_state(c, committed).map(|_| ())
    }
    /// # Errors
    /// Refuses state or committed sinks through the stateless entrypoint.
    pub fn bind_stateless(&self, c: &StreamConfigV1) -> Result<StreamConfigV1> {
        require(
            self.spec.state.is_none() && self.spec.delivery == NativeDeliveryV1::Receipts,
            "stateless capability mismatch",
        )?;
        self.require_original_starts(c)?;
        self.bind_source(c)
    }
    /// # Errors
    /// Retained compatibility validator; execution uses the returned lowered config.
    pub fn validate_stateless(&self, c: &StreamConfigV1) -> Result<()> {
        self.bind_stateless(c).map(|_| ())
    }
    #[must_use]
    pub fn record(&self, partition: i32, offset: i64) -> serde_json::Value {
        let phase = match &self.spec.source.mode {
            KafkaMode::HistoryThenLive { cut }
                if cut
                    .iter()
                    .any(|c| c.partition == partition && offset < c.offset) =>
            {
                "historical"
            }
            _ => "live",
        };
        serde_json::json!({
            "semantic_digest": &self.spec.semantic_digest,
            "partition": partition,
            "offset": offset,
            "phase": phase
        })
    }
}

impl NativeStreamSpecV1 {
    fn lower(plan: &LogicalPlanV1, decision: &Decision) -> Result<Self> {
        let Computation::Kafka {
            source,
            job,
            key_less_than,
            allowance_ms,
            projection,
            window_ms,
            late_policy,
            hash_version,
            timers,
        } = &plan.computation
        else {
            return Err(Error::Admission("Kafka plan"));
        };
        let state = if let Some(width) = *window_ms {
            Some(NativeStateV1 {
                operator: "tumbling_sum_count_v1",
                window_ms: width,
                late_policy: match late_policy.as_deref() {
                    Some("receipt_drop_closed_v1") => "receipt_drop_closed_v1",
                    _ => return Err(Error::Admission("logical late policy")),
                },
                hash_version: *hash_version,
            })
        } else {
            None
        };
        let delivery = match plan.sink {
            Sink::Receipts => NativeDeliveryV1::Receipts,
            Sink::CommittedLocalV3 => NativeDeliveryV1::CommittedLocalV3,
            Sink::Collect => return Err(Error::Admission("Kafka collect sink")),
        };
        let native_job_id = match delivery {
            NativeDeliveryV1::CommittedLocalV3 => job
                .parse()
                .map_err(|_| Error::Admission("logical native job UUID"))?,
            NativeDeliveryV1::Receipts => Uuid::from_bytes(
                plan.bound_job_bytes()
                    .map_err(|_| Error::Admission("bound semantic identity"))?,
            ),
        };
        let native_timers = timers
            .iter()
            .map(|timer| Registration {
                kind: match timer.kind {
                    TimerKind::Processing => Kind::Processing,
                    TimerKind::Event => Kind::Event,
                },
                deadline_ms: timer.deadline_ms,
            })
            .collect();
        Ok(Self {
            version: 1,
            source: source.clone(),
            logical_job: job.clone(),
            key_less_than: *key_less_than,
            allowance_ms: *allowance_ms,
            projection: projection.clone(),
            state,
            timers: native_timers,
            delivery,
            semantic_digest: decision.semantic_digest.clone(),
            native_job_id,
        })
    }
}

/// Remove the opt-in flag before legacy CLI parsing, then strictly compile it.
/// # Errors
/// Rejects duplicate flags, symlinks, special/oversized files and invalid plans.
pub fn take_args(args: &mut Vec<String>) -> Result<Option<Admission>> {
    let mut found = None;
    while let Some(i) = args.iter().position(|s| s == "--logical-plan") {
        require(found.is_none(), "duplicate logical plan")?;
        let path = args
            .get(i + 1)
            .ok_or(Error::Admission("logical plan path"))?;
        let fd = rustix::fs::open(
            path.as_str(),
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK,
            rustix::fs::Mode::empty(),
        )
        .map_err(|_| Error::Admission("logical plan open"))?;
        let f = std::fs::File::from(fd);
        require(
            f.metadata()
                .map_err(|_| Error::Admission("logical metadata"))?
                .is_file(),
            "logical regular file",
        )?;
        let mut bytes = Vec::new();
        f.take(ade_logical::MAX_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::Admission("logical read"))?;
        found = Some(Admission::compile(&bytes)?);
        args.drain(i..i + 2);
    }
    let disables = args
        .iter()
        .filter(|s| s.as_str() == "--disable-optimizer")
        .count();
    require(
        disables <= 1 && (disables == 0 || found.is_some()),
        "disable requires one logical plan",
    )?;
    if disables == 1 {
        args.retain(|s| s != "--disable-optimizer");
        if let Some(a) = &mut found {
            a.decision = Admission::optimize(&a.plan, false)?;
        }
    }
    Ok(found)
}
