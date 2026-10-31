//! Semantic admission before native root/source effects; no alternate stream kernel.
use crate::{Error, Result, StreamConfigV1, require};
use ade_logical::{Choice, Computation, Controls, Decision, Kafka, KafkaMode, LogicalPlanV1, Sink};
use std::io::Read;

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
}
impl Admission {
    #[must_use]
    pub fn decision(&self) -> &Decision {
        &self.decision
    }

    /// # Errors
    /// Compiles strict shared semantics without I/O or native effects.
    pub fn compile(bytes: &[u8]) -> Result<Self> {
        let plan =
            LogicalPlanV1::from_json(bytes).map_err(|_| Error::Admission("strict logical plan"))?;
        let decision = ade_logical::optimize(
            &plan,
            &Controls {
                static_choice: Choice::NativeKafka,
                ..Controls::default()
            },
        )
        .map_err(|_| Error::Admission("no legal streaming target"))?;
        require(
            decision.chosen == Choice::NativeKafka,
            "stream semantic plan required",
        )?;
        Ok(Self { plan, decision })
    }
    #[must_use]
    pub fn source(&self) -> &Kafka {
        let Computation::Kafka { source, .. } = &self.plan.computation else {
            unreachable!("compiled Kafka")
        };
        source
    }
    /// # Errors
    /// The complete native transform/source must match the common semantic plan.
    pub fn validate_source(&self, c: &StreamConfigV1) -> Result<()> {
        c.validate()?;
        let Computation::Kafka {
            source,
            key_less_than,
            allowance_ms,
            timers,
            ..
        } = &self.plan.computation
        else {
            return Err(Error::Admission("Kafka plan"));
        };
        require(
            c.expected_cluster_id == source.cluster
                && c.topic == source.topic
                && c.key_less_than == *key_less_than
                && c.allowance_ms == *allowance_ms
                && serde_json::to_value(&c.timers).ok() == serde_json::to_value(timers).ok(),
            "logical/native source/transform/timers mismatch",
        )?;
        require(
            c.starts.iter().map(|s| (s.partition, s.offset)).eq(source
                .original_starts
                .iter()
                .map(|s| (s.partition, s.offset))),
            "logical original starts mismatch",
        )
    }
    /// # Errors
    /// Stateful identity, source/window and delivery must match before root effects.
    #[cfg(feature = "stateful")]
    pub fn validate_state(&self, c: &crate::state::Config, committed: bool) -> Result<()> {
        self.validate_source(&c.source)?;
        let Computation::Kafka {
            source,
            job,
            window_ms,
            late_policy,
            ..
        } = &self.plan.computation
        else {
            return Err(Error::Admission("Kafka state plan"));
        };
        require(
            c.topic_partitions == source.topic_partitions
                && *window_ms == Some(c.window_ms)
                && late_policy.as_deref() == Some(c.late_policy.as_str()),
            "logical/native state mismatch",
        )?;
        require(
            self.plan.sink
                == if committed {
                    Sink::CommittedLocalV3
                } else {
                    Sink::Receipts
                },
            "logical/native delivery mismatch",
        )?;
        let expected = if committed {
            job.parse()
                .map_err(|_| Error::Admission("logical native job UUID"))?
        } else {
            uuid::Uuid::from_bytes(
                self.plan
                    .bound_job_bytes()
                    .map_err(|_| Error::Admission("bound semantic identity"))?,
            )
        };
        require(c.job_id == expected, "logical job/cut identity mismatch")
    }
    /// # Errors
    /// Refuses state or committed sinks through the stateless entrypoint.
    pub fn validate_stateless(&self, c: &StreamConfigV1) -> Result<()> {
        self.validate_source(c)?;
        require(
            !self.decision.properties.stateful && self.plan.sink == Sink::Receipts,
            "stateless capability mismatch",
        )
    }
    #[must_use]
    pub fn record(&self, partition: i32, offset: i64) -> serde_json::Value {
        let phase = match &self.source().mode {
            KafkaMode::HistoryThenLive { cut }
                if cut
                    .iter()
                    .any(|c| c.partition == partition && offset < c.offset) =>
            {
                "historical"
            }
            _ => "live",
        };
        serde_json::json!({"semantic_digest":self.decision.semantic_digest,"partition":partition,"offset":offset,"phase":phase})
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
            a.decision = ade_logical::optimize(
                &a.plan,
                &Controls {
                    enabled: false,
                    static_choice: Choice::NativeKafka,
                    ..Controls::default()
                },
            )
            .map_err(|_| Error::Admission("static streaming target"))?;
        }
    }
    Ok(found)
}
