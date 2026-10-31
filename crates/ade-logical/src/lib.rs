//! Pure semantic admission and bounded native-choice optimization. No I/O.
use ade_physical::{Field, Operator, PlanV1, Schema, Source, Target, Type};
use serde::{Deserialize, Serialize};

pub const MAX_BYTES: usize = 256 * 1024;
pub const MAX_TRACE_BYTES: usize = 16 * 1024;
#[derive(Debug, thiserror::Error)]
#[error("logical v1 admission: {0}")]
pub struct Error(pub String);
pub type Result<T> = std::result::Result<T, Error>;
fn require(ok: bool, why: &str) -> Result<()> {
    if ok { Ok(()) } else { Err(Error(why.into())) }
}
impl From<ade_physical::Error> for Error {
    fn from(e: ade_physical::Error) -> Self {
        Self(e.to_string())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Offset {
    pub partition: i32,
    pub offset: i64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum KafkaMode {
    Assigned,
    HistoryThenLive {
        cut: Vec<Offset>,
    },
    /// Reserved; finite Kafka termination is not implemented by native v1.
    Range {
        end: Vec<Offset>,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Kafka {
    pub cluster: String,
    pub topic: String,
    pub topic_partitions: Vec<i32>,
    pub original_starts: Vec<Offset>,
    /// Operator-pinned incarnation/history token, never claimed broker proof.
    pub immutable_history: String,
    pub mode: KafkaMode,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimerKind {
    Processing,
    Event,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Timer {
    pub kind: TimerKind,
    pub deadline_ms: i64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[allow(
    clippy::large_enum_variant,
    reason = "One bounded plan owns this 288-byte descriptor; no per-record enum collection"
)]
pub enum Computation {
    ParquetManifest {
        sources: Vec<Source>,
        operators: Vec<Operator>,
        max_output_rows: u64,
        max_output_bytes: u64,
    },
    Kafka {
        source: Kafka,
        job: String,
        key_less_than: u64,
        allowance_ms: i64,
        /// Native projection order; arbitrary stream expressions are refused.
        projection: Vec<String>,
        window_ms: Option<i64>,
        late_policy: Option<String>,
        hash_version: u32,
        timers: Vec<Timer>,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sink {
    Collect,
    Receipts,
    CommittedLocalV3,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalPlanV1 {
    pub version: u32,
    pub computation: Computation,
    pub sink: Sink,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Termination {
    Finite,
    NonTerminating,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Properties {
    pub termination: Termination,
    pub schema: Schema,
    pub ordering: Vec<usize>,
    pub deterministic_for_observed_trace: bool,
    pub replay: &'static str,
    pub phase: &'static str,
    pub event_time: &'static str,
    pub frontier: &'static str,
    pub distribution: &'static str,
    pub delivery: &'static str,
    pub stateful: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Choice {
    Local,
    DistributedAggregate,
    DistributedJoin,
    NativeKafka,
}
impl Choice {
    #[must_use]
    pub fn target(self) -> Option<Target> {
        match self {
            Self::Local => Some(Target::Local),
            Self::DistributedAggregate => Some(Target::DistributedAggregate),
            Self::DistributedJoin => Some(Target::DistributedJoin),
            Self::NativeKafka => None,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Controls {
    pub enabled: bool,
    pub static_choice: Choice,
    pub forced: Option<Choice>,
    pub workers: usize,
    pub distributed_available: bool,
}
impl Default for Controls {
    fn default() -> Self {
        Self {
            enabled: true,
            static_choice: Choice::Local,
            forced: None,
            workers: 2,
            distributed_available: false,
        }
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct Candidate {
    pub choice: Choice,
    pub rejection: Option<String>,
    pub estimated_units: Option<u64>,
}
#[derive(Clone, Debug, Serialize)]
pub struct Decision {
    pub version: u32,
    pub semantic_digest: String,
    pub properties: Properties,
    pub candidates: Vec<Candidate>,
    pub chosen: Choice,
    pub controls: Controls,
    pub provenance: &'static str,
    pub source_rows: u64,
}
impl LogicalPlanV1 {
    /// # Errors
    /// Rejects oversized, duplicate/unknown fields and unsupported semantics.
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        require(bytes.len() <= MAX_BYTES, "plan byte cap")?;
        let p: Self =
            serde_json::from_slice(bytes).map_err(|_| Error("strict plan JSON".into()))?;
        p.properties()?;
        Ok(p)
    }
    /// Extract semantic operators, discarding all physical properties and choices.
    /// # Errors
    /// Refuses malformed legacy input before extracting its semantics.
    pub fn from_physical(p: &PlanV1) -> Result<Self> {
        p.validate()?;
        Ok(Self {
            version: 1,
            sink: Sink::Collect,
            computation: Computation::ParquetManifest {
                sources: p.sources.clone(),
                operators: p.nodes.iter().map(|n| n.operator.clone()).collect(),
                max_output_rows: p.max_output_rows,
                max_output_bytes: p.max_output_bytes,
            },
        })
    }
    fn batch(&self, target: Target) -> Result<PlanV1> {
        let Computation::ParquetManifest {
            sources,
            operators,
            max_output_rows,
            max_output_bytes,
        } = &self.computation
        else {
            return Err(Error("Kafka cannot lower to finite PlanV1".into()));
        };
        require(operators.len() <= 64, "node/depth cap")?;
        require(!sources.is_empty() && sources.len() <= 2, "source cap")?;
        for source in sources {
            source.validate()?;
            require(
                source.replayable == (source.table == "input"),
                "source replay capability",
            )?;
        }
        for op in operators {
            if let Operator::Project { columns, .. } = op {
                require(
                    columns.len() <= 16
                        && columns
                            .iter()
                            .all(|c| !c.alias.is_empty() && c.alias.len() <= 128),
                    "projection cap/alias",
                )?;
            }
        }

        let mut p = PlanV1::new(target, sources.clone());
        p.max_output_rows = *max_output_rows;
        p.max_output_bytes = *max_output_bytes;
        for op in operators {
            p.push(op.clone())?;
        }
        p.validate()?;
        Ok(p)
    }
    /// Recompute properties, never deserialize caller-provided derived facts.
    /// # Errors
    /// Refuses invalid graph/schema/source/time/delivery combinations.
    #[allow(
        clippy::too_many_lines,
        reason = "Closed semantic families and their refusal matrix"
    )]
    pub fn properties(&self) -> Result<Properties> {
        require(self.version == 1, "version")?;

        match &self.computation {
            Computation::ParquetManifest { operators, .. } => {
                require(self.sink == Sink::Collect, "finite sink must collect")?;
                let join = operators
                    .iter()
                    .any(|o| matches!(o, Operator::InnerJoin { .. }));
                let p = self.batch(if join {
                    Target::DistributedJoin
                } else {
                    Target::Local
                })?;
                let n = &p.nodes[p.output as usize];
                Ok(Properties {
                    termination: Termination::Finite,
                    schema: n.schema.clone(),
                    ordering: n.properties.ordering.clone(),
                    deterministic_for_observed_trace: true,
                    replay: if n.properties.replayable {
                        "immutable_manifest"
                    } else {
                        "native_join_no_recovery"
                    },
                    phase: "finite",
                    event_time: "ordinary_column",
                    frontier: "finite_eof",
                    distribution: if join {
                        "join_key"
                    } else {
                        "single_or_fixed_hash"
                    },
                    delivery: "validated_finite_result",
                    stateful: false,
                })
            }
            Computation::Kafka {
                source,
                job,
                allowance_ms,
                projection,
                window_ms,
                late_policy,
                hash_version,
                timers,
                ..
            } => {
                require(
                    self.sink != Sink::Collect,
                    "unbounded collect/sort/global limit/join refused",
                )?;
                require(
                    timers.len() <= 64
                        && timers
                            .iter()
                            .all(|t| t.kind != TimerKind::Processing || t.deadline_ms >= 0),
                    "bounded native timers",
                )?;
                require(
                    window_ms.is_none() || timers.is_empty(),
                    "stateful transient timers refused",
                )?;
                require(
                    valid_token(&source.cluster, 128)
                        && valid_token(&source.topic, 128)
                        && valid_token(&source.immutable_history, 256),
                    "source/history token",
                )?;
                require(valid_token(job, 128), "job identity")?;
                require(
                    *allowance_ms >= 0 && *hash_version == 1,
                    "time/hash version",
                )?;
                require(
                    projection
                        .iter()
                        .map(String::as_str)
                        .eq(["value_i64", "key", "event_time_ms"]),
                    "native projection contract",
                )?;
                require(
                    window_ms.is_none_or(|w| w > 0)
                        && late_policy.as_deref() == window_ms.map(|_| "receipt_drop_closed_v1"),
                    "window/late contract",
                )?;
                require(
                    self.sink != Sink::CommittedLocalV3 || window_ms.is_some(),
                    "committed sink requires state",
                )?;
                require(
                    (1..=4).contains(&source.topic_partitions.len())
                        && source
                            .topic_partitions
                            .iter()
                            .copied()
                            .eq(0..i32::try_from(source.topic_partitions.len()).unwrap_or(0)),
                    "topic inventory",
                )?;
                require(
                    (1..=4).contains(&source.original_starts.len()),
                    "assigned metadata cap",
                )?;
                let assigned = source
                    .original_starts
                    .iter()
                    .map(|s| s.partition)
                    .collect::<Vec<_>>();
                require(
                    !assigned.is_empty()
                        && assigned.len() <= 4
                        && assigned.iter().all(|p| source.topic_partitions.contains(p))
                        && assigned.windows(2).all(|pair| pair[0] < pair[1]),
                    "fixed sorted assigned subset",
                )?;
                validate_offsets(&source.original_starts, &assigned)?;
                let phase = match &source.mode {
                    KafkaMode::Assigned => "live",
                    KafkaMode::HistoryThenLive { cut } => {
                        require(
                            self.sink != Sink::CommittedLocalV3,
                            "transactional hybrid refused before effects",
                        )?;
                        validate_offsets(cut, &assigned)?;
                        require(
                            cut.iter()
                                .zip(&source.original_starts)
                                .all(|(c, s)| c.offset >= s.offset),
                            "cut precedes origin",
                        )?;
                        "history_then_live"
                    }
                    KafkaMode::Range { .. } => {
                        return Err(Error("KafkaRange finite termination unsupported".into()));
                    }
                };
                let schema = if window_ms.is_some() {
                    vec![
                        Field::new("key", Type::U64, false),
                        Field::new("window_start", Type::I64, false),
                        Field::new("window_end", Type::I64, false),
                        Field::new("sum_i64", Type::I64, false),
                        Field::new("count_u64", Type::U64, false),
                    ]
                } else {
                    vec![
                        Field::new("value_i64", Type::I64, false),
                        Field::new("key", Type::U64, false),
                        Field::new("event_time_ms", Type::I64, false),
                    ]
                };
                Ok(Properties {
                    termination: Termination::NonTerminating,
                    schema,
                    ordering: vec![],
                    deterministic_for_observed_trace: true,
                    replay: "operator_attested_trace_relative",
                    phase,
                    event_time: "signed_ms_receipts_including_filtered",
                    frontier: "min_confirmed_unknown_until_all_no_idle_advance",
                    distribution: "fixed_assigned_partitions",
                    delivery: if self.sink == Sink::CommittedLocalV3 {
                        "local_committed_prefix_v3"
                    } else {
                        "at_least_once_receipts"
                    },
                    stateful: window_ms.is_some(),
                })
            }
        }
    }
    /// # Errors
    /// Validates before computing the canonical semantic identity.
    pub fn digest(&self) -> Result<String> {
        self.properties()?;
        // Output ceilings are constraints, not the identity of returned rows.
        let mut identity = self.clone();
        if let Computation::ParquetManifest {
            max_output_rows,
            max_output_bytes,
            ..
        } = &mut identity.computation
        {
            *max_output_rows = 1_000_000;
            *max_output_bytes = 64 * 1024 * 1024;
        }
        let bytes = serde_json::to_vec(&identity).map_err(|_| Error("serialization".into()))?;
        Ok(blake3::hash(&bytes).to_hex().to_string())
    }
    /// Hybrid job identity binds all source/window/cut semantics via native `job_id`.
    /// # Errors
    /// Refuses invalid semantic plans.
    pub fn bound_job_bytes(&self) -> Result<[u8; 16]> {
        let digest = self.digest()?;
        let h = blake3::hash(digest.as_bytes());
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&h.as_bytes()[..16]);
        Ok(bytes)
    }
    /// # Errors
    /// Recomputes the decision; a forged or stale serialized trace is not authority.
    pub fn lower_batch(&self, controls: &Controls) -> Result<(PlanV1, Decision)> {
        let d = optimize(self, controls)?;
        Ok((
            self.batch(
                d.chosen
                    .target()
                    .ok_or_else(|| Error("finite target required".into()))?,
            )?,
            d,
        ))
    }
}
fn valid_token(s: &str, max: usize) -> bool {
    !s.is_empty()
        && s.len() <= max
        && s != "."
        && s != ".."
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
}
fn validate_offsets(offsets: &[Offset], parts: &[i32]) -> Result<()> {
    require(
        offsets.len() == parts.len()
            && offsets
                .iter()
                .zip(parts)
                .all(|(o, p)| o.partition == *p && (0..i64::MAX).contains(&o.offset)),
        "exact sorted offset vector",
    )
}
/// Legality precedes ranking. Three finite candidates or one native stream choice.
/// # Errors
/// Refuses unsatisfiable hard controls and checked cost overflow.
pub fn optimize(plan: &LogicalPlanV1, controls: &Controls) -> Result<Decision> {
    let properties = plan.properties()?;
    let rows = match &plan.computation {
        Computation::ParquetManifest { sources, .. } => sources
            .iter()
            .flat_map(|s| &s.files)
            .try_fold(0u64, |a, f| a.checked_add(f.rows))
            .ok_or_else(|| Error("stats overflow".into()))?,
        Computation::Kafka { .. } => 0,
    };
    let choices = if properties.termination == Termination::Finite {
        vec![
            Choice::Local,
            Choice::DistributedAggregate,
            Choice::DistributedJoin,
        ]
    } else {
        vec![Choice::NativeKafka]
    };
    let mut candidates = Vec::with_capacity(choices.len());
    for choice in choices {
        let legality = if let Some(target) = choice.target() {
            plan.batch(target).and_then(|_| {
                require(
                    choice == Choice::Local
                        || (controls.distributed_available && (2..=4).contains(&controls.workers)),
                    "fixed worker capability unavailable",
                )
            })
        } else {
            Ok(())
        };
        let rejection = legality.err().map(|e| e.0).or_else(|| {
            if controls.forced.is_some_and(|f| f != choice) {
                Some("forced target".into())
            } else if !controls.enabled && choice != controls.static_choice {
                Some("optimizer disabled: static target".into())
            } else {
                None
            }
        });
        let cost = if rejection.is_none() {
            Some(
                match choice {
                    Choice::Local => rows.checked_mul(4),
                    Choice::DistributedAggregate => {
                        rows.checked_mul(2).and_then(|v| v.checked_add(20000))
                    }
                    Choice::DistributedJoin => {
                        rows.checked_mul(4).and_then(|v| v.checked_add(20000))
                    }
                    Choice::NativeKafka => Some(0),
                }
                .ok_or_else(|| Error("cost overflow".into()))?,
            )
        } else {
            None
        };
        candidates.push(Candidate {
            choice,
            rejection,
            estimated_units: cost,
        });
    }
    let chosen = candidates
        .iter()
        .filter_map(|c| c.estimated_units.map(|n| (n, c.choice)))
        .min_by_key(|(n, _)| *n)
        .ok_or_else(|| Error("no legal target satisfies controls".into()))?
        .1;
    let d = Decision {
        version: 1,
        semantic_digest: plan.digest()?,
        properties,
        candidates,
        chosen,
        controls: controls.clone(),
        provenance: "manifest_rows_verified_before_results; fixed_proxy_v1_not_latency_or_dollars",
        source_rows: rows,
    };
    require(
        serde_json::to_vec(&d)
            .map_err(|_| Error("decision encoding".into()))?
            .len()
            <= MAX_TRACE_BYTES,
        "trace cap",
    )?;
    Ok(d)
}
