//! Deterministic constraints-first integer model. No I/O, wall clock, or history queue.
use crate::{Result, state::check};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Placement {
    Local,
    Remote,
    Both,
}
impl Placement {
    #[must_use]
    pub fn local(self) -> bool {
        self != Self::Remote
    }
    #[must_use]
    pub fn remote(self) -> bool {
        self != Self::Local
    }
    #[must_use]
    pub fn copies(self) -> u64 {
        if self == Self::Both { 2 } else { 1 }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    Full,
    Incremental,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Static,
    Adaptive,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prices {
    pub local_write_byte: u64,
    pub local_read_byte: u64,
    pub remote_read_byte: u64,
    pub remote_write_byte: u64,
    pub remote_request: u64,
    pub capacity_byte_ms: u64,
    pub compute_ms: u64,
    pub replay_record: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Constraints {
    pub min_copies: u64,
    pub require_remote: bool,
    pub max_loss_ppm: u64,
    pub local_loss_ppm: u64,
    pub remote_loss_ppm: u64,
    pub both_loss_ppm: u64,
    pub recovery_slo_ms: u64,
    pub failure_ppm: u64,
    pub retention_ms: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Action {
    pub format: Format,
    pub placement: Placement,
    pub records: u64,
    pub ms: u64,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub samples: u64,
    pub logical_bytes: u64,
    pub changed_bytes: u64,
    pub changed_shards: u64,
    pub scan_bytes_per_second: u64,
    pub local_bytes_per_second: u64,
    pub remote_bytes_per_second: u64,
    pub replay_ns_per_record: u64,
    pub depth: u64,
    pub local_available_bytes: u64,
    pub remote_available_bytes: u64,
    pub remote_available_objects: u64,
    pub remaining_requests: u64,
    pub remaining_response_bytes: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Candidate {
    pub action: Action,
    pub score: Option<u64>,
    pub recovery_ms: Option<u64>,
    pub rejection: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Decision {
    pub version: u32,
    pub observation: Observation,
    pub model_inputs: Observation,
    pub input_provenance: String,
    pub chosen: Action,
    pub reason: String,
    pub candidates: Vec<Candidate>,
}
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).ok_or(crate::Error::Overflow("policy add"))
}
fn mul(a: u64, b: u64) -> Result<u64> {
    a.checked_mul(b)
        .ok_or(crate::Error::Overflow("policy multiply"))
}
fn ceil(a: u64, b: u64) -> Result<u64> {
    check(b > 0, "policy divisor")?;
    Ok(a / b + u64::from(!a.is_multiple_of(b)))
}
/// Pure evaluation of one candidate. Scores are configured nano-USD proxies per
/// million records plus configured failure exposure, never billed/observed money.
/// # Errors
/// Checked arithmetic refuses overflows and invalid observations.
#[allow(
    clippy::too_many_lines,
    reason = "Auditable complete integer cost equation"
)]
pub fn evaluate(p: &super::Policy, o: &Observation, action: Action) -> Result<Candidate> {
    let mut result = Candidate {
        action,
        score: None,
        recovery_ms: None,
        rejection: None,
    };
    let c = &p.constraints;
    let loss = match action.placement {
        Placement::Local => c.local_loss_ppm,
        Placement::Remote => c.remote_loss_ppm,
        Placement::Both => c.both_loss_ppm,
    };
    let reject = if action.placement.copies() < c.min_copies
        || c.require_remote && !action.placement.remote()
        || loss > c.max_loss_ppm
    {
        Some("durability_floor")
    } else if !p.placements.contains(&action.placement) {
        Some("placement_disabled")
    } else if action.format == Format::Incremental
        && (o.depth >= p.max_depth || o.changed_shards == 32)
    {
        Some("full_required")
    } else if o.changed_bytes > o.logical_bytes || o.changed_shards > 32 {
        Some("invalid_observation")
    } else {
        None
    };
    if let Some(why) = reject {
        result.rejection = Some(why.into());
        return Ok(result);
    }
    let physical = if action.format == Format::Full {
        o.logical_bytes
    } else {
        o.changed_bytes
    };
    let objects = add(
        if action.format == Format::Full {
            32
        } else {
            o.changed_shards
        },
        4,
    )?;
    let depth = if action.format == Format::Full {
        0
    } else {
        add(o.depth, 1)?
    };
    let requests = add(mul(objects, 3)?, mul(66, add(depth, 1)?)?)?;
    let capacity_reject = action.placement.local() && physical > o.local_available_bytes
        || action.placement.remote()
            && (add(physical, 65_560)? > o.remote_available_bytes
                || objects > o.remote_available_objects
                || requests > o.remaining_requests
                || mul(requests, 1024 * 1024)? > o.remaining_response_bytes);
    if capacity_reject {
        result.rejection = Some("capacity_floor".into());
        return Ok(result);
    }
    let local_ms = if action.placement.local() {
        ceil(mul(physical, 1000)?, o.local_bytes_per_second)?
    } else {
        0
    };
    let remote_ms = if action.placement.remote() {
        ceil(mul(physical, 1000)?, o.remote_bytes_per_second)?
    } else {
        0
    };
    let scan_ms = ceil(mul(o.logical_bytes, 1000)?, o.scan_bytes_per_second)?;
    let restore_ms = ceil(
        mul(o.logical_bytes, 1000)?,
        if action.placement.local() {
            o.local_bytes_per_second
        } else {
            o.remote_bytes_per_second
        },
    )?;
    let replay_ms = ceil(mul(action.records, o.replay_ns_per_record)?, 1_000_000)?;
    let recovery = add(restore_ms, replay_ms)?;
    result.recovery_ms = Some(recovery);
    if recovery > c.recovery_slo_ms {
        result.rejection = Some("recovery_slo".into());
        return Ok(result);
    }
    let prices = &p.prices;
    let mut write = mul(add(scan_ms, add(local_ms, remote_ms)?)?, prices.compute_ms)?;
    if action.placement.local() {
        write = add(write, mul(physical, prices.local_write_byte)?)?;
    }
    if action.placement.remote() {
        // PUT + HEAD + GET per newly written shard; manifest/certificate each3.
        let shards = if action.format == Format::Full {
            32
        } else {
            o.changed_shards
        };
        write = add(
            write,
            add(
                mul(physical, prices.remote_write_byte)?,
                mul(add(mul(shards, 3)?, 6)?, prices.remote_request)?,
            )?,
        )?;
    }
    write = add(
        write,
        mul(
            mul(
                mul(o.logical_bytes, action.placement.copies())?,
                c.retention_ms,
            )?,
            prices.capacity_byte_ms,
        )?,
    )?;
    let amortized = ceil(mul(write, 1_000_000)?, action.records)?;
    let expected = mul(
        c.failure_ppm,
        add(
            mul(action.records.div_ceil(2), prices.replay_record)?,
            mul(restore_ms, prices.compute_ms)?,
        )?,
    )?;
    result.score = Some(add(amortized, expected)?);
    Ok(result)
}
/// # Errors
/// Refuses unsafe controls, all-inadmissible candidate sets and arithmetic overflow.
pub fn decide(
    p: &super::Policy,
    o: &Observation,
    previous: Option<(&Action, u64)>,
    now: u64,
) -> Result<Decision> {
    p.validate()?;
    let calibrated = o.samples >= p.min_samples
        && o.scan_bytes_per_second > 0
        && (!p.placements.iter().any(|x| x.local()) || o.local_bytes_per_second > 0)
        && (!p.placements.iter().any(|x| x.remote()) || o.remote_bytes_per_second > 0)
        && o.replay_ns_per_record > 0;
    let static_action = Action {
        format: Format::Full,
        placement: p.static_placement,
        records: p.min_records,
        ms: p.min_ms,
    };
    // Missing data cannot make remote free: the declared conservative calibration
    // is marked estimated and used only for the safe static control's SLO check.
    let estimate = Observation {
        scan_bytes_per_second: 1000,
        local_bytes_per_second: 1000,
        remote_bytes_per_second: 1000,
        replay_ns_per_record: 1_000_000,
        ..o.clone()
    };
    if !calibrated || p.mode == Mode::Static {
        let candidate = evaluate(p, if calibrated { o } else { &estimate }, static_action)?;
        check(
            candidate.rejection.is_none(),
            "static/fallback violates hard constraints",
        )?;
        return Ok(Decision {
            version: 1,
            observation: o.clone(),
            model_inputs: if calibrated { o.clone() } else { estimate },
            input_provenance: if calibrated {
                "measured throughput/prepare; configured prices/risk; estimated costs/recovery"
            } else {
                "estimated fallback 1000 bytes/s and 1ms/record; configured prices/risk"
            }
            .into(),
            chosen: static_action,
            reason: if p.mode == Mode::Static {
                "static_control"
            } else {
                "missing_samples_estimated_fallback"
            }
            .into(),
            candidates: vec![candidate],
        });
    }
    let mut candidates = Vec::with_capacity(12);
    for placement in [Placement::Local, Placement::Remote, Placement::Both] {
        for format in [Format::Full, Format::Incremental] {
            for (records, ms) in [(p.min_records, p.min_ms), (p.max_records, p.max_ms)] {
                candidates.push(evaluate(
                    p,
                    o,
                    Action {
                        format,
                        placement,
                        records,
                        ms,
                    },
                )?);
            }
        }
    }
    let best = candidates
        .iter()
        .filter(|c| c.rejection.is_none())
        .min_by_key(|c| c.score)
        .ok_or(crate::Error::State("no admissible policy action"))?;
    let mut chosen = best.action;
    let mut reason = "minimum_configured_expected_cost";
    if let Some((old, last)) = previous
        && let Some(candidate) = candidates
            .iter()
            .find(|c| &c.action == old && c.rejection.is_none())
    {
        let old_score = candidate
            .score
            .ok_or(crate::Error::State("candidate score"))?;
        let best_score = best.score.ok_or(crate::Error::State("candidate score"))?;
        if now.saturating_sub(last) < p.cooldown_ms
            || old_score.saturating_sub(best_score) <= p.hysteresis_nano
        {
            chosen = *old;
            reason = "cooldown_or_hysteresis";
        }
    }
    Ok(Decision {
        version: 1,
        observation: o.clone(),
        model_inputs: o.clone(),
        input_provenance:
            "measured throughput/prepare; configured prices/risk; estimated costs/recovery".into(),
        chosen,
        reason: reason.into(),
        candidates,
    })
}

#[derive(Debug, Clone, Serialize)]
pub struct RecoveryDecision {
    pub chosen: String,
    pub logical_bytes: u64,
    pub confirmed_records: u64,
    pub replay_cost_nano: Option<u64>,
    pub restore_cost_nano: u64,
    pub replay_estimated_ms: u64,
    pub restore_estimated_ms: u64,
    pub provenance: String,
}
/// Pure preflight ranking, after complete checkpoint verification. Kafka retains
/// final veto before assignment/output; invalid origin/consent never resets state.
/// # Errors
/// Refuses mismatched consent, SLO failure or arithmetic overflow.
pub fn recovery(
    p: &super::Policy,
    certificate_hash: &str,
    logical_bytes: u64,
    records: u64,
    from: Placement,
) -> Result<RecoveryDecision> {
    let r = &p.recovery;
    if r.allow_replay {
        check(
            r.replay_certificate_blake3.as_deref() == Some(certificate_hash),
            "replay consent is for a different/newer cut",
        )?;
    }
    let read_price = if from == Placement::Local {
        p.prices.local_read_byte
    } else {
        p.prices.remote_read_byte
    };
    let restore_cost = add(
        mul(r.restore_estimated_ms, p.prices.compute_ms)?,
        add(
            mul(logical_bytes, read_price)?,
            if from.remote() {
                mul(66, p.prices.remote_request)?
            } else {
                0
            },
        )?,
    )?;
    let replay = if r.allow_replay
        && r.accept_duplicate_and_conflicting_outputs
        && r.trusted_immutable_history
        && r.replay_estimated_ms <= p.constraints.recovery_slo_ms
    {
        Some(add(
            mul(r.replay_estimated_ms, p.prices.compute_ms)?,
            mul(records, p.prices.replay_record)?,
        )?)
    } else {
        None
    };
    let choose_replay = replay.is_some_and(|cost| cost < restore_cost);
    check(
        choose_replay || r.restore_estimated_ms <= p.constraints.recovery_slo_ms,
        "restore estimated SLO",
    )?;
    Ok(RecoveryDecision{chosen:if choose_replay{"full_replay_explicit_conflict_consent"}else{"checkpoint_restore"}.into(),logical_bytes,confirmed_records:records,replay_cost_nano:replay,restore_cost_nano:restore_cost,replay_estimated_ms:r.replay_estimated_ms,restore_estimated_ms:r.restore_estimated_ms,provenance:"verified selected logical bytes/records; configured read/request/compute prices and recovery latency estimates; single-state-image GET proxy; Kafka live retention veto before assignment".into()})
}
