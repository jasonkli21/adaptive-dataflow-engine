use crate::{
    Error, Result,
    contract::{
        Attempt, Grant, Intent, Job, Ledger, Limits, MAX_JOBS, MAX_LAUNCHES, MAX_NATIVE_JOBS, MIB,
        Mode, Outcome, State, Usage, native_key,
    },
    encode, hash, require,
};
use uuid::Uuid;

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct Totals {
    pub queued: usize,
    pub running: usize,
    pub slots: u64,
    pub managed_bytes: u64,
    pub staging_bytes: u64,
    pub retained_bytes: u64,
    pub cumulative_write_bytes: u64,
    pub cost_microunits: u64,
    pub unpriced_slot_ms: u64,
    pub measured_write_bytes: u64,
    pub unknown_write_attempts: u64,
}

fn add(total: &mut u64, amount: u64) -> Result<()> {
    *total = total
        .checked_add(amount)
        .ok_or(Error("ACCOUNTING_OVERFLOW"))?;
    Ok(())
}

impl Totals {
    pub fn fits(&self, limits: &Limits) -> bool {
        self.queued <= limits.queued
            && self.running <= limits.running
            && self.slots <= limits.slots
            && self.managed_bytes <= limits.managed_bytes
            && self.staging_bytes <= limits.staging_bytes
            && self.retained_bytes <= limits.retained_bytes
            && self.cumulative_write_bytes <= limits.cumulative_write_bytes
            && limits
                .cost_microunits
                .is_none_or(|cap| self.cost_microunits <= cap)
    }

    fn reserve(&mut self, grant: &Grant) -> Result<()> {
        self.running += 1;
        add(&mut self.slots, grant.slots)?;
        add(&mut self.managed_bytes, grant.managed_bytes)?;
        add(&mut self.staging_bytes, grant.staging_bytes)?;
        add(&mut self.retained_bytes, grant.staging_bytes)?;
        add(&mut self.cumulative_write_bytes, grant.write_bytes)?;
        add(&mut self.cost_microunits, grant.cost.unwrap_or(0))
    }

    fn settle(&mut self, attempt: &Attempt, usage: &Usage) -> Result<()> {
        if usage.outcome != Outcome::Refused {
            add(&mut self.cumulative_write_bytes, attempt.grant.write_bytes)?;
        }
        add(&mut self.retained_bytes, usage.committed_bytes)?;
        add(&mut self.cost_microunits, usage.charge.unwrap_or(0))?;
        add(&mut self.unpriced_slot_ms, usage.unpriced_slot_ms)?;
        if let Some(bytes) = usage.written_bytes {
            add(&mut self.measured_write_bytes, bytes)?;
        } else {
            add(&mut self.unknown_write_attempts, 1)?;
        }
        Ok(())
    }
}

pub fn totals(ledger: &Ledger, tenant: Option<Uuid>, omit: Option<Uuid>) -> Result<Totals> {
    let mut total = Totals::default();
    for (id, job) in ledger
        .jobs
        .iter()
        .filter(|(_, j)| tenant.is_none_or(|t| j.tenant == t))
    {
        total.queued += usize::from(job.queue_obligation());
        for attempt in &job.attempts {
            if let Some(usage) = &attempt.usage {
                total.settle(attempt, usage)?;
            } else if Some(*id) == omit {
                // Model the victim's cumulative charge at the full grant. It
                // only frees live capacity after native cancellation drains.
                add(&mut total.cumulative_write_bytes, attempt.grant.write_bytes)?;
                add(&mut total.cost_microunits, attempt.grant.cost.unwrap_or(0))?;
            } else {
                total.reserve(&attempt.grant)?;
            }
        }
    }
    Ok(total)
}

pub fn grant(ledger: &Ledger, job: &Job, cohort: usize, at: u64) -> Result<Grant> {
    let slots = u64::try_from(ledger.config.cohorts[cohort].workers.len())
        .map_err(|_| Error("ACCOUNTING_OVERFLOW"))?;
    let domains = slots + 1; // Worker slots and the coordinator.
    // Reserve both aggregate task roles or the join hash/scratch roles,
    // per-peer exchange buffers, and a separate managed coordinator allowance.
    let managed_bytes = domains * (64 + 8 * slots) * MIB;
    let staging_bytes = job.spec.staging_bytes;
    let write_bytes = domains
        .checked_mul(job.spec.runtime_bytes)
        .and_then(|bytes| bytes.checked_add(staging_bytes))
        .ok_or(Error("ACCOUNTING_OVERFLOW"))?;
    let cost = ledger
        .config
        .price
        .as_ref()
        .map(|price| {
            slots
                .checked_mul(job.expires_ms.saturating_sub(at))
                .and_then(|time| time.checked_mul(price.microunits_per_slot_ms))
                .ok_or(Error("ACCOUNTING_OVERFLOW"))
        })
        .transpose()?;
    Ok(Grant {
        slots,
        managed_bytes,
        staging_bytes,
        write_bytes,
        cost,
    })
}

pub fn settle(
    attempt: &Attempt,
    expires_ms: u64,
    end_ms: u64,
    outcome: Outcome,
    written_bytes: Option<u64>,
    output: Option<(u64, u64, String)>,
) -> Result<Usage> {
    let elapsed = end_ms
        .checked_sub(attempt.admitted_ms)
        .ok_or(Error("CLOCK"))?;
    let slot_ms = elapsed
        .checked_mul(attempt.grant.slots)
        .ok_or(Error("ACCOUNTING_OVERFLOW"))?;
    let priced_ms =
        expires_ms.saturating_sub(attempt.admitted_ms).min(elapsed) * attempt.grant.slots;
    let charge = attempt
        .price
        .as_ref()
        .map(|price| {
            priced_ms
                .checked_mul(price.microunits_per_slot_ms)
                .ok_or(Error("ACCOUNTING_OVERFLOW"))
        })
        .transpose()?;
    require(
        charge
            .zip(attempt.grant.cost)
            .is_none_or(|(actual, cap)| actual <= cap),
        "ACCOUNTING_OVERFLOW",
    )?;
    let (committed_bytes, ipc_bytes, ipc_hash) = output
        .map_or((0, None, None), |(total, bytes, digest)| {
            (total, Some(bytes), Some(digest))
        });
    Ok(Usage {
        end_ms,
        outcome,
        slot_ms,
        charge,
        unpriced_slot_ms: if charge.is_some() {
            slot_ms - priced_ms
        } else {
            slot_ms
        },
        written_bytes,
        committed_bytes,
        ipc_bytes,
        ipc_hash,
    })
}

pub fn can_ever_fit(ledger: &Ledger, job: &Job, now: u64) -> Result<bool> {
    let choices = if ledger.config.mode == Mode::Fifo {
        1
    } else {
        2
    };
    for cohort in 0..choices {
        if native_history(ledger, cohort) >= MAX_NATIVE_JOBS {
            continue;
        }
        let mut trial = Totals::default();
        trial.reserve(&grant(ledger, job, cohort, now)?)?;
        if trial.fits(&ledger.config.limits) && trial.fits(&ledger.tenants[&job.tenant].limits) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn native_history(ledger: &Ledger, cohort: usize) -> usize {
    ledger
        .jobs
        .values()
        .flat_map(|j| j.attempts.iter())
        .filter(|attempt| {
            attempt.cohort == cohort
                && attempt
                    .usage
                    .as_ref()
                    .is_none_or(|usage| usage.outcome != Outcome::Refused)
        })
        .count()
}

pub fn history_exhausted(ledger: &Ledger) -> bool {
    let choices = if ledger.config.mode == Mode::Fifo {
        1
    } else {
        2
    };
    (0..choices).all(|cohort| native_history(ledger, cohort) >= MAX_NATIVE_JOBS)
}

fn eligible(
    ledger: &Ledger,
    job: &Job,
    cohort: usize,
    now: u64,
    omit: Option<Uuid>,
) -> Result<bool> {
    if job.state != State::Queued
        || now >= job.expires_ms
        || ledger.tenants[&job.tenant].fingerprint.is_none()
        || ledger
            .jobs
            .values()
            .flat_map(|j| j.attempts.iter())
            .filter(|a| {
                a.cohort == cohort
                    && a.usage
                        .as_ref()
                        .is_none_or(|u| u.outcome != Outcome::Refused)
            })
            .count()
            >= MAX_NATIVE_JOBS
    {
        return Ok(false);
    }
    let grant = grant(ledger, job, cohort, now)?;
    for (tenant, limit) in [
        (None, &ledger.config.limits),
        (Some(job.tenant), &ledger.tenants[&job.tenant].limits),
    ] {
        let mut total = totals(ledger, tenant, omit)?;
        if !job.spec.preemptible || job.attempts.len() + 1 >= MAX_LAUNCHES {
            total.queued -= 1;
        }
        total.reserve(&grant)?;
        if !total.fits(limit) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn rank(ledger: &Ledger, job: &Job, now: u64) -> u8 {
    // Promote before the original deadline whenever service is still
    // possible. A job whose deadline is <= the configured interval otherwise
    // expires before the starvation guard can ever take effect.
    let age = ledger
        .config
        .aging_ms
        .min(job.spec.deadline_ms.saturating_sub(1));
    if now.saturating_sub(job.queued_ms) >= age {
        2
    } else {
        job.spec.priority
    }
}

fn best(ledger: &Ledger, cohort: usize, now: u64, omit: Option<Uuid>) -> Result<Option<Uuid>> {
    let mut best: Option<(Uuid, &Job)> = None;
    for (&id, job) in &ledger.jobs {
        if !eligible(ledger, job, cohort, now, omit)? {
            continue;
        }
        let preferred = best.is_none_or(|(_, old)| {
            if ledger.config.mode == Mode::Fifo {
                return job.sequence < old.sequence;
            }
            let a = &ledger.tenants[&job.tenant];
            let b = &ledger.tenants[&old.tenant];
            rank(ledger, job, now)
                .cmp(&rank(ledger, old, now))
                .then_with(|| {
                    (u128::from(b.granted_work) * u128::from(a.weight))
                        .cmp(&(u128::from(a.granted_work) * u128::from(b.weight)))
                })
                .then_with(|| old.sequence.cmp(&job.sequence))
                .is_gt()
        });
        if preferred {
            best = Some((id, job));
        }
    }
    Ok(best.map(|(id, _)| id))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Expire(Uuid),
    HistoryFull(Uuid),
    Dispatch(Uuid, usize),
    Activate,
    IdleSince(Option<u64>),
    Deactivate,
    Preempt(Uuid, Uuid),
}

pub fn decide(ledger: &Ledger, now: u64) -> Result<Option<Decision>> {
    for (&id, job) in &ledger.jobs {
        if job.state == State::Queued && history_exhausted_for(ledger) {
            return Ok(Some(Decision::HistoryFull(id)));
        }
    }
    if let Some((&id, _)) = ledger
        .jobs
        .iter()
        .find(|(_, j)| j.state == State::Queued && now >= j.expires_ms)
    {
        return Ok(Some(Decision::Expire(id)));
    }
    let mut busy = [None; 2];
    for (&id, job) in &ledger.jobs {
        if job.state.holds_cohort() {
            busy[job.attempts.last().ok_or(Error("CORRUPT_LEDGER"))?.cohort] = Some(id);
        }
    }
    for (cohort, occupant) in busy.into_iter().enumerate() {
        if occupant.is_none()
            && (cohort == 0 || ledger.standby_on)
            && let Some(id) = best(ledger, cohort, now, None)?
        {
            return Ok(Some(Decision::Dispatch(id, cohort)));
        }
    }
    if ledger.config.mode == Mode::Fifo {
        return Ok(None);
    }
    if !ledger.standby_on && best(ledger, 1, now, None)?.is_some() {
        return Ok(Some(Decision::Activate));
    }
    if ledger.standby_on {
        match (busy[1], ledger.standby_idle_since_ms) {
            (Some(_), Some(_)) => return Ok(Some(Decision::IdleSince(None))),
            (None, None) => return Ok(Some(Decision::IdleSince(Some(now)))),
            (None, Some(since)) if now - since >= ledger.config.standby_idle_ms => {
                return Ok(Some(Decision::Deactivate));
            }
            _ => {}
        }
    }
    for (cohort, victim) in busy
        .into_iter()
        .enumerate()
        .filter_map(|(c, v)| v.map(|id| (c, id)))
    {
        let job = &ledger.jobs[&victim];
        if job.state != State::Running
            || job.intent.is_some()
            || !job.spec.preemptible
            || job.attempts.len() >= MAX_LAUNCHES
            || now >= job.expires_ms
        {
            continue;
        }
        let attempt = job.attempts.last().ok_or(Error("CORRUPT_LEDGER"))?;
        if attempt
            .running_ms
            .is_none_or(|at| now.saturating_sub(at) < ledger.config.preempt_after_ms)
            || !eligible_for_relaunch(ledger, job, cohort, now)?
        {
            continue;
        }
        if let Some(winner) = best(ledger, cohort, now, Some(victim))?
            && rank(ledger, &ledger.jobs[&winner], now) > rank(ledger, job, now)
        {
            return Ok(Some(Decision::Preempt(victim, winner)));
        }
    }
    Ok(None)
}

fn history_exhausted_for(ledger: &Ledger) -> bool {
    let choices = if ledger.config.mode == Mode::Fifo {
        1
    } else {
        2
    };
    (0..choices).all(|cohort| native_history(ledger, cohort) >= MAX_NATIVE_JOBS)
}

fn eligible_for_relaunch(ledger: &Ledger, job: &Job, cohort: usize, now: u64) -> Result<bool> {
    let fresh = grant(ledger, job, cohort, now)?;
    for (principal, limits) in [
        (None, &ledger.config.limits),
        (Some(job.tenant), &ledger.tenants[&job.tenant].limits),
    ] {
        let total = totals(ledger, principal, None)?;
        if total
            .cumulative_write_bytes
            .checked_add(fresh.write_bytes)
            .is_none_or(|sum| sum > limits.cumulative_write_bytes)
            || limits.cost_microunits.is_some_and(|cap| {
                total
                    .cost_microunits
                    .checked_add(fresh.cost.unwrap_or(0))
                    .is_none_or(|sum| sum > cap)
            })
        {
            return Ok(false);
        }
    }
    Ok(true)
}

// Keep receipt construction and the matching state transition in one policy
// transaction so the durable event cannot get ahead of its state change.
#[allow(clippy::too_many_lines)]
pub fn apply(ledger: &mut Ledger, decision: Decision, now: u64) -> Result<()> {
    use crate::contract::{DecisionReceipt, MAX_DECISIONS};
    if !matches!(decision, Decision::IdleSince(_)) {
        require(
            ledger.decisions.len() < MAX_DECISIONS,
            "DECISION_HISTORY_FULL",
        )?;
    }
    let (action, reason, job, related_job, cohort, rank_value) = match decision {
        Decision::Expire(id) => ("expire", "deadline_expired", Some(id), None, None, None),
        Decision::HistoryFull(id) => (
            "refuse",
            "native_history_exhausted",
            Some(id),
            None,
            None,
            None,
        ),
        Decision::Dispatch(id, cohort) => (
            "dispatch",
            "eligible_and_reserved",
            Some(id),
            None,
            Some(cohort),
            Some(rank(ledger, &ledger.jobs[&id], now)),
        ),
        Decision::Activate => (
            "activate",
            "eligible_demand_requires_standby",
            None,
            None,
            Some(1),
            None,
        ),
        Decision::IdleSince(_) => ("standby_idle", "idle_cooldown", None, None, Some(1), None),
        Decision::Deactivate => (
            "deactivate",
            "standby_idle_cooldown_elapsed",
            None,
            None,
            Some(1),
            None,
        ),
        Decision::Preempt(victim, demand) => (
            "preempt",
            "higher_rank_demand",
            Some(victim),
            Some(demand),
            ledger.jobs[&victim].attempts.last().map(|a| a.cohort),
            Some(rank(ledger, &ledger.jobs[&demand], now)),
        ),
    };
    let config_digest = hash(&encode(&ledger.config)?);
    let sequence = ledger
        .decisions
        .last()
        .map_or(Some(1), |receipt| receipt.sequence.checked_add(1))
        .ok_or(Error("DECISION_HISTORY_FULL"))?;
    let queue_depth = ledger
        .jobs
        .values()
        .filter(|job| job.state == State::Queued)
        .count();
    let active_cohorts = ledger
        .jobs
        .values()
        .filter(|job| job.state.holds_cohort())
        .count();
    let selected = job.and_then(|id| ledger.jobs.get(&id));
    let tenant = selected.and_then(|selected| ledger.tenants.get(&selected.tenant));
    let receipt = DecisionReceipt {
        version: 1,
        sequence,
        at_ms: now,
        policy: match ledger.config.mode {
            Mode::Fifo => "fifo-v1",
            Mode::Adaptive => "adaptive-v1",
        }
        .into(),
        config_digest,
        action: action.into(),
        reason: reason.into(),
        job,
        related_job,
        cohort,
        queue_depth,
        rank: rank_value,
        requested_work: selected.map(|job| job.spec.work),
        tenant_granted_work: tenant.map(|tenant| tenant.granted_work),
        tenant_weight: tenant.map(|tenant| tenant.weight),
        active_cohorts,
    };
    match decision {
        Decision::Expire(id) => {
            let job = ledger.jobs.get_mut(&id).ok_or(Error("CORRUPT_LEDGER"))?;
            job.state = State::Expired;
            job.reason = "DEADLINE_EXPIRED".into();
        }
        Decision::HistoryFull(id) => {
            let job = ledger.jobs.get_mut(&id).ok_or(Error("CORRUPT_LEDGER"))?;
            job.state = State::Failed;
            job.reason = "NATIVE_HISTORY_FULL".into();
        }
        Decision::Dispatch(id, cohort) => {
            let job = &ledger.jobs[&id];
            let grant = grant(ledger, job, cohort, now)?;
            let priority = rank(ledger, job, now);
            let price = ledger.config.price.clone();
            let price_hash = price
                .as_ref()
                .map(|p| encode(p).map(|b| hash(&b)))
                .transpose()?;
            let tenant = ledger
                .tenants
                .get_mut(&job.tenant)
                .ok_or(Error("CORRUPT_LEDGER"))?;
            let before = tenant.granted_work;
            add(&mut tenant.granted_work, job.spec.work)?;
            let job = ledger.jobs.get_mut(&id).ok_or(Error("CORRUPT_LEDGER"))?;
            job.state = State::Reserved;
            job.reason = "ADMITTED".into();
            job.attempts.push(Attempt {
                cohort,
                native_key: native_key(id, job.attempts.len() + 1),
                native_id: None,
                instance: None,
                engine_job: None,
                owner: None,
                admitted_ms: now,
                running_ms: None,
                grant,
                price,
                price_hash,
                priority,
                tenant_work_before: before,
                usage: None,
            });
        }
        Decision::Preempt(victim, demand) => {
            let job = ledger
                .jobs
                .get_mut(&victim)
                .ok_or(Error("CORRUPT_LEDGER"))?;
            job.intent = Some(Intent::Preempt);
            job.reason = format!("PREEMPT_FOR_{demand}");
        }
        Decision::IdleSince(since) => ledger.standby_idle_since_ms = since,
        Decision::Activate | Decision::Deactivate => {
            ledger.standby_on = decision == Decision::Activate;
            ledger.standby_changed_ms = now;
            ledger.standby_idle_since_ms = None;
            require(
                ledger.standby_transitions.len() < 2 * MAX_JOBS * MAX_LAUNCHES,
                "HISTORY_FULL",
            )?;
            ledger.standby_transitions.push((now, ledger.standby_on));
        }
    }
    if !matches!(decision, Decision::IdleSince(_)) {
        ledger.decisions.push(receipt);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::{Cohort, Config, Directory, Price, Spec, Tenant};
    use ade_ops::Worker;
    use std::{collections::BTreeMap, path::PathBuf};

    fn id(v: u128) -> Uuid {
        Uuid::from_u128(v)
    }

    fn limits() -> Limits {
        Limits {
            queued: 32,
            running: 2,
            slots: 4,
            managed_bytes: 1024 * MIB,
            staging_bytes: 1024 * 1024,
            retained_bytes: 8 * MIB,
            cumulative_write_bytes: 128 * MIB,
            cost_microunits: Some(1_000_000),
        }
    }

    fn model() -> (tempfile::TempDir, Ledger) {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().canonicalize().unwrap();
        let cohorts = std::array::from_fn(|i| Cohort {
            root: path.join(format!("ops-{i}")),
            store: id(100 + i as u128),
            workers: (0..2)
                .map(|j| Worker {
                    id: j,
                    control: format!("127.0.0.1:{}", 10000 + 10 * i as u64 + j * 2)
                        .parse()
                        .unwrap(),
                    exchange: format!("127.0.0.1:{}", 10001 + 10 * i as u64 + j * 2)
                        .parse()
                        .unwrap(),
                    instance: id(1000 + i as u128 * 2 + u128::from(j)),
                    scratch_root: PathBuf::from(format!("/test/{i}/{j}")),
                })
                .collect(),
        });
        let tenants = (1..=2)
            .map(|j| {
                (
                    id(j),
                    Tenant {
                        source: Directory::capture(path.clone()).unwrap(),
                        weight: 1,
                        limits: limits(),
                        fingerprint: Some("f".repeat(64)),
                        granted_work: 0,
                    },
                )
            })
            .collect();
        let ledger = Ledger {
            generation: 1,
            next_sequence: 0,
            clock_ms: 0,
            admin: "a".repeat(64),
            config: Config {
                version: 1,
                mode: Mode::Adaptive,
                aging_ms: 60,
                preempt_after_ms: 20,
                standby_idle_ms: 50,
                limits: limits(),
                cohorts,
                price: Some(Price {
                    version: 1,
                    currency: "USD".into(),
                    microunits_per_slot_ms: 2,
                }),
            },
            tenants,
            jobs: BTreeMap::new(),
            standby_on: false,
            standby_changed_ms: 0,
            standby_idle_since_ms: None,
            standby_transitions: vec![],
            decisions: vec![],
        };
        (root, ledger)
    }

    fn queue(ledger: &mut Ledger, tenant: u128, priority: u8, work: u64) -> Uuid {
        ledger.next_sequence += 1;
        let sequence = ledger.next_sequence;
        let id = id(100 + u128::from(sequence));
        let spec = Spec {
            version: 1,
            plan: ade_physical::PlanV1::new(ade_physical::Target::DistributedAggregate, vec![]),
            inputs: vec![],
            deadline_ms: 200,
            priority,
            work,
            preemptible: true,
            runtime_bytes: 100,
            staging_bytes: 200,
        };
        ledger.jobs.insert(
            id,
            Job {
                tenant: Uuid::from_u128(tenant),
                key: format!("key-{sequence}"),
                digest: String::new(),
                spec,
                source_pins: vec![],
                sequence,
                queued_ms: 0,
                expires_ms: 200,
                state: State::Queued,
                reason: String::new(),
                intent: None,
                attempts: vec![],
            },
        );
        id
    }

    fn finish(ledger: &mut Ledger, job: Uuid, at: u64) {
        let job = ledger.jobs.get_mut(&job).unwrap();
        let attempt = job.attempts.last_mut().unwrap();
        attempt.usage =
            Some(settle(attempt, job.expires_ms, at, Outcome::Failed, None, None).unwrap());
        job.state = State::Failed;
    }

    #[test]
    fn dispatch_sequences_model_weights_declared_work_and_aging() {
        for weight in 1..=4 {
            let (_root, mut ledger) = model();
            ledger.tenants.get_mut(&id(1)).unwrap().weight = weight;
            for _ in 0..8 {
                queue(&mut ledger, 1, 0, 2);
                queue(&mut ledger, 2, 2, 1);
            }
            for at in 60..70 {
                let expected = ledger
                    .jobs
                    .iter()
                    .filter(|(_, job)| job.state == State::Queued)
                    .min_by_key(|(_, job)| {
                        let tenant = &ledger.tenants[&job.tenant];
                        (tenant.granted_work * (weight / tenant.weight), job.sequence)
                    })
                    .unwrap()
                    .0;
                let decision = decide(&ledger, at).unwrap().unwrap();
                assert_eq!(decision, Decision::Dispatch(*expected, 0));
                let selected = *expected;
                apply(&mut ledger, decision, at).unwrap();
                let receipt = ledger.decisions.last().unwrap();
                assert_eq!(receipt.action, "dispatch");
                assert_eq!(receipt.reason, "eligible_and_reserved");
                assert_eq!(receipt.job, Some(selected));
                assert_eq!(receipt.policy, "adaptive-v1");
                assert_eq!(
                    receipt.config_digest,
                    hash(&encode(&ledger.config).unwrap())
                );
                assert_eq!(
                    receipt.requested_work,
                    Some(ledger.jobs[&selected].spec.work)
                );
                finish(&mut ledger, selected, at + 1);
            }
        }
    }

    #[test]
    fn fifo_priority_deadline_and_preemption_boundaries() {
        let (_root, mut ledger) = model();
        ledger.config.limits.running = 1;
        ledger.config.mode = Mode::Fifo;
        let low = queue(&mut ledger, 1, 0, 1);
        let high = queue(&mut ledger, 2, 2, 1);
        assert_eq!(
            decide(&ledger, 0).unwrap(),
            Some(Decision::Dispatch(low, 0))
        );
        apply(&mut ledger, Decision::Dispatch(low, 0), 0).unwrap();
        let job = ledger.jobs.get_mut(&low).unwrap();
        job.state = State::Running;
        job.attempts[0].running_ms = Some(1);
        assert_eq!(decide(&ledger, 21).unwrap(), None);
        ledger.config.mode = Mode::Adaptive;
        assert_eq!(decide(&ledger, 20).unwrap(), None);
        assert_eq!(
            decide(&ledger, 21).unwrap(),
            Some(Decision::Preempt(low, high))
        );
        assert_eq!(decide(&ledger, 60).unwrap(), None);
        assert_eq!(decide(&ledger, 200).unwrap(), Some(Decision::Expire(high)));
    }

    #[test]
    fn aging_promotes_before_short_equal_and_long_deadlines() {
        let (_root, mut ledger) = model();
        ledger.config.aging_ms = 100;
        for deadline in [99, 100, 101] {
            let low = queue(&mut ledger, 1, 0, 1);
            let job = ledger.jobs.get_mut(&low).unwrap();
            job.spec.deadline_ms = deadline;
            job.expires_ms = deadline;
            assert_eq!(rank(&ledger, &ledger.jobs[&low], deadline - 1), 2);
            if deadline > 1 {
                assert_eq!(rank(&ledger, &ledger.jobs[&low], deadline - 2), 0);
            }
            ledger.jobs.remove(&low);
        }
        let mut short = queue(&mut ledger, 1, 0, 1);
        ledger.jobs.get_mut(&short).unwrap().spec.deadline_ms = 1;
        ledger.jobs.get_mut(&short).unwrap().expires_ms = 1;
        assert_eq!(rank(&ledger, &ledger.jobs[&short], 0), 2);
        short = queue(&mut ledger, 1, 0, 1);
        ledger.jobs.get_mut(&short).unwrap().queued_ms = u64::MAX - 30;
        ledger.jobs.get_mut(&short).unwrap().expires_ms = u64::MAX;
        ledger.jobs.get_mut(&short).unwrap().spec.deadline_ms = 20;
        assert_eq!(rank(&ledger, &ledger.jobs[&short], u64::MAX), 2);

        // Under continuous high-priority arrivals, a deadline-eligible aged
        // tenant receives the next grant through weighted work accounting.
        let (_root, mut ledger) = model();
        ledger.config.aging_ms = 120_000;
        let low = queue(&mut ledger, 1, 0, 1);
        for _ in 0..4 {
            let high = queue(&mut ledger, 2, 2, 1);
            ledger.tenants.get_mut(&id(2)).unwrap().granted_work += 1;
            ledger.jobs.get_mut(&high).unwrap().queued_ms = 98;
            ledger.jobs.get_mut(&high).unwrap().expires_ms = 200;
            ledger.jobs.get_mut(&high).unwrap().spec.deadline_ms = 102;
        }
        ledger.jobs.get_mut(&low).unwrap().queued_ms = 0;
        ledger.jobs.get_mut(&low).unwrap().expires_ms = 100;
        ledger.jobs.get_mut(&low).unwrap().spec.deadline_ms = 100;
        assert_eq!(
            decide(&ledger, 99).unwrap(),
            Some(Decision::Dispatch(low, 0))
        );
    }

    #[test]
    fn native_history_exhaustion_is_permanent_and_mode_aware() {
        for mode in [Mode::Fifo, Mode::Adaptive] {
            let (_root, mut ledger) = model();
            ledger.config.mode = mode;
            for _ in 0..MAX_NATIVE_JOBS - 1 {
                let id = queue(&mut ledger, 1, 0, 1);
                apply(&mut ledger, Decision::Dispatch(id, 0), 1).unwrap();
                finish(&mut ledger, id, 2);
            }
            let pending = queue(&mut ledger, 2, 1, 1);
            assert_eq!(native_history(&ledger, 0), MAX_NATIVE_JOBS - 1);
            assert!(!history_exhausted(&ledger));
            assert!(can_ever_fit(&ledger, &ledger.jobs[&pending], 3).unwrap());
            let final_registration = queue(&mut ledger, 1, 0, 1);
            apply(&mut ledger, Decision::Dispatch(final_registration, 0), 3).unwrap();
            finish(&mut ledger, final_registration, 4);
            if mode == Mode::Adaptive {
                assert_eq!(native_history(&ledger, 0), MAX_NATIVE_JOBS);
                assert_eq!(native_history(&ledger, 1), 0);
                assert!(!history_exhausted(&ledger));
                assert!(can_ever_fit(&ledger, &ledger.jobs[&pending], 5).unwrap());
                assert_ne!(
                    decide(&ledger, 3).unwrap(),
                    Some(Decision::HistoryFull(pending))
                );
                for _ in 0..MAX_NATIVE_JOBS {
                    let id = queue(&mut ledger, 2, 1, 1);
                    apply(&mut ledger, Decision::Dispatch(id, 1), 3).unwrap();
                    finish(&mut ledger, id, 4);
                }
            }
            assert!(history_exhausted(&ledger));
            assert_eq!(
                decide(&ledger, 5).unwrap(),
                Some(Decision::HistoryFull(pending))
            );
            apply(&mut ledger, Decision::HistoryFull(pending), 5).unwrap();
            assert_eq!(ledger.jobs[&pending].state, State::Failed);
            assert_eq!(ledger.jobs[&pending].reason, "NATIVE_HISTORY_FULL");
            assert!(ledger.jobs[&pending].attempts.is_empty());
        }
    }

    #[test]
    fn empty_decision_history_keeps_legacy_ledger_encoding() {
        let (_root, ledger) = model();
        let encoded = encode(&ledger).unwrap();
        assert!(
            !String::from_utf8(encoded)
                .unwrap()
                .contains("\"decisions\"")
        );
    }

    #[test]
    fn settle_and_replay_totals_preserve_ceilings_unknowns_and_overflow() {
        let (_root, mut ledger) = model();
        let job = queue(&mut ledger, 1, 0, 1);
        apply(&mut ledger, Decision::Dispatch(job, 0), 10).unwrap();
        let before = totals(&ledger, None, None).unwrap();
        assert_eq!(before.cost_microunits, 760);
        assert_eq!(before.queued, 1);
        finish(&mut ledger, job, 220);
        let after = totals(&ledger, None, None).unwrap();
        assert_eq!(after.cost_microunits, before.cost_microunits);
        assert_eq!(after.unpriced_slot_ms, 40);
        assert_eq!(after.unknown_write_attempts, 1);
        assert_eq!(after.cumulative_write_bytes, before.cumulative_write_bytes);
        assert_eq!(after.running, 0);
        assert_eq!(after.queued, 0);
        let attempt = &ledger.jobs[&job].attempts[0];
        assert!(settle(attempt, 200, u64::MAX, Outcome::Failed, None, None).is_err());
        assert!(settle(attempt, 200, 9, Outcome::Failed, None, None).is_err());
    }
}
