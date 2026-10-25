//! Offline reconstruction of source and policy facts.
use super::{
    Action, Error, Metrics, Observation, Policy, Result, WorkerId, WorkerState, decide, digest,
    require,
};
/// Reconstruct frozen source/registry/config identity and every pure grant decision.
/// This verifies diagnostics; the independent query oracle remains separate.
/// # Errors
/// Rejects forged identity, coverage, ordering, participation, scores or bounds.
#[allow(
    clippy::too_many_lines,
    reason = "Offline reconstruction keeps all source and decision evidence checks together."
)]
pub fn verify_metrics(metrics: &Metrics) -> Result<()> {
    let task = metrics
        .task
        .as_ref()
        .ok_or_else(|| Error::Invalid("schedule task identity missing".into()))?;
    let workers = metrics.registry.len();
    crate::coordinator::validate_topology(&metrics.registry)?;
    task.validate()?;
    metrics.config.validate(workers)?;
    require(
        task.scheduler == metrics.config
            && metrics.config.leased()
            && metrics.duration_ms <= 120_000
            && metrics.duration_ms > 0,
        "schedule effective config/duration mismatch",
    )?;
    let expected = digest(
        metrics.fence.job,
        &metrics.fence.execution,
        &metrics.registry,
        task,
        &metrics.source,
    )?;
    require(
        metrics.fence.epoch == 1 && metrics.fence.digest == expected,
        "schedule digest mismatch",
    )?;
    metrics.source.validate(
        &metrics
            .source
            .morsels
            .iter()
            .map(|s| s.path.clone())
            .collect::<Vec<_>>(),
    )?;
    let source_bytes = metrics.source.morsels.iter().try_fold(0u64, |total, m| {
        total
            .checked_add(m.bytes)
            .ok_or_else(|| Error::Invalid("source byte overflow".into()))
    })?;
    require(
        source_bytes <= metrics.config.max_source_bytes,
        "source bytes exceed admitted bound",
    )?;
    require(
        !metrics.source.replayable
            && metrics.entries.len() == metrics.source.morsels.len()
            && metrics.entries.len() <= metrics.config.max_morsels
            && metrics.workers == vec![WorkerState::Drained; workers],
        "schedule source/closed worker coverage",
    )?;
    require(
        metrics.control_messages <= metrics.config.max_control_messages
            && metrics.peak_outstanding <= workers
            && metrics.decisions.len() <= metrics.config.audit_capacity
            && metrics.decisions.len() as u64 + metrics.dropped_decisions
                == metrics.entries.len() as u64,
        "schedule metadata/control bounds",
    )?;
    let mut grants = metrics.entries.iter().collect::<Vec<_>>();
    grants.sort_by_key(|e| e.granted_sequence);
    let mut used = std::collections::BTreeSet::new();
    let mut reconstructed = Vec::new();
    for (ordinal, entry) in grants.iter().enumerate() {
        let lease = entry
            .lease
            .as_ref()
            .ok_or_else(|| Error::Invalid("missing source lease".into()))?;
        let completion = entry
            .completion
            .as_ref()
            .ok_or_else(|| Error::Invalid("missing source completion".into()))?;
        let id = usize::try_from(entry.morsel.id.0)
            .map_err(|_| Error::Invalid("morsel index overflow".into()))?;
        let worker = usize::try_from(lease.worker.0)
            .map_err(|_| Error::Invalid("worker index overflow".into()))?;
        require(
            metrics.source.morsels.get(id) == Some(&entry.morsel)
                && lease.morsel == entry.morsel
                && completion.lease == *lease
                && worker < workers
                && entry.preferred == WorkerId((id % workers) as u64)
                && lease.nonce == ordinal as u64 + 1
                && used.insert(entry.granted_sequence),
            "lease/source/nonce/order mismatch",
        )?;
        require(
            entry.completed_sequence > entry.granted_sequence
                && used.insert(entry.completed_sequence)
                && entry.completed_sequence <= metrics.control_messages
                && entry.granted_ms <= entry.completed_ms
                && entry.completed_ms < metrics.duration_ms
                && completion.output_rows <= completion.input_rows
                && completion.input_rows <= metrics.config.max_input_rows
                && entry.last_renewed_ms >= entry.granted_ms
                && entry.last_renewed_ms <= entry.completed_ms
                && entry.completed_ms < entry.last_renewed_ms + metrics.config.lease_ms,
            "lease terminal/counter/time mismatch",
        )?;
        require(
            !metrics.entries.iter().any(|other| {
                other.morsel.id != entry.morsel.id
                    && other
                        .lease
                        .as_ref()
                        .is_some_and(|l| l.worker == lease.worker)
                    && other.granted_sequence < entry.granted_sequence
                    && other.completed_sequence > entry.granted_sequence
            }),
            "overlapping producer leases",
        )?;
        let mut active =
            metrics.config.initial_active == 0 || worker < metrics.config.initial_active;
        for event in &metrics.events {
            if event.worker == lease.worker && event.sequence < entry.granted_sequence {
                active = matches!(event.action, Action::Activate);
            }
        }
        require(active, "lease granted to parked/draining worker")?;
        let mut queued = vec![0u64; workers];
        let mut pending = vec![];
        for candidate in &metrics.entries {
            if candidate.granted_sequence >= entry.granted_sequence {
                let preferred = usize::try_from(candidate.preferred.0)
                    .map_err(|_| Error::Invalid("preferred worker overflow".into()))?;
                require(preferred < workers, "preferred worker out of bounds")?;
                let bytes = candidate.morsel.bytes.max(1);
                queued[preferred] = queued[preferred]
                    .checked_add(bytes)
                    .ok_or_else(|| Error::Invalid("queue overflow".into()))?;
                let index = usize::try_from(candidate.morsel.id.0)
                    .map_err(|_| Error::Invalid("candidate index overflow".into()))?;
                let local = metrics
                    .config
                    .locality
                    .get(worker)
                    .zip(metrics.config.preferred_locality.get(index))
                    .is_some_and(|(a, b)| !a.is_empty() && a == b);
                pending.push((candidate.morsel.id, preferred, bytes, local));
            }
        }
        let decision = decide(&Observation {
            pending,
            queued,
            capacities: if metrics.config.capacities.is_empty() {
                vec![1; workers]
            } else {
                metrics.config.capacities.clone()
            },
            requester: worker,
            critical: entry.granted_ms > metrics.duration_ms * 3 / 4,
            steal: metrics.config.policy == Policy::LeasedV1,
            min_steal_bytes: metrics.config.min_steal_bytes,
        })?
        .ok_or_else(|| Error::Invalid("grant has no policy decision".into()))?;
        require(
            decision.morsel == entry.morsel.id,
            "grant differs from pure policy",
        )?;
        reconstructed.push(decision);
    }
    require(
        reconstructed
            .into_iter()
            .skip(
                usize::try_from(metrics.dropped_decisions)
                    .map_err(|_| Error::Invalid("audit count overflow".into()))?,
            )
            .eq(metrics.decisions.iter().cloned()),
        "decision audit mismatch",
    )?;
    require(
        metrics.events.len() == metrics.config.participation.len(),
        "participation event coverage",
    )?;
    for (event, planned) in metrics.events.iter().zip(&metrics.config.participation) {
        require(
            event.worker == planned.worker
                && event.action == planned.action
                && event.completed == planned.after_completed
                && event.worker.0 < workers as u64
                && metrics
                    .entries
                    .iter()
                    .filter(|e| e.completed_sequence <= event.sequence)
                    .count()
                    == event.completed,
            "participation provenance mismatch",
        )?;
        if !matches!(event.action, Action::Activate) {
            let drained = event
                .drained_ms
                .ok_or_else(|| Error::Invalid("drain never closed terminals".into()))?;
            require(
                drained >= event.at_ms && drained < metrics.duration_ms,
                "drain time mismatch",
            )?;
            if let Action::SpotNotice { grace_ms } = event.action {
                require(drained < event.at_ms + grace_ms, "spot deadline mismatch")?;
            }
        }
    }
    require(
        metrics.renewals == metrics.entries.iter().map(|e| e.renewals).sum::<u64>(),
        "renewal observation mismatch",
    )?;
    Ok(())
}
