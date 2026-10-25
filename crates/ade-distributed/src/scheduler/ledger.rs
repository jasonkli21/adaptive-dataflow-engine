//! Exclusive source ownership transitions under the coordinator lock.
use super::{
    Action, Config, Entry, Error, Event, Fence, Lease, Metrics, Observation, Op, Policy, Response,
    Result, Rpc, SourceSnapshot, VecDeque, WorkerId, WorkerState, decide, require,
};
pub(super) struct Ledger {
    pub(super) metrics: Metrics,
    ready: Vec<bool>,
    closed: Vec<bool>,
    nonce: u64,
    duration_ms: u64,
    next_event: usize,
    spot_deadlines: Vec<Option<u64>>,
    pub(super) failed: Option<String>,
}
impl Ledger {
    pub(super) fn new(
        fence: Fence,
        source: SourceSnapshot,
        c: Config,
        workers: usize,
        duration_ms: u64,
    ) -> Result<Self> {
        c.validate_for_source(workers, source.morsels.len())?;
        require(
            source.morsels.len() <= c.max_morsels
                && (c.preferred_locality.is_empty()
                    || c.preferred_locality.len() == source.morsels.len()),
            "scheduler source catalog bounds",
        )?;
        require(
            source
                .morsels
                .iter()
                .all(|m| m.fingerprint.bytes().all(|b| b.is_ascii_hexdigit())),
            "invalid source fingerprint encoding",
        )?;
        source.validate(
            &source
                .morsels
                .iter()
                .map(|m| m.path.clone())
                .collect::<Vec<_>>(),
        )?;
        require(
            !source.replayable,
            "leased source must explicitly refuse replay",
        )?;
        let active = if c.initial_active == 0 {
            workers
        } else {
            c.initial_active
        };
        let entries = source
            .morsels
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, m)| Entry {
                morsel: m,
                preferred: WorkerId((i % workers) as u64),
                lease: None,
                completion: None,
                granted_ms: 0,
                completed_ms: 0,
                granted_sequence: 0,
                completed_sequence: 0,
                renewals: 0,
                last_renewed_ms: 0,
                expires_ms: 0,
            })
            .collect();
        Ok(Self {
            metrics: Metrics {
                task: None,
                registry: vec![],
                duration_ms,
                fence,
                config: c,
                source,
                entries,
                decisions: VecDeque::new(),
                dropped_decisions: 0,
                events: vec![],
                workers: (0..workers)
                    .map(|w| {
                        if w < active {
                            WorkerState::Active
                        } else {
                            WorkerState::Parked
                        }
                    })
                    .collect(),
                control_messages: 0,
                control_bytes: 0,
                renewals: 0,
                peak_outstanding: 0,
            },
            ready: vec![false; workers],
            closed: vec![false; workers],
            nonce: 0,
            duration_ms,
            next_event: 0,
            spot_deadlines: vec![None; workers],
            failed: None,
        })
    }
    pub(super) fn check(&self, now: u64) -> Result<()> {
        require(
            self.failed.is_none(),
            self.failed.as_deref().unwrap_or("scheduler aborted"),
        )?;
        require(now < self.duration_ms, "scheduler stage deadline")?;
        require(
            self.metrics
                .entries
                .iter()
                .all(|e| e.lease.is_none() || e.completion.is_some() || now < e.expires_ms),
            "ambiguous expired source lease; abort, never reassign",
        )?;
        require(
            self.spot_deadlines
                .iter()
                .enumerate()
                .all(|(w, d)| d.is_none_or(|d| now < d || self.closed[w])),
            "spot notice expired before producer drain",
        )
    }
    pub(super) fn completed(&self) -> usize {
        self.metrics
            .entries
            .iter()
            .filter(|e| e.completion.is_some())
            .count()
    }
    pub(super) fn outstanding(&self, w: usize) -> bool {
        self.metrics.entries.iter().any(|e| {
            e.lease.as_ref().is_some_and(|l| l.worker.0 == w as u64) && e.completion.is_none()
        })
    }
    pub(super) fn transition(&mut self, w: usize, action: Action, now: u64) -> Result<()> {
        let state = &self.metrics.workers[w];
        match action {
            Action::Activate => {
                require(
                    matches!(state, WorkerState::Parked | WorkerState::Active),
                    "cannot reactivate terminated producer",
                )?;
                self.metrics.workers[w] = WorkerState::Active;
            }
            Action::Drain | Action::SpotNotice { .. } => {
                if *state == WorkerState::Active
                    && self.metrics.entries.iter().any(|e| e.lease.is_none())
                {
                    require(
                        self.metrics
                            .workers
                            .iter()
                            .enumerate()
                            .any(|(i, s)| i != w && *s == WorkerState::Active),
                        "cannot drain last active producer with pending work",
                    )?;
                }
                if *state != WorkerState::Drained {
                    self.metrics.workers[w] = WorkerState::Draining;
                }
                if let Action::SpotNotice { grace_ms } = action
                    && self.spot_deadlines[w].is_none()
                {
                    self.spot_deadlines[w] = Some(
                        now.checked_add(grace_ms)
                            .ok_or_else(|| Error::Invalid("spot deadline overflow".into()))?,
                    );
                }
            }
        }
        self.metrics.events.push(Event {
            at_ms: now,
            sequence: self.metrics.control_messages,
            worker: WorkerId(w as u64),
            action,
            completed: self.completed(),
            drained_ms: None,
            in_flight: self
                .metrics
                .entries
                .iter()
                .find(|e| {
                    e.lease.as_ref().is_some_and(|l| l.worker.0 == w as u64)
                        && e.completion.is_none()
                })
                .map(|e| e.morsel.id),
        });
        Ok(())
    }
    #[allow(
        clippy::too_many_lines,
        reason = "One serialized lease transition validates fencing before any mutation."
    )]
    pub(super) fn handle(&mut self, request: Rpc, now: u64) -> Result<Response> {
        self.check(now)?;
        require(
            request.fence == self.metrics.fence && request.worker.0 < self.ready.len() as u64,
            "stale/foreign scheduler fence or worker",
        )?;
        self.metrics.control_messages += 1;
        require(
            self.metrics.control_messages <= self.metrics.config.max_control_messages,
            "scheduler control message cap",
        )?;
        let worker =
            usize::try_from(request.worker.0).map_err(|_| Error::Invalid("worker index".into()))?;
        match request.op {
            Op::Ready => {
                require(!self.closed[worker], "ready after producer closed")?;
                self.ready[worker] = true;
                Ok(Response::Ack)
            }
            Op::Acquire => {
                require(
                    self.ready[worker] && !self.closed[worker] && !self.outstanding(worker),
                    "acquire before ready/after close/with lease",
                )?;
                if !self.ready.iter().all(|request| *request) {
                    return Ok(Response::Wait);
                }
                if self.metrics.workers[worker] == WorkerState::Draining
                    || self.completed() == self.metrics.entries.len()
                {
                    return Ok(Response::Finish);
                }
                if self.metrics.workers[worker] == WorkerState::Parked {
                    return Ok(if self.has_future_activation(worker) {
                        Response::Wait
                    } else {
                        Response::Finish
                    });
                }
                let mut queued = vec![0u64; self.ready.len()];
                let mut pending = vec![];
                for entry in &self.metrics.entries {
                    if entry.lease.is_none() {
                        let p = usize::try_from(entry.preferred.0)
                            .map_err(|_| Error::Invalid("scheduler index overflow".into()))?;
                        let bytes = entry.morsel.bytes.max(1);
                        queued[p] = queued[p]
                            .checked_add(bytes)
                            .ok_or_else(|| Error::Invalid("queued work overflow".into()))?;
                        let local = self
                            .metrics
                            .config
                            .locality
                            .get(worker)
                            .zip(self.metrics.config.preferred_locality.get(
                                usize::try_from(entry.morsel.id.0).map_err(|_| {
                                    Error::Invalid("scheduler index overflow".into())
                                })?,
                            ))
                            .is_some_and(|(a, b)| !a.is_empty() && a == b);
                        pending.push((entry.morsel.id, p, bytes, local));
                    }
                }
                let observation = Observation {
                    pending,
                    queued,
                    active_workers: self
                        .metrics
                        .workers
                        .iter()
                        .map(|state| *state == WorkerState::Active)
                        .collect(),
                    capacities: if self.metrics.config.capacities.is_empty() {
                        vec![1; self.ready.len()]
                    } else {
                        self.metrics.config.capacities.clone()
                    },
                    requester: worker,
                    critical: now > self.duration_ms * 3 / 4,
                    steal: self.metrics.config.policy == Policy::LeasedV1,
                    min_steal_bytes: self.metrics.config.min_steal_bytes,
                };
                let Some(decision) = decide(&observation)? else {
                    return Ok(
                        if observation.pending.is_empty() && !self.has_future_activation(worker)
                            || self.metrics.config.policy == Policy::LeasedNoSteal
                                && !observation.pending.iter().any(|(_, p, _, _)| *p == worker)
                        {
                            Response::Finish
                        } else {
                            Response::Wait
                        },
                    );
                };
                self.nonce += 1;
                let entry = &mut self.metrics.entries[usize::try_from(decision.morsel.0)
                    .map_err(|_| Error::Invalid("scheduler index overflow".into()))?];
                let lease = Lease {
                    worker: request.worker,
                    nonce: self.nonce,
                    morsel: entry.morsel.clone(),
                };
                entry.lease = Some(lease.clone());
                entry.granted_ms = now;
                entry.granted_sequence = self.metrics.control_messages;
                entry.last_renewed_ms = now;
                entry.expires_ms = now + self.metrics.config.lease_ms;
                if self.metrics.decisions.len() == self.metrics.config.audit_capacity {
                    self.metrics.decisions.pop_front();
                    self.metrics.dropped_decisions += 1;
                }
                self.metrics.decisions.push_back(decision);
                let outstanding = self
                    .metrics
                    .entries
                    .iter()
                    .filter(|entry| entry.lease.is_some() && entry.completion.is_none())
                    .count();
                self.metrics.peak_outstanding = self.metrics.peak_outstanding.max(outstanding);
                Ok(Response::Lease(lease))
            }
            Op::Renew(lease) => {
                let expires = now + self.metrics.config.lease_ms;
                let interval = self.metrics.config.heartbeat_ms / 2;
                let entry = self.entry(&lease, request.worker)?;
                require(entry.completion.is_none(), "renew completed lease")?;
                if now < entry.last_renewed_ms + interval {
                    // Ignore early renewals without extending ownership; a new
                    // lease can begin just before the periodic heartbeat tick.
                    return Ok(Response::Ack);
                }
                entry.expires_ms = expires;
                entry.last_renewed_ms = now;
                entry.renewals += 1;
                self.metrics.renewals += 1;
                Ok(Response::Ack)
            }
            Op::Complete(c) => {
                require(
                    c.input_rows <= self.metrics.config.max_input_rows
                        && c.output_rows <= c.input_rows
                        && c.work_ns <= self.duration_ms * 1_000_000,
                    "bogus lease completion counters",
                )?;
                let sequence = self.metrics.control_messages;
                let entry = self.entry(&c.lease, request.worker)?;
                if let Some(prior) = &entry.completion {
                    require(prior == &c, "conflicting duplicate completion")?;
                } else {
                    entry.completion = Some(c);
                    entry.completed_ms = now;
                    entry.completed_sequence = sequence;
                }
                let completed = self.completed();
                let input = self
                    .metrics
                    .entries
                    .iter()
                    .filter_map(|entry| entry.completion.as_ref())
                    .try_fold(0u64, |a, c| {
                        a.checked_add(c.input_rows)
                            .ok_or_else(|| Error::Invalid("lease row overflow".into()))
                    })?;
                require(
                    input <= self.metrics.config.max_input_rows,
                    "scheduler total source row cap",
                )?;
                while let Some(event) = self
                    .metrics
                    .config
                    .participation
                    .get(self.next_event)
                    .cloned()
                {
                    if event.after_completed > completed {
                        break;
                    }
                    self.transition(
                        usize::try_from(event.worker.0)
                            .map_err(|_| Error::Invalid("scheduler index overflow".into()))?,
                        event.action,
                        now,
                    )?;
                    self.next_event += 1;
                }
                Ok(Response::Ack)
            }
            Op::Closed => {
                require(
                    !self.outstanding(worker),
                    "producer closed with unfinished lease",
                )?;
                self.closed[worker] = true;
                self.metrics.workers[worker] = WorkerState::Drained;
                for entry in &mut self.metrics.events {
                    if entry.worker == request.worker && !matches!(entry.action, Action::Activate) {
                        entry.drained_ms = Some(now);
                    }
                }
                Ok(Response::Ack)
            }
        }
    }
    fn entry(&mut self, l: &Lease, w: WorkerId) -> Result<&mut Entry> {
        let e = self
            .metrics
            .entries
            .get_mut(usize::try_from(l.morsel.id.0).unwrap_or(usize::MAX))
            .ok_or_else(|| Error::Invalid("unknown source morsel".into()))?;
        require(
            l.worker == w && e.lease.as_ref() == Some(l),
            "stale/conflicting lease owner/nonce/source",
        )?;
        Ok(e)
    }
    fn has_future_activation(&self, worker: usize) -> bool {
        self.metrics.config.participation[self.next_event..]
            .iter()
            .any(|event| event.worker.0 == worker as u64 && event.action == Action::Activate)
    }
    pub(super) fn finish(&self, now: u64) -> Result<Metrics> {
        self.check(now)?;
        require(
            self.completed() == self.metrics.entries.len()
                && self.closed.iter().all(|v| *v)
                && self.next_event == self.metrics.config.participation.len(),
            "incomplete source leases/producer closures/participation events",
        )?;
        Ok(self.metrics.clone())
    }
}
