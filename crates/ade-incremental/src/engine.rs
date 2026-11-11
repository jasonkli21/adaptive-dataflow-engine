use crate::{
    Action, CancellationToken, Error, Guard, Metrics, Outcome, ReadOutcome, Request, Result,
    ViewRow,
    arrangement::{Hot, Shard, partition},
    require,
    source::{Baseline, History},
    storage::{MANIFEST_CAP, Manifest, Object, Root},
};
use ade_logical::maintained::{MaintainedPlanV1, Route, SHARDS, VIEWS, Work, choose};
use std::{io::BufReader, time::Instant};

/// Verify and optionally execute one exact target epoch under the root's writer lease.
/// # Errors
/// Refuses unsupported plans, unsafe or missing input, conflicting writers, quota/deadline
/// breaches, arithmetic violations and any invalid state or publication.
pub fn run(request: &Request, action: Action, token: CancellationToken) -> Result<Outcome> {
    let started = Instant::now();
    request.validate()?;
    let guard = Guard::new(token, request.limits.timeout_ms);
    let plan = request.load_plan(&guard)?;
    let mut inputs = Inputs::open(request, &plan, &guard)?;
    let mut root = Root::open(
        &request.root,
        action == Action::Apply,
        inputs.baseline.is_some() && inputs.history.full(request.epoch),
        &plan,
        request.limits,
        &guard,
    )?;
    let target = request.epoch;
    let available = root.pending.as_ref().or_else(|| root.head());
    if let Some(path) = &request.delta_tail {
        let anchor = available.ok_or(Error::Invalid("tail requires committed state"))?;
        inputs
            .history
            .bind_tail(path, anchor, request, &plan, &guard)?;
    }
    let mut metrics = inputs.metrics();
    if let Some(m) = available {
        root.verify_objects(m, &guard)?;
        metrics.checkpoint_verified_bytes += m.state_bytes();
    }
    let mut job = Job {
        request,
        plan: &plan,
        guard: &guard,
        inputs,
        metrics,
    };
    job.verify_target(&root)?;
    if action == Action::Apply {
        root.clean_unwitnessed(&guard)?;
    }
    let decision = job.decide(&root)?;
    let (result, recovered) = if action == Action::Explain {
        (None, false)
    } else {
        match job.execute(&mut root, decision.selected) {
            Ok((identity, recovered)) => (Some(identity), recovered),
            Err(error) => {
                // The lease protects bounded cleanup of unpublished scratch. A
                // witnessed stage survives every error for exact resumption.
                let cleanup = Guard::new(CancellationToken::default(), 5000);
                root.clean_unwitnessed(&cleanup)?;
                return Err(error);
            }
        }
    };
    job.metrics.wall_us = started.elapsed().as_micros();
    job.metrics.root_bytes = root.used;
    Ok(Outcome {
        plan: plan.digest().into(),
        properties: plan.properties(),
        epoch: target,
        chain: request.chain.clone(),
        decision,
        result,
        recovered_publication: recovered,
        metrics: job.metrics,
    })
}

struct Inputs {
    baseline: Option<Baseline>,
    history: History,
}
impl Inputs {
    fn open(request: &Request, plan: &MaintainedPlanV1, guard: &Guard) -> Result<Self> {
        let history = History::open(
            request.deltas.as_deref(),
            request.epoch,
            &request.chain,
            plan,
            request.limits,
            guard,
        )?;
        let baseline = request
            .baseline
            .as_ref()
            .map(|p| Baseline::open(p, plan, guard))
            .transpose()?;
        Ok(Self { baseline, history })
    }

    fn metrics(&self) -> Metrics {
        Metrics {
            baseline_verified_bytes: self.baseline.as_ref().map_or(0, |b| b.bytes),
            source_delta_bytes: self.history.deltas.iter().map(|m| m.bytes).sum(),
            defined_views: 2,
            ..Metrics::default()
        }
    }
}

struct Job<'a> {
    request: &'a Request,
    plan: &'a MaintainedPlanV1,
    guard: &'a Guard,
    inputs: Inputs,
    metrics: Metrics,
}

impl Job<'_> {
    fn verify_target(&self, root: &Root) -> Result<()> {
        let epoch = self.request.epoch;
        let history = &self.inputs.history;
        if let Some(pending) = &root.pending {
            return require(
                pending.epoch == epoch && pending.chain == self.request.chain,
                "exact witnessed publication required",
            );
        }
        if let Some(head) = root.head() {
            if epoch == head.epoch {
                require(
                    head.chain == self.request.chain,
                    "already committed epoch has a different chain",
                )?;
            } else {
                require(
                    epoch == head.epoch + 1,
                    "target epoch fork, gap or rollback",
                )?;
                if history.covers(epoch) {
                    require(
                        history.parent(epoch) == head.chain,
                        "delta history differs from committed parent",
                    )?;
                }
            }
        }
        Ok(())
    }

    fn decide(&self, root: &Root) -> Result<ade_logical::maintained::Decision> {
        let epoch = self.request.epoch;
        let target = root
            .pending
            .as_ref()
            .or_else(|| root.head())
            .filter(|m| m.epoch == epoch);
        let delta_bytes = self
            .inputs
            .history
            .deltas
            .iter()
            .try_fold(0u64, |total, d| total.checked_add(d.bytes))
            .ok_or(Error::Invalid("source byte estimate overflow"))?;
        let baseline_bytes = self.inputs.baseline.as_ref().map_or(0, |b| b.bytes);
        let admission_bytes = baseline_bytes
            .checked_add(delta_bytes)
            .and_then(|n| n.checked_add(self.metrics.checkpoint_verified_bytes))
            .ok_or(Error::Invalid("admission byte estimate overflow"))?;
        let restore = target.map(|_| Work {
            admission_bytes,
            route_verify_bytes: if root.pending.is_some() {
                baseline_bytes.saturating_add(delta_bytes)
            } else {
                0
            },
            ..Work::default()
        });
        let parent = root
            .head()
            .filter(|m| m.epoch + 1 == epoch && root.pending.is_none());
        let history = &self.inputs.history;
        let update = parent
            .zip(history.deltas.last().filter(|_| history.covers(epoch)))
            .map(|(m, delta)| {
                let touched_bytes: u64 = delta
                    .touched_shards
                    .iter()
                    .filter_map(|&i| m.objects[i].as_ref())
                    .map(|o| o.bytes)
                    .sum();
                Work {
                    delta_rows: delta.rows,
                    touched_keys: delta.keys,
                    admission_bytes,
                    route_verify_bytes: baseline_bytes
                        .checked_add(delta_bytes)
                        .and_then(|n| n.checked_add(delta.bytes))
                        .and_then(|n| n.checked_add(touched_bytes))
                        .unwrap_or(u64::MAX),
                    write_bytes: reserve(touched_bytes + delta.rows * 60),
                    ..Work::default()
                }
            });
        let full = self
            .inputs
            .baseline
            .as_ref()
            .filter(|_| history.full(epoch) && root.pending.is_none())
            .map(|b| {
                let delta_rows = history.deltas.iter().map(|d| d.rows).sum::<u64>();
                Work {
                    scan_rows: b.rows,
                    delta_rows,
                    admission_bytes,
                    route_verify_bytes: b
                        .bytes
                        .checked_mul(3)
                        .and_then(|n| n.checked_add(delta_bytes.checked_mul(2)?))
                        .unwrap_or(u64::MAX),
                    write_bytes: if target.is_some() {
                        0
                    } else {
                        reserve((b.rows + delta_rows) * 60)
                    },
                    ..Work::default()
                }
            });
        Ok(choose(
            [restore, update, full],
            self.request.policy,
            self.request.limits.disk_bytes.saturating_sub(root.used),
            self.guard.remaining_ms(),
        )?)
    }

    fn execute(&mut self, root: &mut Root, route: Route) -> Result<(String, bool)> {
        let epoch = self.request.epoch;
        if route == Route::Restore {
            if let Some(m) = &root.pending {
                self.attest_inputs()?;
                root.finish_witness(epoch)?;
                self.metrics.state_bytes = m.state_bytes();
                return Ok((m.result.clone(), true));
            }
            let m = root
                .head()
                .ok_or(Error::Invalid("restore requires selected target"))?;
            self.metrics.state_bytes = m.state_bytes();
            return Ok((m.result.clone(), false));
        }

        let existing = root
            .head()
            .filter(|m| m.epoch == epoch)
            .map(|m| m.result.clone());
        let mut hot = Hot::new(self.request.limits.hot_entries);
        let objects = if route == Route::FullRecompute {
            self.recompute(root, &mut hot, existing.is_none())?
        } else {
            self.update(root, &mut hot)?
        };
        self.metrics.hot_entries_peak = hot.peak;
        self.metrics.arrangement_instances = 1;
        if let Some(existing) = existing {
            self.attest_inputs()?;
            let result =
                Manifest::result(self.plan.digest(), epoch, &self.request.chain, &objects)?;
            require(
                result == existing,
                "full replay differs from the committed target",
            )?;
            return Ok((result, false));
        }

        let history = self
            .inputs
            .history
            .deltas
            .iter()
            .map(|m| m.chain.clone())
            .collect();
        let manifest = root.manifest(self.plan, history, epoch, objects)?;
        manifest.check_limits(self.request.limits)?;
        let result = manifest.result.clone();
        root.publish(&manifest, self.request.limits, self.guard, || {
            self.attest_inputs()
        })?;
        self.metrics.checkpoint_verified_bytes += self.metrics.shard_bytes_written;
        Ok((result, false))
    }

    fn attest_inputs(&mut self) -> Result<()> {
        if let Some(b) = &self.inputs.baseline {
            b.attest(self.guard)?;
            self.metrics.baseline_verified_bytes += b.bytes;
        }
        self.inputs
            .history
            .attest(self.plan, self.request.limits, self.guard)?;
        self.metrics.source_delta_bytes += self
            .inputs
            .history
            .deltas
            .iter()
            .map(|m| m.bytes)
            .sum::<u64>();
        Ok(())
    }

    fn recompute(
        &mut self,
        root: &mut Root,
        hot: &mut Hot,
        publish: bool,
    ) -> Result<Vec<Option<Object>>> {
        let baseline = self
            .inputs
            .baseline
            .as_ref()
            .ok_or(Error::Invalid("complete baseline required"))?;
        let mut arrangement = std::array::from_fn(|_| Shard::default());
        baseline.scan(
            &mut arrangement,
            hot,
            self.plan.threshold(),
            self.guard,
            &mut self.metrics,
        )?;
        self.metrics.baseline_verified_bytes += 2 * baseline.bytes;

        for epoch in 1..=self.request.epoch {
            let history = &self.inputs.history;
            let delta = history.load(epoch, self.plan, self.request.limits, self.guard)?;
            let charge = delta.rows() * 2;
            hot.add(charge)?;
            self.metrics.delta_rows_applied += delta.rows() as u64;
            self.metrics.source_delta_bytes += history.deltas[epoch as usize - 1].bytes;
            let changes = partition(delta.changes());
            for (shard, bucket) in arrangement.iter_mut().zip(changes) {
                shard.apply(
                    &bucket,
                    self.plan.threshold(),
                    hot,
                    self.guard,
                    &mut self.metrics,
                )?;
            }
            hot.release(charge);
        }
        self.metrics.state_bytes = arrangement
            .iter()
            .filter(|s| !s.groups.is_empty())
            .map(Shard::len_bytes)
            .sum();
        if self.metrics.state_bytes > self.request.limits.state_bytes {
            return Err(Error::Quota("arrangement encoded bytes"));
        }
        if publish {
            root.start_stage()?;
        }
        let mut objects = Vec::with_capacity(SHARDS);
        for (i, shard) in arrangement.iter().enumerate() {
            objects.push(self.object(root, i, shard, publish)?);
        }
        self.measure_state(&objects)?;
        Ok(objects)
    }

    fn update(&mut self, root: &mut Root, hot: &mut Hot) -> Result<Vec<Option<Object>>> {
        let mut objects = root
            .head()
            .ok_or(Error::Invalid("compatible selected parent required"))?
            .objects
            .clone();
        let epoch = self.request.epoch;
        let history = &self.inputs.history;
        let delta = history.load(epoch, self.plan, self.request.limits, self.guard)?;
        let charge = delta.rows() * 2;
        hot.add(charge)?;
        self.metrics.delta_rows_applied = delta.rows() as u64;
        self.metrics.source_delta_bytes += history.deltas[epoch as usize - 1].bytes;
        let changes = partition(delta.changes());
        root.start_stage()?;
        for (i, (object, rows)) in objects.iter_mut().zip(changes).enumerate() {
            if rows.is_empty() {
                continue;
            }
            let mut shard = if let Some(o) = object {
                let file = root.verified_object(i, o, self.guard)?;
                self.metrics.shards_decoded += 1;
                self.metrics.checkpoint_verified_bytes += o.bytes;
                let shard = Shard::decode(BufReader::new(file), o.bytes, i, None, hot, self.guard)?;
                require(
                    shard.entries() == o.hot_entries,
                    "parent hot entry accounting",
                )?;
                shard
            } else {
                Shard::default()
            };
            shard.apply(
                &rows,
                self.plan.threshold(),
                hot,
                self.guard,
                &mut self.metrics,
            )?;
            *object = self.object(root, i, &shard, true)?;
            hot.release(shard.entries());
        }
        hot.release(charge);
        self.measure_state(&objects)?;
        Ok(objects)
    }

    fn object(
        &mut self,
        root: &mut Root,
        i: usize,
        shard: &Shard,
        publish: bool,
    ) -> Result<Option<Object>> {
        if shard.groups.is_empty() {
            return Ok(None);
        }
        let epoch = self.request.epoch;
        let object = if publish {
            let o = root.write_shard(i, shard, epoch, self.request.limits, self.guard)?;
            self.metrics.shards_written += 1;
            self.metrics.shard_bytes_written += o.bytes;
            o
        } else {
            if shard.len_bytes() > self.request.limits.state_bytes {
                return Err(Error::Quota("shard bytes"));
            }
            let bytes = shard.encode(self.guard)?;
            Object {
                epoch,
                bytes: bytes.len() as u64,
                hash: ade_physical::fingerprint(&bytes),
                view_rows: shard.view_rows(),
                hot_entries: shard.entries(),
            }
        };
        Ok(Some(object))
    }

    fn measure_state(&mut self, objects: &[Option<Object>]) -> Result<()> {
        self.metrics.state_bytes = objects.iter().flatten().map(|o| o.bytes).sum();
        if self.metrics.state_bytes > self.request.limits.state_bytes {
            return Err(Error::Quota("arrangement encoded bytes"));
        }
        Ok(())
    }
}

fn reserve(bytes: u64) -> u64 {
    bytes + 2 * MANIFEST_CAP as u64 + 16 * SHARDS as u64
}

/// Stream both committed named views under a shared root lease. The caller's exact
/// target chain is required even when source paths are absent.
/// # Errors
/// Refuses an unselected target, conflicting writer, invalid state, output failure or quota.
pub fn read(
    request: &Request,
    token: CancellationToken,
    emit: impl FnMut(ViewRow) -> Result<()>,
) -> Result<ReadOutcome> {
    read_selected(request, None, token, emit)
}

/// Read one named view from the selected committed generation.
/// # Errors
/// Refuses an unknown view before opening the root or emitting rows.
pub fn read_view(
    request: &Request,
    view: &str,
    token: CancellationToken,
    emit: impl FnMut(ViewRow) -> Result<()>,
) -> Result<ReadOutcome> {
    let index = VIEWS
        .iter()
        .position(|candidate| *candidate == view)
        .ok_or(Error::Invalid("unknown maintained view"))?;
    read_selected(request, Some(index), token, emit)
}

fn read_selected(
    request: &Request,
    selected_view: Option<usize>,
    token: CancellationToken,
    mut emit: impl FnMut(ViewRow) -> Result<()>,
) -> Result<ReadOutcome> {
    request.validate()?;
    let guard = Guard::new(token, request.limits.timeout_ms);
    let plan = request.load_plan(&guard)?;
    let root = Root::open(&request.root, false, false, &plan, request.limits, &guard)?;
    let m = root
        .head()
        .ok_or(Error::Invalid("no selected generation"))?;
    require(
        m.epoch == request.epoch && m.chain == request.chain,
        "read requires the exact selected target",
    )?;
    root.verify_objects(m, &guard)?;
    let mut hot = Hot::new(request.limits.hot_entries);
    let mut seen = 0u64;
    for (i, o) in m.objects.iter().enumerate() {
        let Some(o) = o else {
            continue;
        };
        let file = root.verified_object(i, o, &guard)?;
        let shard = Shard::decode(
            BufReader::new(file),
            o.bytes,
            i,
            Some(plan.threshold()),
            &mut hot,
            &guard,
        )?;
        require(
            shard.view_rows() == o.view_rows && shard.entries() == o.hot_entries,
            "view and hot entry accounting",
        )?;
        for (&key, group) in &shard.groups {
            guard.check()?;
            for (view_index, (view, total)) in VIEWS
                .into_iter()
                .zip([group.all, group.filtered])
                .enumerate()
            {
                if selected_view.is_some_and(|selected| selected != view_index) {
                    continue;
                }
                if total.count > 0 {
                    seen += 1;
                    emit(ViewRow {
                        view,
                        key,
                        count: total.count,
                        sum: total.sum,
                    })?;
                }
            }
        }
        hot.release(shard.entries());
    }
    Ok(ReadOutcome {
        epoch: m.epoch,
        chain: m.chain.clone(),
        result: m.result.clone(),
        view_rows: seen,
        views_read: if selected_view.is_some() { 1 } else { 2 },
        hot_entries_peak: hot.peak,
    })
}
