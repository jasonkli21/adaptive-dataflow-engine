use crate::{
    Error, Result,
    contract::{
        Attempt, Config, Directory, FRAME, Intent, Job, Ledger, Limits, MAX_JOBS, MAX_LAUNCHES,
        MAX_TENANTS, Outcome, Price, Spec, State, Tenant, TenantConfig, valid_key,
    },
    decode, encode, fault, hash, native, now, policy, require,
    store::{Store, read_bounded},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::{self, Read, Write},
    os::unix::{
        fs::{FileTypeExt, MetadataExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::Path,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use uuid::Uuid;

const PEERS: usize = 8;
const CONTROL_TIMEOUT: Duration = Duration::from_secs(2);

fn credential() -> Result<String> {
    let mut bytes = [0; 32];
    getrandom::fill(&mut bytes).map_err(|_| Error("ENTROPY"))?;
    Ok(blake3::Hash::from_bytes(bytes).to_hex().to_string())
}

fn read_config<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    decode(&read_bounded(
        File::open(path).map_err(|_| Error("CONFIG_IO"))?,
        FRAME,
    )?)
}

/// Initialize a private empty root and return the one-time administrator token.
///
/// # Errors
/// Refuses existing roots, nonempty or invalid native cohorts, or I/O failures.
pub fn initialize(root: &Path, config: &Path) -> Result<String> {
    let config: Config = read_config(config)?;
    native::validate_config(&config, true)?;
    for cohort in &config.cohorts {
        for path in
            std::iter::once(&cohort.root).chain(cohort.workers.iter().map(|w| &w.scratch_root))
        {
            require(
                !path.starts_with(root) && !root.starts_with(path),
                "COHORT_ALIAS",
            )?;
        }
    }
    let token = credential()?;
    Store::init(root, config, hash(token.as_bytes()))?;
    Ok(token)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u32,
    token: String,
    request: Request,
}
#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Submit {
        key: String,
        spec: Box<Spec>,
    },
    List,
    Status {
        job: Uuid,
    },
    Cancel {
        job: Uuid,
    },
    Result {
        job: Uuid,
        offset: u64,
    },
    Provision {
        config: TenantConfig,
    },
    Rotate {
        tenant: Uuid,
    },
    Revoke {
        tenant: Uuid,
    },
    Limits {
        tenant: Option<Uuid>,
        limits: Limits,
    },
    Price {
        price: Price,
    },
    Inspect,
    InspectJob {
        job: Uuid,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Principal {
    Operator,
    Tenant(Uuid),
}
fn authenticate(ledger: &Ledger, token: &str) -> Result<Principal> {
    require(
        token.len() == 64 && token.bytes().all(|b| b.is_ascii_hexdigit()),
        "UNAUTHORIZED",
    )?;
    let candidate = hash(token.as_bytes());
    let equal = |p: &str| bool::from(p.as_bytes().ct_eq(candidate.as_bytes()));
    let mut principal = equal(&ledger.admin).then_some(Principal::Operator);
    for (&id, tenant) in &ledger.tenants {
        if tenant.fingerprint.as_ref().is_some_and(|p| equal(p)) {
            principal = Some(Principal::Tenant(id));
        }
    }
    principal.ok_or(Error("UNAUTHORIZED"))
}

struct Manager {
    store: Mutex<Store>,
    fatal: AtomicBool,
}

impl Manager {
    fn lock(&self) -> Result<MutexGuard<'_, Store>> {
        let store = self.store.lock().map_err(|_| Error("LEDGER_STOPPED"))?;
        require(!self.fatal.load(Ordering::Acquire), "LEDGER_STOPPED")?;
        Ok(store)
    }

    fn edit<T>(
        &self,
        store: &mut Store,
        f: impl FnOnce(&mut Ledger, u64) -> Result<T>,
    ) -> Result<T> {
        let result = store
            .time()
            .and_then(|at| store.change(at, |state| f(state, at)));
        if let Err(error) = result
            && matches!(
                error.0,
                "CLOCK"
                    | "CLOCK_ROLLBACK"
                    | "LEDGER_IO"
                    | "GENERATION_FULL"
                    | "SOURCE_ROOT_DRIFT"
                    | "LEDGER_FULL"
                    | "UNSAFE_PATH"
            )
        {
            self.fatal.store(true, Ordering::Release);
        }
        result
    }

    fn request(&self, envelope: &Envelope) -> Result<Value> {
        require(envelope.version == 1, "VERSION")?;
        let mut store = self.lock()?;
        let principal = authenticate(&store.state, &envelope.token)?;
        match principal {
            Principal::Operator => self.admin(&mut store, &envelope.request),
            Principal::Tenant(tenant) => self.tenant(store, tenant, envelope),
        }
    }

    fn admin(&self, store: &mut Store, request: &Request) -> Result<Value> {
        match request {
            Request::Inspect => inspect(&store.state),
            Request::InspectJob { job } => {
                let current = store.state.jobs.get(job).ok_or(Error("NOT_FOUND"))?;
                Ok(admin_job_view(*job, current))
            }
            Request::Provision { config } => self.provision(store, config),
            Request::Rotate { tenant } => {
                let token = credential()?;
                self.edit(store, |state, _| {
                    let principal = state.tenants.get_mut(tenant).ok_or(Error("NOT_FOUND"))?;
                    require(principal.fingerprint.is_some(), "TENANT_REVOKED")?;
                    principal.fingerprint = Some(hash(token.as_bytes()));
                    Ok(())
                })?;
                Ok(json!({"tenant":tenant,"token":token}))
            }
            Request::Revoke { tenant } => {
                self.edit(store, |state, _| {
                    state
                        .tenants
                        .get_mut(tenant)
                        .ok_or(Error("NOT_FOUND"))?
                        .fingerprint = None;
                    for job in state.jobs.values_mut().filter(|j| j.tenant == *tenant) {
                        request_cancel(job);
                    }
                    Ok(())
                })?;
                Ok(json!({"revoked":true}))
            }
            Request::Limits { tenant, limits } => {
                limits.validate()?;
                require(
                    limits.cost_microunits.is_none() || store.state.config.price.is_some(),
                    "INVALID_LIMITS",
                )?;
                self.edit(store, |state, _| {
                    require(
                        policy::totals(state, *tenant, None)?.fits(limits),
                        "QUOTA_IN_USE",
                    )?;
                    if let Some(tenant) = tenant {
                        state
                            .tenants
                            .get_mut(tenant)
                            .ok_or(Error("NOT_FOUND"))?
                            .limits = limits.clone();
                    } else {
                        state.config.limits = limits.clone();
                    }
                    Ok(())
                })?;
                Ok(json!({"updated":true}))
            }
            Request::Price { price } => {
                price.validate()?;
                // An immutable tariff is pinned per root. A new price is only
                // accepted before any job exists, so queued ceilings stay fixed.
                require(
                    store.state.jobs.is_empty() && store.state.config.price.is_none(),
                    "PRICE_PINNED",
                )?;
                self.edit(store, |state, _| {
                    state.config.price = Some(price.clone());
                    Ok(())
                })?;
                Ok(json!({"updated":true}))
            }
            _ => Err(Error("UNAUTHORIZED")),
        }
    }

    fn provision(&self, store: &mut Store, config: &TenantConfig) -> Result<Value> {
        config.limits.validate()?;
        require(
            (1..=16).contains(&config.weight)
                && (config.limits.cost_microunits.is_none() || store.state.config.price.is_some()),
            "INVALID_TENANT",
        )?;
        let tenant = Uuid::new_v4();
        let token = credential()?;
        let source = Directory::capture(config.source_root.clone())?;
        let overlaps =
            |path: &Path| source.path.starts_with(path) || path.starts_with(&source.path);
        require(
            !overlaps(store.path())
                && store
                    .state
                    .tenants
                    .values()
                    .all(|t| !overlaps(&t.source.path))
                && store.state.config.cohorts.iter().all(|c| {
                    !overlaps(&c.root) && c.workers.iter().all(|w| !overlaps(&w.scratch_root))
                }),
            "SOURCE_SCOPE",
        )?;
        self.edit(store, |state, _| {
            require(
                state.tenants.len() < MAX_TENANTS && !state.tenants.contains_key(&tenant),
                "TENANT_HISTORY_FULL",
            )?;
            state.tenants.insert(
                tenant,
                Tenant {
                    source,
                    weight: config.weight,
                    limits: config.limits.clone(),
                    fingerprint: Some(hash(token.as_bytes())),
                    granted_work: 0,
                },
            );
            Ok(())
        })?;
        Ok(json!({"tenant":tenant,"token":token}))
    }

    fn tenant(
        &self,
        mut store: MutexGuard<'_, Store>,
        tenant: Uuid,
        envelope: &Envelope,
    ) -> Result<Value> {
        match &envelope.request {
            Request::Submit { key, spec } => self.submit(store, tenant, &envelope.token, key, spec),
            Request::List => Ok(json!(
                store
                    .state
                    .jobs
                    .iter()
                    .filter(|(_, j)| j.tenant == tenant)
                    .map(|(id, j)| job_view(*id, j))
                    .collect::<Vec<_>>()
            )),
            Request::Status { job } => Ok(job_view(*job, own(&store.state, tenant, *job)?)),
            Request::Cancel { job } => {
                let current = own(&store.state, tenant, *job)?;
                if matches!(
                    current.state,
                    State::Queued | State::Reserved | State::Starting | State::Running
                ) && current.intent != Some(Intent::Cancel)
                {
                    self.edit(&mut store, |state, _| {
                        request_cancel(state.jobs.get_mut(job).ok_or(Error("NOT_FOUND"))?);
                        Ok(())
                    })?;
                }
                Ok(job_view(*job, own(&store.state, tenant, *job)?))
            }
            Request::Result { job, offset } => {
                let current = own(&store.state, tenant, *job)?;
                require(current.state == State::Succeeded, "RESULT_NOT_READY")?;
                let attempt = current.attempts.last().ok_or(Error("CORRUPT_LEDGER"))?;
                let native_id = attempt.native_id.ok_or(Error("CORRUPT_LEDGER"))?;
                let digest = attempt
                    .usage
                    .as_ref()
                    .and_then(|u| u.ipc_hash.clone())
                    .ok_or(Error("CORRUPT_LEDGER"))?;
                let cohort = store.state.config.cohorts[attempt.cohort].clone();
                drop(store);
                let result = native::read_output(&cohort, native_id, &digest, *offset)?;
                require(
                    authenticate(&self.lock()?.state, &envelope.token)?
                        == Principal::Tenant(tenant),
                    "UNAUTHORIZED",
                )?;
                Ok(result)
            }
            _ => Err(Error("UNAUTHORIZED")),
        }
    }

    fn submit(
        &self,
        store: MutexGuard<'_, Store>,
        tenant: Uuid,
        token: &str,
        key: &str,
        spec: &Spec,
    ) -> Result<Value> {
        require(valid_key(key), "INVALID_KEY")?;
        let digest = spec.digest()?;
        if let Some(reply) = duplicate(&store.state, tenant, key, &digest)? {
            return Ok(reply);
        }
        let workers = store.state.config.cohorts[0].workers.clone();
        spec.validate(&workers)?;
        let source = store.state.tenants[&tenant].source.clone();
        let id = Uuid::new_v4();
        let at = now()?;
        let expires_ms = at.checked_add(spec.deadline_ms).ok_or(Error("CLOCK"))?;
        let mut job = Job {
            tenant,
            key: key.into(),
            digest,
            spec: spec.clone(),
            source_pins: vec![],
            sequence: 0,
            queued_ms: at,
            expires_ms,
            state: State::Queued,
            intent: None,
            reason: "QUEUED".into(),
            attempts: vec![],
        };
        check_submit(&store.state, &job, at)?;
        drop(store);
        job.source_pins = spec.source_pins(&source)?;
        spec.recipe(&workers)
            .verify_inputs(expires_ms)
            .map_err(|_| Error("SOURCE_VERIFICATION"))?;
        job.verify_source_scope(&source)?;
        let mut store = self.lock()?;
        require(
            authenticate(&store.state, token)? == Principal::Tenant(tenant),
            "UNAUTHORIZED",
        )?;
        if let Some(reply) = duplicate(&store.state, tenant, key, &job.digest)? {
            return Ok(reply);
        }
        fault("submission_before_snapshot");
        self.edit(&mut store, |state, committed| {
            require(committed >= at, "CLOCK_ROLLBACK")?;
            require(committed < expires_ms, "DEADLINE_EXPIRED")?;
            check_submit(state, &job, at)?;
            require(!state.jobs.contains_key(&id), "ID_COLLISION")?;
            state.next_sequence = state
                .next_sequence
                .checked_add(1)
                .ok_or(Error("GENERATION_FULL"))?;
            job.sequence = state.next_sequence;
            let response = job_view(id, &job);
            state.jobs.insert(id, job);
            Ok(response)
        })
        .inspect(|_| fault("submitted_before_reply"))
    }
}

fn admin_job_view(id: Uuid, job: &Job) -> Value {
    let mut view = job_view(id, job);
    view["tenant"] = json!(job.tenant);
    view["reason"] = json!(job.reason);
    view["attempts"] = json!(job.attempts);
    view
}

fn inspect(state: &Ledger) -> Result<Value> {
    let global = policy::totals(state, None, None)?;
    let jobs = state
        .jobs
        .iter()
        .map(|(id, job)| admin_job_view(*id, job))
        .collect::<Vec<_>>();
    let mut tenants = Vec::new();
    for (&id, tenant) in &state.tenants {
        tenants.push(
            json!({"id":id,"weight":tenant.weight,"enabled":tenant.fingerprint.is_some(),
                        "granted_work":tenant.granted_work,"limits":tenant.limits,
                        "totals":policy::totals(state, Some(id), None)?}),
        );
    }
    let config = &state.config;
    Ok(json!({"ledger":{
        "generation":state.generation,"clock_ms":state.clock_ms,
        "mode":config.mode,"aging_ms":config.aging_ms,"standby_idle_ms":config.standby_idle_ms,
        "preempt_after_ms":config.preempt_after_ms,"limits":config.limits,"price":config.price,
        "standby_on":state.standby_on,"standby_transitions":state.standby_transitions,
        "decisions":state.decisions,
        "jobs":jobs,"tenants":tenants,
    },"totals":global}))
}

fn own(state: &Ledger, tenant: Uuid, id: Uuid) -> Result<&Job> {
    state
        .jobs
        .get(&id)
        .filter(|j| j.tenant == tenant)
        .ok_or(Error("NOT_FOUND"))
}

fn job_view(id: Uuid, job: &Job) -> Value {
    let mut attempts = serde_json::to_value(&job.attempts).unwrap_or(Value::Null);
    if let Some(attempts) = attempts.as_array_mut() {
        for attempt in attempts {
            if let Some(fields) = attempt.as_object_mut() {
                fields.remove("tenant_work_before");
            }
        }
    }
    json!({"id":id,"state":job.state,"reason":job.reason.split("_FOR_").next(),
        "sequence":job.sequence,"queued_ms":job.queued_ms,"expires_ms":job.expires_ms,
        "attempts":attempts,"priority":job.spec.priority,"work":job.spec.work,
        "intent":job.intent})
}

fn duplicate(state: &Ledger, tenant: Uuid, key: &str, digest: &str) -> Result<Option<Value>> {
    state
        .jobs
        .iter()
        .find(|(_, j)| j.tenant == tenant && j.key == key)
        .map(|(&id, j)| {
            require(j.digest == digest, "KEY_CONFLICT")?;
            Ok(job_view(id, j))
        })
        .transpose()
}

fn check_submit(state: &Ledger, job: &Job, at: u64) -> Result<()> {
    require(state.jobs.len() < MAX_JOBS, "HISTORY_FULL")?;
    require(!policy::history_exhausted(state), "NATIVE_HISTORY_FULL")?;
    require(
        policy::totals(state, None, None)?.queued < state.config.limits.queued
            && policy::totals(state, Some(job.tenant), None)?.queued
                < state.tenants[&job.tenant].limits.queued,
        "QUEUE_FULL",
    )?;
    require(
        policy::can_ever_fit(state, job, at)?,
        "REQUEST_EXCEEDS_CAPACITY",
    )
}

fn request_cancel(job: &mut Job) {
    match job.state {
        State::Queued => {
            job.state = State::Cancelled;
            job.reason = "CANCELLED_BEFORE_ADMISSION".into();
        }
        State::Reserved | State::Starting | State::Running => {
            job.intent = Some(Intent::Cancel);
            job.reason = "CANCEL_REQUESTED".into();
        }
        _ => {}
    }
}

fn active(state: &Ledger, cohort: usize) -> Option<(Uuid, &Job)> {
    state
        .jobs
        .iter()
        .find(|(_, j)| {
            j.state.holds_cohort() && j.attempts.last().is_some_and(|a| a.cohort == cohort)
        })
        .map(|(&id, j)| (id, j))
}

fn known(state: &Ledger, cohort: usize) -> Vec<(String, Option<Uuid>)> {
    state
        .jobs
        .values()
        .flat_map(|j| j.attempts.iter())
        .filter(|a| a.cohort == cohort)
        .map(|a| (a.native_key.clone(), a.native_id))
        .collect()
}

fn run_job(manager: &Manager, id: Uuid, cohort_index: usize) -> Result<()> {
    let store = manager.lock()?;
    let job = store
        .state
        .jobs
        .get(&id)
        .ok_or(Error("CORRUPT_LEDGER"))?
        .clone();
    let cohort = store.state.config.cohorts[cohort_index].clone();
    let source = store.state.tenants[&job.tenant].source.clone();
    let known = known(&store.state, cohort_index);
    drop(store);
    if job.state == State::Reserved {
        match native::register(&cohort, &job, &source, &known)? {
            native::Registration::Mapped(native_id) => {
                let mut store = manager.lock()?;
                manager.edit(&mut store, |state, _| {
                    let job = state.jobs.get_mut(&id).ok_or(Error("CORRUPT_LEDGER"))?;
                    job.attempts
                        .last_mut()
                        .ok_or(Error("CORRUPT_LEDGER"))?
                        .native_id = Some(native_id);
                    job.state = State::Starting;
                    job.reason = "NATIVE_REGISTERED".into();
                    Ok(())
                })?;
                fault("mapped_before_start");
            }
            native::Registration::Refused(reason) => {
                return finish(manager, id, None, reason);
            }
        }
    }
    let job = manager
        .lock()?
        .state
        .jobs
        .get(&id)
        .ok_or(Error("CORRUPT_LEDGER"))?
        .clone();
    let result = native::run(&cohort, &job, &source)?;
    fault("native_terminal_before_meter");
    if result.job.state == ade_ops::JobState::Cancelled {
        fault("drained_before_settlement");
    }
    finish(manager, id, Some(result), "NATIVE_TERMINAL")
}

fn finish(
    manager: &Manager,
    id: Uuid,
    resolution: Option<native::Resolution>,
    reason: &'static str,
) -> Result<()> {
    let mut store = manager.lock()?;
    manager.edit(&mut store, |state, at| {
        let logical = state.jobs.get_mut(&id).ok_or(Error("CORRUPT_LEDGER"))?;
        let launches = logical.attempts.len();
        let attempt = logical.attempts.last_mut().ok_or(Error("CORRUPT_LEDGER"))?;
        require(attempt.usage.is_none(), "CORRUPT_LEDGER")?;
        let (outcome, status, bytes, output, code) =
            if let Some(native::Resolution { job, bytes, output }) = resolution {
                update_native(attempt, &job)?;
                let (outcome, status, code) = match job.state {
                    ade_ops::JobState::Succeeded => {
                        (Outcome::Succeeded, State::Succeeded, "VERIFIED_COMMIT")
                    }
                    ade_ops::JobState::Cancelled
                        if logical.intent == Some(Intent::Preempt)
                            && launches < MAX_LAUNCHES
                            && at < logical.expires_ms
                            && job.attempt == 1 =>
                    {
                        (Outcome::Preempted, State::Queued, "PREEMPTED_AND_DRAINED")
                    }
                    ade_ops::JobState::Cancelled => {
                        (Outcome::Cancelled, State::Cancelled, "NATIVE_CANCELLED")
                    }
                    ade_ops::JobState::Failed => (Outcome::Failed, State::Failed, "NATIVE_FAILED"),
                    _ => return Err(Error("NATIVE_DRAIN_UNPROVED")),
                };
                (outcome, status, bytes, output, code)
            } else {
                (
                    Outcome::Refused,
                    if logical.intent == Some(Intent::Cancel) {
                        State::Cancelled
                    } else if at >= logical.expires_ms {
                        State::Expired
                    } else {
                        State::Failed
                    },
                    Some(0),
                    None,
                    reason,
                )
            };
        attempt.usage = Some(policy::settle(
            attempt,
            logical.expires_ms,
            at,
            outcome,
            bytes,
            output,
        )?);
        logical.state = status;
        logical.intent = None;
        logical.reason = code.into();
        Ok(())
    })?;
    fault("meter_persisted");
    Ok(())
}

fn update_native(attempt: &mut Attempt, job: &ade_ops::Job) -> Result<()> {
    require(
        attempt.native_id == Some(job.id) && attempt.native_key == job.request_key,
        "NATIVE_MAPPING",
    )?;
    if let Some(native) = job.attempts.last() {
        require(
            attempt.instance.is_none_or(|id| id == native.instance)
                && attempt.engine_job.is_none_or(|id| id == native.engine_job)
                && attempt.owner.is_none_or(|owner| owner == native.owner),
            "NATIVE_MAPPING",
        )?;
        attempt.instance = Some(native.instance);
        attempt.engine_job = Some(native.engine_job);
        attempt.owner = Some(native.owner);
    }
    Ok(())
}

fn quarantine(manager: &Manager, id: Uuid, error: Error) -> Result<()> {
    let mut store = manager.lock()?;
    manager.edit(&mut store, |state, _| {
        let job = state.jobs.get_mut(&id).ok_or(Error("CORRUPT_LEDGER"))?;
        if job.state.holds_cohort() {
            job.state = State::Quarantined;
            job.reason = error.0.into();
        }
        Ok(())
    })
}

#[derive(Default)]
struct Dispatch {
    running: Option<JoinHandle<()>>,
    control: Option<JoinHandle<()>>,
    cancel_submitted: Option<Uuid>,
    cancel_completed: Arc<AtomicBool>,
}

impl Dispatch {
    fn reap(&mut self, manager: &Manager) -> Result<()> {
        for handle in [&mut self.control, &mut self.running] {
            if handle.as_ref().is_some_and(JoinHandle::is_finished)
                && handle.take().is_some_and(|h| h.join().is_err())
            {
                manager.fatal.store(true, Ordering::Release);
                return Err(Error("DISPATCH_PANIC"));
            }
        }
        Ok(())
    }
}

fn drive(manager: &Arc<Manager>, runners: &mut [Dispatch; 2]) -> Result<()> {
    for (index, runner) in runners.iter_mut().enumerate() {
        runner.reap(manager)?;
        let store = manager.lock()?;
        let Some((id, job)) = active(&store.state, index) else {
            continue;
        };
        if job.state == State::Quarantined {
            continue;
        }
        if runner.running.is_none() && runner.control.is_none() {
            runner.cancel_submitted = None;
            runner.cancel_completed.store(false, Ordering::Release);
            let manager = Arc::clone(manager);
            runner.running = Some(thread::spawn(move || {
                if let Err(error) = run_job(&manager, id, index)
                    && quarantine(&manager, id, error).is_err()
                {
                    manager.fatal.store(true, Ordering::Release);
                }
            }));
        }
        let job = job.clone();
        let cohort = store.state.config.cohorts[index].clone();
        drop(store);
        if let Some(native_id) = job.attempts.last().and_then(|a| a.native_id) {
            if job.intent.is_some()
                && runner.control.is_none()
                && (runner.cancel_submitted != Some(native_id)
                    || !runner.cancel_completed.load(Ordering::Acquire))
            {
                runner.cancel_submitted = Some(native_id);
                let completed = Arc::clone(&runner.cancel_completed);
                runner.control = Some(thread::spawn(move || {
                    // Resolution still belongs to the runner, which must return
                    // a native terminal/drain proof. A cancel RPC is not a proof.
                    completed.store(
                        native::cancel(&cohort, native_id).is_ok(),
                        Ordering::Release,
                    );
                }));
            } else if job.state == State::Starting
                && job.intent.is_none()
                && let Ok(Some(native_job)) = native::observe(&cohort, native_id)
                && native_job.state == ade_ops::JobState::Running
            {
                let mut store = manager.lock()?;
                if store.state.jobs[&id].state == State::Starting {
                    manager.edit(&mut store, |state, at| {
                        let job = state.jobs.get_mut(&id).ok_or(Error("CORRUPT_LEDGER"))?;
                        let attempt = job.attempts.last_mut().ok_or(Error("CORRUPT_LEDGER"))?;
                        update_native(attempt, &native_job)?;
                        attempt.running_ms = Some(at);
                        job.state = State::Running;
                        job.reason = "NATIVE_RUNNING".into();
                        Ok(())
                    })?;
                }
            }
        }
    }
    let mut store = manager.lock()?;
    let now = store.time()?;
    if let Some(decision) = policy::decide(&store.state, now)? {
        if let policy::Decision::Dispatch(_, cohort) = decision
            && (runners[cohort].running.is_some() || runners[cohort].control.is_some())
        {
            return Ok(());
        }
        let stage = match decision {
            policy::Decision::Dispatch(..) => "reserved_before_registration",
            policy::Decision::Preempt(..) => "preempt_persisted_before_cancel",
            _ => "",
        };
        manager.edit(&mut store, |state, _| policy::apply(state, decision, now))?;
        if !stage.is_empty() {
            fault(stage);
        }
    }
    Ok(())
}

fn socket(root: &Path) -> Result<UnixListener> {
    let path = root.join("control.sock");
    match fs::symlink_metadata(&path) {
        Ok(metadata) => {
            require(
                metadata.file_type().is_socket()
                    && metadata.uid() == rustix::process::geteuid().as_raw(),
                "UNSAFE_SOCKET",
            )?;
            fs::remove_file(&path).map_err(|_| Error("SOCKET_IO"))?;
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => return Err(Error("SOCKET_IO")),
    }
    let listener = UnixListener::bind(&path).map_err(|_| Error("SOCKET_BIND"))?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
        .map_err(|_| Error("SOCKET_IO"))?;
    listener
        .set_nonblocking(true)
        .map_err(|_| Error("SOCKET_IO"))?;
    Ok(listener)
}

/// Run the local control endpoint and bounded manager loop in the foreground.
///
/// # Errors
/// Refuses ambiguous ledger/socket ownership and stops on an unsafe durable update.
pub fn serve(root: &Path) -> Result<()> {
    let store = Store::open(root)?;
    let _cohort_leases = [
        native::lease(&store.state.config.cohorts[0])?,
        native::lease(&store.state.config.cohorts[1])?,
    ];
    let listener = socket(root)?;
    let manager = Arc::new(Manager {
        store: Mutex::new(store),
        fatal: AtomicBool::new(false),
    });
    let mut peers: Vec<JoinHandle<()>> = Vec::new();
    let mut runners: [Dispatch; 2] = Default::default();
    loop {
        drive(&manager, &mut runners)?;
        peers.retain(|join| !join.is_finished());
        for _ in 0..PEERS {
            match listener.accept() {
                Ok((stream, _)) if peers.len() < PEERS => {
                    let manager = Arc::clone(&manager);
                    peers.push(thread::spawn(move || {
                        process(&manager, stream);
                    }));
                }
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(_) => return Err(Error("SOCKET_IO")),
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn process(manager: &Manager, mut stream: UnixStream) {
    let response = match read_frame(&mut stream, CONTROL_TIMEOUT)
        .and_then(|bytes| decode::<Envelope>(&bytes))
        .and_then(|request| manager.request(&request))
    {
        Ok(data) => json!({"ok":true,"data":data}),
        Err(error) => json!({"ok":false,"code":error.0}),
    };
    if let Ok(bytes) = encode(&response) {
        let reply = if bytes.len() > FRAME {
            br#"{"ok":false,"code":"REPLY_TOO_LARGE"}"#.as_slice()
        } else {
            &bytes
        };
        let _ = write_frame(&mut stream, reply);
    }
}

fn read_exact(stream: &mut UnixStream, mut bytes: &mut [u8], deadline: Instant) -> Result<()> {
    while !bytes.is_empty() {
        let wait = deadline
            .checked_duration_since(Instant::now())
            .filter(|wait| !wait.is_zero())
            .ok_or(Error("PEER_TIMEOUT"))?;
        stream
            .set_read_timeout(Some(wait))
            .map_err(|_| Error("SOCKET_IO"))?;
        let n = stream.read(bytes).map_err(|_| Error("PEER_READ"))?;
        require(n > 0, "TRUNCATED_FRAME")?;
        bytes = &mut bytes[n..];
    }
    Ok(())
}
fn read_frame(stream: &mut UnixStream, timeout: Duration) -> Result<Vec<u8>> {
    let deadline = Instant::now() + timeout;
    let mut prefix = [0; 4];
    read_exact(stream, &mut prefix, deadline)?;
    let size = u32::from_be_bytes(prefix) as usize;
    require((1..=FRAME).contains(&size), "FRAME_TOO_LARGE")?;
    let mut bytes = vec![0; size];
    read_exact(stream, &mut bytes, deadline)?;
    Ok(bytes)
}

fn write_frame(stream: &mut UnixStream, bytes: &[u8]) -> Result<()> {
    require((1..=FRAME).contains(&bytes.len()), "FRAME_TOO_LARGE")?;
    let size = u32::try_from(bytes.len()).map_err(|_| Error("FRAME_TOO_LARGE"))?;
    let deadline = Instant::now() + CONTROL_TIMEOUT;
    for mut data in [&size.to_be_bytes()[..], bytes] {
        while !data.is_empty() {
            let wait = deadline
                .checked_duration_since(Instant::now())
                .filter(|wait| !wait.is_zero())
                .ok_or(Error("PEER_TIMEOUT"))?;
            stream
                .set_write_timeout(Some(wait))
                .map_err(|_| Error("SOCKET_IO"))?;
            let n = match stream.write(&data[..data.len().min(4096)]) {
                Ok(n) => n,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1));
                    continue;
                }
                Err(_) => return Err(Error("SOCKET_IO")),
            };
            require(n > 0, "SOCKET_IO")?;
            data = &data[n..];
        }
    }
    Ok(())
}

/// Exchange one bounded JSON request via the Unix socket.
///
/// # Errors
/// Refuses oversized requests, closed peers, malformed replies and timeouts.
pub fn client(path: &Path, body: impl Read) -> Result<Value> {
    let bytes = read_bounded(body, FRAME)?;
    let mut stream = UnixStream::connect(path).map_err(|_| Error("SOCKET_CONNECT"))?;
    write_frame(&mut stream, &bytes)?;
    decode(&read_frame(&mut stream, Duration::from_secs(125))?)
}
