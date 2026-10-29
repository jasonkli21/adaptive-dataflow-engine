//! Phase15 opt-in stateful composition; local working state is never a checkpoint.
pub mod checkpoint;
pub mod store;
use crate::{Engine, Error, Progress, Receipt, Result, StreamConfigV1};
use ade_exec::CancellationToken;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

pub(crate) fn check(ok: bool, why: &'static str) -> Result<()> {
    if ok { Ok(()) } else { Err(Error::State(why)) }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u32,
    pub job_id: Uuid,
    pub source: StreamConfigV1,
    pub topic_partitions: Vec<i32>,
    pub operator: String,
    pub window_ms: i64,
    pub late_policy: String,
    pub hash_version: u32,
    pub owners: usize,
    pub hot_bytes: usize,
    pub cold_bytes: u64,
    pub checkpoint_bytes: u64,
    pub max_generations: usize,
    pub checkpoint_every_records: u64,
    pub checkpoint_every_ms: u64,
    pub working_root: PathBuf,
    pub checkpoint_root: PathBuf,
}
impl Config {
    /// # Errors
    /// Refuses unknown fields, incompatible semantics and unbounded resources.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        check(bytes.len() <= 16384, "stateful config byte cap")?;
        let c: Self =
            serde_json::from_slice(bytes).map_err(|_| Error::State("strict stateful config"))?;
        c.validate()?;
        Ok(c)
    }
    /// # Errors
    /// All static constraints are checked before file/network/output effects.
    pub fn validate(&self) -> Result<()> {
        self.source.validate()?;
        check(
            self.version == 1
                && !self.job_id.is_nil()
                && self.operator == "tumbling_sum_count_v1"
                && self.late_policy == "receipt_drop_closed_v1"
                && self.hash_version == 1,
            "stateful version/operator/window/late/hash",
        )?;
        check(
            !self.topic_partitions.is_empty()
                && self.topic_partitions.len() <= 4
                && self
                    .topic_partitions
                    .iter()
                    .copied()
                    .eq(0..i32::try_from(self.topic_partitions.len()).unwrap_or(0))
                && self
                    .source
                    .starts
                    .iter()
                    .all(|s| self.topic_partitions.contains(&s.partition)),
            "full topic partition inventory",
        )?;
        check(
            self.window_ms > 0 && self.source.timers.is_empty(),
            "positive window/no transient timer registrations",
        )?;
        check([1, 2, 4].contains(&self.owners), "logical owners 1/2/4")?;
        check(
            (store::CHARGE * self.owners..=16 * 1024 * 1024).contains(&self.hot_bytes)
                && (128..=128 * 1024 * 1024).contains(&self.cold_bytes),
            "hot/cold caps",
        )?;
        check(
            (checkpoint::MIN_CHECKPOINT_BYTES..=256 * 1024 * 1024).contains(&self.checkpoint_bytes),
            "checkpoint byte quota minimum 66048..268435456",
        )?;
        check(
            (2..=64).contains(&self.max_generations),
            "checkpoint generation count",
        )?;
        check(
            (1..=1_000_000).contains(&self.checkpoint_every_records)
                && (100..=3_600_000).contains(&self.checkpoint_every_ms),
            "fixed checkpoint cadence",
        )?;
        for p in [&self.working_root, &self.checkpoint_root] {
            check(
                p.is_absolute()
                    && p.as_os_str().len() <= 1024
                    && p.components().all(|c| {
                        matches!(
                            c,
                            std::path::Component::RootDir | std::path::Component::Normal(_)
                        )
                    }),
                "absolute canonical roots",
            )?;
        }
        check(
            !self.working_root.starts_with(&self.checkpoint_root)
                && !self.checkpoint_root.starts_with(&self.working_root),
            "separate working/checkpoint roots",
        )
    }
    /// Immutable semantic/source fingerprint. Resume offsets, owners, resource knobs,
    /// endpoint/credentials and session UUID are deliberately excluded.
    /// # Errors
    /// Returns serialization errors without echoing sensitive configuration.
    pub fn fingerprint(&self) -> Result<String> {
        let v = serde_json::json!({"version":1,"job":self.job_id,"schema":"EventV1/u64/i64/i64","operator":self.operator,"width":self.window_ms,"late":self.late_policy,"hash":self.hash_version,
            "cluster":self.source.expected_cluster_id,"topic":self.source.topic,"partitions":self.source.starts.iter().map(|s|s.partition).collect::<Vec<_>>(),"topic_partitions":self.topic_partitions,"key_less_than":self.source.key_less_than,"allowance":self.source.allowance_ms,"history":"immutable-uncompressed-32768-v1"});
        Ok(
            blake3::hash(&serde_json::to_vec(&v).map_err(|_| Error::State("fingerprint"))?)
                .to_hex()
                .to_string(),
        )
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    pub start: i64,
    pub key: u64,
    pub sum: i64,
    pub count: u64,
}
impl Entry {
    #[must_use]
    pub fn id(self) -> (i64, u64) {
        (self.start, self.key)
    }
}
/// Stable hash contract: low five bits of BLAKE3(u64 little endian).
#[must_use]
pub fn shard(key: u64) -> usize {
    usize::from(blake3::hash(&key.to_le_bytes()).as_bytes()[0] & 31)
}
/// # Errors
/// Refuses alignment/end outside signed milliseconds.
pub fn window(time: i64, width: i64) -> Result<(i64, i64)> {
    check(width > 0, "window width")?;
    let start = time
        .div_euclid(width)
        .checked_mul(width)
        .ok_or(Error::Overflow("window start"))?;
    Ok((start, end(start, width)?))
}
pub(crate) fn end(start: i64, width: i64) -> Result<i64> {
    start
        .checked_add(width)
        .ok_or(Error::Overflow("window end"))
}
#[derive(Debug, Clone, Serialize)]
pub struct WindowOutput {
    pub r#type: &'static str,
    pub output_version: u32,
    pub job_id: Uuid,
    pub key: u64,
    pub window_start: i64,
    pub window_end: i64,
    pub sum_i64: i64,
    pub count_u64: u64,
    pub output_id: String,
    /// Canonical semantic payload token; compare it when an `output_id` repeats.
    pub payload_fingerprint: String,
}
pub struct State {
    pub config: Config,
    pub store: store::Store,
    pub closed_window_late: u64,
    pub last_closed_late: bool,
    input_pending: bool,
    pending_close: Option<Entry>,
    pub generation: Option<checkpoint::Pointer>,
    #[cfg(feature = "adaptive-checkpoint")]
    pub adaptive: Option<Box<crate::adaptive::Adaptive>>,
    last_records: u64,
    last_ms: u64,
    pub test_pause_at: Option<String>,
    failed: bool,
    _lock: checkpoint::Lock,
}
impl State {
    /// # Errors
    /// Refuses unsafe/occupied roots; restore is fully verified before rebuilding.
    pub fn open(
        config: Config,
        session: Uuid,
        token: CancellationToken,
        restore: bool,
    ) -> Result<(Self, Option<Progress>)> {
        Self::open_version(config, session, token, restore, false)
    }
    fn open_version(
        config: Config,
        session: Uuid,
        token: CancellationToken,
        restore: bool,
        v2: bool,
    ) -> Result<(Self, Option<Progress>)> {
        config.validate()?;
        check(
            v2 || !config.checkpoint_root.join("CURRENT2").exists(),
            "v2 root requires adaptive mode",
        )?;
        let lock = checkpoint::Lock::acquire(&config)?;
        let loaded = if restore {
            Some(checkpoint::load(&config, &token)?)
        } else {
            check(
                !config.checkpoint_root.join("CURRENT").exists(),
                "existing checkpoint requires explicit restore",
            )?;
            None
        };
        let mut store = store::Store::new(&config, session, token)?;
        let mut progress = None;
        let mut generation = None;
        let mut closed_window_late = 0;
        let mut last_records = 0;
        if let Some((pointer, manifest)) = loaded {
            // Validation above completed for every shard before any state is adopted.
            for s in 0..store::SHARDS {
                let path = config
                    .checkpoint_root
                    .join(pointer.generation.to_string())
                    .join(format!("shard-{s:02}.bin"));
                let mut f = store::regular(&path, config.checkpoint_bytes)?;
                while let Some(e) = store::read(&mut f)? {
                    store.restore_entry(e)?;
                }
            }
            let mut p = manifest.progress;
            p.session = session;
            for c in p.partitions.values_mut() {
                c.start = c.next_offset_suggestion;
                c.seen = c.forwarded;
            }
            last_records = p.forwarded_records;
            closed_window_late = manifest.closed_window_late;
            generation = Some(pointer);
            progress = Some(p);
        }
        Ok((
            Self {
                config,
                store,
                closed_window_late,
                last_closed_late: false,
                input_pending: false,
                pending_close: None,
                generation,
                #[cfg(feature = "adaptive-checkpoint")]
                adaptive: None,
                last_records,
                last_ms: 0,
                test_pause_at: None,
                failed: false,
                _lock: lock,
            },
            progress,
        ))
    }
    pub(crate) fn apply(&mut self, r: &Receipt, frontier: Option<i64>) -> Result<()> {
        check(
            !self.failed && !self.input_pending && self.pending_close.is_none(),
            "state input/close pending",
        )?;
        self.input_pending = true;
        self.last_closed_late = false;
        if let Some(e) = &r.projected {
            let (start, end) = window(e.event_time_ms, self.config.window_ms)?;
            if frontier.is_some_and(|f| end <= f) {
                self.closed_window_late = self
                    .closed_window_late
                    .checked_add(1)
                    .ok_or(Error::Overflow("closed late"))?;
                self.last_closed_late = true;
            } else {
                let old = self.store.get((start, e.key))?;
                let mut next = old.unwrap_or(Entry {
                    start,
                    key: e.key,
                    sum: 0,
                    count: 0,
                });
                next.sum = next
                    .sum
                    .checked_add(e.value_i64)
                    .ok_or(Error::Overflow("window sum"))?;
                next.count = next
                    .count
                    .checked_add(1)
                    .ok_or(Error::Overflow("window count"))?;
                self.store.put(next, old.is_none())?;
            }
        }
        Ok(())
    }
    pub(crate) fn poison(&mut self) {
        self.failed = true;
    }
    pub(crate) fn confirm_input(&mut self) {
        self.input_pending = false;
    }
    /// One outstanding close record; scanning uses fixed scratch and no output queue.
    /// # Errors
    /// Refuses corrupt tiers, arithmetic overflow or an unmatched input.
    pub fn next_close(&mut self, frontier: Option<i64>) -> Result<Option<WindowOutput>> {
        check(
            !self.failed && !self.input_pending,
            "failed attempt/input receipt pending",
        )?;
        if self.pending_close.is_none() {
            self.pending_close = self.store.first_closed(frontier, self.config.window_ms)?;
        }
        let Some(e) = self.pending_close else {
            return Ok(None);
        };
        let end = end(e.start, self.config.window_ms)?;
        Ok(Some(WindowOutput {
            r#type: "window",
            output_version: 1,
            job_id: self.config.job_id,
            key: e.key,
            window_start: e.start,
            window_end: end,
            sum_i64: e.sum,
            count_u64: e.count,
            output_id: format!("{}:{}:{}:{}:1", self.config.job_id, e.key, e.start, end),
            payload_fingerprint: format!("window-payload-v1:{}:{}", e.sum, e.count),
        }))
    }
    /// # Errors
    /// Only called after the whole close frame reaches the sink.
    pub fn confirm_close(&mut self) -> Result<()> {
        let e = self.pending_close.ok_or(Error::State("no pending close"))?;
        self.store.remove(e)?;
        self.pending_close = None;
        Ok(())
    }
    #[must_use]
    pub fn has_pending_close(&self) -> bool {
        self.pending_close.is_some()
    }
    #[must_use]
    pub fn due(&self, p: &Progress, ms: u64) -> bool {
        let changed = p.forwarded_records != self.last_records;
        #[cfg(feature = "adaptive-checkpoint")]
        if let Some(a) = &self.adaptive {
            return a.due(
                p.forwarded_records.saturating_sub(self.last_records),
                ms.saturating_sub(self.last_ms),
            );
        }
        changed
            && (p.forwarded_records.saturating_sub(self.last_records)
                >= self.config.checkpoint_every_records
                || ms.saturating_sub(self.last_ms) >= self.config.checkpoint_every_ms)
    }
    /// # Errors
    /// Refuses any incomplete cut; source demand must already be paused by caller.
    pub fn checkpoint(
        &mut self,
        p: &Progress,
        ms: u64,
        hook: &mut dyn FnMut(&str) -> Result<()>,
    ) -> Result<checkpoint::Pointer> {
        check(
            !self.failed
                && !self.input_pending
                && self.pending_close.is_none()
                && self
                    .store
                    .first_closed(p.frontier, self.config.window_ms)?
                    .is_none(),
            "checkpoint requires quiescent cut",
        )?;
        #[cfg(feature = "adaptive-checkpoint")]
        if self.adaptive.is_some() {
            check(p.forwarded_records > self.last_records, "v2 unchanged cut")?;
        }
        self.failed = true;
        #[cfg(feature = "adaptive-checkpoint")]
        let pointer = if let Some(mut a) = self.adaptive.take() {
            let result = crate::adaptive::snapshot::publish(self, &mut a, p, ms, hook);
            self.adaptive = Some(a);
            result?
        } else {
            checkpoint::publish(self, p, hook)?
        };
        #[cfg(not(feature = "adaptive-checkpoint"))]
        let pointer = checkpoint::publish(self, p, hook)?;
        self.failed = false;
        self.last_records = p.forwarded_records;
        self.last_ms = ms;
        self.generation = Some(pointer.clone());
        Ok(pointer)
    }
}
impl Engine {
    /// # Errors
    /// Opens fresh/verified restored state before Kafka assignment/output.
    pub fn stateful(
        mut config: Config,
        session: Uuid,
        token: CancellationToken,
        restore: bool,
    ) -> Result<Self> {
        let (state, progress) = State::open(config.clone(), session, token, restore)?;
        if let Some(p) = &progress {
            for start in &mut config.source.starts {
                start.offset = p.partitions[&start.partition].next_offset_suggestion;
            }
        }
        let mut engine = Self::new(config.source, session)?;
        if let Some(p) = progress {
            engine.progress = p;
        }
        engine.state = Some(state);
        Ok(engine)
    }
}

#[cfg(feature = "adaptive-checkpoint")]
impl Engine {
    /// Open a new v2 job or explicitly recover a completely verified selected copy.
    /// The CLI verifies live Kafka origin/restore ranges before assignment/output.
    /// # Errors
    /// Refuses mixed roots, unsafe policy, corrupt generations and unconsented replay.
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        clippy::needless_pass_by_value,
        reason = "Pre-output admission transaction"
    )]
    pub fn adaptive_stateful(
        config: Config,
        policy: crate::adaptive::Policy,
        session: Uuid,
        token: CancellationToken,
        recover: bool,
        from: crate::adaptive::policy::Placement,
        certificate: Option<crate::adaptive::snapshot::Selector>,
    ) -> Result<(Self, String)> {
        use crate::adaptive::{
            Adaptive,
            policy::{Observation, Placement},
            snapshot,
        };
        policy.validate_config(&config)?;
        #[cfg(feature = "remote-checkpoint")]
        if (!recover && policy.remote.is_some())
            || (recover && from == crate::adaptive::policy::Placement::Remote)
        {
            crate::adaptive::remote::validate_environment()?;
        }

        check(
            !config.checkpoint_root.join("CURRENT").exists(),
            "v1/v2 mixed root refused",
        )?;
        let mut disk_loss =
            certificate.is_some() && !config.checkpoint_root.join("CURRENT2").exists();
        check(
            recover || (!config.checkpoint_root.join("CURRENT2").exists() && certificate.is_none()),
            "existing v2 cut requires explicit recovery",
        )?;
        let selector = if recover {
            let selected = if let Some(s) = certificate {
                check(
                    s.version == 2
                        && snapshot::hash(&snapshot::encode(&s.certificate)?)
                            == s.certificate_blake3,
                    "certificate hash/version",
                )?;
                if config.checkpoint_root.join("CURRENT2").exists() {
                    check(
                        snapshot::selection(&config)?.certificate_blake3 == s.certificate_blake3,
                        "newer/different selected certificate",
                    )?;
                }
                s
            } else {
                snapshot::selection(&config)?
            };
            Some(selected)
        } else {
            None
        };
        if selector
            .as_ref()
            .is_some_and(|s| s.certificate.placement.remote())
            && !config.checkpoint_root.join("REMOTE-LEDGER").exists()
        {
            disk_loss = true;
        }
        let (state, _) = State::open_version(config.clone(), session, token.clone(), false, true)?;
        let mut engine = Self::new(config.source.clone(), session)?;
        engine.state = Some(state);
        let mut a = Adaptive {
            policy,
            current: None,
            current_hash: None,
            decision: None,
            observation: Observation::default(),
            last_action: None,
            last_metrics: snapshot::Metrics::default(),
            disk_loss_restore: disk_loss,
            selected_copy: from,
            recovery_decision: None,
            #[cfg(feature = "remote-checkpoint")]
            client: None,
        };
        let mut recovery = "fresh".to_string();
        if let Some(s) = selector {
            #[cfg(feature = "remote-checkpoint")]
            if from == Placement::Remote {
                let remote = s
                    .certificate
                    .remote
                    .as_ref()
                    .ok_or(crate::Error::State("selected cut has no remote copy"))?;
                a.client = Some(crate::adaptive::remote::Client::new(
                    remote,
                    &a.policy.limits,
                    None,
                    token.clone(),
                )?);
                let cert_bytes = snapshot::encode(&s.certificate)?;
                a.client
                    .as_mut()
                    .ok_or(crate::Error::State("remote client"))?
                    .fetch(
                        &format!("{}/certificate.json", s.certificate.manifest.generation),
                        cert_bytes.len() as u64,
                        &s.certificate_blake3,
                    )?;
            }
            let manifest = snapshot::verify(&mut a, &config, &s.certificate, from, &token)?;
            let choice = crate::adaptive::policy::recovery(
                &a.policy,
                &s.certificate_blake3,
                manifest.shards.iter().map(|s| s.bytes).sum(),
                manifest.progress.forwarded_records,
                from,
            )?;
            let replay = choice.chosen == "full_replay_explicit_conflict_consent";
            a.recovery_decision = Some(choice);
            if replay {
                recovery = "full_replay_explicit_conflict_consent".into();
            } else {
                snapshot::hydrate(
                    &mut a,
                    engine.state.as_mut().ok_or(crate::Error::State("state"))?,
                    &manifest,
                    from,
                )?;
                let mut p = manifest.progress;
                p.session = session;
                for cursor in p.partitions.values_mut() {
                    cursor.start = cursor.next_offset_suggestion;
                    cursor.seen = cursor.forwarded;
                }
                let state = engine.state.as_mut().ok_or(crate::Error::State("state"))?;
                state.last_records = p.forwarded_records;
                state.closed_window_late = manifest.closed_window_late;
                for start in &mut engine.config.starts {
                    start.offset = p.partitions[&start.partition].next_offset_suggestion;
                }
                engine.progress = p;
                recovery = "checkpoint_restore".into();
            }
            engine
                .state
                .as_mut()
                .ok_or(crate::Error::State("state"))?
                .generation = Some(checkpoint::Pointer {
                version: 2,
                generation: s.certificate.manifest.generation,
                manifest_blake3: s.certificate.manifest.blake3.clone(),
            });
            a.current = Some(s.certificate);
            a.current_hash = Some(s.certificate_blake3);
            // A clean disk-loss root receives only the trusted selector, never local shard copies.
            if disk_loss {
                snapshot::install_certificate(
                    &config,
                    &snapshot::Selector {
                        version: 2,
                        certificate: a
                            .current
                            .clone()
                            .ok_or(crate::Error::State("certificate"))?,
                        certificate_blake3: a
                            .current_hash
                            .clone()
                            .ok_or(crate::Error::State("certificate"))?,
                    },
                )?;
            }
        }
        #[cfg(feature = "remote-checkpoint")]
        if !recover && let Some(remote) = a.policy.remote.as_ref() {
            let mut client = crate::adaptive::remote::Client::new(
                remote,
                &a.policy.limits,
                Some(&config.checkpoint_root),
                token,
            )?;
            client.probe()?;
            a.client = Some(client);
        }
        engine
            .state
            .as_mut()
            .ok_or(crate::Error::State("state"))?
            .adaptive = Some(Box::new(a));
        Ok((engine, recovery))
    }
}
