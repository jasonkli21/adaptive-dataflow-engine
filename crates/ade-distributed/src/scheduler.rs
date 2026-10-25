//! File leases in one fixed producer/final registry. Policy never mutates ownership.
use crate::{
    Error, Result, TaskConfig, WorkerEndpoint,
    ids::{JobId, MorselId, WorkerId},
    lineage::{SourceMorsel, SourceSnapshot},
    protocol, require,
};
use ade_exec::CancellationToken;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    net::{SocketAddr, TcpListener},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Policy {
    #[default]
    Static,
    LeasedNoSteal,
    LeasedV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Participation {
    pub after_completed: usize,
    pub worker: WorkerId,
    pub action: Action,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Activate,
    Drain,
    SpotNotice { grace_ms: u64 },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    #[cfg(feature = "research-hooks")]
    pub research_delay_worker: Option<u64>,
    #[cfg(feature = "research-hooks")]
    pub research_delay_ms: u64,
    #[cfg(feature = "research-hooks")]
    pub research_exit_after_emit_worker: Option<u64>,
    #[cfg(feature = "research-hooks")]
    pub research_exit_final_worker: Option<u64>,
    #[cfg(feature = "research-hooks")]
    pub research_no_heartbeat_worker: Option<u64>,
    pub policy: Policy,
    pub recovery_enabled: bool,
    pub max_morsels: usize,
    pub lease_ms: u64,
    pub heartbeat_ms: u64,
    pub max_control_messages: u64,
    pub audit_capacity: usize,
    pub min_steal_bytes: u64,
    pub capacities: Vec<u64>,
    pub locality: Vec<String>,
    pub preferred_locality: Vec<String>,
    /// Zero means all registered producers start active.
    pub initial_active: usize,
    pub participation: Vec<Participation>,
    pub max_source_bytes: u64,
    pub max_input_rows: u64,
    pub max_exchange_bytes: u64,
    pub max_exchange_frames: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            #[cfg(feature = "research-hooks")]
            research_delay_worker: None,
            #[cfg(feature = "research-hooks")]
            research_delay_ms: 0,
            #[cfg(feature = "research-hooks")]
            research_exit_after_emit_worker: None,
            #[cfg(feature = "research-hooks")]
            research_exit_final_worker: None,
            #[cfg(feature = "research-hooks")]
            research_no_heartbeat_worker: None,
            policy: Policy::Static,
            recovery_enabled: true,
            max_morsels: 256,
            lease_ms: 10_000,
            heartbeat_ms: 250,
            max_control_messages: 100_000,
            audit_capacity: 256,
            min_steal_bytes: 1,
            capacities: vec![],
            locality: vec![],
            preferred_locality: vec![],
            initial_active: 0,
            participation: vec![],
            max_source_bytes: 1 << 30,
            max_input_rows: 100_000_000,
            max_exchange_bytes: 1 << 30,
            max_exchange_frames: 1_000_000,
        }
    }
}
impl Config {
    #[must_use]
    pub fn leased(&self) -> bool {
        self.policy != Policy::Static
    }
    /// # Errors
    /// Refuses unbounded/invalid controls and unsupported participation schedules.
    pub fn validate(&self, workers: usize) -> Result<()> {
        require((2..=4).contains(&workers), "scheduler worker registry")?;
        require(
            (1..=1024).contains(&self.max_morsels) && (1..=512).contains(&self.audit_capacity),
            "scheduler metadata bounds",
        )?;
        require(
            (20..=1000).contains(&self.heartbeat_ms)
                && self.lease_ms >= 4 * self.heartbeat_ms
                && self.lease_ms <= 120_000,
            "scheduler lease/heartbeat bounds",
        )?;
        require(
            (100..=1_000_000).contains(&self.max_control_messages)
                && self.min_steal_bytes <= (1 << 40),
            "scheduler control/steal bounds",
        )?;
        require(
            self.initial_active <= workers && self.participation.len() <= 16,
            "scheduler participation bounds",
        )?;
        require(
            self.capacities.is_empty()
                || self.capacities.len() == workers
                    && self.capacities.iter().all(|c| (1..=1000).contains(c)),
            "scheduler capacities must be integer weights1..1000 per worker",
        )?;
        require(
            self.locality.is_empty() || self.locality.len() == workers,
            "scheduler locality registry",
        )?;
        require(
            self.preferred_locality.len() <= self.max_morsels
                && self
                    .locality
                    .iter()
                    .chain(&self.preferred_locality)
                    .all(|s| {
                        s.len() <= 64
                            && s.bytes()
                                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                    }),
            "scheduler locality labels",
        )?;
        require(
            (1..=1 << 40).contains(&self.max_source_bytes)
                && (1..=100_000_000).contains(&self.max_input_rows)
                && (1..=1 << 30).contains(&self.max_exchange_bytes)
                && (1..=1_000_000).contains(&self.max_exchange_frames),
            "scheduler ingress bounds",
        )?;
        for e in &self.participation {
            require(
                e.worker.0 < workers as u64
                    && e.after_completed > 0
                    && e.after_completed <= self.max_morsels,
                "scheduler participation event",
            )?;
            if let Action::SpotNotice { grace_ms } = e.action {
                require((1..=120_000).contains(&grace_ms), "scheduler spot grace")?;
            }
        }
        require(
            self.participation
                .windows(2)
                .all(|w| w[0].after_completed <= w[1].after_completed),
            "scheduler events must be ordered",
        )?;
        #[cfg(feature = "research-hooks")]
        require(
            self.research_delay_worker
                .is_none_or(|w| w < workers as u64)
                && self
                    .research_exit_after_emit_worker
                    .is_none_or(|w| w < workers as u64)
                && self
                    .research_exit_final_worker
                    .is_none_or(|w| w < workers as u64)
                && self
                    .research_no_heartbeat_worker
                    .is_none_or(|w| w < workers as u64)
                && self.research_delay_ms <= 10_000
                && (self.research_delay_ms == 0 || self.research_delay_worker.is_some()),
            "scheduler research hook bounds",
        )?;
        require(
            self.policy != Policy::LeasedNoSteal
                || self.initial_active == 0 && self.participation.is_empty(),
            "no-steal control requires all producers active without participation events",
        )?;
        if self.leased() {
            require(
                !self.recovery_enabled,
                "leased scheduler requires recovery_enabled=false",
            )?;
        }
        Ok(())
    }
}
/// Pure policy observation: estimated work is physical bytes, capacity is declared.
#[derive(Debug, Clone)]
pub struct Observation {
    pub pending: Vec<(MorselId, usize, u64, bool)>,
    pub queued: Vec<u64>,
    pub capacities: Vec<u64>,
    pub requester: usize,
    pub critical: bool,
    pub steal: bool,
    pub min_steal_bytes: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decision {
    pub morsel: MorselId,
    pub preferred: WorkerId,
    pub actual: WorkerId,
    pub normalized_queue: u64,
    pub locality_match: bool,
    pub critical: bool,
    pub reason: String,
}
/// # Errors
/// Invalid indices, weights and overflowing score observations fail explicitly.
pub fn decide(o: &Observation) -> Result<Option<Decision>> {
    require(
        (2..=4).contains(&o.capacities.len())
            && o.queued.len() == o.capacities.len()
            && o.requester < o.queued.len()
            && o.capacities.iter().all(|c| (1..=1000).contains(c)),
        "invalid scheduling observation",
    )?;
    let own = o.pending.iter().any(|(_, p, _, _)| *p == o.requester);
    let mut candidates = Vec::new();
    for &(id, p, bytes, local) in &o.pending {
        require(
            p < o.queued.len() && bytes > 0 && bytes <= o.queued[p],
            "bogus scheduling work observation",
        )?;
        if p != o.requester && (!o.steal || bytes < o.min_steal_bytes) {
            continue;
        }
        let score = u64::try_from(u128::from(o.queued[p]) * 1000 / u128::from(o.capacities[p]))
            .map_err(|_| Error::Invalid("scheduler score overflow".into()))?;
        if own && p != o.requester {
            let projected_requester = (u128::from(o.queued[o.requester]) + u128::from(bytes))
                * 1000
                / u128::from(o.capacities[o.requester]);
            let projected_donor =
                u128::from(o.queued[p] - bytes) * 1000 / u128::from(o.capacities[p]);
            if projected_requester.max(projected_donor) * 10 >= u128::from(score) * 9 {
                continue;
            }
        }
        candidates.push((id, p, bytes, local, score));
    }
    candidates.sort_by(|a, b| {
        let ka = if o.critical {
            (a.4, u64::from(a.3), a.2)
        } else {
            (u64::from(a.3), a.4, a.2)
        };
        let kb = if o.critical {
            (b.4, u64::from(b.3), b.2)
        } else {
            (u64::from(b.3), b.4, b.2)
        };
        kb.cmp(&ka).then(a.0.cmp(&b.0)).then(a.1.cmp(&b.1))
    });
    Ok(candidates
        .first()
        .map(|&(id, p, _, local, score)| Decision {
            morsel: id,
            preferred: WorkerId(p as u64),
            actual: WorkerId(o.requester as u64),
            normalized_queue: score,
            locality_match: local,
            critical: o.critical,
            reason: if p == o.requester {
                "preferred_queue"
            } else {
                "steal_pending_longest_queue"
            }
            .into(),
        }))
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fence {
    pub job: JobId,
    pub execution: String,
    pub epoch: u64,
    pub digest: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub fence: Fence,
    pub address: SocketAddr,
}
impl Spec {
    pub(crate) fn validate(
        &self,
        job: JobId,
        execution: &str,
        workers: &[WorkerEndpoint],
        config: &TaskConfig,
        source: &SourceSnapshot,
    ) -> Result<()> {
        require(
            self.address.ip().is_loopback()
                && self.address.port() != 0
                && self.fence.job == job
                && self.fence.execution == execution
                && self.fence.epoch == 1,
            "schedule scope/endpoint mismatch",
        )?;
        require(
            self.fence.digest == digest(job, execution, workers, config, source)?,
            "schedule digest mismatch",
        )
    }
}
fn digest(
    job: JobId,
    execution: &str,
    workers: &[WorkerEndpoint],
    config: &TaskConfig,
    source: &SourceSnapshot,
) -> Result<String> {
    let mut h = blake3::Hasher::new();
    h.update(b"ade-scheduler-v1-epoch1-hash1-fixed-finals");
    h.update(&serde_json::to_vec(&(
        job, execution, workers, config, source,
    ))?);
    Ok(h.finalize().to_hex().to_string())
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lease {
    pub worker: WorkerId,
    pub nonce: u64,
    pub morsel: SourceMorsel,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub lease: Lease,
    pub input_rows: u64,
    pub output_rows: u64,
    pub work_ns: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerState {
    Parked,
    Active,
    Draining,
    Drained,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    pub morsel: SourceMorsel,
    pub preferred: WorkerId,
    pub lease: Option<Lease>,
    pub completion: Option<Completion>,
    pub granted_ms: u64,
    pub completed_ms: u64,
    pub granted_sequence: u64,
    pub completed_sequence: u64,
    pub renewals: u64,
    pub last_renewed_ms: u64,
    #[serde(skip)]
    expires_ms: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub at_ms: u64,
    pub sequence: u64,
    pub worker: WorkerId,
    pub action: Action,
    pub completed: usize,
    pub drained_ms: Option<u64>,
    pub in_flight: Option<MorselId>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metrics {
    pub task: Option<TaskConfig>,
    pub registry: Vec<WorkerEndpoint>,
    pub duration_ms: u64,
    pub fence: Fence,
    pub config: Config,
    pub source: SourceSnapshot,
    pub entries: Vec<Entry>,
    pub decisions: VecDeque<Decision>,
    pub dropped_decisions: u64,
    pub events: Vec<Event>,
    pub workers: Vec<WorkerState>,
    pub control_messages: u64,
    pub control_bytes: u64,
    pub renewals: u64,
    pub peak_outstanding: usize,
}
mod ledger;
use ledger::Ledger;
mod control;
pub(crate) use control::{Client, Service, verify_sources};
use control::{Op, Response, Rpc};
mod evidence;
pub use evidence::verify_metrics;
#[cfg(test)]
mod tests;
