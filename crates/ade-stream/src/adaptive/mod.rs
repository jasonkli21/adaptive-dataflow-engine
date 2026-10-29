//! Default-off v2 checkpoints. The accepted v1 format is never reinterpreted.
#[cfg(feature = "remote-checkpoint")]
mod http;
pub mod policy;
#[cfg(feature = "remote-checkpoint")]
pub mod remote;
pub mod snapshot;
use crate::{
    Result, Start,
    state::{Config, check},
};
use policy::{Action, Constraints, Decision, Mode, Observation, Placement, Prices};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Remote {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub prefix: String,
    pub namespace: uuid::Uuid,
    pub dev_allow_http: bool,
}
impl Remote {
    /// # Errors
    /// Explicit signed endpoint only; secret-bearing or unsafe URLs refuse.
    pub fn validate(&self) -> Result<()> {
        check(!self.namespace.is_nil(), "remote namespace")?;
        for s in [&self.bucket, &self.region, &self.prefix] {
            check(
                (3..=63).contains(&s.len())
                    && s.bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                    && s.as_bytes()[0].is_ascii_alphanumeric()
                    && s.as_bytes()[s.len() - 1].is_ascii_alphanumeric(),
                "remote canonical component",
            )?;
        }
        #[cfg(feature = "remote-checkpoint")]
        {
            let u =
                url::Url::parse(&self.endpoint).map_err(|_| crate::Error::State("remote URL"))?;
            check(
                self.endpoint.len() <= 256
                    && u.username().is_empty()
                    && u.password().is_none()
                    && u.query().is_none()
                    && u.fragment().is_none()
                    && u.path() == "/"
                    && u.host_str().is_some(),
                "remote bare endpoint",
            )?;
            check(
                u.scheme() == "https"
                    || (u.scheme() == "http"
                        && self.dev_allow_http
                        && u.host_str().is_some_and(|h| {
                            h.trim_matches(['[', ']'])
                                .parse::<std::net::IpAddr>()
                                .is_ok_and(|ip| ip.is_loopback())
                        })),
                "remote TLS/literal loopback",
            )?;
            Ok(())
        }
        #[cfg(not(feature = "remote-checkpoint"))]
        {
            Err(crate::Error::State("remote-checkpoint feature required"))
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub object_bytes: u64,
    pub remote_objects: u64,
    pub remote_write_bytes: u64,
    pub requests: u64,
    pub response_bytes: u64,
    pub io_timeout_ms: u64,
    pub trace_bytes: u64,
}
impl Limits {
    /// # Errors
    /// Refuses unbounded envelopes before any allocation or I/O.
    pub fn validate(&self) -> Result<()> {
        check(
            (32768..=1024 * 1024).contains(&self.object_bytes)
                && (40..=8192).contains(&self.remote_objects)
                && (65536..=256 * 1024 * 1024).contains(&self.remote_write_bytes)
                && (128..=65536).contains(&self.requests)
                && (1024 * 1024..=64 * 1024 * 1024 * 1024).contains(&self.response_bytes)
                && (100..=5000).contains(&self.io_timeout_ms)
                && (8192..=64 * 1024 * 1024).contains(&self.trace_bytes),
            "remote resource limits",
        )
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Recovery {
    pub original_starts: Vec<Start>,
    pub trusted_immutable_history: bool,
    pub allow_replay: bool,
    pub accept_duplicate_and_conflicting_outputs: bool,
    /// Explicit consent to replay instead of this exact committed certificate.
    pub replay_certificate_blake3: Option<String>,
    pub replay_estimated_ms: u64,
    pub restore_estimated_ms: u64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub version: u32,
    pub mode: Mode,
    pub min_records: u64,
    pub max_records: u64,
    pub min_ms: u64,
    pub max_ms: u64,
    pub max_depth: u64,
    pub min_samples: u64,
    pub cooldown_ms: u64,
    pub hysteresis_nano: u64,
    pub placements: Vec<Placement>,
    pub static_placement: Placement,
    pub constraints: Constraints,
    pub prices: Prices,
    pub limits: Limits,
    pub remote: Option<Remote>,
    pub recovery: Recovery,
}
impl Policy {
    /// # Errors
    /// Strict descriptor admission, including duplicate/unknown fields.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        check(bytes.len() <= 16384, "policy byte cap")?;
        let p: Self = serde_json::from_slice(bytes)
            .map_err(|_| crate::Error::State("strict adaptive policy"))?;
        p.validate()?;
        Ok(p)
    }
    /// # Errors
    /// Reject unsafe combinations, including an inadmissible fallback.
    #[allow(
        clippy::too_many_lines,
        reason = "Single strict admission transition before effects"
    )]
    pub fn validate(&self) -> Result<()> {
        check(
            self.version == 1
                && (1..=1_000_000).contains(&self.min_records)
                && self.min_records <= self.max_records
                && self.max_records <= 1_000_000
                && (100..=3_600_000).contains(&self.min_ms)
                && self.min_ms <= self.max_ms
                && self.max_ms <= 3_600_000
                && (1..=4).contains(&self.max_depth)
                && (2..=64).contains(&self.min_samples)
                && self.cooldown_ms <= 3_600_000
                && self.hysteresis_nano <= 1_000_000_000_000,
            "policy version/cadence/calibration bounds",
        )?;
        check(
            !self.placements.is_empty()
                && self.placements.len() <= 3
                && self
                    .placements
                    .iter()
                    .enumerate()
                    .all(|(i, p)| !self.placements[..i].contains(p))
                && self.placements.contains(&self.static_placement),
            "policy placements/control",
        )?;
        let c = &self.constraints;
        check(
            [1, 2].contains(&c.min_copies)
                && [
                    c.max_loss_ppm,
                    c.local_loss_ppm,
                    c.remote_loss_ppm,
                    c.both_loss_ppm,
                    c.failure_ppm,
                ]
                .iter()
                .all(|x| *x <= 1_000_000)
                && c.both_loss_ppm <= c.local_loss_ppm.min(c.remote_loss_ppm)
                && (1..=86_400_000).contains(&c.recovery_slo_ms)
                && c.retention_ms <= 86_400_000,
            "risk/SLO bounds",
        )?;
        let loss = match self.static_placement {
            Placement::Local => c.local_loss_ppm,
            Placement::Remote => c.remote_loss_ppm,
            Placement::Both => c.both_loss_ppm,
        };
        check(
            self.static_placement.copies() >= c.min_copies
                && (!c.require_remote || self.static_placement.remote())
                && loss <= c.max_loss_ppm,
            "fallback durability floor",
        )?;
        for price in [
            self.prices.local_write_byte,
            self.prices.local_read_byte,
            self.prices.remote_read_byte,
            self.prices.remote_write_byte,
            self.prices.remote_request,
            self.prices.capacity_byte_ms,
            self.prices.compute_ms,
            self.prices.replay_record,
        ] {
            check(price <= 1_000_000_000, "price bound")?;
        }
        self.limits.validate()?;
        check(
            self.placements.iter().any(|p| p.remote()) == self.remote.is_some(),
            "remote descriptor iff remote allowed",
        )?;
        if let Some(r) = &self.remote {
            r.validate()?;
        }
        let r = &self.recovery;
        check(
            r.trusted_immutable_history
                && !r.original_starts.is_empty()
                && r.original_starts.len() <= 4
                && r.original_starts.iter().enumerate().all(|(i, s)| {
                    s.partition >= 0
                        && s.partition < 4
                        && s.offset >= 0
                        && s.offset < i64::MAX
                        && (i == 0 || s.partition > r.original_starts[i - 1].partition)
                }),
            "trusted original source inventory",
        )?;
        check(
            !r.allow_replay
                || (r.accept_duplicate_and_conflicting_outputs
                    && r.replay_certificate_blake3
                        .as_deref()
                        .is_some_and(snapshot::digest)),
            "replay requires exact cut/duplicate/conflict consent",
        )?;
        check(
            r.replay_estimated_ms > 0
                && r.restore_estimated_ms > 0
                && r.replay_estimated_ms <= 86_400_000
                && r.restore_estimated_ms <= 86_400_000,
            "recovery estimates bounded",
        )
    }
    /// # Errors
    /// Origin semantics are pinned independently from v1's offset-excluding identity.
    pub fn validate_config(&self, c: &Config) -> Result<()> {
        self.validate()?;
        c.validate()?;
        check(
            c.checkpoint_bytes >= 262_144,
            "v2 minimum checkpoint quota 262144",
        )?;
        check(
            self.recovery
                .original_starts
                .iter()
                .map(|s| (s.partition, s.offset))
                .eq(c.source.starts.iter().map(|s| (s.partition, s.offset))),
            "config must retain exact original starts",
        )
    }
}
pub struct Adaptive {
    pub policy: Policy,
    pub current: Option<snapshot::Certificate>,
    pub current_hash: Option<String>,
    pub decision: Option<Decision>,
    pub observation: Observation,
    pub last_action: Option<(Action, u64)>,
    pub last_metrics: snapshot::Metrics,
    #[cfg(feature = "remote-checkpoint")]
    pub client: Option<remote::Client>,
    pub disk_loss_restore: bool,
    pub selected_copy: Placement,
    pub recovery_decision: Option<policy::RecoveryDecision>,
}
impl Adaptive {
    #[must_use]
    pub fn due(&self, changed: u64, elapsed: u64) -> bool {
        if changed == 0 {
            return false;
        }
        let (records, ms) = self
            .last_action
            .as_ref()
            .map_or((self.policy.min_records, self.policy.min_ms), |(a, _)| {
                (a.records, a.ms)
            });
        changed >= records
            || elapsed >= ms
            || changed >= self.policy.max_records
            || elapsed >= self.policy.max_ms
    }
}
