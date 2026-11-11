//! Phase 19 finite epoch semantics and deterministic policy. No runtime I/O.
use crate::{Error, Result, require};
use ade_physical::{Field, Source, SourceFile, Type};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const PLAN_BYTES: usize = 32 * 1024;
pub const DELTA_BYTES: usize = 2 * 1024 * 1024;
pub const DELTA_ROWS: usize = 32_768;
pub const MAX_EPOCH: u32 = 32;
pub const SHARDS: usize = 128;
pub const VIEWS: [&str; 2] = ["all_v1", "filtered_v1"];

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    version: u32,
    namespace: String,
    threshold: i64,
    baseline: Vec<SourceFile>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Properties {
    pub bounded: bool,
    pub deterministic: bool,
    pub replayable: bool,
    pub ordering: &'static str,
    pub time: &'static str,
    pub delivery: &'static str,
    pub views: [&'static str; 2],
}

/// A validated semantic descriptor. Only admission can construct it.
#[derive(Clone, Debug)]
pub struct MaintainedPlanV1 {
    descriptor: Descriptor,
    identity: String,
    source: Source,
}

pub(crate) fn json<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| Error(e.to_string()))
}

#[must_use]
pub fn identity(domain: &str, bytes: &[u8]) -> String {
    let mut h = blake3::Hasher::new();
    h.update(domain.as_bytes());
    h.update(b"\0");
    h.update(bytes);
    h.finalize().to_hex().to_string()
}

#[must_use]
pub fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl MaintainedPlanV1 {
    /// # Errors
    /// Refuses invalid or oversized JSON, unsupported versions or invalid native source inventories.
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        require(bytes.len() <= PLAN_BYTES, "maintained plan byte cap")?;
        let descriptor: Descriptor =
            serde_json::from_slice(bytes).map_err(|e| Error(e.to_string()))?;
        require(descriptor.version == 1, "maintained plan version")?;
        let label = descriptor.namespace.as_bytes();
        require(
            !label.is_empty()
                && label.len() <= 64
                && label
                    .iter()
                    .all(|b| b.is_ascii_alphanumeric() || *b == b'-' || *b == b'_'),
            "maintained namespace grammar",
        )?;
        let schema = vec![
            Field::new("key", Type::U64, false),
            Field::new("value", Type::I64, false),
        ];
        let manifest = identity("maintained-baseline-v1", &json(&descriptor.baseline)?);
        let source = Source {
            id: 0,
            table: "input".into(),
            manifest: manifest.clone(),
            logical: manifest,
            schema_identity: identity("maintained-schema-v1", &json(&schema)?),
            schema,
            files: descriptor.baseline.clone(),
            replayable: true,
        };
        source.validate()?;
        Ok(Self {
            identity: identity("maintained-plan-v1", &json(&descriptor)?),
            descriptor,
            source,
        })
    }

    #[must_use]
    pub fn digest(&self) -> &str {
        &self.identity
    }

    #[must_use]
    pub fn genesis(&self) -> String {
        identity("maintained-genesis-v1", self.identity.as_bytes())
    }

    #[must_use]
    pub fn threshold(&self) -> i64 {
        self.descriptor.threshold
    }

    #[must_use]
    pub fn source(&self) -> &Source {
        &self.source
    }

    #[must_use]
    pub fn properties(&self) -> Properties {
        Properties {
            bounded: true,
            deterministic: true,
            replayable: self.source.replayable,
            ordering: "epoch_then_shard_then_key",
            time: "finite_epoch",
            delivery: "local_committed_generation",
            views: VIEWS,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u32,
    plan: String,
    epoch: u32,
    parent: String,
    rows: Vec<(u64, i64, i64)>,
}

pub type Row = (u64, i64);
pub type Changes = BTreeMap<Row, i128>;

/// Exact source bytes define lineage. A separate normalized representation drives the operators.
#[derive(Debug)]
pub struct DeltaBatchV1 {
    chain: String,
    changes: Changes,
    rows: usize,
    keys: usize,
    touched: [bool; SHARDS],
}
impl DeltaBatchV1 {
    /// # Errors
    /// Refuses malformed, oversized, foreign or noncontiguous deltas.
    pub fn from_json(
        bytes: &[u8],
        plan: &MaintainedPlanV1,
        epoch: u32,
        parent: &str,
    ) -> Result<Self> {
        require(bytes.len() <= DELTA_BYTES, "delta byte cap")?;
        let raw: Envelope = serde_json::from_slice(bytes).map_err(|e| Error(e.to_string()))?;
        require(
            raw.version == 1 && raw.plan == plan.digest(),
            "delta version or plan",
        )?;
        require(
            (1..=MAX_EPOCH).contains(&epoch)
                && raw.epoch == epoch
                && is_digest(parent)
                && raw.parent == parent,
            "delta epoch gap, fork or parent mismatch",
        )?;
        let rows = raw.rows.len();
        require(rows <= DELTA_ROWS, "delta row cap")?;
        let mut changes: Changes = BTreeMap::new();
        for (key, value, weight) in raw.rows {
            let net = changes.entry((key, value)).or_default();
            *net = net
                .checked_add(i128::from(weight))
                .ok_or_else(|| Error("delta normalization overflow".into()))?;
        }
        changes.retain(|_, w| *w != 0);
        let mut touched = [false; SHARDS];
        let mut keys = 0;
        let mut previous = None;
        for &(key, _) in changes.keys() {
            if previous != Some(key) {
                keys += 1;
                touched[shard(key)] = true;
                previous = Some(key);
            }
        }
        Ok(Self {
            chain: identity("maintained-delta-v1", bytes),
            keys,
            touched,
            rows,
            changes,
        })
    }

    #[must_use]
    pub fn chain(&self) -> &str {
        &self.chain
    }

    #[must_use]
    pub fn changes(&self) -> &Changes {
        &self.changes
    }

    #[must_use]
    pub fn rows(&self) -> usize {
        self.rows
    }

    #[must_use]
    pub fn keys(&self) -> usize {
        self.keys
    }

    pub fn touched_shards(&self) -> impl Iterator<Item = usize> + '_ {
        self.touched
            .iter()
            .enumerate()
            .filter_map(|(i, &yes)| yes.then_some(i))
    }
}

#[must_use]
pub fn shard(key: u64) -> usize {
    usize::from(key.to_le_bytes()[0]) % SHARDS
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Route {
    Restore,
    IncrementalUpdate,
    FullRecompute,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "route",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Policy {
    #[default]
    Adaptive,
    Disabled,
    Force(Route),
}

#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
pub struct Work {
    pub scan_rows: u64,
    pub delta_rows: u64,
    pub touched_keys: u64,
    /// Bytes paid by shared source/checkpoint admission before route choice.
    pub admission_bytes: u64,
    /// Additional mandatory route-specific verification and decoding reads.
    pub route_verify_bytes: u64,
    pub write_bytes: u64,
}

impl Work {
    fn units(self) -> Option<u64> {
        self.scan_rows
            .checked_mul(4)?
            .checked_add(self.delta_rows)?
            .checked_add(self.touched_keys.checked_mul(6)?)?
            .checked_add(self.admission_bytes / 64)?
            .checked_add(self.route_verify_bytes / 64)?
            .checked_add(self.write_bytes / 32)
    }
}

#[derive(Debug, Serialize)]
pub struct Candidate {
    pub route: Route,
    pub estimate: Option<Work>,
    pub work_units: Option<u64>,
    pub projected_ms: Option<u64>,
    pub refusal: Option<&'static str>,
}

#[derive(Debug, Serialize)]
pub struct Decision {
    pub selected: Route,
    pub provenance: &'static str,
    pub uncertainty: &'static str,
    pub candidates: [Candidate; 3],
}

/// Policy consumes runtime-attested route capabilities. The resulting record has no execution authority.
/// # Errors
/// Refuses if no legal route fits, including an unavailable forced or disabled choice.
pub fn choose(
    options: [Option<Work>; 3],
    policy: Policy,
    write_budget: u64,
    timeout_ms: u64,
) -> Result<Decision> {
    let [restore, update, replay] = options;
    let limit = timeout_ms
        .checked_mul(100_000)
        .ok_or_else(|| Error("deadline estimate overflow".into()))?;
    let candidates = [
        (Route::Restore, restore),
        (Route::IncrementalUpdate, update),
        (Route::FullRecompute, replay),
    ]
    .map(|(route, estimate)| {
        let work_units = estimate.and_then(Work::units);
        let refusal = match (estimate, work_units) {
            (None, _) => Some("missing compatible verified inputs"),
            (Some(_), None) => Some("work estimate overflow"),
            (Some(w), _) if w.write_bytes > write_budget => Some("projected disk budget"),
            (_, Some(units)) if units > limit => Some("projected deadline"),
            _ => None,
        };
        Candidate {
            route,
            estimate,
            work_units,
            projected_ms: work_units.map(|units| units.div_ceil(100_000)),
            refusal,
        }
    });
    let forced = match policy {
        Policy::Adaptive => None,
        Policy::Disabled => Some(Route::FullRecompute),
        Policy::Force(route) => Some(route),
    };
    let selected = candidates
        .iter()
        .filter(|c| c.refusal.is_none() && forced.is_none_or(|r| r == c.route))
        .min_by_key(|c| c.work_units)
        .ok_or_else(|| {
            Error(format!(
                "no legal maintained route for {policy:?}: {candidates:?}"
            ))
        })?
        .route;
    Ok(Decision {
        selected,
        provenance: "shared_admission_bytes_plus_route_reads_and_verified_rows_delta_generation_inventory",
        uncertainty: "integer work proxy includes mandatory known reads; storage latency and deadline estimates are not guarantees",
        candidates,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(threshold: i64) -> MaintainedPlanV1 {
        let doc = serde_json::json!({
            "version": 1, "namespace": "test", "threshold": threshold,
            "baseline": [{"name":"test.parquet","bytes":12,"rows":1,"fingerprint":"a".repeat(64)}]
        });
        MaintainedPlanV1::from_json(&json(&doc).unwrap()).unwrap()
    }

    #[test]
    fn raw_delta_order_is_lineage_but_normalized_bag_order_is_not() {
        let plan = plan(0);
        let a = serde_json::json!({
            "version": 1, "plan": plan.digest(), "epoch": 1, "parent": plan.genesis(),
            "rows": [[5, 0, -1], [5, 0, 1], [5, 1, 2]]
        });
        let mut b = a.clone();
        b["rows"].as_array_mut().unwrap().reverse();
        let decode =
            |doc| DeltaBatchV1::from_json(&json(doc).unwrap(), &plan, 1, &plan.genesis()).unwrap();
        let (a, b) = (decode(&a), decode(&b));
        assert_ne!(a.chain(), b.chain());
        assert_eq!(a.changes(), b.changes());
        assert_eq!(a.changes().len(), 1);
    }

    #[test]
    fn strict_identity_and_policy_refusal() {
        assert_ne!(plan(-1).digest(), plan(0).digest());
        assert!(MaintainedPlanV1::from_json(br#"{"version":1,"version":1}"#).is_err());
        assert!(MaintainedPlanV1::from_json(&vec![b' '; PLAN_BYTES + 1]).is_err());
        let capped = Work {
            write_bytes: 10,
            ..Work::default()
        };
        let overflow = Work {
            scan_rows: u64::MAX,
            ..Work::default()
        };
        let options = [Some(Work::default()), Some(capped), Some(overflow)];
        let decision = choose(options, Policy::Adaptive, 9, 1).unwrap();
        assert_eq!(decision.selected, Route::Restore);
        assert_eq!(
            decision.candidates[1].refusal,
            Some("projected disk budget")
        );
        assert_eq!(
            decision.candidates[2].refusal,
            Some("work estimate overflow")
        );
        assert!(choose(options, Policy::Force(Route::IncrementalUpdate), 9, 1).is_err());
        assert!(choose(options, Policy::Disabled, 9, 1).is_err());
    }
}
