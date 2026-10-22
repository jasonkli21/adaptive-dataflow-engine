//! Pure bounded key ownership policy. Observation quality never affects semantics.
use crate::{Error, Result, partition::hash_v1, require};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const SHARDS: usize = 48;
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionPolicy {
    #[default]
    Static,
    /// Explicit static diagnostic baseline with destination frame packing.
    StaticPacked,
    SampledV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TopologyConfig {
    pub policy: PartitionPolicy,
    pub sample_rows: usize,
    pub sample_bytes: usize,
    pub rows_per_file: usize,
    pub max_files: usize,
    pub split_min: u64,
    pub coalesce_max: u64,
    pub min_gain_pct: u64,
}
impl Default for TopologyConfig {
    fn default() -> Self {
        Self {
            policy: PartitionPolicy::Static,
            sample_rows: 16384,
            sample_bytes: 8 * 1024 * 1024,
            rows_per_file: 4096,
            max_files: 4,
            split_min: 256,
            coalesce_max: 64,
            min_gain_pct: 20,
        }
    }
}
impl TopologyConfig {
    /// # Errors
    /// Rejects zero/overlarge samples and unsafe arithmetic or thresholds.
    pub fn validate(&self) -> Result<()> {
        require(
            (1..=16384).contains(&self.sample_rows)
                && (1..=16 * 1024 * 1024).contains(&self.sample_bytes)
                && (1..=4096).contains(&self.rows_per_file)
                && (1..=4).contains(&self.max_files),
            "invalid topology sample bounds",
        )?;
        require(
            self.coalesce_max < self.split_min
                && self.split_min <= 65536
                && (1..=100).contains(&self.min_gain_pct),
            "invalid topology thresholds",
        )
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Layout {
    pub version: u32,
    pub hash_version: u32,
    pub workers: usize,
    pub owners: Vec<usize>,
    pub digest: String,
}
impl Layout {
    fn create(workers: usize, owners: Vec<usize>) -> Self {
        let mut layout = Self {
            version: 1,
            hash_version: 1,
            workers,
            owners,
            digest: String::new(),
        };
        layout.digest = layout.fingerprint();
        layout
    }
    fn fingerprint(&self) -> String {
        let mut hash = blake3::Hasher::new();
        hash.update(b"ade-key-layout-v1");
        hash.update(&self.version.to_le_bytes());
        hash.update(&self.hash_version.to_le_bytes());
        hash.update(&(self.workers as u64).to_le_bytes());
        for owner in &self.owners {
            hash.update(&(*owner as u64).to_le_bytes());
        }
        hash.finalize().to_hex().to_string()
    }
    /// # Errors
    /// Rejects malformed, foreign or unsupported layouts.
    pub fn validate(&self, workers: usize) -> Result<()> {
        require(
            (2..=4).contains(&workers)
                && self.workers == workers
                && self.version == 1
                && self.hash_version == 1
                && self.owners.len() == SHARDS
                && self.owners.iter().all(|p| *p < workers)
                && self.digest == self.fingerprint(),
            "invalid topology layout/version/digest",
        )
    }
    /// # Errors
    /// Rejects unsupported worker counts.
    pub fn static_layout(workers: usize) -> Result<Self> {
        require((2..=4).contains(&workers), "layout requires 2..4 workers")?;
        Ok(Self::create(
            workers,
            (0..SHARDS).map(|s| s % workers).collect(),
        ))
    }
    #[must_use]
    pub fn active(&self) -> BTreeSet<u64> {
        self.owners.iter().map(|p| *p as u64).collect()
    }
    #[must_use]
    pub fn route(&self, key: u64) -> usize {
        self.owners[shard(key)]
    }
}
#[must_use]
pub fn shard(key: u64) -> usize {
    usize::try_from(hash_v1(key) % SHARDS as u64).unwrap_or(0)
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub producer: u64,
    pub counts: Vec<u64>,
    pub rows: usize,
    pub files: usize,
    /// True only when every assigned source row was included in the observation.
    /// Tiny coalescing is unsafe when this is false because a prefix can hide a large suffix.
    pub source_complete: bool,
    pub row_groups_selected: usize,
    /// Sum of sizes of opened files, NOT physical bytes fetched by Parquet.
    pub opened_file_bytes: u64,
    pub elapsed_ns: u64,
    pub read_bytes: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Decision {
    pub layout: Layout,
    pub reason: String,
    pub observations: Vec<Observation>,
    pub static_load: Vec<u64>,
    pub planned_load: Vec<u64>,
    pub moved_shards: usize,
}
fn loads(owners: &[usize], counts: &[u64], workers: usize) -> Result<Vec<u64>> {
    let mut load = vec![0u64; workers];
    for (owner, count) in owners.iter().zip(counts) {
        load[*owner] = load[*owner]
            .checked_add(*count)
            .ok_or_else(|| Error::Invalid("topology load overflow".into()))?;
    }
    Ok(load)
}
/// # Errors
/// Rejects duplicate, incomplete, overlarge or overflowing observations/configuration.
pub fn decide(
    config: &TopologyConfig,
    workers: usize,
    observations: Vec<Observation>,
) -> Result<Decision> {
    config.validate()?;
    let base = Layout::static_layout(workers)?;
    require(observations.len() == workers, "probe coverage mismatch")?;
    let mut counts = vec![0u64; SHARDS];
    for (p, observation) in observations.iter().enumerate() {
        require(
            observation.producer == p as u64
                && observation.counts.len() == SHARDS
                && observation.rows <= config.sample_rows
                && observation.files <= config.max_files
                && observation.rows <= observation.files * config.rows_per_file
                && observation.read_bytes <= config.sample_bytes as u64
                && observation.row_groups_selected <= config.max_files * config.rows_per_file
                && (observation.rows == 0 || observation.files > 0),
            "invalid probe identity/bounds",
        )?;
        let mut distinct = 0u64;
        for (sum, count) in counts.iter_mut().zip(&observation.counts) {
            distinct = distinct
                .checked_add(*count)
                .ok_or_else(|| Error::Invalid("probe overflow".into()))?;
            *sum = sum
                .checked_add(*count)
                .ok_or_else(|| Error::Invalid("probe overflow".into()))?;
        }
        require(
            distinct <= observation.rows as u64,
            "distinct probe exceeds rows",
        )?;
    }
    let static_load = loads(&base.owners, &counts, workers)?;
    let total: u64 = static_load.iter().sum(); // bounded by validated sample rows * 4
    let peak = *static_load.iter().max().unwrap_or(&0);
    let mut owners = base.owners.clone();
    let mut reason = "no_benefit";
    let occupied_shards = counts.iter().filter(|c| **c > 0).count();
    if total <= config.coalesce_max {
        if observations
            .iter()
            .all(|observation| observation.source_complete)
        {
            owners.fill(0);
            reason = "coalesce_tiny";
        } else {
            reason = "insufficient_sample_coverage";
        }
    } else if occupied_shards <= 1 {
        // One occupied virtual shard can contain many distinct keys. This v1
        // planner cannot refine a shard without changing its ownership model.
        reason = "one_occupied_shard";
    } else if total >= config.split_min && peak * workers as u64 * 2 > total * 3 {
        let mut order: Vec<_> = (0..SHARDS).collect();
        order.sort_by_key(|s| (std::cmp::Reverse(counts[*s]), *s));
        let mut load = vec![0u64; workers];
        for s in order {
            let owner = (0..workers).min_by_key(|p| (load[*p], *p)).unwrap_or(0);
            owners[s] = owner;
            load[owner] += counts[s];
        }
        let new_peak = *load.iter().max().unwrap_or(&0);
        if new_peak * 100 <= peak * (100 - config.min_gain_pct) {
            reason = "split_oversized_bucket";
        } else {
            owners.clone_from(&base.owners);
        }
    }
    let planned_load = loads(&owners, &counts, workers)?;
    let moved_shards = owners
        .iter()
        .zip(&base.owners)
        .filter(|(a, b)| a != b)
        .count();
    Ok(Decision {
        layout: Layout::create(workers, owners),
        reason: reason.into(),
        observations,
        static_load,
        planned_load,
        moved_shards,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn observed(n: usize, counts: &[u64]) -> Vec<Observation> {
        (0..n)
            .map(|p| Observation {
                producer: p as u64,
                files: usize::from(counts.iter().any(|c| *c > 0)),
                rows: usize::try_from(counts.iter().sum::<u64>()).unwrap(),
                source_complete: true,
                counts: counts.to_vec(),
                ..Default::default()
            })
            .collect()
    }
    #[test]
    fn static_vectors_conservation_and_validation() {
        for n in 2..=4 {
            let layout = Layout::static_layout(n).unwrap();
            layout.validate(n).unwrap();
            for key in 0..10000 {
                assert_eq!(
                    layout.route(key),
                    crate::partition::destination(key, n).unwrap()
                );
            }
            let mut bad = layout.clone();
            bad.owners[0] = n;
            assert!(bad.validate(n).is_err());
            bad = layout.clone();
            bad.digest.clear();
            assert!(bad.validate(n).is_err());
            bad = layout;
            bad.version = 2;
            assert!(bad.validate(n).is_err());
        }
    }
    #[test]
    fn deterministic_split_coalesce_and_negative_controls() {
        let config = TopologyConfig::default();
        for n in 2..=4 {
            let mut counts = vec![0; SHARDS];
            for s in (0..SHARDS).step_by(n) {
                counts[s] = 100;
            }
            let d = decide(&config, n, observed(n, &counts)).unwrap();
            assert_eq!(d.reason, "split_oversized_bucket");
            assert_eq!(
                d.layout,
                decide(&config, n, observed(n, &counts)).unwrap().layout
            );
            assert_eq!(d.layout.active().len(), n);
            let tiny = decide(&config, n, observed(n, &[1; SHARDS])).unwrap();
            if n == 2 {
                assert_eq!(tiny.reason, "no_benefit");
            }
            let mut small = vec![0; SHARDS];
            small[0] = 1;
            small[1] = 1;
            assert_eq!(
                decide(&config, n, observed(n, &small))
                    .unwrap()
                    .layout
                    .active()
                    .len(),
                1
            );
            let mut incomplete = observed(n, &small);
            for observation in &mut incomplete {
                observation.source_complete = false;
            }
            let decision = decide(&config, n, incomplete).unwrap();
            assert_eq!(decision.reason, "insufficient_sample_coverage");
            assert_eq!(decision.layout.active().len(), n);
            let mut single = vec![0; SHARDS];
            single[0] = 1000;
            assert_eq!(
                decide(&config, n, observed(n, &single)).unwrap().reason,
                "one_occupied_shard"
            );
        }
        let mut invalid = observed(2, &[1; SHARDS]);
        invalid[0].counts.fill(u64::MAX);
        assert!(decide(&config, 2, invalid.clone()).is_err());
        invalid = observed(2, &[1; SHARDS]);
        invalid[0].read_bytes = config.sample_bytes as u64 + 1;
        assert!(decide(&config, 2, invalid.clone()).is_err());
        invalid[0].read_bytes = 0;
        invalid[0].rows = config.rows_per_file + 1;
        assert!(decide(&config, 2, invalid.clone()).is_err());
        invalid[0].rows = SHARDS;
        invalid[1].producer = 0;
        assert!(decide(&config, 2, invalid).is_err());
    }

    #[test]
    fn distinct_keys_in_one_virtual_shard_are_reported_as_one_occupied_shard() {
        // These distinct keys collide into shard zero. The fixed 48-shard v1
        // ownership map cannot distribute them without a finer routing domain.
        let keys = [38, 103, 160, 175, 209, 363, 481, 537, 575, 634, 656, 666];
        assert!(keys.iter().all(|key| shard(*key) == 0));
        for workers in 2..=4 {
            let mut counts = vec![0; SHARDS];
            counts[0] = keys.len() as u64 * 100;
            let decision = decide(
                &TopologyConfig::default(),
                workers,
                observed(workers, &counts),
            )
            .unwrap();
            assert_eq!(decision.reason, "one_occupied_shard");
            assert_eq!(
                decision.layout.owners,
                Layout::static_layout(workers).unwrap().owners
            );
            let owner = decision.layout.route(keys[0]);
            assert!(keys.iter().all(|key| decision.layout.route(*key) == owner));
        }
    }
}
