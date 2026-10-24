//! Pure, versioned join strategy selection. No I/O or runtime mutation.
use crate::{Result, require};
use ade_exec::join::Side;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Policy {
    #[default]
    ObservedV1,
    ForcedRepartition,
    ForcedBroadcast,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Distribution {
    HashRepartition,
    BroadcastBuild,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub worker: u64,
    pub side: Side,
    pub rows: u64,
    pub assigned_rows: u64,
    pub assigned_files: usize,
    pub completed_files: usize,
    pub read_bytes: u64,
    pub metadata_bytes: u64,
    pub decode_peak_bytes: u64,
    pub batch_peak_bytes: usize,
    pub complete: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Decision {
    pub version: u32,
    pub distribution: Distribution,
    pub build: Side,
    pub reason: String,
    pub rows: [u64; 2],
    pub complete: [bool; 2],
    pub state_upper_bytes: [u64; 2],
    pub observations: Vec<Observation>,
    pub digest: String,
}
/// # Errors
/// Rejects inconsistent coverage, overflow and inadmissible forced broadcasting.
pub fn choose(
    config: &super::join::Config,
    observations: Vec<Observation>,
    workers: usize,
) -> Result<Decision> {
    require(
        (2..=4).contains(&workers) && observations.len() == workers * 2,
        "join observation cardinality",
    )?;
    let mut rows = [0u64; 2];
    let mut complete = [true; 2];
    for (i, o) in observations.iter().enumerate() {
        let side = i % 2;
        require(
            o.worker == (i / 2) as u64
                && o.side.index() == side
                && o.rows <= o.assigned_rows
                && o.completed_files <= o.assigned_files
                && o.assigned_files <= 64
                && (!o.complete
                    || (o.rows == o.assigned_rows && o.completed_files == o.assigned_files))
                && o.rows <= config.probe_rows
                && o.read_bytes <= config.probe_read_bytes
                && o.metadata_bytes <= 1024 * 1024
                && o.decode_peak_bytes <= 1024 * 1024
                && o.batch_peak_bytes <= 64 * 1024,
            "invalid join observation",
        )?;
        rows[side] = rows[side]
            .checked_add(o.rows)
            .ok_or_else(|| crate::Error::Invalid("join observation overflow".into()))?;
        complete[side] &= o.complete;
    }
    // Row slots + hash heads + conservative allocator slack. Complete coverage is mandatory.
    let state_upper_bytes = rows.map(|r| r.saturating_mul(64).saturating_add(8192));
    let table_upper = config.local.hash_rows as u64 * 64 + 192 * 1024;
    let safe = |s: usize| {
        table_upper <= config.local.memory_limit_bytes as u64
            && complete[s]
            && rows[s] <= config.broadcast_rows
            && rows[s] <= config.local.hash_rows as u64
            && state_upper_bytes[s] <= config.broadcast_bytes
            && state_upper_bytes[s]
                <= (config.local.memory_limit_bytes as u64).saturating_sub(192 * 1024)
            && rows[s].saturating_mul(16).saturating_mul(workers as u64)
                <= config.broadcast_amplification_bytes
    };
    let build = config.build.unwrap_or_else(|| match (safe(0), safe(1)) {
        (true, true) if rows[0] < rows[1] => Side::Left,
        (true, false) => Side::Left,
        _ => Side::Right,
    });
    let admissible = safe(build.index());
    require(
        config.policy != Policy::ForcedBroadcast || admissible,
        "forced broadcast lacks complete bounded build proof",
    )?;
    let distribution = if config.policy != Policy::ForcedRepartition && admissible {
        Distribution::BroadcastBuild
    } else {
        Distribution::HashRepartition
    };
    let reason = match config.policy {
        Policy::ForcedRepartition | Policy::ForcedBroadcast => "forced_control",
        Policy::ObservedV1 if admissible => "complete_small_side",
        Policy::ObservedV1 => "neither_selected_side_proven_or_fanout_cap",
    }
    .into();
    Ok(Decision {
        version: 1,
        distribution,
        build,
        reason,
        rows,
        complete,
        state_upper_bytes,
        observations,
        digest: String::new(),
    })
}
