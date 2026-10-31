//! Immutable committed output. Diagnostics and provisional files are not this API.
use super::{Config, epoch};
use crate::{
    Result,
    state::{WindowOutput, check, window},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Output {
    pub output_version: u32,
    pub job_id: Uuid,
    pub key: u64,
    pub window_start: i64,
    pub window_end: i64,
    pub sum_i64: i64,
    pub count_u64: u64,
    pub output_id: String,
    pub payload_fingerprint: String,
}
impl From<WindowOutput> for Output {
    fn from(w: WindowOutput) -> Self {
        Self {
            output_version: w.output_version,
            job_id: w.job_id,
            key: w.key,
            window_start: w.window_start,
            window_end: w.window_end,
            sum_i64: w.sum_i64,
            count_u64: w.count_u64,
            output_id: w.output_id,
            payload_fingerprint: w.payload_fingerprint,
        }
    }
}
impl Output {
    /// # Errors
    /// Validates semantic fields, never ID-only deduplication.
    pub fn validate(&self, c: &Config) -> Result<()> {
        check(
            self.output_version == 1
                && self.job_id == c.state.job_id
                && self.count_u64 > 0
                && self.key < c.state.source.key_less_than
                && window(self.window_start, c.state.window_ms)?
                    == (self.window_start, self.window_end)
                && self.output_id
                    == format!(
                        "{}:{}:{}:{}:1",
                        self.job_id, self.key, self.window_start, self.window_end
                    )
                && self.payload_fingerprint
                    == format!("window-payload-v1:{}:{}", self.sum_i64, self.count_u64),
            "output identity/payload",
        )
    }
}
/// # Errors
/// Snapshot the selector once; authenticate its entire bounded closure before returning output.
pub fn read(c: &Config) -> Result<Vec<Output>> {
    c.validate()?;
    epoch::root_identity(c)?;
    let Some(p) = epoch::selection(&c.state.checkpoint_root)? else {
        return Ok(Vec::new());
    };
    let epochs = epoch::verify(c, &p)?;
    let mut records = Vec::new();
    for e in epochs {
        records.extend(epoch::output(c, &e)?);
    }
    Ok(records)
}
/// Duplicate/conflicting IDs are errors, never suppressed.
/// # Errors
/// Refuses any repeated committed identity.
pub fn unique(map: &mut BTreeMap<String, String>, o: &Output) -> Result<()> {
    if let Some(old) = map.insert(o.output_id.clone(), o.payload_fingerprint.clone()) {
        check(
            old == o.payload_fingerprint,
            "conflicting committed output payload",
        )?;
        return Err(crate::Error::State("duplicate committed output"));
    }
    Ok(())
}
