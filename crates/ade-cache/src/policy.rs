//! Pure expected-value admission and victim selection. All costs are in microseconds.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    Disabled,
    Never,
    Always,
    ExpectedValueV1,
}

impl Mode {
    pub(crate) fn uses_root(self) -> bool {
        matches!(self, Self::Always | Self::ExpectedValueV1)
    }
}

/// Operator forecast in integer microseconds, milli-uses and basis points.
/// The invalidation and rent estimates cover the full retention horizon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Calibration {
    pub uses_milli: u64,
    pub survival_bps: u64,
    pub recompute_us: u64,
    pub restore_us: u64,
    pub write_us: u64,
    pub hold_us: u64,
    pub invalidation_us: u64,
}

impl Calibration {
    /// # Errors
    /// Refuses out-of-range estimates and checked arithmetic overflow.
    pub fn value(self, include_write: bool) -> Result<i64> {
        let invalid = || Error::Request("policy coefficient out of range");
        if self.uses_milli > 1_000_000
            || self.survival_bps > 10_000
            || [
                self.recompute_us,
                self.restore_us,
                self.write_us,
                self.hold_us,
                self.invalidation_us,
            ]
            .iter()
            .any(|&n| n > 120_000_000)
        {
            return Err(invalid());
        }
        let savings = self.recompute_us.saturating_sub(self.restore_us);
        let benefit = self
            .uses_milli
            .checked_mul(self.survival_bps)
            .and_then(|n| n.checked_mul(savings))
            .ok_or_else(invalid)?
            / 10_000_000;
        let costs = self
            .hold_us
            .checked_add(self.invalidation_us)
            .and_then(|n| n.checked_add(if include_write { self.write_us } else { 0 }))
            .ok_or_else(invalid)?;
        i64::try_from(benefit)
            .map_err(|_| invalid())?
            .checked_sub(i64::try_from(costs).map_err(|_| invalid())?)
            .ok_or_else(invalid)
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Alternative {
    pub(crate) key: String,
    pub(crate) bytes: u64,
    pub(crate) remaining_us: Option<i64>,
    pub(crate) expired: bool,
    pub(crate) created_ms: u64,
    pub(crate) expires_ms: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct Decision {
    pub(crate) reason: &'static str,
    pub(crate) candidate_us: Option<i64>,
    pub(crate) victims: Vec<String>,
    pub(crate) alternatives: Vec<Alternative>,
}

impl Decision {
    pub(crate) fn admit(&self) -> bool {
        self.reason == "admit"
    }
}

pub(crate) fn select(
    mode: Mode,
    calibration: Option<Calibration>,
    reserved: u64,
    available: u64,
    max_entries: usize,
    inventory: Vec<Alternative>,
) -> Result<Decision> {
    let candidate_us = calibration
        .map(|estimate| estimate.value(true))
        .transpose()?;
    let mut decision = Decision {
        reason: "admit",
        candidate_us,
        victims: vec![],
        alternatives: inventory,
    };
    if mode == Mode::ExpectedValueV1 {
        match decision.candidate_us {
            None => decision.reason = "missing_calibration",
            Some(score) if score <= 0 => decision.reason = "nonpositive_value",
            _ => {}
        }
    }
    if !decision.admit() {
        return Ok(decision);
    }

    let mut used = decision
        .alternatives
        .iter()
        .try_fold(0u64, |a, e| a.checked_add(e.bytes))
        .ok_or(Error::Capacity)?;
    let mut count = decision.alternatives.len();
    let mut displaced = 0i64;
    let fits = |bytes: u64, entries: usize| {
        bytes
            .checked_add(reserved)
            .is_some_and(|sum| sum <= available)
            && entries < max_entries
    };
    // A forced incumbent with no calibration can only be evicted at expiry.
    let mut eligible: Vec<_> = decision
        .alternatives
        .iter()
        .filter_map(|entry| {
            if entry.expired {
                Some((0, &entry.key, entry.bytes))
            } else if mode == Mode::ExpectedValueV1 {
                entry
                    .remaining_us
                    .map(|value| (value.max(0), &entry.key, entry.bytes))
            } else {
                None
            }
        })
        .collect();
    eligible.sort_unstable();
    for (value, key, bytes) in eligible {
        if fits(used, count) {
            break;
        }
        displaced = displaced.checked_add(value).ok_or(Error::Capacity)?;
        if mode == Mode::ExpectedValueV1 && decision.candidate_us.is_some_and(|c| displaced >= c) {
            decision.reason = "protected_value";
            break;
        }
        decision.victims.push(key.clone());
        used = used.checked_sub(bytes).ok_or(Error::Capacity)?;
        count -= 1;
    }
    if !fits(used, count) && decision.admit() {
        decision.reason = "insufficient_capacity";
    }
    if !decision.admit() {
        decision.victims.clear();
    }
    Ok(decision)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_forecast_and_conservative_ties() {
        let estimate = Calibration {
            uses_milli: 1250,
            survival_bps: 8000,
            recompute_us: 200,
            restore_us: 100,
            write_us: 20,
            hold_us: 2,
            invalidation_us: 3,
        };
        assert_eq!(estimate.value(true).unwrap(), 75);
        assert_eq!(estimate.value(false).unwrap(), 95);
        assert!(
            Calibration {
                uses_milli: u64::MAX,
                ..estimate
            }
            .value(true)
            .is_err()
        );
        assert_eq!(
            Calibration {
                uses_milli: 0,
                ..estimate
            }
            .value(true)
            .unwrap(),
            -25
        );
        let other = Alternative {
            key: "a".into(),
            bytes: 100,
            remaining_us: Some(75),
            expired: false,
            created_ms: 0,
            expires_ms: 10,
        };
        let decision = select(
            Mode::ExpectedValueV1,
            Some(estimate),
            100,
            100,
            1,
            vec![other],
        )
        .unwrap();
        assert_eq!(decision.reason, "protected_value");
        assert_eq!(decision.victims, Vec::<String>::new());
        let decision = select(Mode::ExpectedValueV1, None, 10, 100, 1, vec![]).unwrap();
        assert_eq!(decision.reason, "missing_calibration");
    }
}
