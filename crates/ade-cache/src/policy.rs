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

pub(crate) fn hit_is_worthwhile(calibration: Option<Calibration>) -> bool {
    calibration.is_some_and(|estimate| estimate.recompute_us > estimate.restore_us)
}

/// Remaining stationary-use value over the unexpired part of a v1 forecast horizon.
/// A clock rollback cannot increase value and is treated as zero remaining value.
pub(crate) fn remaining_value(
    calibration: Calibration,
    now_ms: u64,
    created_ms: u64,
    expires_ms: u64,
) -> Result<i64> {
    let horizon = expires_ms
        .checked_sub(created_ms)
        .filter(|horizon| *horizon > 0)
        .ok_or(Error::Integrity("invalid forecast horizon"))?;
    if now_ms < created_ms || now_ms >= expires_ms {
        return Ok(0);
    }
    let full_value =
        u128::try_from(calibration.value(false)?.max(0)).map_err(|_| Error::Capacity)?;
    let remaining_ms = u128::from(expires_ms - now_ms);
    let scaled = full_value
        .checked_mul(remaining_ms)
        .ok_or(Error::Capacity)?
        / u128::from(horizon);
    i64::try_from(scaled).map_err(|_| Error::Capacity)
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Alternative {
    pub(crate) key: String,
    pub(crate) bytes: u64,
    pub(crate) remaining_us: Option<i64>,
    pub(crate) expired: bool,
    pub(crate) created_ms: u64,
    pub(crate) expires_ms: u64,
    pub(crate) observed_ms: u64,
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

    let used = decision
        .alternatives
        .iter()
        .try_fold(0u64, |a, e| a.checked_add(e.bytes))
        .ok_or(Error::Capacity)?;
    let count = decision.alternatives.len();
    let byte_need = used
        .checked_add(reserved)
        .ok_or(Error::Capacity)?
        .saturating_sub(available);
    let count_need = count.saturating_add(1).saturating_sub(max_entries);
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
    eligible.sort_unstable_by(|left, right| left.1.cmp(right.1));
    let selected = least_loss_subset(&eligible, byte_need, count_need)?;
    if let Some(selected) = selected {
        let displaced = selected.iter().try_fold(0i64, |sum, index| {
            sum.checked_add(eligible[*index].0).ok_or(Error::Capacity)
        })?;
        if mode == Mode::ExpectedValueV1
            && decision
                .candidate_us
                .is_some_and(|candidate| displaced >= candidate)
        {
            decision.reason = "protected_value";
        } else {
            decision.victims = selected
                .iter()
                .map(|index| eligible[*index].1.clone())
                .collect();
            decision.victims.sort();
        }
    } else {
        decision.reason = "insufficient_capacity";
    }
    if !decision.admit() {
        decision.victims.clear();
    }
    Ok(decision)
}

/// Exact bounded meet-in-the-middle selection for the root's <=32 entries.
/// Select the least retained value that frees both required bytes and slots.
fn least_loss_subset(
    entries: &[(i64, &String, u64)],
    byte_need: u64,
    count_need: usize,
) -> Result<Option<Vec<usize>>> {
    if byte_need == 0 && count_need == 0 {
        return Ok(Some(Vec::new()));
    }
    if entries.len() > 32 {
        return Err(Error::Capacity);
    }
    let mid = entries.len() / 2;
    let left = subsets(&entries[..mid])?;
    let right = subsets(&entries[mid..])?;
    let mut by_count: Vec<Vec<Subset>> = (0..=entries.len() - mid).map(|_| Vec::new()).collect();
    for item in right {
        by_count[item.2 as usize].push(item);
    }
    // For each exact count, order by freed bytes and keep suffix minima by loss.
    let mut indexed = Vec::with_capacity(by_count.len());
    for group in &mut by_count {
        group.sort_unstable_by_key(|item| item.0);
        let mut minima = vec![0usize; group.len()];
        let mut best = None;
        for i in (0..group.len()).rev() {
            if best.is_none_or(|j: usize| group[i].rank() < group[j].rank()) {
                best = Some(i);
            }
            minima[i] = best.expect("nonempty suffix");
        }
        indexed.push((group, minima));
    }
    let mut best: Option<(i64, u64, Vec<usize>)> = None;
    for l in left {
        let needed = byte_need.saturating_sub(l.0);
        let need_count = count_need.saturating_sub(l.2 as usize);
        for (group, minima) in indexed.iter().skip(need_count) {
            let position = group.partition_point(|item| item.0 < needed);
            if position >= group.len() {
                continue;
            }
            let r = &group[minima[position]];
            let loss = l.1.checked_add(r.1).ok_or(Error::Capacity)?;
            let bytes = l.0.checked_add(r.0).ok_or(Error::Capacity)?;
            let mut ids = Vec::new();
            for bit in 0..mid {
                if l.3 & (1 << bit) != 0 {
                    ids.push(bit);
                }
            }
            for bit in 0..(entries.len() - mid) {
                if r.3 & (1 << bit) != 0 {
                    ids.push(mid + bit);
                }
            }
            let candidate = (loss, bytes, ids);
            if best.as_ref().is_none_or(|old| candidate < *old) {
                best = Some(candidate);
            }
        }
    }
    Ok(best.map(|(_, _, ids)| ids))
}

#[derive(Clone, Copy)]
struct Subset(u64, i64, u32, u32);

impl Subset {
    fn rank(self) -> (i64, u64, u32) {
        // Each bucket has a fixed cardinality. At the first differing key,
        // selecting the earlier key makes the sorted key list smaller.
        (self.1, self.0, !self.3.reverse_bits())
    }
}

fn subsets(entries: &[(i64, &String, u64)]) -> Result<Vec<Subset>> {
    let mut result = vec![Subset(0, 0, 0, 0)];
    for (index, (value, _, bytes)) in entries.iter().enumerate() {
        let current = result.clone();
        for Subset(total_bytes, total_value, count, mask) in current {
            result.push(Subset(
                total_bytes.checked_add(*bytes).ok_or(Error::Capacity)?,
                total_value.checked_add(*value).ok_or(Error::Capacity)?,
                count.checked_add(1).ok_or(Error::Capacity)?,
                mask | (1 << index),
            ));
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_subset_ties_use_sorted_key_order_within_a_bucket() {
        let keys = ["a", "b", "c", "d", "e", "f", "g", "h"].map(String::from);
        let entries = [
            (10, 1),
            (10, 1),
            (10, 1),
            (10, 1),
            (1, 1),
            (1, 2),
            (1, 4),
            (1, 5),
        ];
        let entries = entries
            .into_iter()
            .zip(&keys)
            .map(|((loss, bytes), key)| (loss, key, bytes))
            .collect::<Vec<_>>();
        assert_eq!(least_loss_subset(&entries, 6, 0).unwrap(), Some(vec![4, 7]));
    }

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
            observed_ms: 0,
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

    fn alt(key: &str, bytes: u64, value: i64) -> Alternative {
        Alternative {
            key: key.into(),
            bytes,
            remaining_us: Some(value),
            expired: false,
            created_ms: 0,
            expires_ms: 100,
            observed_ms: 0,
        }
    }

    #[test]
    fn remaining_value_decays_and_clock_anomalies_are_conservative() {
        let c = Calibration {
            uses_milli: 1000,
            survival_bps: 10_000,
            recompute_us: 100,
            restore_us: 0,
            write_us: 0,
            hold_us: 0,
            invalidation_us: 0,
        };
        assert_eq!(remaining_value(c, 0, 0, 100).unwrap(), 100);
        assert_eq!(remaining_value(c, 50, 0, 100).unwrap(), 50);
        assert_eq!(remaining_value(c, 99, 0, 100).unwrap(), 1);
        assert_eq!(remaining_value(c, 100, 0, 100).unwrap(), 0);
        assert_eq!(remaining_value(c, 0, 10, 100).unwrap(), 0);
        assert!(remaining_value(c, 0, 10, 10).is_err());
    }

    #[test]
    fn an_existing_hit_does_not_pay_the_sunk_write_cost() {
        let forecast = Calibration {
            uses_milli: 1000,
            survival_bps: 10_000,
            recompute_us: 100_000,
            restore_us: 20_000,
            write_us: 100_000,
            hold_us: 0,
            invalidation_us: 0,
        };
        assert_eq!(forecast.value(true).unwrap(), -20_000);
        assert_eq!(forecast.value(false).unwrap(), 80_000);
        assert!(hit_is_worthwhile(Some(forecast)));
        assert!(!hit_is_worthwhile(Some(Calibration {
            recompute_us: 10,
            restore_us: 20,
            ..forecast
        })));
        let miss = select(Mode::ExpectedValueV1, Some(forecast), 10, 100, 2, vec![]).unwrap();
        assert_eq!(miss.reason, "nonpositive_value");
    }

    #[test]
    fn exact_subset_finds_profitable_large_victim() {
        let entries = vec![alt("a", 60, 50), alt("b", 20, 30), alt("c", 20, 31)];
        let result = select(
            Mode::ExpectedValueV1,
            Some(Calibration {
                uses_milli: 1000,
                survival_bps: 10_000,
                recompute_us: 55,
                restore_us: 0,
                write_us: 0,
                hold_us: 0,
                invalidation_us: 0,
            }),
            40,
            100,
            4,
            entries,
        )
        .unwrap();
        assert_eq!(result.reason, "admit");
        assert_eq!(result.victims, ["a"]);
    }

    #[test]
    fn exact_subset_keeps_incumbents_on_equal_value_and_handles_entry_cap() {
        let equal = select(
            Mode::ExpectedValueV1,
            Some(Calibration {
                uses_milli: 1000,
                survival_bps: 10_000,
                recompute_us: 50,
                restore_us: 0,
                write_us: 0,
                hold_us: 0,
                invalidation_us: 0,
            }),
            40,
            90,
            4,
            vec![alt("a", 60, 50)],
        )
        .unwrap();
        assert_eq!(equal.reason, "protected_value");
        assert_eq!(equal.victims, Vec::<String>::new());
        let slot = select(
            Mode::ExpectedValueV1,
            Some(Calibration {
                uses_milli: 1000,
                survival_bps: 10_000,
                recompute_us: 100,
                restore_us: 0,
                write_us: 0,
                hold_us: 0,
                invalidation_us: 0,
            }),
            1,
            100,
            2,
            vec![alt("a", 10, 100), alt("b", 10, 20)],
        )
        .unwrap();
        assert_eq!(slot.victims, ["b"]);
    }

    #[test]
    fn exact_subset_is_bounded_at_the_root_entry_cap_and_checks_capacity_overflow() {
        let entries: Vec<_> = (0..32)
            .map(|index| alt(&format!("{index:02}"), 1, i64::from(index + 1)))
            .collect();
        let selected = select(
            Mode::ExpectedValueV1,
            Some(Calibration {
                uses_milli: 1000,
                survival_bps: 10_000,
                recompute_us: 100_000,
                restore_us: 0,
                write_us: 0,
                hold_us: 0,
                invalidation_us: 0,
            }),
            1,
            22,
            32,
            entries,
        )
        .unwrap();
        assert_eq!(
            selected.victims,
            (0..11).map(|i| format!("{i:02}")).collect::<Vec<_>>()
        );

        let overflow = select(
            Mode::ExpectedValueV1,
            Some(Calibration {
                uses_milli: 1000,
                survival_bps: 10_000,
                recompute_us: 100,
                restore_us: 0,
                write_us: 0,
                hold_us: 0,
                invalidation_us: 0,
            }),
            u64::MAX,
            u64::MAX,
            2,
            vec![alt("huge", u64::MAX, 1)],
        );
        assert!(overflow.is_err());
    }
}
