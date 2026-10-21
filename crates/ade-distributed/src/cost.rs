//! Pure fixed-point economics. Prices/probability/throughputs are configured,
//! operator work and byte counts are observations; none are health predictions.
use crate::{Error, Result, require};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializationMode {
    #[default]
    Expected,
    Always,
    Never,
}
/// Object request prices cover the transfer path only. Job-prefix cleanup
/// LIST/DELETE billing is outside this deliberately narrow v1 proxy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectRequestCostScope {
    PutHeadGetProxyV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CostConfig {
    pub mode: MaterializationMode,
    pub failure_probability_ppm: u64,
    pub compute_nano_usd_per_work_ns: u64,
    pub source_read_nano_usd_per_byte: u64,
    pub source_read_nano_usd_per_request: u64,
    pub object_write_nano_usd_per_byte: u64,
    pub object_write_nano_usd_per_request: u64,
    pub object_read_nano_usd_per_byte: u64,
    pub object_read_nano_usd_per_request: u64,
    pub object_capacity_nano_usd_per_byte_ms: u64,
    pub expected_retention_ms: u64,
    pub source_read_bytes_per_second: u64,
    pub object_write_bytes_per_second: u64,
    pub object_read_bytes_per_second: u64,
    pub deadline_slack_ms: u64,
    pub slo_penalty_nano_usd_per_ms: u64,
    pub min_expected_savings_nano_usd: u64,
}
impl Default for CostConfig {
    fn default() -> Self {
        Self {
            mode: MaterializationMode::Expected,
            failure_probability_ppm: 1000,
            compute_nano_usd_per_work_ns: 1,
            source_read_nano_usd_per_byte: 0,
            source_read_nano_usd_per_request: 0,
            object_write_nano_usd_per_byte: 1,
            object_write_nano_usd_per_request: 1000,
            object_read_nano_usd_per_byte: 1,
            object_read_nano_usd_per_request: 1000,
            object_capacity_nano_usd_per_byte_ms: 0,
            expected_retention_ms: 1000,
            source_read_bytes_per_second: 100_000_000,
            object_write_bytes_per_second: 100_000_000,
            object_read_bytes_per_second: 100_000_000,
            deadline_slack_ms: 120_000,
            slo_penalty_nano_usd_per_ms: 0,
            min_expected_savings_nano_usd: 0,
        }
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Production {
    /// Sum of exclusive ade-exec operator work, not CPU billing or wall elapsed.
    pub operator_work_ns: u64,
    pub wall_elapsed_ns: u64,
    /// Whole-producer closure, intentionally not divided among destinations.
    pub source_bytes: u64,
    pub source_requests: u64,
    pub output_bytes: u64,
    pub output_segments: u64,
    /// Fixed topology: exactly one logical final consumer per transfer.
    pub consumer_count: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Decision {
    pub config: CostConfig,
    pub observation: Production,
    pub recompute_nano_usd: u64,
    pub restore_nano_usd: u64,
    pub write_nano_usd: u64,
    pub recompute_ms: u64,
    pub restore_ms: u64,
    pub write_ms: u64,
    /// Numerators in nano-USD * ppm, avoiding probability rounding ties.
    pub no_persist_nano_usd_ppm: u128,
    pub persist_nano_usd_ppm: u128,
    pub persist: bool,
    pub reason: String,
    pub object_request_cost_scope: ObjectRequestCostScope,
}
fn overflow() -> Error {
    Error::Invalid("cost arithmetic overflow".into())
}
fn product(a: u64, b: u64) -> Result<u64> {
    a.checked_mul(b).ok_or_else(overflow)
}
fn sum(values: &[u64]) -> Result<u64> {
    values
        .iter()
        .try_fold(0u64, |a, b| a.checked_add(*b).ok_or_else(overflow))
}
fn time_ms(bytes: u64, throughput: u64) -> Result<u64> {
    u64::try_from((u128::from(bytes) * 1000).div_ceil(u128::from(throughput)))
        .map_err(|_| overflow())
}
#[derive(Debug, Clone, Copy)]
struct Estimates {
    recompute_base: u64,
    restore_base: u64,
    write_base: u64,
    recompute_ms: u64,
    restore_ms: u64,
    write_ms: u64,
}
impl CostConfig {
    /// # Errors
    /// Rejects unsupported objectives and arithmetic ranges before execution.
    pub fn validate(&self) -> Result<()> {
        require(
            self.failure_probability_ppm <= 1_000_000,
            "cost probability exceeds ppm",
        )?;
        for throughput in [
            self.source_read_bytes_per_second,
            self.object_write_bytes_per_second,
            self.object_read_bytes_per_second,
        ] {
            require(throughput > 0, "cost throughput must be positive")?;
        }
        // Explicit finite objective bounds. Observed arithmetic is also checked.
        let value = serde_json::to_value(self)?;
        for number in value
            .as_object()
            .ok_or_else(|| Error::Invalid("cost config must be an object".into()))?
            .values()
            .filter_map(serde_json::Value::as_u64)
        {
            require(
                number <= 1_000_000_000_000,
                "cost configuration exceeds v1 bound",
            )?;
        }
        product(
            self.object_capacity_nano_usd_per_byte_ms,
            self.expected_retention_ms,
        )?;
        Ok(())
    }
    fn penalty_after(&self, elapsed_ms: u64) -> Result<u64> {
        product(
            elapsed_ms.saturating_sub(self.deadline_slack_ms),
            self.slo_penalty_nano_usd_per_ms,
        )
    }
    fn estimates(&self, o: &Production) -> Result<Estimates> {
        // Phase 6 synthesizes empty replay terminals without reopening the producer
        // closure. Keep observed production intact but price the actual zero-work action.
        let (replay_work, reread_bytes, reread_requests) = if o.output_segments == 0 {
            (0, 0, 0)
        } else {
            (o.operator_work_ns, o.source_bytes, o.source_requests)
        };
        let recompute_ms = sum(&[
            replay_work.div_ceil(1_000_000),
            time_ms(reread_bytes, self.source_read_bytes_per_second)?,
        ])?;
        let restore_ms = time_ms(o.output_bytes, self.object_read_bytes_per_second)?;
        let write_ms = time_ms(o.output_bytes, self.object_write_bytes_per_second)?;
        let recompute_base = sum(&[
            product(replay_work, self.compute_nano_usd_per_work_ns)?,
            product(reread_bytes, self.source_read_nano_usd_per_byte)?,
            product(reread_requests, self.source_read_nano_usd_per_request)?,
        ])?;
        let restore_base = sum(&[
            product(o.output_bytes, self.object_read_nano_usd_per_byte)?,
            product(
                product(o.output_segments, 2)?,
                self.object_read_nano_usd_per_request,
            )?,
        ])?;
        let write_base = sum(&[
            product(o.output_bytes, self.object_write_nano_usd_per_byte)?,
            product(o.output_segments, self.object_write_nano_usd_per_request)?,
            product(
                o.output_bytes,
                product(
                    self.object_capacity_nano_usd_per_byte_ms,
                    self.expected_retention_ms,
                )?,
            )?,
        ])?;
        Ok(Estimates {
            recompute_base,
            restore_base,
            write_base,
            recompute_ms,
            restore_ms,
            write_ms,
        })
    }
    /// Compare recovery actions after the supplied portion of the job's critical
    /// path has elapsed. This lets sequential recovery closures consume one shared
    /// deadline budget instead of each receiving the original static slack.
    /// # Errors
    /// Rejects arithmetic overflow in modeled work, cost, or elapsed time.
    pub(crate) fn recovery_action_costs(
        &self,
        o: &Production,
        elapsed_before_ms: u64,
    ) -> Result<(u64, u64, u64, u64)> {
        let estimates = self.estimates(o)?;
        let recompute_elapsed = elapsed_before_ms
            .checked_add(estimates.recompute_ms)
            .ok_or_else(overflow)?;
        let restore_elapsed = elapsed_before_ms
            .checked_add(estimates.restore_ms)
            .ok_or_else(overflow)?;
        Ok((
            sum(&[
                estimates.recompute_base,
                self.penalty_after(recompute_elapsed)?,
            ])?,
            sum(&[estimates.restore_base, self.penalty_after(restore_elapsed)?])?,
            estimates.recompute_ms,
            estimates.restore_ms,
        ))
    }
    /// # Errors
    /// Rejects invalid topology, configuration or overflowing observed economics.
    pub fn decide(&self, o: Production) -> Result<Decision> {
        self.validate()?;
        require(
            o.consumer_count == 1,
            "v1 requires one consumer per transfer",
        )?;
        require(
            (o.output_segments == 0) == (o.output_bytes == 0),
            "cost output coverage mismatch",
        )?;
        let estimates = self.estimates(&o)?;
        let recompute = sum(&[
            estimates.recompute_base,
            self.penalty_after(estimates.recompute_ms)?,
        ])?;
        // A persisted failure path is sequential: the producer PUT finishes before
        // the loss gate, and restore begins only after that loss. Keep the two
        // durations together before applying the one deadline clamp.
        let persist_failure_elapsed = estimates
            .write_ms
            .checked_add(estimates.restore_ms)
            .ok_or_else(overflow)?;
        let restore = sum(&[
            estimates.restore_base,
            self.penalty_after(persist_failure_elapsed)?,
        ])?;
        let write = estimates.write_base;
        let no_persist = u128::from(self.failure_probability_ppm) * u128::from(recompute);
        let p = u128::from(self.failure_probability_ppm);
        let persist_cost = u128::from(write) * 1_000_000
            + u128::from(1_000_000 - self.failure_probability_ppm)
                * u128::from(self.penalty_after(estimates.write_ms)?)
            + p * u128::from(restore);
        let (persist, reason) = match self.mode {
            MaterializationMode::Always => (true, "forced_complete_copy_baseline"),
            MaterializationMode::Never => (false, "never_complete_copy_baseline"),
            MaterializationMode::Expected => (
                persist_cost + u128::from(self.min_expected_savings_nano_usd) * 1_000_000
                    < no_persist,
                "expected_cost_strict_savings_skip_ties",
            ),
        };
        Ok(Decision {
            config: self.clone(),
            observation: o,
            recompute_nano_usd: recompute,
            restore_nano_usd: restore,
            write_nano_usd: write,
            recompute_ms: estimates.recompute_ms,
            restore_ms: estimates.restore_ms,
            write_ms: estimates.write_ms,
            no_persist_nano_usd_ppm: no_persist,
            persist_nano_usd_ppm: persist_cost,
            persist,
            reason: reason.into(),
            object_request_cost_scope: ObjectRequestCostScope::PutHeadGetProxyV1,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn observation() -> Production {
        Production {
            operator_work_ns: 1_000_000,
            source_bytes: 10_000,
            source_requests: 1,
            output_bytes: 1000,
            output_segments: 1,
            consumer_count: 1,
            ..Default::default()
        }
    }
    #[test]
    fn endpoints_ties_margin_and_determinism() {
        let mut c = CostConfig {
            failure_probability_ppm: 0,
            ..Default::default()
        };
        assert!(!c.decide(observation()).unwrap().persist);
        c.failure_probability_ppm = 1_000_000;
        let a = c.decide(observation()).unwrap();
        assert!(a.persist);
        assert_eq!(a, c.decide(observation()).unwrap());
        assert_eq!(
            a.object_request_cost_scope,
            ObjectRequestCostScope::PutHeadGetProxyV1
        );
        c.min_expected_savings_nano_usd =
            a.recompute_nano_usd - a.restore_nano_usd - a.write_nano_usd;
        assert!(!c.decide(observation()).unwrap().persist);
        c.failure_probability_ppm += 1;
        assert!(c.validate().is_err());
    }
    #[test]
    fn empty_transfer_replay_has_no_source_or_compute_cost() {
        let mut o = observation();
        o.output_bytes = 0;
        o.output_segments = 0;
        let decision = CostConfig {
            failure_probability_ppm: 1_000_000,
            source_read_nano_usd_per_byte: 1000,
            ..Default::default()
        }
        .decide(o.clone())
        .unwrap();
        assert_eq!(decision.recompute_nano_usd, 0);
        assert_eq!(decision.recompute_ms, 0);
        assert!(!decision.persist);
        assert_eq!(decision.observation, o); // Actual original production is preserved.
        o.output_segments = 1;
        assert!(CostConfig::default().decide(o).is_err());
    }
    #[test]
    fn source_requests_capacity_and_storage_prices_have_real_effect() {
        let mut c = CostConfig {
            failure_probability_ppm: 1_000_000,
            compute_nano_usd_per_work_ns: 0,
            source_read_nano_usd_per_request: 10_000,
            object_read_nano_usd_per_byte: 0,
            object_read_nano_usd_per_request: 0,
            ..Default::default()
        };
        assert!(c.decide(observation()).unwrap().persist);
        c.object_capacity_nano_usd_per_byte_ms = 1;
        assert!(!c.decide(observation()).unwrap().persist);
        c.object_capacity_nano_usd_per_byte_ms = 0;
        c.object_read_nano_usd_per_request = 10_000;
        assert!(!c.decide(observation()).unwrap().persist);
        c.object_read_nano_usd_per_request = 0;
        c.source_read_nano_usd_per_request = 0;
        assert!(!c.decide(observation()).unwrap().persist);
        c.source_read_nano_usd_per_byte = 1000;
        assert!(c.decide(observation()).unwrap().persist);
        c.object_write_nano_usd_per_request = 10_000_000;
        assert!(!c.decide(observation()).unwrap().persist);
        c.object_capacity_nano_usd_per_byte_ms = 1_000_000_000_000;
        c.expected_retention_ms = 1_000_000_000_000;
        assert!(c.validate().is_err());
    }
    #[test]
    fn active_slo_and_checked_overflow() {
        let mut c = CostConfig {
            failure_probability_ppm: 1_000_000,
            compute_nano_usd_per_work_ns: 0,
            object_write_bytes_per_second: 1,
            deadline_slack_ms: 2_000_000,
            ..Default::default()
        };
        assert!(!c.decide(observation()).unwrap().persist);
        c.object_write_nano_usd_per_byte = 0;
        c.object_write_nano_usd_per_request = 0;
        c.object_read_nano_usd_per_byte = 0;
        c.object_read_nano_usd_per_request = 0;
        c.slo_penalty_nano_usd_per_ms = 100;
        c.source_read_bytes_per_second = 1;
        c.object_write_bytes_per_second = 100_000_000;
        c.deadline_slack_ms = 1;
        assert!(c.decide(observation()).unwrap().persist);
        c.object_write_bytes_per_second = 1;
        c.source_read_bytes_per_second = 100_000_000;
        assert!(!c.decide(observation()).unwrap().persist);
        c.compute_nano_usd_per_work_ns = u64::MAX;
        assert!(c.validate().is_err());
        c.compute_nano_usd_per_work_ns = 1_000_000_000_000;
        let mut o = observation();
        o.operator_work_ns = u64::MAX;
        assert!(c.decide(o).is_err());
        c.object_read_bytes_per_second = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn sequential_write_then_restore_uses_one_deadline_penalty() {
        let o = Production {
            operator_work_ns: 90_000_000,
            source_bytes: 1_000,
            source_requests: 0,
            output_bytes: 70,
            output_segments: 1,
            consumer_count: 1,
            ..Default::default()
        };
        let c = CostConfig {
            failure_probability_ppm: 1_000_000,
            compute_nano_usd_per_work_ns: 1,
            object_write_nano_usd_per_byte: 0,
            object_write_nano_usd_per_request: 0,
            object_read_nano_usd_per_byte: 0,
            object_read_nano_usd_per_request: 0,
            source_read_bytes_per_second: 1_000_000,
            object_write_bytes_per_second: 1_000,
            object_read_bytes_per_second: 1_000,
            deadline_slack_ms: 100,
            slo_penalty_nano_usd_per_ms: 3_000_000,
            ..Default::default()
        };
        let decision = c.decide(o.clone()).unwrap();
        assert_eq!(decision.recompute_ms, 91);
        assert_eq!(decision.write_ms, 70);
        assert_eq!(decision.restore_ms, 70);
        assert_eq!(decision.restore_nano_usd, 120_000_000);
        assert!(!decision.persist);

        let mut relaxed = c;
        relaxed.deadline_slack_ms = 200;
        assert!(relaxed.decide(o).unwrap().persist);
    }

    #[test]
    fn sequential_recovery_closures_share_remaining_slack() {
        let c = CostConfig {
            compute_nano_usd_per_work_ns: 1,
            source_read_bytes_per_second: 1_000,
            object_read_bytes_per_second: 1_000,
            deadline_slack_ms: 100,
            slo_penalty_nano_usd_per_ms: 1_000_000,
            ..Default::default()
        };
        let o = Production {
            operator_work_ns: 10_000_000,
            source_bytes: 30,
            source_requests: 0,
            output_bytes: 60,
            output_segments: 1,
            consumer_count: 1,
            ..Default::default()
        };
        let (first_recompute, first_restore, _, first_restore_ms) =
            c.recovery_action_costs(&o, 0).unwrap();
        assert!(first_restore < first_recompute);
        assert_eq!(first_restore_ms, 60);

        // Each restore alone is within 100 ms. After the first 60 ms restore,
        // a second restore would finish at 120 ms while recompute still fits.
        let (second_recompute, second_restore, _, _) =
            c.recovery_action_costs(&o, first_restore_ms).unwrap();
        assert!(second_recompute < second_restore);
    }
}
