//! Pure threshold policy. No I/O, payload ownership, routing or recovery.
use crate::{Result, require};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdaptiveConfig {
    pub buffer_high_pct: u8,
    /// Escalate only with unconsumed spill backlog. Zero disables remote escalation.
    pub remote_after_bytes: u64,
}
impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            buffer_high_pct: 80,
            remote_after_bytes: 64 * 1024 * 1024,
        }
    }
}
impl AdaptiveConfig {
    /// # Errors
    /// Rejects invalid percentages and thresholds beyond 1 TiB.
    pub fn validate(&self) -> Result<()> {
        require(
            (1..=100).contains(&self.buffer_high_pct),
            "adaptive_buffer_high_pct must be 1..100",
        )?;
        require(
            self.remote_after_bytes <= 1 << 40,
            "adaptive_remote_after_bytes exceeds 1 TiB",
        )
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveTier {
    #[default]
    Direct,
    Buffered,
    Spill,
    ObjectStore,
}
impl AdaptiveTier {
    pub(crate) fn index(self) -> usize {
        self as usize
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdaptiveObservation {
    pub incoming_bytes: usize,
    pub buffer_bytes: usize,
    pub capacity_bytes: usize,
    pub spill_backlog_bytes: usize,
    pub consumer_ready: bool,
    pub producer_wait_ns: u64,
    pub consumer_wait_ns: u64,
    pub produced_bytes: u64,
    pub produced_segments: u64,
    pub consumed_segments: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveReason {
    ConsumerNotReady,
    BufferPressure,
    SpillBacklogAndSize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdaptiveTransition {
    pub old: AdaptiveTier,
    pub new: AdaptiveTier,
    /// Destination acceptance ordinal (cross-producer order is unspecified).
    pub boundary: u64,
    pub transfer: crate::ids::TransferId,
    pub sequence: u64,
    pub reason: AdaptiveReason,
    pub projected_buffer_pct: u64,
    pub lag_segments: u64,
    pub observation: AdaptiveObservation,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdaptiveMetrics {
    pub policy: String,
    pub segments_by_tier: [u64; 4],
    pub bytes_by_tier: [u64; 4],
    /// At most three records per destination, independent of segment count.
    pub transitions: Vec<AdaptiveTransition>,
    pub extents: usize,
}
/// Deterministic, monotone `threshold_v1`. Re-evaluate at the same boundary to skip
/// tiers if the incoming frame already exceeds the available buffer threshold.
#[must_use]
pub fn decide(
    config: &AdaptiveConfig,
    tier: AdaptiveTier,
    o: &AdaptiveObservation,
) -> Option<(AdaptiveTier, AdaptiveReason)> {
    match tier {
        AdaptiveTier::Direct
            if !o.consumer_ready || o.buffer_bytes > 0 || o.incoming_bytes > o.capacity_bytes =>
        {
            Some((AdaptiveTier::Buffered, AdaptiveReason::ConsumerNotReady))
        }
        AdaptiveTier::Buffered
            if (o.buffer_bytes.saturating_add(o.incoming_bytes) as u128) * 100
                >= (o.capacity_bytes as u128) * u128::from(config.buffer_high_pct) =>
        {
            Some((AdaptiveTier::Spill, AdaptiveReason::BufferPressure))
        }
        AdaptiveTier::Spill
            if config.remote_after_bytes > 0
                && o.spill_backlog_bytes > 0
                && o.produced_bytes.saturating_add(o.incoming_bytes as u64)
                    >= config.remote_after_bytes =>
        {
            Some((
                AdaptiveTier::ObjectStore,
                AdaptiveReason::SpillBacklogAndSize,
            ))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn configuration_serialization_is_strict_and_roundtrips() {
        let config = crate::ExchangeConfig {
            mode: crate::ExchangeMode::Adaptive,
            ..Default::default()
        };
        config.validate().unwrap();
        let json = serde_json::to_string(&config).unwrap();
        let roundtrip: crate::ExchangeConfig = serde_json::from_str(&json).unwrap();
        roundtrip.validate().unwrap();
        assert_eq!(serde_json::to_string(&roundtrip).unwrap(), json);
        for json in [
            r#"{"buffer_high_pct":80,"remote_after_bytes":0,"unknown":1}"#,
            r#"{"buffer_high_pct":80,"buffer_high_pct":90,"remote_after_bytes":0}"#,
            r#"{"buffer_high_pct":256,"remote_after_bytes":0}"#,
        ] {
            assert!(serde_json::from_str::<AdaptiveConfig>(json).is_err());
        }
    }
    #[test]
    fn deterministic_monotone_thresholds_and_bounds() {
        let c = AdaptiveConfig {
            buffer_high_pct: 80,
            remote_after_bytes: 100,
        };
        let mut o = AdaptiveObservation {
            incoming_bytes: 80,
            capacity_bytes: 100,
            consumer_ready: true,
            ..Default::default()
        };
        assert_eq!(decide(&c, AdaptiveTier::Direct, &o), None);
        o.consumer_ready = false;
        assert_eq!(
            decide(&c, AdaptiveTier::Direct, &o),
            Some((AdaptiveTier::Buffered, AdaptiveReason::ConsumerNotReady))
        );
        o.incoming_bytes = 79;
        assert_eq!(decide(&c, AdaptiveTier::Buffered, &o), None);
        o.incoming_bytes = 80;
        assert_eq!(
            decide(&c, AdaptiveTier::Buffered, &o),
            Some((AdaptiveTier::Spill, AdaptiveReason::BufferPressure))
        );
        assert_eq!(decide(&c, AdaptiveTier::Spill, &o), None);
        o.spill_backlog_bytes = 1;
        o.produced_bytes = 20;
        assert_eq!(
            decide(&c, AdaptiveTier::Spill, &o),
            Some((
                AdaptiveTier::ObjectStore,
                AdaptiveReason::SpillBacklogAndSize
            ))
        );
        let run = || {
            let mut tier = AdaptiveTier::Direct;
            let mut count = 0;
            for _ in 0..10000 {
                if let Some((next, _)) = decide(&c, tier, &o) {
                    tier = next;
                    count += 1;
                }
            }
            (tier, count)
        };
        assert_eq!(run(), run());
        assert_eq!(run(), (AdaptiveTier::ObjectStore, 3));
        o.produced_bytes = u64::MAX;
        assert!(decide(&c, AdaptiveTier::Spill, &o).is_some());
        assert!(
            decide(
                &AdaptiveConfig {
                    remote_after_bytes: 0,
                    ..c.clone()
                },
                AdaptiveTier::Spill,
                &o
            )
            .is_none()
        );
        for pct in [0, 101, 255] {
            assert!(
                AdaptiveConfig {
                    buffer_high_pct: pct,
                    ..c.clone()
                }
                .validate()
                .is_err()
            );
        }
        assert!(
            AdaptiveConfig {
                remote_after_bytes: u64::MAX,
                ..c
            }
            .validate()
            .is_err()
        );
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdaptiveSummary {
    pub partitions: usize,
    pub max_extents_per_partition: usize,
    pub segments_by_tier: [u64; 4],
    pub bytes_by_tier: [u64; 4],
    /// Adjacent monotone edges, in tier order; also reason counts in v1.
    pub transitions_by_edge: [u64; 3],
}
impl AdaptiveSummary {
    pub(crate) fn add(&mut self, m: &AdaptiveMetrics) {
        self.partitions += 1;
        self.max_extents_per_partition = self.max_extents_per_partition.max(m.extents);
        for i in 0..4 {
            self.segments_by_tier[i] += m.segments_by_tier[i];
            self.bytes_by_tier[i] += m.bytes_by_tier[i];
        }
        for transition in &m.transitions {
            self.transitions_by_edge[transition.old.index()] += 1;
        }
    }
}
