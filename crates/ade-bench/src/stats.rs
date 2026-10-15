//! Descriptive statistics. Percentiles use linear interpolation at (n - 1) * p (R-7).
use serde::{Deserialize, Serialize};

/// Nanosecond summary; standard deviation is the population standard deviation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SummaryMetrics {
    pub count: usize,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub median: f64,
    pub stddev: f64,
    pub p90: f64,
    pub p95: f64,
}
impl SummaryMetrics {
    /// Returns no summary for an empty sample; no best-iteration substitution.
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "R-7 uses floating interpolation; indices lie in the sample bounds, and raw u64 measurements are retained."
    )]
    pub fn new(samples: &[u64]) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }
        let mut v: Vec<f64> = samples.iter().map(|&x| x as f64).collect();
        v.sort_by(f64::total_cmp);
        let count = v.len();
        let mean = v.iter().sum::<f64>() / count as f64;
        let percentile = |p: f64| {
            let index = (count - 1) as f64 * p;
            let lo = index.floor() as usize;
            let hi = index.ceil() as usize;
            v[lo] + (v[hi] - v[lo]) * index.fract()
        };
        Some(Self {
            count,
            min: v[0],
            max: v[count - 1],
            mean,
            median: percentile(0.5),
            stddev: (v.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / count as f64).sqrt(),
            p90: percentile(0.9),
            p95: percentile(0.95),
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn known_statistics() {
        let s = SummaryMetrics::new(&[5, 1, 3, 2, 4]).unwrap();
        assert_eq!((s.min, s.max, s.mean, s.median), (1., 5., 3., 3.));
        assert!((s.stddev - 2_f64.sqrt()).abs() < 1e-12);
        assert!((s.p90 - 4.6).abs() < 1e-12);
        assert!((s.p95 - 4.8).abs() < 1e-12);
        assert!(SummaryMetrics::new(&[]).is_none());
        assert_eq!(SummaryMetrics::new(&[7]).unwrap().stddev, 0.);
    }
}
