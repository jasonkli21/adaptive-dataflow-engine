//! Compatibility checks precede all timing arithmetic.
use crate::{Error, Result, result::BenchmarkResultV1};
use serde::Serialize;
/// A timing delta is a presentation aid, not a statistical significance claim.
#[derive(Debug, Serialize)]
pub struct Comparison {
    pub classification: String,
    pub baseline_median_ns: f64,
    pub candidate_median_ns: f64,
    pub baseline_mean_ns: f64,
    pub candidate_mean_ns: f64,
    pub baseline_peak_rss_bytes: Option<u64>,
    pub candidate_peak_rss_bytes: Option<u64>,
    pub peak_rss_delta_percent: Option<f64>,
    pub median_delta_percent: f64,
    pub mean_delta_percent: f64,
    pub baseline_stddev_ns: f64,
    pub candidate_stddev_ns: f64,
    pub rows_output: u64,
    pub warnings: Vec<String>,
}
/// Compares validated compatible runs using median query time.
///
/// # Errors
/// Rejects invalid schemas, failed correctness, incompatible workloads/data/output or zero baseline timing.
#[allow(
    clippy::cast_precision_loss,
    reason = "Resource percentage deltas are approximate presentation values; raw byte counts remain exact."
)]
pub fn compare(a: &BenchmarkResultV1, b: &BenchmarkResultV1) -> Result<Comparison> {
    a.validate()?;
    b.validate()?;
    let incompatible = |s: &str| Err(Error::Incompatible(s.into()));
    if !a.valid || !b.valid {
        return incompatible("required validation failed");
    }
    if a.workload != b.workload {
        return incompatible("semantic workload differs");
    }
    if a.dataset.dataset_id != b.dataset.dataset_id
        || a.dataset.dataset_version != b.dataset.dataset_version
        || a.dataset.logical_fingerprint != b.dataset.logical_fingerprint
        || a.dataset.schema_fingerprint != b.dataset.schema_fingerprint
    {
        return incompatible("logical input identity differs");
    }
    let left = a.iterations[0]
        .outcome
        .as_ref()
        .ok_or_else(|| Error::Incompatible("missing outcome".into()))?;
    let right = b.iterations[0]
        .outcome
        .as_ref()
        .ok_or_else(|| Error::Incompatible("missing outcome".into()))?;
    if left.result_fingerprint != right.result_fingerprint || left.rows_output != right.rows_output
    {
        return incompatible("canonical validated outputs differ");
    }
    let mut warnings = Vec::new();
    if a.environment != b.environment {
        warnings.push("hardware/OS differs".into());
    }
    if a.build != b.build {
        warnings.push("compiler/build differs".into());
    }
    if a.run_config != b.run_config {
        warnings.push("run configuration/verification policy differs".into());
    }
    if a.engine != b.engine {
        warnings.push("engine version/configuration differs".into());
    }
    if a.repository.git_dirty == Some(true) || b.repository.git_dirty == Some(true) {
        warnings.push("dirty source tree".into());
    }
    if a.repository.git_commit.is_none() || b.repository.git_commit.is_none() {
        warnings.push("source commit unavailable".into());
    }
    if a.dataset_manifest_fingerprint != b.dataset_manifest_fingerprint {
        warnings.push("physical dataset/manifest differs".into());
    }
    let x = a
        .summary
        .as_ref()
        .ok_or_else(|| Error::Incompatible("missing summary".into()))?;
    let y = b
        .summary
        .as_ref()
        .ok_or_else(|| Error::Incompatible("missing summary".into()))?;
    if x.median <= 0. || x.mean <= 0. {
        return incompatible("zero baseline timing");
    }
    let median_delta_percent = (y.median / x.median - 1.) * 100.;
    let mean_delta_percent = (y.mean / x.mean - 1.) * 100.;
    let peak = |r: &BenchmarkResultV1| {
        r.iterations
            .iter()
            .map(|i| i.peak_rss_bytes)
            .collect::<Option<Vec<_>>>()
            .and_then(|v| v.into_iter().max())
    };
    let baseline_peak_rss_bytes = peak(a);
    let candidate_peak_rss_bytes = peak(b);
    let peak_rss_delta_percent = baseline_peak_rss_bytes
        .zip(candidate_peak_rss_bytes)
        .filter(|(x, _)| *x > 0)
        .map(|(x, y)| (y as f64 / x as f64 - 1.) * 100.);
    Ok(Comparison {
        baseline_median_ns: x.median,
        candidate_median_ns: y.median,
        baseline_mean_ns: x.mean,
        candidate_mean_ns: y.mean,
        baseline_peak_rss_bytes,
        candidate_peak_rss_bytes,
        peak_rss_delta_percent,
        classification: if median_delta_percent > 3. {
            "regression"
        } else if median_delta_percent < -3. {
            "improvement"
        } else {
            "neutral"
        }
        .into(),
        median_delta_percent,
        mean_delta_percent,
        baseline_stddev_ns: x.stddev,
        candidate_stddev_ns: y.stddev,
        rows_output: left.rows_output,
        warnings,
    })
}
