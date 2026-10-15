//! Sequential parent/child process orchestration; each child executes exactly once.
use crate::{
    Error, Result,
    engine::{EngineConfig, ExecutionOutcome},
    environment::{BuildInfo, EnvironmentInfo, RepositoryInfo},
    result::{BenchmarkResultV1, IterationResult, RunConfig},
    spec::BenchmarkCase,
    stats::SummaryMetrics,
};
use std::{
    path::Path,
    process::{Command, Output},
    time::Instant,
};

/// Decode a child outcome only after successful exit and mandatory correctness.
/// Checks successful exit, child schema and required correctness.
///
/// # Errors
/// Returns an error for invalid input/configuration or a malformed/inconsistent protocol; child execution failures are retained in the result.
pub fn decode_child(output: &Output) -> Result<ExecutionOutcome> {
    if !output.status.success() {
        return Err(Error::Child(format!(
            "exit {:?}; {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    let outcome: ExecutionOutcome = serde_json::from_slice(&output.stdout)
        .map_err(|e| Error::Child(format!("malformed child outcome: {e}")))?;
    if outcome.schema_version != 1 || !outcome.validated || outcome.result_fingerprint.is_empty() {
        return Err(Error::Child(
            "unsupported or unvalidated child result".into(),
        ));
    }
    Ok(outcome)
}
/// Execute one OS process; arguments are passed directly, never through a shell.
/// Spawns a child and retains its status, protocol outcome and diagnostics.
///
/// # Errors
/// Returns an error for invalid input/configuration or a malformed/inconsistent protocol; child execution failures are retained in the result.
pub fn invoke(command: &mut Command, index: u32, warmup: bool) -> Result<IterationResult> {
    let start = Instant::now();
    let output = command.output();
    let process_time_ns = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let peak_rss_bytes = output
        .as_ref()
        .ok()
        .and_then(|o| parse_peak_rss(&String::from_utf8_lossy(&o.stderr)));
    let (exit_code, outcome, error) = match output {
        Ok(o) => match decode_child(&o) {
            Ok(value) => (o.status.code(), Some(value), None),
            Err(e) => (o.status.code(), None, Some(e.to_string())),
        },
        Err(e) => (None, None, Some(e.to_string())),
    };
    Ok(IterationResult {
        index,
        warmup,
        process_time_ns,
        exit_code,
        outcome,
        error,
        cpu_time_ns: None,
        peak_rss_bytes,
    })
}
/// All iterations are retained. Failed children produce an invalid diagnostic artifact.
/// Runs isolated children and constructs a validated result artifact.
///
/// # Errors
/// Returns an error for invalid input/configuration or a malformed/inconsistent protocol; child execution failures are retained in the result.
pub fn run(
    executable: &Path,
    spec_path: &Path,
    root: &Path,
    engine_config: &EngineConfig,
    config: RunConfig,
) -> Result<BenchmarkResultV1> {
    crate::require(config.iterations > 0, "iterations must be positive")?;
    engine_config.validate()?;
    let case = BenchmarkCase::load(spec_path)?;
    let manifest = crate::dataset::inspect(
        root,
        config.dataset_verification == crate::result::DatasetVerification::Full,
    )?;
    crate::require(
        case.dataset.required_schema == manifest.required_schema,
        "dataset/spec mismatch",
    )?;
    let engine = engine_config.info();
    let time_flags = resource_time_flags();
    let mut expected_output = None;
    let mut warmups = Vec::new();
    let mut iterations = Vec::new();
    for (warmup, count) in [(true, config.warmups), (false, config.iterations)] {
        for index in 0..count {
            let mut command = child_command(
                executable,
                spec_path,
                root,
                engine_config,
                time_flags.as_deref(),
            );
            let mut it = invoke(&mut command, index, warmup)?;
            if let Some(o) = &it.outcome {
                let identity = (o.result_fingerprint.clone(), o.rows_output);
                let protocol_error = if o.engine != engine {
                    Some("child engine metadata differs from requested configuration")
                } else if expected_output
                    .as_ref()
                    .is_some_and(|expected| expected != &identity)
                {
                    Some("child output differs across isolated iterations")
                } else {
                    None
                };
                if let Some(message) = protocol_error {
                    it.outcome = None;
                    it.error = Some(message.into());
                } else {
                    expected_output = Some(identity);
                }
            }
            if let Some(e) = &it.error {
                tracing::warn!(iteration = index, warmup, "{e}");
            }
            if warmup {
                warmups.push(it);
            } else {
                iterations.push(it);
            }
        }
    }
    let valid = warmups.iter().chain(&iterations).all(|i| {
        i.outcome.as_ref().is_some_and(|o| o.validated)
            && i.error.is_none()
            && i.exit_code == Some(0)
    });
    let summary = if valid {
        SummaryMetrics::new(
            &iterations
                .iter()
                .filter_map(|i| i.outcome.as_ref().map(|o| o.query_time_ns))
                .collect::<Vec<_>>(),
        )
    } else {
        None
    };
    let result = BenchmarkResultV1 {
        schema_version: 1,
        run_id: uuid::Uuid::new_v4().to_string(),
        created_at: chrono::Utc::now(),
        repository: RepositoryInfo::collect(),
        environment: EnvironmentInfo::collect(),
        build: BuildInfo::default(),
        engine,
        workload: case,
        dataset_manifest_fingerprint: manifest.identity()?,
        dataset: manifest,
        run_config: config,
        valid,
        warmups,
        iterations,
        summary,
    };
    result.validate()?;
    Ok(result)
}

fn child_command(
    executable: &Path,
    spec_path: &Path,
    root: &Path,
    engine_config: &EngineConfig,
    time_flags: Option<&[&str]>,
) -> Command {
    let mut command = if let Some(flags) = time_flags {
        let mut c = Command::new("/usr/bin/time");
        c.args(flags).arg(executable);
        c
    } else {
        Command::new(executable)
    };
    command
        .arg("__child-run")
        .arg("--spec")
        .arg(spec_path)
        .arg("--dataset")
        .arg(root)
        .arg("--engine-option")
        .arg(format!(
            "target_partitions={}",
            engine_config.target_partitions
        ))
        .arg("--engine-option")
        .arg(format!("batch_size={}", engine_config.batch_size))
        .arg("--engine-option")
        .arg(format!(
            "collect_statistics={}",
            engine_config.collect_statistics
        ));
    command
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    #[test]
    fn failure_protocols() {
        for script in [
            "exit 2",
            "kill -KILL $$",
            "printf garbage",
            "printf '{\"schema_version\":99}'",
        ] {
            let it = invoke(Command::new("sh").args(["-c", script]), 0, false).unwrap();
            assert!(it.error.is_some());
            assert!(it.outcome.is_none());
        }
        let it = invoke(&mut Command::new("/nonexistent/ade-child"), 0, false).unwrap();
        assert!(it.error.is_some());
    }
}

// The external time utility observes the complete child lifetime, including validation.
fn parse_peak_rss(stderr: &str) -> Option<u64> {
    for line in stderr.lines() {
        if let Some(value) = line.trim().strip_prefix("ade_peak_rss_kib=") {
            return value.parse::<u64>().ok().and_then(|n| n.checked_mul(1024));
        }
        if line.contains("maximum resident set size") {
            return line.split_whitespace().next()?.parse().ok();
        }
    }
    None
}

#[cfg(all(test, unix))]
mod resource_tests {
    use super::*;
    #[test]
    fn resource_units_and_unvalidated_protocol() {
        assert_eq!(
            parse_peak_rss("  123456 maximum resident set size"),
            Some(123_456)
        );
        assert_eq!(parse_peak_rss("ade_peak_rss_kib=123"), Some(125_952));
        assert_eq!(parse_peak_rss("unavailable"), None);
        let value = ExecutionOutcome {
            schema_version: 1,
            validated: false,
            engine: crate::engine::EngineInfo {
                name: "datafusion".into(),
                version: "test".into(),
                configuration: std::collections::BTreeMap::new(),
            },
            setup_time_ns: 1,
            query_time_ns: 1,
            rows_output: 1,
            result_fingerprint: "x".into(),
        };
        let output = Command::new("sh")
            .args([
                "-c",
                "printf '%s' \"$1\"",
                "child",
                &serde_json::to_string(&value).unwrap(),
            ])
            .output()
            .unwrap();
        assert!(decode_child(&output).is_err());
    }
}

// Probe once outside measured iterations: restricted hosts may forbid resource queries.
fn resource_time_flags() -> Option<Vec<&'static str>> {
    let flags = if cfg!(target_os = "macos") {
        vec!["-l"]
    } else if cfg!(target_os = "linux") {
        vec!["-f", "ade_peak_rss_kib=%M"]
    } else {
        return None;
    };
    let probe = Command::new("/usr/bin/time")
        .args(&flags)
        .arg("/usr/bin/true")
        .output()
        .ok()?;
    (probe.status.success() && parse_peak_rss(&String::from_utf8_lossy(&probe.stderr)).is_some())
        .then_some(flags)
}
