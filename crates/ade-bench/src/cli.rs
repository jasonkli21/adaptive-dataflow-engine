//! Command-line interface. Machine output is stdout; diagnostics go to stderr.
use crate::{
    Result,
    dataset::{self, SyntheticConfig},
    engine::{self, EngineConfig},
    environment::{BuildInfo, EnvironmentInfo, RepositoryInfo},
    result::{BenchmarkResultV1, DatasetVerification, RunConfig},
    spec::BenchmarkCase,
};
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Format {
    Human,
    Json,
}
#[derive(Debug, Parser)]
#[command(
    name = "ade-bench",
    version,
    about = "Phase 0 architecture lab and correctness-first benchmark harness"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}
#[derive(Debug, Subcommand)]
pub enum Commands {
    Env {
        #[arg(long, value_enum, default_value = "human")]
        format: Format,
    },
    Data {
        #[command(subcommand)]
        command: DataCommand,
    },
    Validate(CaseArgs),
    Run {
        #[command(flatten)]
        case: CaseArgs,
        #[arg(long, default_value_t = 1)]
        warmups: u32,
        #[arg(long, value_enum, default_value = "metadata")]
        verify_data: DatasetVerification,
        #[arg(long, default_value_t = 5)]
        iterations: u32,
        #[arg(long)]
        output: PathBuf,
    },
    Compare {
        baseline: PathBuf,
        candidate: PathBuf,
        #[arg(long, value_enum, default_value = "human")]
        format: Format,
    },
    /// Check version and cross-field integrity of a result artifact.
    CheckResult {
        path: PathBuf,
    },
    #[command(name = "__child-run", hide = true)]
    ChildRun(CaseArgs),
}
#[derive(Debug, Args)]
pub struct CaseArgs {
    #[arg(long, default_value = "datafusion", value_parser = ["datafusion"])]
    pub engine: String,
    #[arg(long)]
    pub spec: PathBuf,
    #[arg(long)]
    pub dataset: PathBuf,
    #[arg(long = "engine-option")]
    pub engine_options: Vec<String>,
}
#[derive(Debug, Subcommand)]
pub enum DataCommand {
    Generate {
        #[command(subcommand)]
        kind: GenerateCommand,
    },
    Inspect {
        path: PathBuf,
    },
}
#[derive(Debug, Subcommand)]
pub enum GenerateCommand {
    Synthetic {
        #[arg(long)]
        dataset_id: String,
        #[arg(long)]
        output: PathBuf,
        #[command(flatten)]
        config: SyntheticConfig,
    },
    Tpch {
        #[arg(long, default_value_t = 1.0)]
        scale_factor: f64,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value = "tpchgen-cli")]
        generator: PathBuf,
    },
}
fn json(value: &impl serde::Serialize) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
/// Executes one CLI command.
///
/// # Errors
/// Returns structured configuration, dataset, engine, validation, comparison or I/O errors.
#[allow(
    clippy::too_many_lines,
    reason = "Flat command dispatch keeps CLI argument handling together."
)]
pub async fn execute(cli: Cli) -> Result<()> {
    match cli.command {
        Commands::Env { format } => {
            let env = EnvironmentInfo::collect();
            let build = BuildInfo::default();
            let repository = RepositoryInfo::collect();
            match format {
                Format::Json => json(
                    &serde_json::json!({"schema_version":1,"environment":env,"build":build,"repository":repository}),
                )?,
                Format::Human => {
                    println!(
                        "{} {}\nCPU: {:?}\ncores: {:?}\nmemory bytes: {:?}\n{}; profile {}\nsource: {:?}, dirty {:?}",
                        env.os,
                        env.arch,
                        env.cpu_model,
                        env.logical_cores,
                        env.memory_bytes,
                        build.rustc,
                        build.profile,
                        repository.git_commit,
                        repository.git_dirty
                    );
                }
            }
        }
        Commands::Data { command } => match command {
            DataCommand::Generate { kind } => match kind {
                GenerateCommand::Synthetic {
                    dataset_id,
                    output,
                    config,
                } => json(&dataset::generate(&output, &dataset_id, &config)?)?,
                GenerateCommand::Tpch {
                    scale_factor,
                    output,
                    generator,
                } => json(&dataset::generate_tpch(&output, scale_factor, &generator)?)?,
            },
            DataCommand::Inspect { path } => json(&dataset::inspect(&path, true)?)?,
        },
        Commands::Validate(case) => execute_case(&case, true).await?,
        Commands::ChildRun(case) => execute_case(&case, false).await?,
        Commands::Run {
            case,
            warmups,
            iterations,
            verify_data,
            output,
        } => {
            crate::require(
                !output.exists(),
                "result output exists; choose a new artifact path",
            )?;
            let config = EngineConfig::from_options(&case.engine_options)?;
            let result = crate::runner::run(
                &std::env::current_exe()?,
                &case.spec,
                &case.dataset,
                &config,
                RunConfig {
                    warmups,
                    iterations,
                    cache_policy: "warm".into(),
                    log_level: "warn".into(),
                    dataset_verification: verify_data,
                },
            )?;
            crate::result::write_atomic(&output, &result)?;
            if !result.valid {
                return Err(crate::Error::Child(
                    "run failed; diagnostic result was written".into(),
                ));
            }
            let summary = result
                .summary
                .as_ref()
                .ok_or_else(|| crate::Error::Invalid("missing summary".into()))?;
            println!(
                "valid: {} measured iterations; query median {:.3} ms; stddev {:.3} ms",
                summary.count,
                summary.median / 1e6,
                summary.stddev / 1e6
            );
        }
        Commands::Compare {
            baseline,
            candidate,
            format,
        } => {
            let a = BenchmarkResultV1::load(&baseline)?;
            let b = BenchmarkResultV1::load(&candidate)?;
            let comparison = crate::compare::compare(&a, &b)?;
            match format {
                Format::Json => json(&comparison)?,
                Format::Human => {
                    println!("Metric               Baseline       Candidate       Delta");
                    println!(
                        "median (ms)       {:12.3}    {:12.3}    {:+.2}%",
                        comparison.baseline_median_ns / 1e6,
                        comparison.candidate_median_ns / 1e6,
                        comparison.median_delta_percent
                    );
                    println!(
                        "mean (ms)         {:12.3}    {:12.3}    {:+.2}%",
                        comparison.baseline_mean_ns / 1e6,
                        comparison.candidate_mean_ns / 1e6,
                        comparison.mean_delta_percent
                    );
                    println!(
                        "stddev (ms)       {:12.3}    {:12.3}",
                        comparison.baseline_stddev_ns / 1e6,
                        comparison.candidate_stddev_ns / 1e6
                    );
                    if let (Some(x), Some(y), Some(delta)) = (
                        comparison.baseline_peak_rss_bytes,
                        comparison.candidate_peak_rss_bytes,
                        comparison.peak_rss_delta_percent,
                    ) {
                        println!("peak RSS (bytes)  {x:12}    {y:12}    {delta:+.2}%");
                    }
                    println!(
                        "status: {}; output rows {}",
                        comparison.classification, comparison.rows_output
                    );
                    for warning in comparison.warnings {
                        println!("warning: {warning}");
                    }
                }
            }
        }
        Commands::CheckResult { path } => {
            BenchmarkResultV1::load(&path)?;
            println!("result schema/integrity valid");
        }
    }
    Ok(())
}
async fn execute_case(case: &CaseArgs, full_inspection: bool) -> Result<()> {
    let config = EngineConfig::from_options(&case.engine_options)?;
    let manifest = dataset::inspect(&case.dataset, full_inspection)?;
    let spec = BenchmarkCase::load(&case.spec)?;
    json(&engine::execute_validated(&case.dataset, &manifest, &spec, &config).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cli_contract() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
        assert!(Cli::try_parse_from(["ade-bench", "env", "--format", "json"]).is_ok());
        assert!(Cli::try_parse_from(["ade-bench", "run"]).is_err());
    }
}
