#![cfg_attr(not(any(target_os = "linux", target_os = "macos")), allow(unused))]

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn main() -> std::process::ExitCode {
    cli::main()
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn main() -> std::process::ExitCode {
    eprintln!("ade-cache requires Linux or macOS");
    std::process::ExitCode::FAILURE
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod cli {
    use ade_cache::policy::{Calibration, Mode};
    use ade_cache::{
        Control, Error, Job, Limits, collect_expired, initialize, inspect, purge_entry, run,
    };
    use ade_exec::CancellationToken;
    use ade_physical::{MAX_PLAN_BYTES, PlanV1};
    use arrow::array::{Int64Array, UInt64Array};
    use clap::{Parser, Subcommand, ValueEnum};
    use serde_json::{Value, json};
    use std::{
        fs::File,
        io::{Read, Write},
        path::{Path, PathBuf},
        process::ExitCode,
        time::Duration,
    };

    #[derive(Parser)]
    #[command(about = "Run bounded native plans with explicit local shared materialization")]
    struct Args {
        #[arg(long, global = true, default_value_t = 30_000)]
        timeout_ms: u64,
        #[command(subcommand)]
        command: Command,
    }

    #[derive(Clone, Copy, ValueEnum)]
    enum Policy {
        Disabled,
        Never,
        Always,
        #[value(name = "expected_value_v1")]
        ExpectedValueV1,
    }

    impl From<Policy> for Mode {
        fn from(p: Policy) -> Self {
            match p {
                Policy::Disabled => Self::Disabled,
                Policy::Never => Self::Never,
                Policy::Always => Self::Always,
                Policy::ExpectedValueV1 => Self::ExpectedValueV1,
            }
        }
    }

    #[derive(Subcommand)]
    enum Command {
        Init {
            #[arg(long)]
            cache: PathBuf,
            #[arg(long, default_value_t = Limits::default().quota_bytes)]
            quota_bytes: u64,
            #[arg(long, default_value_t = Limits::default().max_entries)]
            max_entries: usize,
        },
        Run {
            #[arg(long)]
            plan: PathBuf,
            #[arg(long)]
            source: PathBuf,
            #[arg(long)]
            catalog_pin: String,
            #[arg(long)]
            cache: Option<PathBuf>,
            #[arg(long, default_value_t = 2)]
            cut: u32,
            #[arg(long, value_enum, default_value = "disabled")]
            policy: Policy,
            #[arg(long)]
            calibration: Option<PathBuf>,
            #[arg(long, default_value_t = 4 * 1024 * 1024)]
            stage_bytes: u64,
            #[arg(long, default_value_t = 3_600_000)]
            ttl_ms: u64,
        },
        Inspect {
            #[arg(long)]
            cache: PathBuf,
        },
        Gc {
            #[arg(long)]
            cache: PathBuf,
        },
        Purge {
            #[arg(long)]
            cache: PathBuf,
            #[arg(long)]
            key: String,
        },
    }

    pub(super) fn main() -> ExitCode {
        let args = Args::parse();
        match invoke(args) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!(
                    "{}",
                    json!({"version": 1, "action": "refuse", "error": error.to_string()})
                );
                ExitCode::FAILURE
            }
        }
    }

    fn read(path: &Path, cap: usize) -> Result<Vec<u8>, Error> {
        let file = File::from(rustix::fs::open(
            path,
            rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK,
            rustix::fs::Mode::empty(),
        )?);
        if !file.metadata()?.is_file() {
            return Err(Error::Request("request file must be regular"));
        }
        let mut data = Vec::new();
        file.take(cap as u64 + 1).read_to_end(&mut data)?;
        if data.len() > cap {
            return Err(Error::Capacity);
        }
        Ok(data)
    }

    fn invoke(args: Args) -> Result<(), Error> {
        let token = CancellationToken::default();
        let signal = token.clone();
        ctrlc::set_handler(move || signal.cancel())
            .map_err(|_| Error::Request("signal handler unavailable"))?;
        let control = Control::new(token, Duration::from_millis(args.timeout_ms))?;
        let output = match args.command {
            Command::Init {
                cache,
                quota_bytes,
                max_entries,
            } => {
                initialize(
                    &cache,
                    Limits {
                        quota_bytes,
                        max_entries,
                    },
                    &control,
                )?;
                json!({"version": 1, "action": "initialized"})
            }
            Command::Inspect { cache } => serde_json::to_value(inspect(&cache, &control)?)?,
            Command::Gc { cache } => {
                json!({"version": 1, "evicted": collect_expired(&cache, &control)?})
            }
            Command::Purge { cache, key } => {
                purge_entry(&cache, &key, &control)?;
                json!({"version": 1, "purged": key})
            }
            Command::Run {
                plan,
                source,
                catalog_pin,
                cache,
                cut,
                policy,
                calibration,
                stage_bytes,
                ttl_ms,
            } => {
                let plan = PlanV1::from_json(&read(&plan, MAX_PLAN_BYTES)?)
                    .map_err(|_| Error::Request("PlanV1"))?;
                let mode = policy.into();
                let calibration: Option<Calibration> =
                    if matches!(mode, Mode::Always | Mode::ExpectedValueV1) {
                        calibration
                            .map(|p| serde_json::from_slice(&read(&p, 4096)?).map_err(Error::from))
                            .transpose()?
                    } else {
                        None
                    };
                // Aggregate results are finite. Keep the CLI envelope and JSON copy bounded
                // separately from the managed Arrow output held by the underlying SDK.
                if !matches!(mode, Mode::Always | Mode::ExpectedValueV1)
                    && !plan
                        .nodes
                        .iter()
                        .any(|n| matches!(n.operator, ade_physical::Operator::GroupSumCount { .. }))
                {
                    return Err(Error::Request("CLI requires a native sum/count aggregate"));
                }
                let output = run(
                    &Job {
                        plan: &plan,
                        source_dir: &source,
                        catalog_pin: &catalog_pin,
                        cache_dir: cache.as_deref(),
                        cut,
                        mode,
                        calibration,
                        stage_bytes,
                        ttl_ms,
                    },
                    &control,
                )?;
                result_json(&plan, &output, &control)?
            }
        };
        let mut bytes = serde_json::to_vec(&output)?;
        bytes.push(b'\n');
        control.check()?;
        std::io::stdout().lock().write_all(&bytes)?;
        Ok(())
    }
    fn result_json(
        plan: &PlanV1,
        output: &ade_cache::JobOutput,
        control: &Control,
    ) -> Result<Value, Error> {
        let schema = &plan.nodes[plan.output as usize].schema;
        let mut rows = Vec::new();
        for batch in &output.result.batches {
            control.check()?;
            for row in 0..batch.num_rows() {
                let values = batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column.is_null(row) {
                            Ok(Value::Null)
                        } else if let Some(a) = column.as_any().downcast_ref::<Int64Array>() {
                            Ok(json!(a.value(row)))
                        } else if let Some(a) = column.as_any().downcast_ref::<UInt64Array>() {
                            Ok(json!(a.value(row)))
                        } else {
                            Err(Error::Request("CLI result type"))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                rows.push(values);
            }
        }
        Ok(json!({"audit": output.audit, "schema": schema, "rows": rows}))
    }
}
