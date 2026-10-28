use ade_ops::{Error, Limits, Recipe, Store};
use clap::{Parser, Subcommand};
use std::{fs::File, io::Read, path::PathBuf};
use uuid::Uuid;
#[derive(Parser)]
#[command(about = "Bounded same-host batch operations with fixed managed workers")]
struct Cli {
    #[arg(long)]
    state: PathBuf,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Init,
    Submit {
        #[arg(long)]
        key: String,
        #[arg(long)]
        recipe: PathBuf,
    },
    Status {
        job: Uuid,
    },
    List {
        #[arg(long)]
        after: Option<Uuid>,
        #[arg(long, default_value_t = 16)]
        limit: usize,
    },
    History {
        job: Uuid,
    },
    Cancel {
        job: Uuid,
    },
    Recover,
    VerifySources {
        job: Uuid,
    },
    VerifyOutput {
        job: Uuid,
    },
    Gc {
        job: Uuid,
        #[arg(long)]
        apply: bool,
    },
    Run {
        job: Uuid,
    },
    Cleanup,
    Capabilities,
    #[cfg(feature = "fault-injection")]
    #[command(hide = true)]
    ExerciseClaim {
        job: Uuid,
    },
    #[cfg(feature = "fault-injection")]
    #[command(hide = true)]
    ExercisePublication,
    #[cfg(feature = "fault-injection")]
    #[command(hide = true)]
    ExerciseOwner,
}
fn summary(job: &ade_ops::Job) -> serde_json::Value {
    serde_json::json!({"job":job.id,"state":job.state,"owner":job.owner,"attempt":job.attempt,"instance":job.instance,"engine_job":job.engine_job,"attempt_history":job.attempts,"recipe_hash":job.recipe_hash,"coordinator_compat":job.coordinator_compat,"expires_ms":job.expires_ms,"cancel_requested":job.cancel_requested,"sequence":job.sequence})
}
fn list_summary(job: &ade_ops::Job) -> serde_json::Value {
    let mut value = summary(job);
    value["recorded_state"] = serde_json::json!(job.state);
    value["output_verification"] = serde_json::json!("NOT_CHECKED");
    value
}
#[allow(
    clippy::needless_pass_by_value,
    clippy::too_many_lines,
    reason = "CLI dispatch owns parsed arguments and keeps JSON routing in one place."
)]
fn run(cli: Cli) -> ade_ops::Result<serde_json::Value> {
    if matches!(cli.command, Command::Capabilities) {
        return Ok(
            serde_json::json!({"phase12":"COMPLETE_BOUNDED_V1","store_version":2,"recipe_version":1,"native_execution":true,"distributed_aggregate":true,"distributed_join":true,"worker_drain":true,"whole_job_retry":true,"native_output_commit":true,"queued_cancel":true,"active_cancel":true,"metadata_snapshot_cap":8_388_608,"runtime_disk_quota":true,"publication_primitive_only":false,"managed_protocol":"ADEOPS01","legacy_protocol":"ADETCP12"}),
        );
    }
    #[cfg(feature = "fault-injection")]
    if matches!(cli.command, Command::ExercisePublication) {
        return exercise_publication(&cli.state);
    }
    if let Command::Submit { key, recipe } = &cli.command {
        let file = File::open(recipe)?;
        if !file.metadata()?.is_file() || file.metadata()?.len() > 512 * 1024 {
            return Err(Error::Quota("recipe regular file/512KiB"));
        }
        let mut bytes = vec![];
        file.take(512 * 1024 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > 512 * 1024 {
            return Err(Error::Quota("recipe 512KiB"));
        }
        let parsed = serde_json::from_slice::<Recipe>(&bytes)?;
        let job = Store::open(&cli.state)?.submit(key, parsed)?;
        return Ok(summary(&job));
    }
    match &cli.command {
        Command::Run { job } => {
            let result = ade_ops::runner::run(&cli.state, *job)?;
            if result.state != ade_ops::JobState::Succeeded {
                return Err(Error::JobOutcome {
                    job: *job,
                    state: result.state,
                });
            }
            return Ok(summary(&result));
        }
        Command::Recover => {
            return Ok(serde_json::Value::Array(
                ade_ops::runner::recover(&cli.state)?
                    .iter()
                    .map(summary)
                    .collect(),
            ));
        }
        Command::Cancel { job } => return Ok(summary(&ade_ops::runner::cancel(&cli.state, *job)?)),
        Command::VerifyOutput { job } => {
            return Ok(serde_json::to_value(ade_ops::runner::verify_output(
                &cli.state, *job,
            )?)?);
        }
        Command::Gc { job, apply } => {
            return Ok(serde_json::to_value(ade_ops::runner::gc(
                &cli.state, *job, *apply,
            )?)?);
        }
        _ => {}
    }
    let mut store = if matches!(cli.command, Command::Init) {
        Store::init(&cli.state, Limits::default())?
    } else {
        Store::open(&cli.state)?
    };
    match cli.command {
        Command::Init => {
            Ok(serde_json::json!({"store":store.id(),"generation":store.generation()}))
        }
        Command::Submit { .. } => unreachable!(),
        Command::Status { job } => {
            let result = store.status(job)?;
            drop(store);
            if result.state == ade_ops::JobState::Succeeded {
                ade_ops::runner::verify_output(&cli.state, job)?;
                let mut value = summary(&result);
                value["output_verification"] = serde_json::json!("VERIFIED");
                return Ok(value);
            }
            Ok(summary(&result))
        }
        Command::List { after, limit } => Ok(serde_json::Value::Array(
            store.list(after, limit)?.iter().map(list_summary).collect(),
        )),
        Command::History { job } => Ok(serde_json::to_value(store.status(job)?.events)?),
        Command::Cancel { .. }
        | Command::Recover
        | Command::VerifyOutput { .. }
        | Command::Gc { .. } => unreachable!(),
        Command::VerifySources { job } => {
            store.verify_sources(job)?;
            Ok(serde_json::json!({"job":job,"verified":true}))
        }
        Command::Run { job } => {
            store.run(job)?;
            unreachable!("partial operations never runs native execution")
        }
        Command::Cleanup => Ok(serde_json::json!({"transaction_temp_removed":store.cleanup()?})),
        Command::Capabilities => unreachable!(),
        #[cfg(feature = "fault-injection")]
        Command::ExerciseClaim { job } => {
            store.exercise_claim(job)?;
            Ok(serde_json::json!({"metadata_claim":job}))
        }
        #[cfg(feature = "fault-injection")]
        Command::ExercisePublication => unreachable!(),
        #[cfg(feature = "fault-injection")]
        Command::ExerciseOwner => {
            store.exercise_owner()?;
            Ok(serde_json::json!({"generation":store.generation()}))
        }
    }
}
#[cfg(feature = "fault-injection")]
fn exercise_publication(root: &std::path::Path) -> ade_ops::Result<serde_json::Value> {
    use ade_ops::publication::{Completion, Identity, Limits, Publisher, Terminal};
    use ade_physical::{Field, Type};
    let schema = vec![Field::new("value", Type::I64, false)];
    let identity = Identity {
        store: Uuid::from_u128(1),
        job: Uuid::from_u128(2),
        instance: Uuid::from_u128(3),
        owner: 1,
        engine_job: 1,
        plan_hash: "a".repeat(64),
        source_hash: "b".repeat(64),
        config_hash: "c".repeat(64),
        workers_hash: "d".repeat(64),
    };
    let completion = Completion {
        workers: 2,
        terminals: (0..2)
            .map(|partition| Terminal {
                partition,
                attempt: 0,
                rows: 0,
                fingerprint: "e".repeat(64),
            })
            .collect(),
    };
    let output = Publisher::stage(
        root,
        identity,
        schema,
        completion,
        Limits {
            rows: 0,
            bytes: 4096,
        },
        ade_exec::CancellationToken::default(),
        std::time::Instant::now() + std::time::Duration::from_secs(30),
    )?
    .publish()?;
    Ok(serde_json::to_value(output)?)
}
fn main() {
    match run(Cli::parse()) {
        Ok(value) => println!("{}", serde_json::json!({"schema_version":1,"result":value})),
        Err(error) => {
            let mut message = error.to_string();
            if message.len() > 1024 {
                let mut end = 1024;
                while !message.is_char_boundary(end) {
                    end -= 1;
                }
                message.truncate(end);
            }
            eprintln!(
                "{}",
                serde_json::json!({"schema_version":1,"code":error.code(),"error":message,"phase12":"COMPLETE_BOUNDED_V1"})
            );
            std::process::exit(1);
        }
    }
}
