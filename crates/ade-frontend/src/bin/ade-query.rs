use ade_frontend::{Catalog, Target, execution::Options};
use clap::{Parser, ValueEnum};
use std::{io::Read, path::PathBuf};
#[derive(Clone, Copy, ValueEnum)]
enum Action {
    PlanSql,
    Explain,
    Execute,
}
#[derive(Clone, Copy, ValueEnum)]
enum NativeTarget {
    Local,
    DistributedAggregate,
    DistributedJoin,
}
#[derive(Parser)]
#[command(about = "Bounded SQL compiled to ADE physical v1 and native operators")]
struct Args {
    #[arg(value_enum)]
    action: Action,
    #[arg(long)]
    dataset: PathBuf,
    #[arg(long)]
    manifest: Option<PathBuf>,
    #[arg(
        long,
        conflicts_with = "sql_file",
        required_unless_present = "sql_file"
    )]
    sql: Option<String>,
    #[arg(long)]
    sql_file: Option<PathBuf>,
    #[arg(long, value_enum, default_value = "local")]
    target: NativeTarget,
    #[arg(long, default_value_t = 1000)]
    max_output_rows: u64,
    #[arg(long, default_value_t = 16777216)]
    max_output_bytes: u64,
    #[arg(long, default_value_t = 33554432)]
    memory_limit_bytes: usize,
    #[arg(long, default_value_t = 2)]
    workers: usize,
    #[arg(long)]
    worker_bin: Option<PathBuf>,
    #[arg(
        long,
        default_value_t = 120000,
        help = "End-to-end budget for registration, planning, and execution (1..=120000 ms)"
    )]
    timeout_ms: u64,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let timeout = std::time::Duration::from_millis(args.timeout_ms);
    if timeout.is_zero() || timeout > std::time::Duration::from_secs(120) {
        return Err("timeout-ms must be between 1 and 120000".into());
    }
    let deadline = std::time::Instant::now() + timeout;
    let target = match args.target {
        NativeTarget::Local => Target::Local,
        NativeTarget::DistributedAggregate => Target::DistributedAggregate,
        NativeTarget::DistributedJoin => Target::DistributedJoin,
    };
    let sql = if let Some(file) = args.sql_file {
        let file = std::fs::File::open(file)?;
        if !file.metadata()?.is_file() {
            return Err("SQL input must be a regular file".into());
        }
        let mut sql = String::new();
        file.take(16 * 1024 + 1).read_to_string(&mut sql)?;
        if sql.len() > 16 * 1024 {
            return Err("SQL file cap 16KiB".into());
        }
        sql
    } else {
        args.sql.ok_or("missing SQL")?
    };
    check_deadline(deadline)?;
    let mut catalog = Catalog::default();
    let manifest = args
        .manifest
        .unwrap_or_else(|| args.dataset.join("manifest.json"));
    let tables = if target == Target::DistributedJoin {
        vec!["ade_left", "ade_right"]
    } else {
        vec!["input"]
    };
    for table in tables {
        catalog.register_manifest_until(table, &args.dataset, &manifest, deadline)?;
    }
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    let mut plan = tokio::time::timeout(remaining, catalog.plan_sql(&sql, target))
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "query planning deadline expired",
            )
        })??;
    check_deadline(deadline)?;
    plan.max_output_rows = args.max_output_rows;
    plan.max_output_bytes = args.max_output_bytes;
    plan.validate()?;
    match args.action {
        Action::PlanSql | Action::Explain => println!(
            "{}\nplan_hash={}\nexecution_policy=native explicit configuration; no storage tier in IR",
            plan.explain()?,
            plan.digest()?
        ),
        Action::Execute => {
            let mut options = Options {
                workers: args.workers,
                worker_binary: args.worker_bin,
                timeout: deadline.saturating_duration_since(std::time::Instant::now()),
                ..Options::default()
            };
            options.local.memory_limit_bytes = args.memory_limit_bytes;
            options.aggregate.memory_limit_bytes = args.memory_limit_bytes;
            options.join.local.memory_limit_bytes = args.memory_limit_bytes;
            let output = catalog.execute_until(&plan, options, deadline)?;
            // No output is printed until execution and postflight validation succeed.
            println!("{}", serde_json::to_string_pretty(&output.diagnostics)?);
            println!(
                "{}",
                arrow::util::pretty::pretty_format_batches(output.batches())?
            );
        }
    }
    Ok(())
}

fn check_deadline(deadline: std::time::Instant) -> Result<(), std::io::Error> {
    if std::time::Instant::now() < deadline {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "query deadline expired",
        ))
    }
}
