//! Query API that invokes native graph entrypoints, never `DataFusion` execution.
use crate::{Catalog, Result, require};
use ade_physical::{PlanV1, Target};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
#[derive(Debug, Clone)]
pub struct Options {
    pub local: ade_exec::ExecutionConfig,
    pub batch_size: usize,
    pub timeout: Duration,
    pub workers: usize,
    pub worker_binary: Option<PathBuf>,
    pub aggregate: ade_distributed::TaskConfig,
    pub join: ade_distributed::join::Config,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            local: ade_exec::ExecutionConfig::default(),
            batch_size: 1024,
            timeout: Duration::from_secs(120),
            workers: 2,
            worker_binary: None,
            aggregate: ade_distributed::TaskConfig::default(),
            join: ade_distributed::join::Config::default(),
        }
    }
}
pub enum NativeOutput {
    Local(Box<ade_exec::ExecutionOutput>),
    Distributed(Box<ade_distributed::physical::Output>),
}
pub struct Output {
    pub native: NativeOutput,
    pub diagnostics: serde_json::Value,
}
impl Output {
    #[must_use]
    pub fn batches(&self) -> &[arrow::record_batch::RecordBatch] {
        match &self.native {
            NativeOutput::Local(o) => &o.batches,
            NativeOutput::Distributed(o) => &o.batches,
        }
    }
}
impl Catalog {
    /// Execute a previously compiled graph against exactly its registered sources.
    /// # Errors
    /// Unsupported targets/configurations/source drift or native failure return no result.
    pub fn execute(&self, plan: &PlanV1, options: Options) -> Result<Output> {
        require(
            !options.timeout.is_zero() && options.timeout <= Duration::from_secs(120),
            "timeout must be <=120s and positive",
        )?;
        let deadline = Instant::now() + options.timeout;
        self.execute_until(plan, options, deadline)
    }

    /// Execute a graph using a caller-owned absolute deadline shared with setup.
    ///
    /// # Errors
    /// Refuses expired or overlong deadlines, unsupported targets/configurations,
    /// source drift, or native failures without returning partial output.
    #[allow(
        clippy::too_many_lines,
        reason = "One absolute deadline is threaded through validation and native execution."
    )]
    pub fn execute_until(
        &self,
        plan: &PlanV1,
        mut options: Options,
        deadline: Instant,
    ) -> Result<Output> {
        options.timeout = deadline.saturating_duration_since(Instant::now());
        require(
            !options.timeout.is_zero() && options.timeout <= Duration::from_secs(120),
            "timeout must be <=120s and positive",
        )?;
        plan.validate()?;
        let catalog = self.bind(plan)?;
        require(Instant::now() < deadline, "query execution deadline")?;
        options.join.local.max_output_rows = plan.max_output_rows;
        let mut effective = match plan.target {
            Target::Local => {
                serde_json::json!({"memory_limit_bytes":options.local.memory_limit_bytes,"spill_enabled":options.local.spill_enabled,"prefetch_batches":options.local.prefetch_batches,"output_batch_rows":options.local.output_batch_rows,"batch_size":options.batch_size,"timeout_ms":options.timeout.as_millis()})
            }
            Target::DistributedAggregate => {
                serde_json::json!({"task":options.aggregate,"workers":options.workers,"timeout_ms":options.timeout.as_millis()})
            }
            Target::DistributedJoin => {
                serde_json::json!({"task":options.join,"workers":options.workers,"timeout_ms":options.timeout.as_millis(),"recovery_enabled":false,"exchange_mode":"spill"})
            }
        };
        let native = if plan.target == Target::Local {
            let remaining = deadline.saturating_duration_since(Instant::now());
            require(!remaining.is_zero(), "query execution deadline")?;
            NativeOutput::Local(Box::new(ade_exec::physical::execute(
                plan,
                &catalog,
                options.local,
                options.batch_size,
                ade_exec::CancellationToken::default(),
                remaining,
            )?))
        } else {
            require((2..=4).contains(&options.workers), "workers must be 2..4")?;
            if plan.target == Target::DistributedAggregate {
                options.aggregate.validate()?;
                require(
                    options.aggregate.batch_size <= 8192,
                    "physical aggregate batch cap 8192",
                )?;
                require(
                    !options.aggregate.scheduler.leased()
                        && options.aggregate.partial.mode == ade_exec::partial::Mode::FixedHash
                        && options.aggregate.topology.policy
                            == ade_distributed::topology::PartitionPolicy::Static,
                    "only static fixed-hash distributed aggregate is admitted",
                )?;
            } else {
                options.join.validate()?;
            }
            catalog.verify(
                plan,
                &ade_exec::CancellationToken::default(),
                deadline,
                true,
            )?;
            let worker = options.worker_binary.ok_or_else(|| {
                crate::Error::Unsupported(
                    "distributed target requires explicit ade-worker binary".into(),
                )
            })?;
            let cluster = if options.aggregate.exchange.cost.is_some()
                && plan.target == Target::DistributedAggregate
            {
                ade_distributed::coordinator::LocalCluster::spawn_with_materialization(
                    &worker,
                    options.workers,
                )?
            } else {
                ade_distributed::coordinator::LocalCluster::spawn(&worker, options.workers)?
            };
            options.timeout = deadline.saturating_duration_since(Instant::now());
            effective["native_remaining_timeout_ms"] =
                serde_json::json!(options.timeout.as_millis());
            NativeOutput::Distributed(Box::new(ade_distributed::physical::execute(
                ade_distributed::physical::Request {
                    plan,
                    catalog: &catalog,
                    workers: cluster.workers.clone(),
                    job: ade_distributed::ids::JobId(0),
                    options: ade_distributed::physical::Options {
                        aggregate: options.aggregate,
                        join: options.join,
                        timeout: options.timeout,
                    },
                },
            )?))
        };
        let metrics = match &native {
            NativeOutput::Local(o) => local_diagnostics(&o.metrics),
            NativeOutput::Distributed(o) => match &o.metrics {
                ade_distributed::physical::Metrics::Aggregate(m) => {
                    serde_json::json!({"native_entrypoint":"ade_distributed::coordinator::execute","metrics":m})
                }
                ade_distributed::physical::Metrics::Join(m) => {
                    serde_json::json!({"native_entrypoint":"ade_distributed::join::execute","metrics":m})
                }
            },
        };
        Ok(Output {
            native,
            diagnostics: serde_json::json!({"ir_version":1,"plan_hash":plan.digest()?,"target":plan.target,"sources":plan.sources,"output_schema":plan.nodes[plan.output as usize].schema,"effective_options":effective,"runtime":metrics,"policy":"native configuration; no SQL storage-tier selection","control_protocol":"ADETCP12","exchange_wire_version":2,"hash_version":1}),
        })
    }
}

fn local_diagnostics(metrics: &ade_exec::ExecutionMetrics) -> serde_json::Value {
    let operators=metrics.operators.iter().map(|(id,m)|serde_json::json!({"id":id.0,"name":m.name,"input_rows":m.input_rows,"input_batches":m.input_batches,"output_rows":m.output_rows,"output_batches":m.output_batches,"work_time_ns":m.work_time.as_nanos(),"observed_current_bytes":m.memory.current,"observed_peak_bytes":m.memory.peak})).collect::<Vec<_>>();
    let managed = &metrics.managed;
    let spill = &metrics.spill;
    let prefetch = &metrics.prefetch;
    serde_json::json!({"native_entrypoint":"ade_exec::physical::execute","metrics":{"operators":operators,"output_rows":metrics.output_rows,"output_batches":metrics.output_batches,"managed":{"limit":managed.limit,"current":managed.current,"peak":managed.peak,"attempts":managed.attempts,"denials":managed.denials,"pressure_events":managed.pressure_events,"owner_peak":managed.owner_peak.iter().map(|(id,bytes)|(id.0.to_string(),bytes)).collect::<std::collections::BTreeMap<_,_>>()},"spill":{"files":spill.files,"events":spill.events,"sort_runs":spill.sort_runs,"sort_merges":spill.sort_merges,"write_bytes":spill.write_bytes,"read_bytes":spill.read_bytes},"prefetch":{"produced":prefetch.produced,"consumed":prefetch.consumed,"queue_peak":prefetch.queue_peak,"observed_batch_peak":prefetch.observed_batch_peak}}})
}
