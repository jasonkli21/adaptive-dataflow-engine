//! Narrow IR-to-native distributed adapters. No frontend dependency.
use crate::{
    Error, Result, TaskConfig, WorkerEndpoint,
    coordinator::{self, StaticAggregateJob},
    ids::{JobId, MorselId},
    join,
    lineage::{SourceMorsel, SourceSnapshot},
    require,
};
use ade_exec::{CancellationToken, physical::Catalog};
use ade_physical::{Operator, PlanV1, Target};
use arrow::record_batch::RecordBatch;
use std::time::{Duration, Instant};

pub struct Options {
    pub aggregate: TaskConfig,
    pub join: join::Config,
    pub timeout: Duration,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            aggregate: TaskConfig::default(),
            join: join::Config::default(),
            timeout: crate::TASK_TIMEOUT,
        }
    }
}
pub struct Request<'a> {
    pub plan: &'a PlanV1,
    pub catalog: &'a Catalog,
    pub workers: Vec<WorkerEndpoint>,
    pub job: JobId,
    pub options: Options,
}
pub enum Metrics {
    Aggregate(Box<crate::metrics::DistributedMetrics>),
    Join(Box<join::Metrics>),
}
pub struct Output {
    pub batches: Vec<RecordBatch>,
    pub metrics: Metrics,
}
/// Execute exactly the admitted graph, binding source fingerprints into native jobs.
/// # Errors
/// Refuses unsupported DAGs/configs before preparation; failures return no output.
#[allow(
    clippy::too_many_lines,
    reason = "The two admitted native job lifecycles share one preflight/postflight boundary."
)]
pub fn execute(request: Request<'_>) -> Result<Output> {
    let Request {
        plan,
        catalog,
        workers,
        job,
        options,
    } = request;
    plan.validate().map_err(|e| Error::Invalid(e.to_string()))?;
    coordinator::validate_topology(&workers)?;
    require(
        !options.timeout.is_zero() && options.timeout <= crate::TASK_TIMEOUT,
        "physical timeout bound",
    )?;
    require(
        plan.target != Target::Local,
        "use ade-exec for local target",
    )?;
    if plan.target == Target::DistributedAggregate {
        options.aggregate.validate()?;
        require(
            options.aggregate.batch_size <= 8192,
            "physical aggregate batch cap 8192",
        )?;
        require(
            !options.aggregate.scheduler.leased()
                && options.aggregate.partial.mode == ade_exec::partial::Mode::FixedHash
                && options.aggregate.topology.policy == crate::topology::PartitionPolicy::Static,
            "frontend aggregate requires static fixed_hash topology",
        )?;
    } else {
        options.join.validate()?;
    }
    let deadline = Instant::now() + options.timeout;
    let token = CancellationToken::default();
    catalog.verify(plan, &token, deadline, true)?;
    let mut base = plan.output;
    while let Operator::Project { input, .. } = plan.nodes[base as usize].operator {
        base = input;
    }
    let (batches, metrics) = match plan.target {
        Target::DistributedAggregate => {
            let b = &catalog.bindings[0];
            let source = &b.source;
            let snapshot = SourceSnapshot {
                version: 1,
                manifest_fingerprint: source.manifest.clone(),
                schema_fingerprint: source.schema_identity.clone(),
                logical_fingerprint: source.logical.clone(),
                replayable: source.replayable,
                morsels: b
                    .files
                    .iter()
                    .zip(&source.files)
                    .enumerate()
                    .map(|(i, (path, f))| SourceMorsel {
                        id: MorselId(i as u64),
                        path: path.clone(),
                        fingerprint: f.fingerprint.clone(),
                        bytes: f.bytes,
                    })
                    .collect(),
            };
            let output = coordinator::execute(&StaticAggregateJob {
                id: job,
                files: b.files.clone(),
                workers,
                config: options.aggregate,
                task_timeout: deadline.saturating_duration_since(Instant::now()),
                snapshot: Some(snapshot),
            })?;
            (output.batches, Metrics::Aggregate(Box::new(output.metrics)))
        }
        Target::DistributedJoin => {
            let mut sources = vec![];
            for (b, side) in catalog
                .bindings
                .iter()
                .zip([ade_exec::join::Side::Left, ade_exec::join::Side::Right])
            {
                let source = join::Source::capture(
                    side,
                    b.files
                        .iter()
                        .zip(&b.source.files)
                        .map(|(path, f)| (path.clone(), f.rows))
                        .collect(),
                )?;
                require(
                    source.files.iter().zip(&b.source.files).all(|(a, b)| {
                        a.blake3 == b.fingerprint && a.bytes == b.bytes && a.rows == b.rows
                    }),
                    "join source drift during capture",
                )?;
                sources.push(source);
            }
            let mut config = options.join;
            config.local.max_output_rows = plan.max_output_rows;
            let output = join::execute(
                &join::Spec {
                    job,
                    execution: uuid::Uuid::new_v4().to_string(),
                    workers,
                    sources: sources
                        .try_into()
                        .map_err(|_| Error::Invalid("join sources".into()))?,
                    config,
                },
                deadline.saturating_duration_since(Instant::now()),
            )?;
            (output.batches, Metrics::Join(Box::new(output.metrics)))
        }
        Target::Local => return Err(Error::Invalid("local target".into())),
    };
    catalog.verify(plan, &token, deadline, false)?;
    let mut result = vec![];
    let (mut rows, mut bytes) = (0u64, 0u64);
    for batch in batches {
        let batch = adapt(plan, base, &batch)?;
        rows = rows
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| Error::Invalid("output row overflow".into()))?;
        bytes = bytes
            .checked_add(batch.get_array_memory_size() as u64 + 128)
            .ok_or_else(|| Error::Invalid("output byte overflow".into()))?;
        require(
            rows <= plan.max_output_rows && bytes <= plan.max_output_bytes,
            "physical output cap; no committed output",
        )?;
        result.push(batch);
    }
    Ok(Output {
        batches: result,
        metrics,
    })
}
fn adapt(plan: &PlanV1, base: u32, batch: &RecordBatch) -> Result<RecordBatch> {
    let schema = ade_exec::physical::arrow_schema(&plan.nodes[base as usize].schema);
    require(
        batch.num_columns() == schema.fields().len(),
        "native base schema width",
    )?;
    let arrays = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(a, f)| {
            if a.data_type() == f.data_type() {
                Ok(a.clone())
            } else {
                arrow::compute::cast_with_options(
                    a,
                    f.data_type(),
                    &arrow::compute::CastOptions {
                        safe: false,
                        ..Default::default()
                    },
                )
            }
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut batch = RecordBatch::try_new(schema, arrays)?;
    for n in &plan.nodes[base as usize + 1..] {
        let Operator::Project { columns, .. } = &n.operator else {
            return Err(Error::Invalid("nonprojection after native base".into()));
        };
        batch = RecordBatch::try_new(
            ade_exec::physical::arrow_schema(&n.schema),
            columns
                .iter()
                .map(|c| batch.column(c.column).clone())
                .collect(),
        )?;
    }
    Ok(batch)
}
