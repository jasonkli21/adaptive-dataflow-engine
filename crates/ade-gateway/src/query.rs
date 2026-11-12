use crate::worker::{ABI, Registry, Worker};
use ade_exec::{CancellationToken, ExecutionConfig, ExecutionOutput};
use ade_frontend::Catalog;
use ade_physical::{Operator, PlanV1, Selection, Target};
use arrow::datatypes::SchemaRef;
use serde::Deserialize;
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tonic::Status;

pub const REQUEST_BYTES: usize = 16 * 1024;
pub const BATCH_ROWS: usize = 256;
pub const INPUT_ROWS: u64 = 32_768;
pub const RESULT_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_TIME: Duration = Duration::from_secs(10);
pub const QUERY_TYPE: &str = "ade.gateway/QueryRequestV1";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    version: u32,
    target: String,
    table: String,
    timeout_ms: u64,
    operations: Vec<Operation>,
    function: Option<Function>,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Operation {
    Filter { column: String, less_than: String },
    Project { columns: Vec<String> },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Function {
    id: String,
    abi: u32,
    digest: String,
}

pub struct Bound {
    pub plan: PlanV1,
    pub deadline: Instant,
    function: bool,
}

fn invalid() -> Status {
    Status::invalid_argument("invalid query request")
}

pub fn expiry() -> Status {
    Status::deadline_exceeded("query deadline")
}

impl Query {
    pub fn parse(bytes: &[u8]) -> Result<Self, Status> {
        if bytes.len() > REQUEST_BYTES {
            return Err(Status::resource_exhausted("query request cap"));
        }
        let query: Self = serde_json::from_slice(bytes).map_err(|_| invalid())?;
        if query.version != 1 || query.target != "local" {
            return Err(Status::unimplemented("query version or target"));
        }
        if query.timeout_ms == 0 || query.timeout_ms > 10_000 || query.operations.len() > 8 {
            return Err(invalid());
        }
        Ok(query)
    }

    pub fn bind(
        self,
        catalog: &Catalog,
        registry: Option<&Registry>,
        start: Instant,
    ) -> Result<Bound, Status> {
        if self.table != "input" {
            return Err(Status::not_found("unknown table"));
        }
        let function = self.function.is_some();
        if let Some(request) = self.function {
            let registry = registry
                .filter(|_| request.id == "map_secondary")
                .ok_or_else(|| Status::not_found("unknown function"))?;
            if request.abi != ABI {
                return Err(Status::unimplemented("function ABI"));
            }
            if request.digest != registry.digest {
                return Err(Status::failed_precondition("function identity mismatch"));
            }
            if self.operations.is_empty() {
                return Err(invalid());
            }
        }
        let (mut plan, _) = catalog
            .plan(Target::Local, &["input"])
            .map_err(|_| invalid())?;
        plan.push(Operator::Scan { source: 0 })
            .map_err(|_| invalid())?;
        for operation in self.operations {
            let input = plan.output;
            let schema = &plan.nodes[input as usize].schema;
            let column_index = |name: &str| {
                schema
                    .iter()
                    .position(|f| f.name == name)
                    .ok_or_else(invalid)
            };
            let operator = match operation {
                Operation::Filter { column, less_than } => {
                    let threshold = less_than.parse::<u64>().map_err(|_| invalid())?;
                    if threshold.to_string() != less_than {
                        return Err(invalid());
                    }
                    Operator::Filter {
                        input,
                        column: column_index(&column)?,
                        less_than: threshold,
                    }
                }
                Operation::Project { columns } => Operator::Project {
                    input,
                    columns: columns
                        .into_iter()
                        .map(|alias| {
                            Ok(Selection {
                                column: column_index(&alias)?,
                                alias,
                            })
                        })
                        .collect::<Result<_, Status>>()?,
                },
            };
            plan.push(operator).map_err(|_| invalid())?;
        }
        Bound::new(
            plan,
            start + Duration::from_millis(self.timeout_ms),
            function,
        )
    }
}

impl Bound {
    pub fn new(mut plan: PlanV1, deadline: Instant, function: bool) -> Result<Self, Status> {
        plan.max_output_rows = INPUT_ROWS;
        plan.max_output_bytes = RESULT_BYTES as u64;
        plan.validate().map_err(|_| invalid())?;
        Ok(Self {
            plan,
            deadline,
            function,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        ade_exec::physical::arrow_schema(&self.plan.nodes[self.plan.output as usize].schema)
    }

    pub fn execute(
        self,
        catalog: &Catalog,
        root: &Path,
        registry: Option<&Registry>,
        token: CancellationToken,
    ) -> Result<(ExecutionOutput, serde_json::Value), Status> {
        let native = catalog.bind(&self.plan).map_err(|_| invalid())?;
        for path in &native.bindings[0].files {
            if !path.starts_with(root) || path.canonicalize().ok().as_ref() != Some(path) {
                return Err(Status::failed_precondition("source path drift"));
            }
        }
        let plan_hash = self.plan.digest().map_err(|_| invalid())?;
        let request_id = uuid::Uuid::new_v4().to_string();
        let attempt = uuid::Uuid::new_v4().to_string();
        let registry = registry.filter(|_| self.function);
        let function_hash = registry.map(|r| r.digest.as_str());
        let identity = serde_json::json!({
            "protocol":1, "plan":plan_hash,
            "function":function_hash.map(|digest| serde_json::json!({"id":"map_secondary","abi":ABI,"digest":digest}))
        });
        let query_hash = ade_physical::fingerprint(identity.to_string().as_bytes());
        let worker = registry
            .map(|r| {
                let frame_identity =
                    ade_physical::fingerprint(format!("{query_hash}/{attempt}").as_bytes());
                r.bind(frame_identity, self.deadline).map(Arc::new)
            })
            .transpose()
            .map_err(|_| Status::failed_precondition("function artifact drift"))?;
        let timeout = self.deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(expiry());
        }
        let config = ExecutionConfig {
            memory_limit_bytes: 16 * 1024 * 1024,
            spill_enabled: false,
            prefetch_batches: 0,
            output_batch_rows: BATCH_ROWS,
        };
        let map = worker
            .clone()
            .map(|worker| worker as Arc<dyn ade_exec::BatchMap>);
        let started = Instant::now();
        let output = ade_exec::physical::execute_with_map(
            &self.plan,
            &native,
            config,
            BATCH_ROWS,
            token,
            timeout,
            map.as_ref(),
        )
        .map_err(|e| {
            let status = native_error(&e, self.deadline);
            eprintln!(
                "{}",
                serde_json::json!({
                    "request_id": request_id, "attempt": attempt, "query_hash": query_hash,
                    "identity": identity, "source": self.plan.sources[0].manifest,
                    "status": status.code() as i32, "reason": status.message(),
                    "worker": worker.as_deref().map(Worker::metrics),
                })
            );
            status
        })?;
        let operators = output.metrics.operators.values().map(|m| {
            serde_json::json!({"name":m.name,"input_rows":m.input_rows,"output_rows":m.output_rows,"output_batches":m.output_batches})
        }).collect::<Vec<_>>();
        let diagnostic = serde_json::json!({
            "identity":identity, "query_hash":query_hash, "attempt":attempt, "request_id":request_id,
            "native_wall_ns":started.elapsed().as_nanos(),
            "source":self.plan.sources[0].manifest, "replayable":!self.function,
            "native_peak_bytes":output.metrics.managed.peak, "operators":operators,
            "spill_bytes":output.metrics.spill.write_bytes,
            "worker":worker.as_deref().map(Worker::metrics)
        });
        Ok((output, diagnostic))
    }
}

fn native_error(error: &ade_exec::Error, deadline: Instant) -> Status {
    use ade_exec::Error;
    if Instant::now() >= deadline || matches!(error, Error::Deadline(_)) {
        return expiry();
    }
    match error {
        Error::BatchMap(reason) => match *reason {
            "input cap" | "input IPC cap" | "encoded cap" => {
                Status::resource_exhausted(format!("UDF {reason}"))
            }
            _ => Status::failed_precondition(format!("UDF {reason}")),
        },
        Error::Cancelled => Status::cancelled("query cancelled"),
        Error::MemoryLimit { .. } | Error::SpillDisabled => {
            Status::resource_exhausted("native resource cap")
        }
        Error::Invalid(message) if message.starts_with("physical output cap") => {
            Status::resource_exhausted("native result cap")
        }
        Error::Invalid(message) if message.starts_with("source ") => {
            Status::failed_precondition("source verification failed")
        }
        _ => Status::failed_precondition("native execution failed"),
    }
}
