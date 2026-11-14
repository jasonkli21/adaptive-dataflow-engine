//! Process driver for Phase 24's detached acceptance harness.

use ade_ext_graph as graph;
use ade_ext_sdk as sdk;
use ade_ext_vector::{Operation, VectorV1};
use arrow::array::{Float64Array, UInt64Array};
use sdk::Extension;
use serde::Deserialize;
use serde_json::json;
use std::{
    fs::File,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

type Error = Box<dyn std::error::Error>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VectorRequest {
    source: sdk::SourcePin,
    path: PathBuf,
    executable: sdk::Digest,
    model_path: PathBuf,
    model_pin: sdk::Digest,
    operation: String,
    dimensions: u8,
    batch_rows: usize,
    #[serde(default)]
    cancel: bool,
    #[serde(default)]
    result_bytes: Option<u64>,
    #[serde(default)]
    batch_bytes: Option<usize>,
    #[serde(default)]
    output_timeout_ms: Option<u64>,
    #[serde(default)]
    panic: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphRequest {
    native: graph::Request,
    stage_parent: PathBuf,
    stage_bytes: usize,
}

struct Identity(VectorV1);

impl sdk::Extension for Identity {
    type Kernel = IdentityKernel;

    fn descriptor(&self) -> sdk::ExtensionDescriptorV1 {
        let mut descriptor = self.0.descriptor();
        descriptor.id = "phase24.identity-control".into();
        descriptor.mutable.clear();
        descriptor
    }

    fn prepare(&self, _: &[u8]) -> sdk::Result<IdentityKernel> {
        Ok(IdentityKernel)
    }
}

struct IdentityKernel;
impl sdk::RowKernel for IdentityKernel {
    fn transform(&self, _: &mut sdk::Row) -> sdk::Result<()> {
        Ok(())
    }
}

fn read_bounded(path: &Path, cap: usize) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(cap as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > cap {
        return Err("input byte cap".into());
    }
    Ok(bytes)
}

fn write_json(value: &impl serde::Serialize, timeout: Duration) -> Result<(), Error> {
    use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
    let mut bytes = serde_json::to_vec(value)?;
    if bytes.len() >= 4 * 1024 * 1024 {
        return Err("fixture stdout cap".into());
    }
    bytes.push(b'\n');
    let mut stdout = io::stdout().lock();
    fcntl_setfl(&stdout, fcntl_getfl(&stdout)? | OFlags::NONBLOCK)?;
    let deadline = Instant::now() + timeout;
    let mut remaining = bytes.as_slice();
    while !remaining.is_empty() {
        if Instant::now() >= deadline {
            return Err("fixture output stalled".into());
        }
        match stdout.write(remaining) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero).into()),
            Ok(n) => remaining = &remaining[n..],
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    stdout.flush()?;
    Ok(())
}

#[cfg(feature = "fault-injection")]
struct Panicking(VectorV1);

#[cfg(feature = "fault-injection")]
impl sdk::Extension for Panicking {
    type Kernel = Self;

    fn descriptor(&self) -> sdk::ExtensionDescriptorV1 {
        self.0.descriptor()
    }

    fn prepare(&self, _: &[u8]) -> sdk::Result<Self> {
        Ok(Self(self.0))
    }
}

#[cfg(feature = "fault-injection")]
impl sdk::RowKernel for Panicking {
    fn transform(&self, _: &mut sdk::Row) -> sdk::Result<()> {
        panic!("phase24 armed kernel panic");
    }
}

fn vector(bytes: &[u8]) -> Result<(), Error> {
    let config: VectorRequest = serde_json::from_slice(bytes)?;
    let operation = match config.operation.as_str() {
        "combined" | "identity" => Operation::NormalizeThenScore,
        "prep" => Operation::Normalize,
        "score" => Operation::Score,
        _ => return Err("unsupported fixture operation".into()),
    };
    let installed = VectorV1 {
        operation,
        dimensions: config.dimensions,
    };
    let identity = Identity(installed);
    let descriptor = if config.operation == "identity" {
        identity.descriptor()
    } else {
        installed.descriptor()
    };
    let start = Instant::now();
    let model = read_bounded(&config.model_path, sdk::MAX_MODEL_BYTES)?;
    let model_read_ns = start.elapsed().as_nanos();
    let projection: &[usize] = if config.operation == "prep" {
        &[0, 1, 2, 3, 4, 5]
    } else {
        &[0, 5]
    };
    let request = sdk::Request {
        descriptor: &descriptor,
        executable: config.executable,
        model_bytes: &model,
        model_pin: config.model_pin,
        source: config.source,
        path: &config.path,
        projection,
        limits: sdk::Limits {
            batch_rows: config.batch_rows,
            batch_bytes: config.batch_bytes.unwrap_or(sdk::MAX_BATCH_BYTES),
            result_bytes: config.result_bytes.unwrap_or(sdk::MAX_RESULT_BYTES),
        },
        deadline: start + sdk::MAX_DEADLINE,
    };
    let cancel = sdk::CancellationToken::default();
    if config.cancel {
        cancel.cancel();
    }
    if config.panic {
        #[cfg(feature = "fault-injection")]
        sdk::execute(&request, &Panicking(installed), &cancel)?;
        return Err("fault injection requires feature and a populated batch".into());
    }
    let output = if config.operation == "identity" {
        sdk::execute(&request, &identity, &cancel)?
    } else {
        sdk::execute(&request, &installed, &cancel)?
    };
    let execute_ns = start.elapsed().as_nanos();
    let record = vector_output(&output, execute_ns, model_read_ns)?;
    write_json(
        &record,
        Duration::from_millis(config.output_timeout_ms.unwrap_or(10_000)),
    )
}

fn vector_output(
    output: &sdk::Output,
    execute_ns: u128,
    model_read_ns: u128,
) -> Result<serde_json::Value, Error> {
    let mut ids = Vec::new();
    let mut numbers = Vec::new();
    for batch in &output.native.batches {
        let input = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or("row_id")?;
        ids.extend_from_slice(input.values());
        let columns = batch.columns()[1..]
            .iter()
            .map(|array| {
                array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or("Float64")
            })
            .collect::<Result<Vec<_>, _>>()?;
        for i in 0..batch.num_rows() {
            numbers.extend(columns.iter().map(|column| column.value(i)));
        }
    }
    let metrics = output
        .native
        .metrics
        .operators
        .values()
        .map(|metric| {
            (
                metric.name,
                metric.input_rows,
                metric.output_rows,
                metric.work_time.as_nanos(),
            )
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "receipt": output.receipt, "ids": ids, "values": numbers, "metrics": metrics,
        "managed_peak": output.native.metrics.managed.peak, "execute_ns": execute_ns, "model_read_ns": model_read_ns
    }))
}

fn graph_run(operation: &str, bytes: &[u8]) -> Result<(), Error> {
    let command = match operation {
        "graph-run" => graph::Command::Run,
        "graph-read" => graph::Command::Read,
        _ => return Err("unknown graph command".into()),
    };
    let request = graph::Request::decode(bytes)?;
    let summary = graph::operate(
        command,
        &request,
        graph::CancellationToken::default(),
        &mut io::stdout().lock(),
        |event, guard| {
            guard.check()?;
            writeln!(io::stderr().lock(), "{}", serde_json::to_string(&event)?)?;
            Ok(())
        },
    )?;
    writeln!(io::stderr().lock(), "{}", serde_json::to_string(&summary)?)?;
    Ok(())
}

fn graph_reduce(bytes: &[u8]) -> Result<(), Error> {
    let request: GraphRequest = serde_json::from_slice(bytes)?;
    let output = graph::component_sizes_v1(
        &request.native,
        &request.stage_parent,
        graph::Limits {
            stage_bytes: request.stage_bytes,
            deadline: Instant::now() + Duration::from_secs(120),
        },
        &graph::CancellationToken::default(),
    )?;
    write_json(&output, Duration::from_secs(10))
}

fn run() -> Result<(), Error> {
    let mut args = std::env::args_os().skip(1);
    let command = args
        .next()
        .and_then(|s| s.into_string().ok())
        .ok_or("missing command")?;
    let file = args.next().map(PathBuf::from).ok_or("missing request")?;
    if args.next().is_some() {
        return Err("unexpected argument".into());
    }
    let request = read_bounded(&file, 64 * 1024)?;
    match command.as_str() {
        "vector" => vector(&request),
        "graph-run" | "graph-read" => graph_run(&command, &request),
        "graph-reduce" => graph_reduce(&request),
        _ => Err("unknown fixture command".into()),
    }
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            let _ = writeln!(io::stderr().lock(), "{error}");
            std::process::ExitCode::FAILURE
        }
    }
}
