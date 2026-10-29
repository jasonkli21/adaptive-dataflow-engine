//! Explicit, default-off remote-object static aggregate with stage-boundary turnover.
mod coordinator;
mod http;
mod store;
mod wire;
mod worker;

pub use coordinator::{
    Cleanup, Event, Output, Receipt, Run, execute, execute_observed, verify_receipt,
};
pub use wire::Boot;
pub use worker::serve;

use crate::{Result, ids::JobId, require};
use ade_physical::{Field, Operator, PlanV1, Target, Type};
use serde::{Deserialize, Serialize};
use std::{net::IpAddr, path::PathBuf};

pub const VERSION: u32 = 1;
pub const MAX_JSON: usize = 1024 * 1024;
pub const TASK_MEMORY: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub dev_allow_http: bool,
}
impl S3Config {
    /// Validate explicit provider identity without touching the service.
    /// # Errors
    /// Refuses embedded secrets, unsafe namespaces and non-TLS endpoints except explicit loopback development.
    pub fn validate(&self) -> Result<()> {
        let u = url::Url::parse(&self.endpoint)
            .map_err(|_| crate::Error::Invalid("remote endpoint URL".into()))?;
        require(
            u.username().is_empty()
                && u.password().is_none()
                && u.query().is_none()
                && u.fragment().is_none()
                && u.path() == "/"
                && u.host_str().is_some()
                && self.endpoint.len() <= 256,
            "remote endpoint must contain only scheme/host/port",
        )?;
        require(
            u.scheme() == "https"
                || (u.scheme() == "http"
                    && self.dev_allow_http
                    && u.host_str().is_some_and(|h| {
                        h.trim_matches(['[', ']'])
                            .parse::<IpAddr>()
                            .is_ok_and(|a| a.is_loopback())
                    })),
            "remote requires TLS; dev HTTP requires literal loopback",
        )?;
        require(
            (3..=63).contains(&self.bucket.len())
                && component(&self.bucket)
                && !self.bucket.contains('_')
                && component(&self.prefix)
                && component(&self.region),
            "remote bucket/prefix/region must be bounded canonical components",
        )
    }
    /// # Errors
    /// Refuses invalid provider configuration.
    pub fn digest(&self) -> Result<String> {
        self.validate()?;
        digest(self)
    }
}
fn component(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
        && s.as_bytes()[0].is_ascii_alphanumeric()
        && s.as_bytes()[s.len() - 1].is_ascii_alphanumeric()
}
fn uuid(s: &str) -> bool {
    uuid::Uuid::parse_str(s).is_ok_and(|id| !id.is_nil() && id.to_string() == s)
}
fn hash(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn digest(value: &impl Serialize) -> Result<String> {
    Ok(blake3::hash(&json(value)?).to_hex().to_string())
}
fn json(value: &impl Serialize) -> Result<Vec<u8>> {
    let mut buf = crate::exchange::BoundedBuffer::new(MAX_JSON);
    serde_json::to_writer(&mut buf, value)?;
    Ok(buf.0)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    /// Per producer, across all destinations.
    pub segments: u64,
    /// Cumulative attempted object body bytes per actor, including manifests/probes.
    pub write_bytes: u64,
    pub objects: u64,
    /// All requests, including unsuccessful creates, retries and cleanup.
    pub requests: u64,
    pub timeout_ms: u64,
    pub io_timeout_ms: u64,
    pub read_retries: u32,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            segments: 256,
            write_bytes: 16 * 1024 * 1024,
            objects: 272,
            requests: 4096,
            timeout_ms: 120_000,
            io_timeout_ms: 5000,
            read_retries: 1,
        }
    }
}
impl Limits {
    /// # Errors
    /// Refuses zero, excessive or overflowing resource limits before I/O.
    pub fn validate(&self) -> Result<()> {
        require(
            (1..=256).contains(&self.segments)
                && (1..=16 * 1024 * 1024).contains(&self.write_bytes)
                && (1..=272).contains(&self.objects)
                && (1..=4096).contains(&self.requests)
                && (1..=120_000).contains(&self.timeout_ms)
                && (1..=5000).contains(&self.io_timeout_ms)
                && self.read_retries <= 2,
            "remote resource limit outside bounded v1",
        )
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Counters {
    pub put: u64,
    pub head: u64,
    pub get: u64,
    pub delete: u64,
    pub write_bytes: u64,
    pub read_bytes: u64,
    pub retries: u64,
    pub elapsed_ns: u64,
    pub http_requests: u64,
    pub http_request_body_bytes: u64,
    pub http_response_body_bytes: u64,
}
impl Counters {
    /// # Errors
    /// Refuses overflowing observations.
    pub fn add(&mut self, b: &Self) -> Result<()> {
        self.put = add(self.put, b.put)?;
        self.head = add(self.head, b.head)?;
        self.get = add(self.get, b.get)?;
        self.delete = add(self.delete, b.delete)?;
        self.write_bytes = add(self.write_bytes, b.write_bytes)?;
        self.read_bytes = add(self.read_bytes, b.read_bytes)?;
        self.retries = add(self.retries, b.retries)?;
        self.elapsed_ns = add(self.elapsed_ns, b.elapsed_ns)?;
        self.http_requests = add(self.http_requests, b.http_requests)?;
        self.http_request_body_bytes =
            add(self.http_request_body_bytes, b.http_request_body_bytes)?;
        self.http_response_body_bytes =
            add(self.http_response_body_bytes, b.http_response_body_bytes)?;
        Ok(())
    }
}
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b)
        .ok_or_else(|| crate::Error::Invalid("remote counter/cost overflow".into()))
}
fn mul(a: u64, b: u64) -> Result<u64> {
    a.checked_mul(b)
        .ok_or_else(|| crate::Error::Invalid("remote cost overflow".into()))
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rates {
    pub worker_placement: String,
    pub store_placement: String,
    pub put_nano_usd: u64,
    pub head_nano_usd: u64,
    pub get_nano_usd: u64,
    pub delete_nano_usd: u64,
    pub write_byte_nano_usd: u64,
    pub read_byte_nano_usd: u64,
    pub egress_byte_nano_usd: u64,
    pub storage_byte_ms_nano_usd: u64,
    pub retention_ms: u64,
}
impl Rates {
    /// # Errors
    /// Refuses unsafe labels or rates outside the configured estimate envelope.
    pub fn validate(&self) -> Result<()> {
        require(
            [&self.worker_placement, &self.store_placement]
                .iter()
                .all(|s| component(s))
                && self.retention_ms <= 86_400_000
                && [
                    self.put_nano_usd,
                    self.head_nano_usd,
                    self.get_nano_usd,
                    self.delete_nano_usd,
                    self.write_byte_nano_usd,
                    self.read_byte_nano_usd,
                    self.egress_byte_nano_usd,
                    self.storage_byte_ms_nano_usd,
                ]
                .iter()
                .all(|v| *v <= 1_000_000_000_000),
            "remote configured rates/placement limits",
        )
    }
    /// Configured estimate only; labels never establish an observed AZ or provider bill.
    /// # Errors
    /// Refuses invalid rates and arithmetic overflow.
    pub fn estimate_nano_usd(&self, c: &Counters) -> Result<u64> {
        self.validate()?;
        let terms = [
            mul(c.put, self.put_nano_usd)?,
            mul(c.head, self.head_nano_usd)?,
            mul(c.get, self.get_nano_usd)?,
            mul(c.delete, self.delete_nano_usd)?,
            mul(c.write_bytes, self.write_byte_nano_usd)?,
            mul(c.read_bytes, self.read_byte_nano_usd)?,
            mul(c.read_bytes, self.egress_byte_nano_usd)?,
            mul(
                mul(c.write_bytes, self.retention_ms)?,
                self.storage_byte_ms_nano_usd,
            )?,
        ];
        terms.into_iter().try_fold(0, add)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    pub version: u32,
    pub job: JobId,
    pub plan: PlanV1,
    pub files: Vec<PathBuf>,
    pub manifest: PathBuf,
    pub workers: usize,
    pub readers: usize,
    pub store: S3Config,
    pub limits: Limits,
    pub rates: Rates,
}
impl Job {
    /// # Errors
    /// Refuses every shape except the original static event GroupBySum/Count.
    pub fn validate(&self) -> Result<()> {
        require(
            self.version == VERSION && self.job.0 > 0,
            "remote job version/identity",
        )?;
        self.store.validate()?;
        self.limits.validate()?;
        self.rates.validate()?;
        self.plan
            .validate()
            .map_err(|e| crate::Error::Invalid(e.to_string()))?;
        let expected_schema = [
            ("row_id", Type::U64),
            ("key", Type::U64),
            ("secondary", Type::U64),
            ("value_i64", Type::I64),
            ("value_f64", Type::F64),
            ("category", Type::U32),
            ("payload", Type::Utf8),
            ("event_time", Type::TimestampMs),
        ]
        .into_iter()
        .map(|(name, ty)| Field::new(name, ty, false))
        .collect::<Vec<_>>();
        require(
            (2..=4).contains(&self.workers)
                && (1..=self.workers).contains(&self.readers)
                && self.plan.target == Target::DistributedAggregate
                && self.plan.sources.len() == 1
                && self.plan.nodes.len() == 2
                && self.plan.output == 1
                && matches!(self.plan.nodes[0].operator, Operator::Scan { source: 0 })
                && matches!(
                    self.plan.nodes[1].operator,
                    Operator::GroupSumCount {
                        input: 0,
                        key: 1,
                        value: 3
                    }
                )
                && self.plan.sources[0].table == "input"
                && self.plan.sources[0].schema == expected_schema
                && self.files.len() == self.plan.sources[0].files.len()
                && self
                    .files
                    .iter()
                    .all(|p| p.is_absolute() && p.to_string_lossy().len() <= 4096),
            "remote_object admits only fixed 2..4 static original GroupBySum, local immutable inputs",
        )?;
        require(json(self)?.len() <= MAX_JSON, "remote job frame cap")
    }
    fn verify(
        &self,
        token: &ade_exec::CancellationToken,
        until: std::time::Instant,
        preflight: bool,
    ) -> Result<()> {
        use std::io::Read;
        #[derive(Deserialize)]
        struct InputFile {
            relative_path: String,
            bytes: u64,
            rows: u64,
            fingerprint: String,
        }
        #[derive(Deserialize)]
        struct InputManifest {
            schema_version: u32,
            dataset_version: u32,
            required_schema: String,
            schema_fingerprint: String,
            logical_fingerprint: String,
            format: String,
            row_count: u64,
            files: Vec<InputFile>,
        }
        for path in &self.files {
            require(
                path.canonicalize()? == *path,
                "remote source canonical path",
            )?;
            for parent in path.ancestors() {
                require(
                    !std::fs::symlink_metadata(parent)?.file_type().is_symlink(),
                    "remote source symlink",
                )?;
            }
        }
        require(
            self.manifest.is_absolute() && self.manifest.canonicalize()? == self.manifest,
            "remote source manifest canonical path",
        )?;
        for p in self.manifest.ancestors() {
            require(
                !std::fs::symlink_metadata(p)?.file_type().is_symlink(),
                "remote manifest symlink",
            )?;
        }
        let mut file = std::fs::File::open(&self.manifest)?;
        require(
            file.metadata()?.is_file() && file.metadata()?.len() <= MAX_JSON as u64,
            "remote source manifest cap",
        )?;
        let mut data = Vec::new();
        loop {
            token.check()?;
            require(
                std::time::Instant::now() < until,
                "remote manifest deadline",
            )?;
            let mut buf = [0; 8192];
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            require(
                n <= MAX_JSON.saturating_sub(data.len()),
                "remote manifest grew past cap",
            )?;
            data.extend_from_slice(&buf[..n]);
        }
        let source = &self.plan.sources[0];
        require(
            blake3::hash(&data).to_hex().as_str() == source.manifest,
            "remote source manifest hash drift",
        )?;
        let manifest: InputManifest = serde_json::from_slice(&data)?;
        require(manifest.schema_version == 1 && manifest.dataset_version == 1 && manifest.format == "parquet"
            && manifest.required_schema == "synthetic-events-v1"
            && manifest.schema_fingerprint == source.schema_identity && manifest.logical_fingerprint == source.logical
            && manifest.schema_fingerprint == blake3::hash(b"synthetic-events-v1:row_id=u64,key=u64,secondary=u64,value_i64=i64,value_f64=f64,category=u32,payload=utf8,event_time=timestamp_ms;all_nonnull").to_hex().as_str()
            && manifest.files.len() == source.files.len()
            && manifest.row_count == source.files.iter().try_fold(0, |a, f| add(a, f.rows))?, "remote source manifest identity/coverage")?;
        let root = self
            .manifest
            .parent()
            .ok_or_else(|| crate::Error::Invalid("remote manifest root".into()))?;
        for ((f, expected), path) in manifest.files.iter().zip(&source.files).zip(&self.files) {
            require(
                f.relative_path == expected.name
                    && f.bytes == expected.bytes
                    && f.rows == expected.rows
                    && f.fingerprint == expected.fingerprint
                    && root.join(&expected.name).canonicalize()? == *path,
                "remote manifest file binding",
            )?;
        }
        self.catalog().verify(&self.plan, token, until, preflight)?;
        Ok(())
    }
    fn catalog(&self) -> ade_exec::physical::Catalog {
        ade_exec::physical::Catalog {
            bindings: vec![ade_exec::physical::Binding {
                source: self.plan.sources[0].clone(),
                files: self.files.clone(),
            }],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Context {
    pub version: u32,
    pub execution: String,
    pub attempt: u64,
    pub job: JobId,
    pub workers: usize,
    pub plan: String,
    pub source: String,
    pub config: String,
    pub store: String,
    pub topology: String,
}
impl Context {
    fn new(job: &Job) -> Result<Self> {
        let c = Self {
            version: VERSION,
            execution: uuid::Uuid::new_v4().to_string(),
            attempt: 0,
            job: job.job,
            workers: job.workers,
            plan: job
                .plan
                .digest()
                .map_err(|e| crate::Error::Invalid(e.to_string()))?,
            source: digest(&job.plan.sources)?,
            config: digest(job)?,
            store: job.store.digest()?,
            topology: digest(&("static-hash1", job.workers))?,
        };
        c.validate()?;
        Ok(c)
    }
    pub(crate) fn validate(&self) -> Result<()> {
        require(
            self.version == VERSION
                && uuid(&self.execution)
                && self.attempt == 0
                && self.job.0 > 0
                && (2..=4).contains(&self.workers)
                && [
                    &self.plan,
                    &self.source,
                    &self.config,
                    &self.store,
                    &self.topology,
                ]
                .iter()
                .all(|s| hash(s))
                && self.topology == digest(&("static-hash1", self.workers))?,
            "remote context fence/version",
        )
    }
    fn namespace(&self, store: &S3Config) -> Result<String> {
        self.validate()?;
        require(self.store == store.digest()?, "remote backend mismatch")?;
        Ok(format!(
            "{}/remote-v1/{}/attempt-0",
            store.prefix, self.execution
        ))
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Segment {
    pub sequence: u64,
    pub rows: u64,
    pub bytes: u64,
    pub blake3: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Terminal {
    pub context: Context,
    pub producer: usize,
    pub partition: usize,
    pub segments: Vec<Segment>,
    pub terminal: crate::lineage::TransferManifest,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalRef {
    pub producer: usize,
    pub partition: usize,
    pub bytes: u64,
    pub blake3: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub context: Context,
    pub terminals: Vec<TerminalRef>,
}
impl Manifest {
    fn validate(&self, context: &Context) -> Result<()> {
        context.validate()?;
        require(
            &self.context == context && self.terminals.len() == context.workers * context.workers,
            "remote manifest context/terminal coverage",
        )?;
        for (i, t) in self.terminals.iter().enumerate() {
            require(
                t.producer == i / context.workers
                    && t.partition == i % context.workers
                    && (1..=MAX_JSON as u64).contains(&t.bytes)
                    && hash(&t.blake3),
                "remote manifest terminal identity/order/count",
            )?;
        }
        Ok(())
    }
}
fn transfer(c: &Context, p: usize, w: usize) -> crate::ids::TransferId {
    crate::ids::transfer(c.job, 0, p as u64, w as u64)
}
fn transfer_hash(c: &Context, p: usize, w: usize) -> Result<blake3::Hasher> {
    let mut h = blake3::Hasher::new();
    h.update(b"ade-transfer-v1");
    h.update(&serde_json::to_vec(&transfer(c, p, w).logical())?);
    Ok(h)
}
fn terminal_hash(mut h: blake3::Hasher, n: u64) -> String {
    h.update(b"terminal-v1");
    h.update(&n.to_le_bytes());
    h.finalize().to_hex().to_string()
}
impl Terminal {
    fn validate(&self, c: &Context, limits: &Limits) -> Result<()> {
        require(
            &self.context == c
                && self.producer < c.workers
                && self.partition < c.workers
                && self.segments.len() as u64 <= limits.segments,
            "remote transfer fence/segments",
        )?;
        let mut rows = 0;
        let mut bytes = 0;
        for (i, s) in self.segments.iter().enumerate() {
            require(
                s.sequence == i as u64
                    && (1..=crate::exchange::MAX_BATCH_ROWS as u64).contains(&s.rows)
                    && (1..=crate::exchange::MAX_FRAME_BYTES as u64).contains(&s.bytes)
                    && hash(&s.blake3),
                "remote segment identity/envelope",
            )?;
            rows = add(rows, s.rows)?;
            bytes = add(bytes, s.bytes)?;
        }
        require(
            self.terminal.logical == transfer(c, self.partition, self.producer).logical()
                && self.terminal.attempt.0 == 0
                && self.terminal.segments == self.segments.len() as u64
                && self.terminal.rows == rows
                && self.terminal.payload_bytes == bytes
                && bytes <= limits.write_bytes
                && rows <= 1_000_000
                && hash(&self.terminal.fingerprint),
            "remote terminal counts/identity",
        )
    }
}

#[cfg(test)]
mod tests;
