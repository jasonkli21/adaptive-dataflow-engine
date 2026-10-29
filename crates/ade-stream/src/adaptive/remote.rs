//! Owned sequential S3 transport. Persistent conservative reservations survive crashes.
use super::{
    Limits, Remote, http,
    snapshot::{self, bytes, encode, hash},
};
use crate::{
    Error, Result,
    state::{
        check,
        store::{self, io, sync_dir},
    },
};
use ade_exec::CancellationToken;
use futures::{StreamExt, future::Either};
use object_store::{
    ClientOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, RetryConfig,
    aws::{AmazonS3Builder, S3ConditionalPut},
    path::Path,
};
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    io::Write,
    path::{Path as FsPath, PathBuf},
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ledger {
    pub version: u32,
    pub remote_blake3: String,
    pub objects: u64,
    pub write_bytes: u64,
    pub requests: u64,
    pub reserved_response_bytes: u64,
}
#[derive(Clone, Default, Debug, Serialize)]
pub struct Metrics {
    pub put: u64,
    pub head: u64,
    pub get: u64,
    pub elapsed_ns: u64,
    pub http_requests: u64,
    pub http_request_bytes: u64,
    pub http_response_bytes: u64,
}
pub struct Client {
    backend: Arc<dyn ObjectStore>,
    runtime: tokio::runtime::Runtime,
    config: Remote,
    limits: Limits,
    root: Option<PathBuf>,
    token: CancellationToken,
    poisoned: bool,
    pub ledger: Ledger,
    pub metrics: Metrics,
    transport: Arc<http::Observations>,
    until: Instant,
}
#[derive(Debug)]
enum Failure {
    Exists,
    Other,
}
/// # Errors
/// Reads the exact local reservation authority; never discovers remote objects.
pub fn reservations(root: &FsPath, remote: &Remote) -> Result<Ledger> {
    let digest = hash(&encode(remote)?);
    if root.join("REMOTE-LEDGER").exists() {
        let ledger: Ledger = snapshot::json(&bytes(&root.join("REMOTE-LEDGER"), 4096)?)?;
        check(
            ledger.version == 1 && ledger.remote_blake3 == digest,
            "remote reservation namespace/version",
        )?;
        Ok(ledger)
    } else {
        Ok(Ledger {
            version: 1,
            remote_blake3: digest,
            ..Ledger::default()
        })
    }
}
/// # Errors
/// Checks environment-only credential presence/size without network/filesystem effects.
pub fn validate_environment() -> Result<()> {
    for name in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"] {
        let value = std::env::var(name)
            .map_err(|_| Error::State("remote environment credentials required"))?;
        check(
            !value.is_empty() && value.len() <= 4096,
            "remote credential size",
        )?;
    }
    if let Ok(value) = std::env::var("AWS_SESSION_TOKEN") {
        check(
            !value.is_empty() && value.len() <= 16384,
            "remote session credential cap",
        )?;
    }
    Ok(())
}
impl Client {
    /// # Errors
    /// Refuses unsafe endpoints, missing credentials, stale reservations or unavailable provider.
    pub fn new(
        config: &Remote,
        limits: &Limits,
        root: Option<&FsPath>,
        token: CancellationToken,
    ) -> Result<Self> {
        config.validate()?;
        limits.validate()?;
        let key = std::env::var("AWS_ACCESS_KEY_ID")
            .map_err(|_| Error::State("remote environment credentials required"))?;
        let secret = std::env::var("AWS_SECRET_ACCESS_KEY")
            .map_err(|_| Error::State("remote environment credentials required"))?;
        check(
            !key.is_empty() && !secret.is_empty() && key.len() <= 4096 && secret.len() <= 4096,
            "remote credential size",
        )?;
        let transport = Arc::new(http::Observations::default());
        let mut builder = AmazonS3Builder::new()
            .with_access_key_id(key)
            .with_secret_access_key(secret)
            .with_endpoint(&config.endpoint)
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_allow_http(config.dev_allow_http)
            .with_skip_signature(false)
            .with_virtual_hosted_style_request(false)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_http_connector(http::Connector {
                allow_http: config.dev_allow_http,
                limits: limits.clone(),
                observed: transport.clone(),
            })
            .with_retry(RetryConfig {
                max_retries: 0,
                retry_timeout: Duration::from_millis(limits.io_timeout_ms),
                ..Default::default()
            })
            .with_client_options(
                ClientOptions::new()
                    .with_allow_http(config.dev_allow_http)
                    .with_timeout(Duration::from_millis(limits.io_timeout_ms))
                    .with_connect_timeout(Duration::from_millis(limits.io_timeout_ms)),
            );
        if let Ok(t) = std::env::var("AWS_SESSION_TOKEN") {
            check(
                !t.is_empty() && t.len() <= 16384,
                "remote session credential cap",
            )?;
            builder = builder.with_token(t);
        }
        let backend = Arc::new(
            builder
                .build()
                .map_err(|_| Error::State("remote signed provider initialization"))?,
        );
        let remote_blake3 = hash(&encode(config)?);
        let ledger = if let Some(root) = root
            && root.join("REMOTE-LEDGER").exists()
        {
            let l: Ledger = snapshot::json(&bytes(&root.join("REMOTE-LEDGER"), 4096)?)?;
            check(
                l.version == 1 && l.remote_blake3 == remote_blake3,
                "remote reservation namespace/version",
            )?;
            l
        } else {
            Ledger {
                version: 1,
                remote_blake3,
                ..Ledger::default()
            }
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| Error::State("owned remote runtime"))?;
        let result = Self {
            backend,
            runtime,
            config: config.clone(),
            limits: limits.clone(),
            root: root.map(FsPath::to_path_buf),
            token,
            poisoned: false,
            ledger,
            metrics: Metrics::default(),
            transport,
            until: Instant::now() + Duration::from_secs(120),
        };
        result.check_budget()?;
        Ok(result)
    }
    #[must_use]
    pub fn config(&self) -> &Remote {
        &self.config
    }
    #[must_use]
    pub fn is_writer(&self) -> bool {
        self.root.is_some()
    }
    fn check_budget(&self) -> Result<()> {
        check(
            self.ledger.objects <= self.limits.remote_objects
                && self.ledger.write_bytes <= self.limits.remote_write_bytes
                && self.ledger.requests <= self.limits.requests
                && self.ledger.reserved_response_bytes <= self.limits.response_bytes,
            "remote persistent/orphan resource quota",
        )
    }
    fn persist(&self) -> Result<()> {
        if let Some(root) = &self.root {
            let path = root.join("ledger-next");
            let mut f = store::create_file(&path)?;
            io(f.write_all(&encode(&self.ledger)?))?;
            io(f.sync_all())?;
            io(std::fs::rename(path, root.join("REMOTE-LEDGER")))?;
            sync_dir(root)?;
        }
        Ok(())
    }
    fn ready(&self) -> Result<()> {
        check(!self.poisoned, "remote poisoned")?;
        self.token.check().map_err(|_| Error::Cancelled)?;
        check(Instant::now() < self.until, "remote cut deadline")
    }
    fn path(&self, suffix: &str) -> Result<Path> {
        check(
            suffix.len() <= 128
                && !suffix.contains("..")
                && !suffix.starts_with('/')
                && suffix
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"-./".contains(&b)),
            "remote derived exact key",
        )?;
        Ok(Path::from(format!(
            "{}/checkpoint-v2/{}/{suffix}",
            self.config.prefix, self.config.namespace
        )))
    }
    fn one<T>(
        &mut self,
        kind: u8,
        future: impl Future<Output = object_store::Result<T>>,
    ) -> std::result::Result<T, Failure> {
        self.ready().map_err(|_| Failure::Other)?;
        self.ledger.requests = self.ledger.requests.checked_add(1).ok_or(Failure::Other)?;
        // Reserve the full error/body envelope before sending, including HEAD errors.
        self.ledger.reserved_response_bytes = self
            .ledger
            .reserved_response_bytes
            .checked_add(1024 * 1024)
            .ok_or(Failure::Other)?;
        self.check_budget()
            .and_then(|()| self.persist())
            .map_err(|_| Failure::Other)?;
        let n = match kind {
            0 => &mut self.metrics.put,
            1 => &mut self.metrics.head,
            _ => &mut self.metrics.get,
        };
        *n = n.checked_add(1).ok_or(Failure::Other)?;
        let token = self.token.clone();
        let start = Instant::now();
        let timeout = Duration::from_millis(self.limits.io_timeout_ms)
            .min(self.until.saturating_duration_since(start));
        let result = self.runtime.block_on(async {
            let work = tokio::time::timeout(timeout, future);
            let cancel = async {
                loop {
                    if token.check().is_err() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            };
            futures::pin_mut!(work, cancel);
            match futures::future::select(work, cancel).await {
                Either::Left((Ok(Ok(v)), _)) => Ok(v),
                Either::Left((
                    Ok(Err(
                        object_store::Error::AlreadyExists { .. }
                        | object_store::Error::Precondition { .. },
                    )),
                    _,
                )) => Err(Failure::Exists),
                _ => Err(Failure::Other),
            }
        });
        self.metrics.elapsed_ns = self
            .metrics
            .elapsed_ns
            .checked_add(u64::try_from(start.elapsed().as_nanos()).map_err(|_| Failure::Other)?)
            .ok_or(Failure::Other)?;
        self.metrics.http_requests = self.transport.requests.load(Ordering::Relaxed);
        self.metrics.http_request_bytes = self.transport.request_bytes.load(Ordering::Relaxed);
        self.metrics.http_response_bytes = self.transport.response_bytes.load(Ordering::Relaxed);
        result
    }
    fn failed<T>(&mut self) -> Result<T> {
        self.poisoned = true;
        self.token.check().map_err(|_| Error::Cancelled)?;
        Err(Error::State(
            "remote operation failed/ambiguous; poisoned, reservations retained",
        ))
    }
    fn put_raw(&mut self, key: &str, b: Vec<u8>) -> std::result::Result<(), Failure> {
        self.ready().map_err(|_| Failure::Other)?;
        if self.root.is_none() || b.len() as u64 > self.limits.object_bytes {
            return Err(Failure::Other);
        }
        self.ledger.objects = self.ledger.objects.checked_add(1).ok_or(Failure::Other)?;
        self.ledger.write_bytes = self
            .ledger
            .write_bytes
            .checked_add(b.len() as u64)
            .ok_or(Failure::Other)?;
        self.check_budget()
            .and_then(|()| self.persist())
            .map_err(|_| Failure::Other)?;
        let path = self.path(key).map_err(|_| Failure::Other)?;
        let backend = self.backend.clone();
        self.one(0, async move {
            backend
                .put_opts(
                    &path,
                    b.into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await?;
            Ok(())
        })
    }
    /// # Errors
    /// Single conditional PUT; exact HEAD/GET readback. Any ambiguity poisons without retry.
    pub fn put(&mut self, key: &str, b: Vec<u8>) -> Result<()> {
        let n = b.len() as u64;
        let h = hash(&b);
        if self.put_raw(key, b).is_err() {
            return self.failed();
        }
        self.fetch(key, n, &h).map(|_| ())
    }
    /// # Errors
    /// Requires real immutable conditional-create and read-after-write behavior.
    pub fn probe(&mut self) -> Result<()> {
        let key = format!("probe-{}", uuid::Uuid::new_v4());
        if self.put_raw(&key, b"immutable-v2".to_vec()).is_err() {
            return self.failed();
        }
        if !matches!(
            self.put_raw(&key, b"divergent-v2".to_vec()),
            Err(Failure::Exists)
        ) {
            return self.failed();
        }
        self.fetch(&key, 12, &hash(b"immutable-v2"))?;
        Ok(())
    }
    /// # Errors
    /// Exact HEAD/GET, including zero-length shards, bounded body, no fallback or retries.
    pub fn fetch(&mut self, key: &str, n: u64, h: &str) -> Result<Vec<u8>> {
        self.ready()?;
        if n > self.limits.object_bytes || !snapshot::digest(h) {
            return self.failed();
        }
        let path = self.path(key)?;
        let backend = self.backend.clone();
        let head_path = path.clone();
        let Ok(meta) = self.one(1, async move { backend.head(&head_path).await }) else {
            return self.failed();
        };
        if meta.size != n {
            return self.failed();
        }
        let backend = self.backend.clone();
        let tag = meta.e_tag;
        let Ok(b) = self.one(2, async move {
            let result = backend
                .get_opts(
                    &path,
                    object_store::GetOptions {
                        if_match: tag,
                        ..Default::default()
                    },
                )
                .await?;
            let mut stream = result.into_stream();
            let mut b = Vec::with_capacity(usize::try_from(n).map_err(|_| provider())?);
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                if chunk.len() as u64 > n.saturating_sub(b.len() as u64) {
                    return Err(provider());
                }
                b.extend_from_slice(&chunk);
            }
            if b.len() as u64 != n {
                return Err(provider());
            }
            Ok(b)
        }) else {
            return self.failed();
        };
        if hash(&b) != h {
            return self.failed();
        }
        Ok(b)
    }
    /// Reset only the per-cut deadline; lifetime reservations never reset.
    pub fn begin_cut(&mut self) {
        self.until = Instant::now() + Duration::from_secs(120);
    }
}
fn provider() -> object_store::Error {
    object_store::Error::Generic {
        store: "checkpoint-v2",
        source: "bounded integrity response".into(),
    }
}
