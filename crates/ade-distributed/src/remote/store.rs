//! Owned serial S3 I/O. SDK retries are disabled so every attempt is counted.
use super::{Context, Counters, Limits, MAX_JSON, S3Config, add, hash};
use crate::{Error, Result, require};
use ade_exec::CancellationToken;
use futures::{StreamExt, future::Either};
use object_store::{
    ClientOptions, GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
    RetryConfig,
    aws::{AmazonS3Builder, S3ConditionalPut},
    path::Path,
};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

pub(super) struct Client {
    backend: Arc<dyn ObjectStore>,
    runtime: tokio::runtime::Runtime,
    namespace: String,
    limits: Limits,
    token: CancellationToken,
    until: Instant,
    poisoned: bool,
    objects: u64,
    requests: u64,
    pub counters: Counters,
    transport: Option<Arc<super::http::Observations>>,
}
#[derive(Debug, Clone, Copy)]
enum Failure {
    Cancelled,
    Timeout,
    Exists,
    Provider(bool),
    Bounds,
}
impl Client {
    /// Construct a signed provider from explicit config and ordinary AWS credential environment.
    /// # Errors
    /// Refuses missing credentials, unsafe endpoint/namespace or incompatible provider configuration.
    pub fn new(
        config: &S3Config,
        context: &Context,
        limits: &Limits,
        token: CancellationToken,
        until: Instant,
    ) -> Result<Self> {
        config.validate()?;
        limits.validate()?;
        let key = std::env::var("AWS_ACCESS_KEY_ID")
            .map_err(|_| Error::Invalid("remote credentials required".into()))?;
        let secret = std::env::var("AWS_SECRET_ACCESS_KEY")
            .map_err(|_| Error::Invalid("remote credentials required".into()))?;
        require(
            !key.is_empty() && !secret.is_empty() && key.len() <= 4096 && secret.len() <= 4096,
            "remote credentials required/bounded",
        )?;
        let transport = Arc::new(super::http::Observations::default());
        let mut builder = AmazonS3Builder::new()
            .with_http_connector(super::http::Connector {
                allow_http: config.dev_allow_http,
                limits: limits.clone(),
                observed: transport.clone(),
            })
            .with_access_key_id(key)
            .with_secret_access_key(secret)
            .with_endpoint(&config.endpoint)
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_allow_http(config.dev_allow_http)
            .with_skip_signature(false)
            .with_virtual_hosted_style_request(false)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
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
        if let Ok(token) = std::env::var("AWS_SESSION_TOKEN") {
            require(
                !token.is_empty() && token.len() <= 16_384,
                "remote session token limit",
            )?;
            builder = builder.with_token(token);
        }
        let backend = builder
            .build()
            .map_err(|_| Error::Invalid("S3 provider initialization refused".into()))?;
        let mut client =
            Self::with_backend(Arc::new(backend), config, context, limits, token, until)?;
        client.transport = Some(transport);
        Ok(client)
    }
    pub(super) fn with_backend(
        backend: Arc<dyn ObjectStore>,
        config: &S3Config,
        context: &Context,
        limits: &Limits,
        token: CancellationToken,
        until: Instant,
    ) -> Result<Self> {
        Ok(Self {
            backend,
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?,
            namespace: context.namespace(config)?,
            limits: limits.clone(),
            token,
            until,
            poisoned: false,
            objects: 0,
            requests: 0,
            counters: Counters::default(),
            transport: None,
        })
    }
    pub(crate) fn key(&self, suffix: &str) -> String {
        format!("{}/{suffix}", self.namespace)
    }
    fn checked_key(&self, key: &str) -> Result<Path> {
        require(
            key.starts_with(&format!("{}/", self.namespace))
                && key.len() <= 256
                && !key.contains("..")
                && !key.contains('%')
                && key
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"/_-.".contains(&b)),
            "remote exact private key",
        )?;
        Ok(Path::from(key))
    }
    fn ready(&self) -> Result<()> {
        require(!self.poisoned, "remote I/O poisoned")?;
        self.token.check()?;
        require(Instant::now() < self.until, "remote stage deadline")
    }
    fn failed<T>(&mut self, error: Failure) -> Result<T> {
        self.poisoned = true;
        let message = match error {
            Failure::Cancelled => "remote I/O cancelled; attempt quarantined",
            Failure::Timeout => "remote I/O deadline; attempt quarantined",
            Failure::Exists => "remote immutable create conflict; attempt quarantined",
            Failure::Provider(_) => "remote provider request refused; attempt quarantined",
            Failure::Bounds => "remote request/byte/object cap; attempt quarantined",
        };
        Err(Error::Invalid(message.into()))
    }
    fn one<T>(
        &mut self,
        kind: u8,
        future: impl Future<Output = object_store::Result<T>>,
    ) -> std::result::Result<T, Failure> {
        if self.poisoned {
            return Err(Failure::Bounds);
        }
        if self.token.check().is_err() {
            return Err(Failure::Cancelled);
        }
        let timeout = self
            .until
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(self.limits.io_timeout_ms));
        if timeout.is_zero() {
            return Err(Failure::Timeout);
        }
        self.requests = self.requests.checked_add(1).ok_or(Failure::Bounds)?;
        if self.requests > self.limits.requests {
            return Err(Failure::Bounds);
        }
        let count = match kind {
            0 => &mut self.counters.put,
            1 => &mut self.counters.head,
            2 => &mut self.counters.get,
            _ => &mut self.counters.delete,
        };
        *count = count.checked_add(1).ok_or(Failure::Bounds)?;
        let start = Instant::now();
        let token = self.token.clone();
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
                Either::Right(_) => Err(Failure::Cancelled),
                Either::Left((Err(_), _)) => Err(Failure::Timeout),
                Either::Left((Ok(Ok(value)), _)) => Ok(value),
                Either::Left((Ok(Err(error)), _)) => Err(match error {
                    object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. } => Failure::Exists,
                    object_store::Error::Generic { .. } => Failure::Provider(true),
                    _ => Failure::Provider(false),
                }),
            }
        });
        self.counters.elapsed_ns = self
            .counters
            .elapsed_ns
            .checked_add(u64::try_from(start.elapsed().as_nanos()).map_err(|_| Failure::Bounds)?)
            .ok_or(Failure::Bounds)?;
        if let Some(t) = &self.transport {
            self.counters.http_requests = t.requests.load(Ordering::Relaxed);
            self.counters.http_request_body_bytes = t.request_bytes.load(Ordering::Relaxed);
            self.counters.http_response_body_bytes = t.response_bytes.load(Ordering::Relaxed);
        }
        result
    }
    fn put_raw(&mut self, key: &str, bytes: Vec<u8>) -> std::result::Result<(), Failure> {
        let path = self.checked_key(key).map_err(|_| Failure::Bounds)?;
        self.objects = self.objects.checked_add(1).ok_or(Failure::Bounds)?;
        self.counters.write_bytes = self
            .counters
            .write_bytes
            .checked_add(bytes.len() as u64)
            .ok_or(Failure::Bounds)?;
        if self.objects > self.limits.objects
            || self.counters.write_bytes > self.limits.write_bytes
            || bytes.len() > MAX_JSON
        {
            return Err(Failure::Bounds);
        }
        let backend = self.backend.clone();
        self.one(0, async move {
            backend
                .put_opts(
                    &path,
                    bytes.into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await?;
            Ok(())
        })
    }
    pub(crate) fn put(&mut self, key: &str, bytes: Vec<u8>) -> Result<()> {
        self.ready()?;
        let n = bytes.len() as u64;
        let h = blake3::hash(&bytes).to_hex().to_string();
        match self.put_raw(key, bytes) {
            Ok(()) => {}
            Err(e) => return self.failed(e),
        }
        self.fetch(key, n, &h).map(|_| ())
    }
    pub(crate) fn probe(&mut self) -> Result<()> {
        let key = self.key("CAPABILITY");
        match self.put_raw(&key, b"create-only-v1".to_vec()) {
            Ok(()) => {}
            Err(e) => return self.failed(e),
        }
        match self.put_raw(&key, b"divergent".to_vec()) {
            Err(Failure::Exists) => {}
            _ => return self.failed(Failure::Provider(false)),
        }
        self.fetch(&key, 14, blake3::hash(b"create-only-v1").to_hex().as_str())?;
        self.delete(&key)
    }
    pub(crate) fn fetch(&mut self, key: &str, bytes: u64, checksum: &str) -> Result<Vec<u8>> {
        self.ready()?;
        require(
            (1..=MAX_JSON as u64).contains(&bytes) && hash(checksum),
            "remote expected read envelope",
        )?;
        let path = self.checked_key(key)?;
        let mut retry = 0;
        let meta = loop {
            let backend = self.backend.clone();
            let path = path.clone();
            match self.one(1, async move { backend.head(&path).await }) {
                Ok(meta) => break meta,
                Err(Failure::Provider(true)) if retry < self.limits.read_retries => {
                    retry += 1;
                    self.counters.retries = add(self.counters.retries, 1)?;
                }
                Err(e) => return self.failed(e),
            }
        };
        if meta.size != bytes {
            return self.failed(Failure::Provider(false));
        }
        let requested =
            usize::try_from(bytes).map_err(|_| Error::Invalid("remote read size".into()))?;
        let mut retry = 0;
        loop {
            let backend = self.backend.clone();
            let path = path.clone();
            let tag = meta.e_tag.clone();
            let observed = Arc::new(AtomicU64::new(0));
            let observed_read = observed.clone();
            let result = self.one(2, async move {
                let result = backend
                    .get_opts(
                        &path,
                        GetOptions {
                            range: Some(GetRange::Bounded(0..bytes)),
                            if_match: tag,
                            ..Default::default()
                        },
                    )
                    .await?;
                if result.range != (0..bytes) {
                    return Err(provider_error());
                }
                let mut stream = result.into_stream();
                let mut data = Vec::new();
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk?;
                    observed_read.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                    if chunk.len() > requested.saturating_sub(data.len()) {
                        return Err(provider_error());
                    }
                    data.reserve_exact(chunk.len());
                    data.extend_from_slice(&chunk);
                }
                if data.len() as u64 != bytes {
                    return Err(provider_error());
                }
                Ok(data)
            });
            self.counters.read_bytes =
                add(self.counters.read_bytes, observed.load(Ordering::Relaxed))?;
            if self.counters.read_bytes > 64 * 1024 * 1024 {
                return self.failed(Failure::Bounds);
            }
            match result {
                Ok(data) => {
                    if self.counters.read_bytes > 64 * 1024 * 1024
                        || blake3::hash(&data).to_hex().as_str() != checksum
                    {
                        return self.failed(Failure::Bounds);
                    }
                    return Ok(data);
                }
                Err(Failure::Provider(true)) if retry < self.limits.read_retries => {
                    retry += 1;
                    self.counters.retries = add(self.counters.retries, 1)?;
                }
                Err(e) => return self.failed(e),
            }
        }
    }
    pub(crate) fn delete(&mut self, key: &str) -> Result<()> {
        self.ready()?;
        let path = self.checked_key(key)?;
        let backend = self.backend.clone();
        match self.one(3, async move { backend.delete(&path).await }) {
            Ok(()) => Ok(()),
            Err(e) => self.failed(e),
        }
    }
}
fn provider_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "remote_object",
        source: "remote bounded response mismatch".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::JobId;
    fn fixture(limits: &Limits) -> (Client, Arc<dyn ObjectStore>) {
        let config = S3Config {
            endpoint: "https://fixture.invalid".into(),
            bucket: "test-bucket".into(),
            prefix: "private".into(),
            region: "us-east-1".into(),
            dev_allow_http: false,
        };
        let context = Context {
            version: 1,
            execution: uuid::Uuid::new_v4().to_string(),
            attempt: 0,
            job: JobId(1),
            workers: 2,
            plan: "a".repeat(64),
            source: "b".repeat(64),
            config: "c".repeat(64),
            store: config.digest().unwrap(),
            topology: super::super::digest(&("static-hash1", 2usize)).unwrap(),
        };
        let backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let c = Client::with_backend(
            backend.clone(),
            &config,
            &context,
            limits,
            CancellationToken::default(),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        (c, backend)
    }
    #[test]
    fn immutable_create_wrong_size_checksum_namespace_and_caps_poison() {
        for attack in [
            "duplicate",
            "size",
            "checksum",
            "request",
            "bytes",
            "objects",
            "namespace",
        ] {
            let mut limits = Limits::default();
            if attack == "request" {
                limits.requests = 2;
            }
            if attack == "bytes" {
                limits.write_bytes = 2;
            }
            if attack == "objects" {
                limits.objects = 1;
            }
            let (mut c, _) = fixture(&limits);
            let key = c.key("one");
            if attack == "bytes" {
                assert!(c.put(&key, b"abc".to_vec()).is_err());
                continue;
            }
            if attack == "request" {
                assert!(c.put(&key, b"abc".to_vec()).is_err());
                continue;
            }
            c.put(&key, b"abc".to_vec()).unwrap();
            let checksum = blake3::hash(b"abc").to_hex().to_string();
            let result = match attack {
                "duplicate" => c.put(&key, b"abc".to_vec()),
                "size" => c.fetch(&key, 2, &checksum).map(|_| ()),
                "checksum" => c.fetch(&key, 3, &"d".repeat(64)).map(|_| ()),
                "objects" => c.put(&c.key("two"), b"abc".to_vec()),
                "namespace" => c.put("private/remote-v1/foreign/attempt-0/one", b"abc".to_vec()),
                _ => unreachable!(),
            };
            assert!(result.is_err(), "{attack}");
            assert!(c.put(&c.key("after-error"), vec![1]).is_err(), "{attack}");
        }
    }
    #[test]
    fn cancellation_deadline_drop_future_and_no_late_local_publication() {
        let (mut c, backend) = fixture(&Limits {
            io_timeout_ms: 10,
            ..Limits::default()
        });
        let key = Path::from(c.key("late"));
        let store = backend.clone();
        let failure = c
            .one(0, async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                store.put(&key, vec![1].into()).await?;
                Ok(())
            })
            .unwrap_err();
        assert!(matches!(failure, Failure::Timeout));
        assert!(c.failed::<()>(failure).is_err());
        c.runtime.block_on(tokio_delay());
        assert!(
            c.runtime
                .block_on(backend.head(&Path::from(c.key("late"))))
                .is_err()
        );
        assert!(c.put(&c.key("MANIFEST.json"), vec![1]).is_err());
        let (mut c, _) = fixture(&Limits::default());
        c.token.cancel();
        assert!(c.probe().is_err());
        assert_eq!(c.counters.put, 0);
    }
    async fn tokio_delay() {
        tokio::time::sleep(Duration::from_millis(75)).await;
    }
    #[test]
    fn conditional_capability_probe_and_exact_operation_counts() {
        let (mut c, _) = fixture(&Limits::default());
        c.probe().unwrap();
        assert_eq!(
            (
                c.counters.put,
                c.counters.head,
                c.counters.get,
                c.counters.delete
            ),
            (2, 1, 1, 1)
        );
        assert_eq!(c.counters.write_bytes, 23);
        assert_eq!(c.counters.read_bytes, 14);
    }
}
