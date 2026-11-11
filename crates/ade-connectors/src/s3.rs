//! Signed whole-object GET over an explicitly pinned S3-compatible endpoint.
use crate::{
    CatalogEntryV1, ConnectorSpecV1, Context, Error, PinnedSplit, ResolvedSnapshotV1, Result,
    Source,
};
use futures::StreamExt;
use http_body_util::BodyExt;
use object_store::{
    ClientOptions, GetOptions, ObjectStore, RetryConfig,
    aws::AmazonS3Builder,
    client::{
        HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse,
        HttpResponseBody, HttpService,
    },
};
use std::{
    io::{Cursor, Read},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use url::{Host, Url};

const MAX_OBJECT: u64 = 16 * 1024 * 1024;

pub struct Endpoint {
    pub url: Url,
    /// Explicit IPv4 address for DNS pinning. The URL hostname still controls TLS.
    pub address: Ipv4Addr,
    pub bucket: String,
    pub region: String,
    pub fixture_http: bool,
}

impl Endpoint {
    fn validate(&self) -> Result<()> {
        let url = &self.url;
        let addr = self.address;
        let valid_transport = match (url.scheme(), url.host()) {
            ("http", Some(Host::Ipv4(ip))) => self.fixture_http && ip == addr && ip.is_loopback(),
            ("https", Some(Host::Domain(name))) => {
                !self.fixture_http && crate::network::public_endpoint(name, addr)
            }
            _ => false,
        };
        let safe_name = |s: &str| {
            (3..=63).contains(&s.len())
                && s.as_bytes()[0].is_ascii_alphanumeric()
                && s.as_bytes()[s.len() - 1].is_ascii_alphanumeric()
                && s.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        };
        if !valid_transport
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
            || !safe_name(&self.bucket)
            || !safe_name(&self.region)
        {
            return Err(Error::Admission("S3 endpoint or bucket policy"));
        }
        Ok(())
    }
}

pub struct S3 {
    endpoint: Endpoint,
    snapshot: ResolvedSnapshotV1,
    access: String,
    secret: String,
    token: Option<String>,
}

impl S3 {
    /// Create a connector without network I/O. Credentials come from the
    /// explicit AWS environment variables and never enter the descriptor.
    ///
    /// # Errors
    /// Refuses unsupported endpoint, credentials or snapshot.
    pub fn new(endpoint: Endpoint, snapshot: ResolvedSnapshotV1) -> Result<Self> {
        endpoint.validate()?;
        snapshot.digest()?;
        if snapshot.entry.connector.kind != "s3_parquet"
            || snapshot.splits.iter().any(|s| s.bytes > MAX_OBJECT)
        {
            return Err(Error::Admission("S3 connector kind or per-object cap"));
        }
        let credential = |name, max| -> Result<String> {
            let value =
                std::env::var(name).map_err(|_| Error::Admission("missing S3 credential"))?;
            if value.is_empty() || value.len() > max || !value.bytes().all(|c| c.is_ascii_graphic())
            {
                return Err(Error::Admission("invalid S3 credential"));
            }
            Ok(value)
        };
        let token = std::env::var_os("AWS_SESSION_TOKEN")
            .map(|_| credential("AWS_SESSION_TOKEN", 16384))
            .transpose()?;
        Ok(Self {
            endpoint,
            snapshot,
            access: credential("AWS_ACCESS_KEY_ID", 4096)?,
            secret: credential("AWS_SECRET_ACCESS_KEY", 4096)?,
            token,
        })
    }

    async fn get(&self, split: &PinnedSplit) -> Result<Vec<u8>> {
        let mut builder = AmazonS3Builder::new()
            .with_endpoint(self.endpoint.url.as_str())
            .with_bucket_name(&self.endpoint.bucket)
            .with_region(&self.endpoint.region)
            .with_access_key_id(&self.access)
            .with_secret_access_key(&self.secret)
            .with_allow_http(self.endpoint.fixture_http)
            .with_retry(RetryConfig {
                max_retries: 0,
                ..RetryConfig::default()
            })
            .with_http_connector(Transport {
                url: self.endpoint.url.clone(),
                address: self.endpoint.address,
                cap: usize::try_from(split.bytes)
                    .map_err(|_| Error::Limit("S3 object address space"))?,
            });
        if let Some(token) = &self.token {
            builder = builder.with_token(token);
        }
        let client = builder.build().map_err(|_| Error::Admission("S3 client"))?;
        let path =
            object_store::path::Path::parse(&split.key).map_err(|_| Error::Admission("S3 key"))?;
        let object = client
            .get_opts(&path, GetOptions::new())
            .await
            .map_err(|_| Error::Protocol("S3 GET"))?;
        if object.range != (0..split.bytes) || object.meta.size != split.bytes {
            return Err(Error::Identity("S3 declared size"));
        }
        let mut body = object.into_stream();
        let cap =
            usize::try_from(split.bytes).map_err(|_| Error::Limit("S3 object address space"))?;
        let mut data = Vec::with_capacity(cap);
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|_| Error::Protocol("S3 response body"))?;
            if chunk.len() > cap - data.len() {
                return Err(Error::Identity("S3 body size"));
            }
            data.extend_from_slice(&chunk);
        }
        if data.len() != cap {
            return Err(Error::Identity("S3 truncated body"));
        }
        Ok(data)
    }
}

impl Source for S3 {
    fn validate(&self, spec: &ConnectorSpecV1) -> Result<()> {
        if spec != &self.snapshot.entry.connector {
            return Err(Error::Identity("S3 connector spec"));
        }
        Ok(())
    }

    fn resolve(&self, entry: &CatalogEntryV1, context: &Context) -> Result<ResolvedSnapshotV1> {
        context.check()?;
        if entry != &self.snapshot.entry {
            return Err(Error::Identity("S3 catalog entry"));
        }
        Ok(self.snapshot.clone())
    }

    fn open(&self, split: &PinnedSplit, context: &Context) -> Result<Box<dyn Read + '_>> {
        context.check()?;
        if self
            .snapshot
            .splits
            .binary_search_by_key(&split.key.as_str(), |p| p.key.as_str())
            .ok()
            .is_none_or(|index| self.snapshot.splits[index] != *split)
        {
            return Err(Error::Identity("S3 split is absent from snapshot"));
        }
        let data = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?;
                    runtime.block_on(async {
                        let read = self.get(split);
                        tokio::pin!(read);
                        loop {
                            context.check()?;
                            tokio::select! {
                                result = &mut read => return result,
                                () = tokio::time::sleep(Duration::from_millis(25)) => {}
                            }
                        }
                    })
                })
                .join()
        })
        .map_err(|_| Error::Protocol("S3 transport task"))??;
        Ok(Box::new(Cursor::new(data)))
    }
}

#[derive(Debug)]
struct Transport {
    url: Url,
    address: Ipv4Addr,
    cap: usize,
}

impl HttpConnector for Transport {
    fn connect(&self, _: &ClientOptions) -> object_store::Result<HttpClient> {
        let host = self
            .url
            .host_str()
            .ok_or_else(|| client_error("endpoint host"))?;
        let port = self
            .url
            .port_or_known_default()
            .ok_or_else(|| client_error("endpoint port"))?;
        let inner = reqwest::Client::builder()
            .resolve(host, SocketAddr::new(IpAddr::V4(self.address), port))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .https_only(self.url.scheme() == "https")
            .timeout(Duration::from_secs(15))
            .connect_timeout(Duration::from_secs(5))
            .no_gzip()
            .no_brotli()
            .no_zstd()
            .no_deflate()
            .build()
            .map_err(|_| client_error("HTTP configuration"))?;
        Ok(HttpClient::new(Service {
            inner: HttpClient::new(inner),
            endpoint: self.url.clone(),
            cap: self.cap,
            used: AtomicBool::new(false),
        }))
    }
}

fn client_error(message: &'static str) -> object_store::Error {
    object_store::Error::Generic {
        store: "ade-connectors",
        source: message.into(),
    }
}

#[derive(Debug)]
struct Service {
    inner: HttpClient,
    endpoint: Url,
    cap: usize,
    used: AtomicBool,
}

fn refused() -> HttpError {
    HttpError::new(
        HttpErrorKind::Decode,
        std::io::Error::other("S3 request or response policy"),
    )
}

#[async_trait::async_trait]
impl HttpService for Service {
    async fn call(&self, request: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
        let uri = Url::parse(&request.uri().to_string()).map_err(|_| refused())?;
        if request.method().as_str() != "GET"
            || uri.origin() != self.endpoint.origin()
            || self.used.swap(true, Ordering::AcqRel)
        {
            return Err(refused());
        }
        let response = self.inner.execute(request).await?;
        let (parts, body) = response.into_parts();
        // Reject provider errors before the SDK attempts to decode an XML body.
        if parts.status.as_u16() != 200
            || parts.headers.contains_key("content-range")
            || parts.headers.contains_key("content-encoding")
            || parts
                .headers
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<usize>().ok())
                != Some(self.cap)
        {
            return Err(refused());
        }
        let limited = http_body_util::Limited::new(body, self.cap).map_err(|_| refused());
        Ok(HttpResponse::from_parts(
            parts,
            HttpResponseBody::new(limited),
        ))
    }
}
