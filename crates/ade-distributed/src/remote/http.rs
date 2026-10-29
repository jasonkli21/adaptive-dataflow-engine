//! Strict public `object_store` connector: no redirects/proxies, bounded HTTP bodies.
use super::{Limits, MAX_JSON};
use http_body_util::BodyExt;
use object_store::{
    ClientOptions,
    client::{
        HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse,
        HttpResponseBody, HttpService,
    },
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
#[derive(Debug, Default)]
pub(super) struct Observations {
    pub requests: AtomicU64,
    pub request_bytes: AtomicU64,
    pub response_bytes: AtomicU64,
}
#[derive(Debug)]
pub(super) struct Connector {
    pub allow_http: bool,
    pub limits: Limits,
    pub observed: Arc<Observations>,
}
impl HttpConnector for Connector {
    fn connect(&self, _: &ClientOptions) -> object_store::Result<HttpClient> {
        let timeout = Duration::from_millis(self.limits.io_timeout_ms);
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .https_only(!self.allow_http)
            .timeout(timeout)
            .connect_timeout(timeout)
            .pool_max_idle_per_host(1)
            .no_gzip()
            .no_brotli()
            .no_zstd()
            .no_deflate()
            .build()
            .map_err(|_| object_store::Error::Generic {
                store: "remote_object",
                source: "strict HTTP client configuration refused".into(),
            })?;
        Ok(HttpClient::new(Service {
            inner: HttpClient::new(client),
            observed: self.observed.clone(),
            requests: self.limits.requests,
        }))
    }
}
#[derive(Debug)]
struct Service {
    inner: HttpClient,
    observed: Arc<Observations>,
    requests: u64,
}
fn refused() -> HttpError {
    HttpError::new(
        HttpErrorKind::Decode,
        std::io::Error::other("remote HTTP response/request cap"),
    )
}
#[async_trait::async_trait]
impl HttpService for Service {
    async fn call(&self, request: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
        let n = self.observed.requests.fetch_add(1, Ordering::Relaxed);
        if n >= self.requests || request.body().content_length() > MAX_JSON {
            return Err(refused());
        }
        self.observed
            .request_bytes
            .fetch_add(request.body().content_length() as u64, Ordering::Relaxed);
        let response = self.inner.execute(request).await?;
        let (parts, body) = response.into_parts();
        let remaining = (64u64 * 1024 * 1024)
            .saturating_sub(self.observed.response_bytes.load(Ordering::Relaxed));
        let limit = usize::try_from(remaining.min(MAX_JSON as u64)).map_err(|_| refused())?;
        let observed = self.observed.clone();
        // Limited also clamps the body's advertised size_hint before SDK error XML collection.
        let body = http_body_util::Limited::new(body, limit)
            .map_err(|_| refused())
            .map_frame(move |frame| {
                if let Some(bytes) = frame.data_ref() {
                    observed
                        .response_bytes
                        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                }
                frame
            });
        Ok(HttpResponse::from_parts(parts, HttpResponseBody::new(body)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    #[derive(Debug)]
    struct Fixed(Bytes);
    #[async_trait::async_trait]
    impl HttpService for Fixed {
        async fn call(&self, _: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
            let mut response = HttpResponse::new(HttpResponseBody::new(
                http_body_util::Full::new(self.0.clone()).map_err(|e| match e {}),
            ));
            response
                .headers_mut()
                .insert("content-length", "18446744073709551615".parse().unwrap());
            Ok(response)
        }
    }
    #[test]
    fn body_allocation_hint_per_response_total_bytes_and_actual_request_caps() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let observed = Arc::new(Observations::default());
            let s = Service {
                inner: HttpClient::new(Fixed(Bytes::from(vec![0; MAX_JSON + 1]))),
                observed: observed.clone(),
                requests: 3,
            };
            let response = s.call(HttpRequest::new(Vec::new().into())).await.unwrap();
            assert!(response.into_body().bytes().await.is_err());
            assert_eq!(observed.response_bytes.load(Ordering::Relaxed), 0);
            let s = Service {
                inner: HttpClient::new(Fixed(Bytes::from(vec![0; MAX_JSON]))),
                observed: observed.clone(),
                requests: 100,
            };
            for _ in 0..64 {
                assert_eq!(
                    s.call(HttpRequest::new(Vec::new().into()))
                        .await
                        .unwrap()
                        .into_body()
                        .bytes()
                        .await
                        .unwrap()
                        .len(),
                    MAX_JSON
                );
            }
            assert!(
                s.call(HttpRequest::new(Vec::new().into()))
                    .await
                    .unwrap()
                    .into_body()
                    .bytes()
                    .await
                    .is_err()
            );
            assert_eq!(
                observed.response_bytes.load(Ordering::Relaxed),
                64 * 1024 * 1024
            );
            let s = Service {
                inner: HttpClient::new(Fixed(Bytes::new())),
                observed,
                requests: 1,
            };
            assert!(s.call(HttpRequest::new(Vec::new().into())).await.is_err());
        });
    }
}
