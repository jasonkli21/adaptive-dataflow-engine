//! Bounded read-only Arrow Flight `DoGet` capture.
use crate::{
    CatalogEntryV1, Context, Error, Result, Stage,
    batch::capture::Capture,
    network::{guarded, public_endpoint},
};
use arrow::{
    datatypes::Schema,
    ipc::{MessageHeader, root_as_message},
};
use arrow_flight::{
    Ticket, flight_service_client::FlightServiceClient, utils::flight_data_to_arrow_batch,
};
use std::{collections::HashMap, net::Ipv4Addr, path::Path, time::Duration};
use tonic::transport::{ClientTlsConfig, Endpoint as Channel};

const MAX_MESSAGE: usize = 1024 * 1024;

pub struct Endpoint {
    pub host: String,
    pub address: Ipv4Addr,
    pub port: u16,
    pub fixture_plaintext: bool,
    pub fixture_ca_pem: Option<Vec<u8>>,
}

impl Endpoint {
    fn channel(&self) -> Result<Channel> {
        let secure = !self.fixture_plaintext;
        let fixture_tls = self
            .fixture_ca_pem
            .as_ref()
            .is_some_and(|pem| (1..=8192).contains(&pem.len()))
            && self.address.is_loopback()
            && self.host == "localhost";
        let valid = if self.fixture_ca_pem.is_some() {
            secure && fixture_tls
        } else if secure {
            public_endpoint(&self.host, self.address)
        } else {
            self.address.is_loopback() && self.host == self.address.to_string()
        };
        if !valid || self.port == 0 {
            return Err(Error::Admission("Flight endpoint policy"));
        }
        let scheme = if secure { "https" } else { "http" };
        let channel = Channel::from_shared(format!("{scheme}://{}:{}", self.address, self.port))
            .map_err(|_| Error::Admission("Flight endpoint"))?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(15))
            .initial_stream_window_size(256 * 1024)
            .initial_connection_window_size(512 * 1024);
        if secure {
            let mut tls = ClientTlsConfig::new().domain_name(&self.host);
            tls = if let Some(ca) = &self.fixture_ca_pem {
                tls.ca_certificate(tonic::transport::Certificate::from_pem(ca))
            } else {
                tls.with_webpki_roots()
            };
            return channel
                .tls_config(tls)
                .map_err(|_| Error::Admission("Flight TLS configuration"));
        }
        Ok(channel)
    }
}

/// Capture one `DoGet` response. An opaque ticket remains one-shot. A fresh
/// capture always has its own materialized content identity.
///
/// # Errors
/// Refuses unsupported endpoints, schema or IPC semantics, over-budget
/// streams, cancellation, timeout and transport/protocol failure.
pub async fn capture(
    endpoint: &Endpoint,
    ticket: &[u8],
    entry: CatalogEntryV1,
    parent: &Path,
    context: &Context,
) -> Result<Stage> {
    context.check()?;
    if entry.connector.kind != "flight" || ticket.is_empty() || ticket.len() > 4096 {
        return Err(Error::Admission("Flight entry or ticket"));
    }
    let channel = endpoint.channel()?;
    let mut capture = Capture::new(entry, parent)?;
    guarded(context, async {
        let connection = channel
            .connect()
            .await
            .map_err(|_| Error::Protocol("Flight connection or TLS"))?;
        let mut client =
            FlightServiceClient::new(connection).max_decoding_message_size(MAX_MESSAGE);
        let mut response = client
            .do_get(Ticket {
                ticket: ticket.to_vec().into(),
            })
            .await
            .map_err(|_| Error::Protocol("Flight DoGet"))?
            .into_inner();
        let mut schema = false;
        let mut messages = 0usize;
        let mut encoded = 0;
        while let Some(data) = response
            .message()
            .await
            .map_err(|_| Error::Protocol("Flight response"))?
        {
            messages += 1;
            encoded += data.data_header.len() + data.data_body.len();
            if encoded > 16 * MAX_MESSAGE {
                return Err(Error::Limit("Flight encoded stream bytes"));
            }
            if messages > 1024
                || data.data_header.len() > 64 * 1024
                || !data.app_metadata.is_empty()
                || data.flight_descriptor.is_some()
            {
                return Err(Error::Limit("Flight messages or metadata"));
            }
            let header = root_as_message(&data.data_header)
                .map_err(|_| Error::Protocol("Flight IPC header"))?;
            if usize::try_from(header.bodyLength()).ok() != Some(data.data_body.len()) {
                return Err(Error::Protocol("Flight IPC body length"));
            }
            match header.header_type() {
                MessageHeader::Schema if !schema && data.data_body.is_empty() => {
                    if &Schema::try_from(&data).map_err(|_| Error::Protocol("Flight IPC schema"))?
                        != capture.schema().as_ref()
                    {
                        return Err(Error::Identity("Flight declared schema"));
                    }
                    schema = true;
                }
                MessageHeader::RecordBatch if schema => {
                    let batch = header
                        .header_as_record_batch()
                        .ok_or(Error::Protocol("Flight IPC record batch"))?;
                    if batch.compression().is_some() || !(0..=8192).contains(&batch.length()) {
                        return Err(Error::Limit("Flight compression or batch rows"));
                    }
                    let batch = flight_data_to_arrow_batch(
                        &data,
                        capture.schema().clone(),
                        &HashMap::new(),
                    )
                    .map_err(|_| Error::Protocol("Flight IPC record batch decode"))?;
                    capture.push(&batch, context)?;
                }
                _ => {
                    return Err(Error::Admission(
                        "Flight dictionary, schema drift or message order",
                    ));
                }
            }
        }
        if !schema {
            return Err(Error::Protocol("Flight stream missing schema"));
        }
        // Tickets can be bearer secrets. Do not put their bytes or hash in diagnostics.
        capture.finish(
            ade_physical::fingerprint(b"flight-doget-one-shot-v1"),
            context,
        )
    })
    .await
}
