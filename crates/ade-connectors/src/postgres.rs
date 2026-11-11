//! Bounded bigint export from one PostgreSQL repeatable-read snapshot.
mod wire;

use crate::{
    CatalogEntryV1, Context, Error, Result, Stage,
    batch::capture::Capture,
    contract::identifier,
    network::{guarded, public_endpoint},
};
use arrow::{
    array::{ArrayRef, Int64Array},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio_postgres::{
    Client, Config, IsolationLevel, NoTls,
    config::SslMode,
    tls::MakeTlsConnect,
    types::{ToSql, Type},
};

pub struct Endpoint {
    pub host: String,
    pub address: Ipv4Addr,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub fixture_plaintext: bool,
    pub fixture_ca_pem: Option<Vec<u8>>,
}

impl Endpoint {
    fn config(&self) -> Result<Config> {
        let fixture_tls = self
            .fixture_ca_pem
            .as_ref()
            .is_some_and(|pem| (1..=8192).contains(&pem.len()))
            && self.address.is_loopback()
            && self.host == "localhost";
        let transport = if self.fixture_ca_pem.is_some() {
            !self.fixture_plaintext && fixture_tls
        } else if self.fixture_plaintext {
            self.address.is_loopback() && self.host == self.address.to_string()
        } else {
            public_endpoint(&self.host, self.address)
        };
        if !transport || self.port == 0 || !identifier(&self.database) || !identifier(&self.user) {
            return Err(Error::Admission("Postgres endpoint or user policy"));
        }
        let mut config = Config::new();
        config
            .user(&self.user)
            .dbname(&self.database)
            .application_name("ade-connectors")
            .ssl_mode(if self.fixture_plaintext {
                SslMode::Disable
            } else {
                SslMode::Require
            });
        if !self.fixture_plaintext && !fixture_tls {
            let password = std::env::var("ADE_CONNECTOR_PG_PASSWORD")
                .map_err(|_| Error::Admission("Postgres password required"))?;
            if password.is_empty() || password.len() > 4096 || password.contains('\0') {
                return Err(Error::Admission("Postgres password size"));
            }
            config.password(password);
        }
        Ok(config)
    }
}

pub struct Table {
    pub schema: String,
    pub name: String,
}

/// Capture an allowlisted bigint projection from one ordinary table. Table
/// and column identifiers are selected by the caller and validated as names.
/// The capture is one-shot because the transaction ends before native SQL.
///
/// # Errors
/// Refuses unsafe endpoints, incompatible types, query/transport failure,
/// NULL values, limits and incomplete captures.
pub async fn capture(
    endpoint: &Endpoint,
    table: &Table,
    entry: CatalogEntryV1,
    parent: &Path,
    context: &Context,
) -> Result<Stage> {
    context.check()?;
    if entry.connector.kind != "postgres"
        || !identifier(&table.schema)
        || !identifier(&table.name)
        || entry
            .schema
            .iter()
            .any(|f| f.data_type != ade_physical::Type::I64)
    {
        return Err(Error::Admission("Postgres table or type allowlist"));
    }
    let config = endpoint.config()?;
    let authority = ade_physical::fingerprint(
        format!(
            "postgres-v1/{}/{}/{}",
            endpoint.database, table.schema, table.name
        )
        .as_bytes(),
    );
    let capture = Capture::new(entry.clone(), parent)?;
    let exhausted = Arc::new(AtomicBool::new(false));
    let extract = async {
        let address = SocketAddrV4::new(endpoint.address, endpoint.port);
        let socket = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::net::TcpStream::connect(address),
        )
        .await
        .map_err(|_| Error::Protocol("Postgres connection timeout"))??;
        let wire = wire::Wire::new(socket, exhausted.clone());
        if endpoint.fixture_plaintext {
            let (mut client, connection) = config
                .connect_raw(wire, NoTls)
                .await
                .map_err(|_| Error::Protocol("Postgres connection"))?;
            tokio::select! {
                result = export(&mut client, table, &entry, capture, authority, context) => result,
                _ = connection => Err(Error::Protocol("Postgres connection lost")),
            }
        } else {
            let mut builder = native_tls::TlsConnector::builder();
            if let Some(pem) = &endpoint.fixture_ca_pem {
                let cert = native_tls::Certificate::from_pem(pem)
                    .map_err(|_| Error::Admission("Postgres fixture CA"))?;
                builder.add_root_certificate(cert);
            }
            let tls = builder
                .build()
                .map_err(|_| Error::Protocol("Postgres TLS setup"))?;
            let mut connector = postgres_native_tls::MakeTlsConnector::new(tls);
            let client_tls =
                MakeTlsConnect::<wire::Wire>::make_tls_connect(&mut connector, &endpoint.host)
                    .map_err(|_| Error::Admission("Postgres TLS configuration"))?;
            let (mut client, connection) = config
                .connect_raw(wire, client_tls)
                .await
                .map_err(|_| Error::Protocol("Postgres TLS or connection"))?;
            tokio::select! {
                result = export(&mut client, table, &entry, capture, authority, context) => result,
                _ = connection => Err(Error::Protocol("Postgres connection lost")),
            }
        }
    };
    let result = guarded(context, extract).await;
    if exhausted.load(Ordering::Relaxed) {
        return Err(Error::Limit("Postgres wire bytes"));
    }
    result
}

async fn export(
    client: &mut Client,
    table: &Table,
    entry: &CatalogEntryV1,
    mut capture: Capture,
    authority: String,
    context: &Context,
) -> Result<Stage> {
    let tx = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .map_err(|_| Error::Protocol("Postgres snapshot begin"))?;
    tx.batch_execute("SET LOCAL statement_timeout = '15s'; SET LOCAL lock_timeout = '5s'; SET LOCAL search_path = pg_catalog;")
        .await.map_err(|_| Error::Protocol("Postgres snapshot settings"))?;
    let qualified = format!("\"{}\".\"{}\"", table.schema, table.name);
    tx.batch_execute(&format!("LOCK TABLE ONLY {qualified} IN ACCESS SHARE MODE"))
        .await
        .map_err(|_| Error::Protocol("Postgres table lock"))?;
    let valid = tx.query_opt("SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r' AND NOT c.relispartition AND NOT c.relrowsecurity",
        &[&table.schema, &table.name]).await.map_err(|_| Error::Protocol("Postgres relation metadata"))?;
    if valid.is_none() {
        return Err(Error::Admission("Postgres ordinary table required"));
    }
    let columns = entry
        .schema
        .iter()
        .map(|f| format!("\"{}\"", f.name))
        .collect::<Vec<_>>()
        .join(", ");
    let statement = tx
        .prepare(&format!(
            "SELECT {columns} FROM ONLY {qualified} LIMIT 65537"
        ))
        .await
        .map_err(|_| Error::Protocol("Postgres projection"))?;
    if statement.columns().len() != entry.schema.len()
        || statement
            .columns()
            .iter()
            .zip(&entry.schema)
            .any(|(c, f)| c.name() != f.name || *c.type_() != Type::INT8)
    {
        return Err(Error::Admission("Postgres bigint projection required"));
    }
    {
        let rows = tx
            .query_raw(&statement, std::iter::empty::<&(dyn ToSql + Sync)>())
            .await
            .map_err(|_| Error::Protocol("Postgres export"))?;
        tokio::pin!(rows);
        let mut columns = vec![Vec::<i64>::with_capacity(256); entry.schema.len()];
        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|_| Error::Protocol("Postgres row stream"))?
        {
            context.check()?;
            for (i, values) in columns.iter_mut().enumerate() {
                values.push(
                    row.try_get(i)
                        .map_err(|_| Error::Identity("Postgres NULL or incompatible value"))?,
                );
            }
            if columns[0].len() == 256 {
                append(&mut columns, &mut capture, context)?;
            }
        }
        if !columns[0].is_empty() {
            append(&mut columns, &mut capture, context)?;
        }
    }
    let stage = capture.finish(authority, context)?;
    tx.commit()
        .await
        .map_err(|_| Error::Protocol("Postgres snapshot close"))?;
    Ok(stage)
}

fn append(columns: &mut [Vec<i64>], capture: &mut Capture, context: &Context) -> Result<()> {
    let arrays = columns
        .iter_mut()
        .map(|values| {
            Arc::new(Int64Array::from(std::mem::replace(
                values,
                Vec::with_capacity(256),
            ))) as ArrayRef
        })
        .collect();
    let batch = RecordBatch::try_new(capture.schema().clone(), arrays)
        .map_err(|_| Error::Protocol("Postgres Arrow batch"))?;
    capture.push(&batch, context)
}
