use crate::{
    query::{Bound, INPUT_ROWS, MAX_TIME, QUERY_TYPE, Query, REQUEST_BYTES, RESULT_BYTES, expiry},
    worker::Registry,
};
use ade_exec::CancellationToken;
use ade_frontend::Catalog;
use ade_physical::Target;
use arrow::{datatypes::Schema, ipc::writer::IpcWriteOptions, record_batch::RecordBatch};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
    flight_descriptor::DescriptorType,
    flight_service_server::FlightService,
    sql::{
        Any, Command, ProstMessageExt, SqlInfo, TicketStatementQuery,
        metadata::{SqlInfoData, SqlInfoDataBuilder},
    },
    utils::batches_to_flight_data,
};
use futures::{Stream, stream};
use prost::Message;
use std::{
    collections::HashMap,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;
use tonic::{Request, Response, Status, Streaming};

const TICKET_SLOTS: usize = 16;
const TTL: Duration = Duration::from_secs(15);
type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;
type Rpc<T> = Result<Response<T>, Status>;

pub struct Gateway {
    catalog: Arc<Catalog>,
    registry: Option<Arc<Registry>>,
    root: PathBuf,
    authorization: blake3::Hash,
    queries: Arc<Semaphore>,
    readers: Arc<Semaphore>,
    tickets: Arc<Mutex<HashMap<[u8; 16], TicketEntry>>>,
    metadata: SqlInfoData,
}

struct TicketEntry {
    owner: blake3::Hash,
    expires: Instant,
    bytes: usize,
    frames: Arc<Vec<FlightData>>,
}

struct Materialized {
    info: FlightInfo,
    frames: Arc<Vec<FlightData>>,
    receipt: serde_json::Value,
}

fn internal() -> Status {
    Status::internal("gateway execution failure")
}

fn unsupported() -> Status {
    Status::unimplemented("Flight SQL operation unsupported")
}

fn ticket_missing() -> Status {
    Status::not_found("ticket unknown or expired")
}

fn hex_secret(value: &[u8]) -> bool {
    value.len() == 64
        && value
            .iter()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
}

struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl Gateway {
    pub fn new(
        catalog: Catalog,
        registry: Option<Registry>,
        root: PathBuf,
        secret: &str,
    ) -> Result<Self, Status> {
        if !hex_secret(secret.as_bytes()) {
            return Err(Status::invalid_argument(
                "operator token requires 64 lowercase hex characters",
            ));
        }
        let source = &catalog.native().bindings[0].source;
        if source
            .files
            .iter()
            .try_fold(0u64, |total, f| total.checked_add(f.rows))
            .is_none_or(|rows| rows > INPUT_ROWS)
            || source
                .files
                .iter()
                .try_fold(0u64, |total, f| total.checked_add(f.bytes))
                .is_none_or(|bytes| bytes > 64 * 1024 * 1024)
        {
            return Err(Status::resource_exhausted("gateway source cap"));
        }
        let mut metadata = SqlInfoDataBuilder::new();
        metadata.append(SqlInfo::FlightSqlServerName, "ADE");
        metadata.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
        metadata.append(SqlInfo::FlightSqlServerReadOnly, true);
        metadata.append(SqlInfo::FlightSqlServerSql, true);
        metadata.append(SqlInfo::FlightSqlServerSubstrait, false);
        metadata.append(SqlInfo::FlightSqlServerCancel, false);
        metadata.append(
            10001_u32,
            if registry.is_some() {
                "ade.gateway/QueryRequestV1;local;map_secondary/1"
            } else {
                "ade.gateway/QueryRequestV1;local"
            },
        );
        let tickets = Arc::new(Mutex::new(HashMap::<[u8; 16], TicketEntry>::new()));
        let cleanup = Arc::downgrade(&tickets);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let Some(tickets) = cleanup.upgrade() else {
                    return;
                };
                let Ok(mut entries) = tickets.lock() else {
                    return;
                };
                entries.retain(|_, entry| entry.expires > Instant::now());
            }
        });
        Ok(Self {
            catalog: Arc::new(catalog),
            registry: registry.map(Arc::new),
            root,
            authorization: blake3::hash(format!("Bearer {secret}").as_bytes()),
            queries: Arc::new(Semaphore::new(1)),
            readers: Arc::new(Semaphore::new(4)),
            tickets,
            metadata: metadata.build().map_err(|_| internal())?,
        })
    }

    fn session<T>(&self, request: &Request<T>) -> Result<blake3::Hash, Status> {
        let invalid = || Status::unauthenticated("gateway credential required");
        let metadata = request.metadata();
        if metadata.get_all("authorization").iter().count() != 1
            || metadata.get_all("x-ade-session").iter().count() != 1
        {
            return Err(invalid());
        }
        let auth = metadata
            .get("authorization")
            .ok_or_else(invalid)?
            .as_bytes();
        if !bool::from(
            blake3::hash(auth)
                .as_bytes()
                .ct_eq(self.authorization.as_bytes()),
        ) {
            return Err(invalid());
        }
        let session = metadata
            .get("x-ade-session")
            .ok_or_else(invalid)?
            .as_bytes();
        if !hex_secret(session) {
            return Err(invalid());
        }
        Ok(blake3::hash(session))
    }

    async fn bind(&self, command: Command, start: Instant) -> Result<Bound, Status> {
        match command {
            Command::CommandStatementQuery(query) => {
                if query.transaction_id.is_some() {
                    return Err(unsupported());
                }
                if query.query.len() > REQUEST_BYTES {
                    return Err(Status::resource_exhausted("SQL byte cap"));
                }
                let deadline = start + MAX_TIME;
                let plan = tokio::time::timeout_at(
                    deadline.into(),
                    self.catalog.plan_sql(&query.query, Target::Local),
                )
                .await
                .map_err(|_| expiry())?
                .map_err(|_| Status::invalid_argument("SQL admission refused"))?;
                Bound::new(plan, deadline, false)
            }
            Command::Unknown(any) if any.type_url == QUERY_TYPE => {
                Query::parse(&any.value)?.bind(&self.catalog, self.registry.as_deref(), start)
            }
            _ => Err(unsupported()),
        }
    }

    fn ensure_slot(&self) -> Result<(), Status> {
        let mut tickets = self.tickets.lock().map_err(|_| internal())?;
        tickets.retain(|_, entry| entry.expires > Instant::now());
        if tickets.len() >= TICKET_SLOTS {
            return Err(Status::resource_exhausted("ticket capacity"));
        }
        Ok(())
    }

    fn publish(
        &self,
        owner: blake3::Hash,
        mut result: Materialized,
        deadline: Instant,
    ) -> Rpc<FlightInfo> {
        let mut tickets = self.tickets.lock().map_err(|_| internal())?;
        let now = Instant::now();
        if now >= deadline {
            return Err(expiry());
        }
        tickets.retain(|_, entry| entry.expires > now);
        if tickets.len() >= TICKET_SLOTS {
            return Err(Status::resource_exhausted("ticket capacity"));
        }
        let id = *uuid::Uuid::new_v4().as_bytes();
        if tickets.contains_key(&id) {
            return Err(internal());
        }
        let ticket = TicketStatementQuery {
            statement_handle: id.to_vec().into(),
        };
        let expiration = SystemTime::now() + TTL;
        result.info.endpoint.push(
            FlightEndpoint::new()
                .with_ticket(Ticket::new(ticket.as_any().encode_to_vec()))
                .with_expiration_time(prost_types::Timestamp::from(expiration)),
        );
        let bytes = usize::try_from(result.info.total_bytes).map_err(|_| internal())?;
        result.receipt["retention"] = serde_json::json!({
            "ticket_count": tickets.len() + 1,
            "ticket_wire_bytes": bytes + tickets.values().map(|entry| entry.bytes).sum::<usize>(),
            "spool_bytes": 0,
        });
        result.info.app_metadata = result.receipt.to_string().into();
        tickets.insert(
            id,
            TicketEntry {
                owner,
                expires: now + TTL,
                bytes,
                frames: result.frames,
            },
        );
        Ok(Response::new(result.info))
    }
}

fn command(descriptor: &FlightDescriptor) -> Result<Command, Status> {
    let invalid = || Status::invalid_argument("invalid Flight SQL descriptor");
    if descriptor.r#type() != DescriptorType::Cmd || !descriptor.path.is_empty() {
        return Err(invalid());
    }
    let message = Any::decode(descriptor.cmd.clone()).map_err(|_| invalid())?;
    Command::try_from(message).map_err(|_| invalid())
}

fn materialize(
    descriptor: FlightDescriptor,
    schema: &Schema,
    batches: &[RecordBatch],
    mut receipt: serde_json::Value,
) -> Result<Materialized, Status> {
    if batches.iter().any(|b| b.schema().as_ref() != schema) {
        return Err(internal());
    }
    let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    let frames = batches_to_flight_data(schema, batches.to_vec()).map_err(|_| internal())?;
    let mut bytes = 0usize;
    let mut digest = blake3::Hasher::new();
    for frame in &frames {
        let frame = frame.encode_to_vec();
        bytes += frame.len();
        if bytes > RESULT_BYTES {
            return Err(Status::resource_exhausted("Flight result cap"));
        }
        digest.update(&(frame.len() as u64).to_le_bytes());
        digest.update(&frame);
    }
    receipt["result"] = serde_json::json!({
        "rows": rows, "flight_bytes": bytes, "flight_digest": digest.finalize().to_hex().as_str()
    });
    let info = FlightInfo::new()
        .try_with_schema(schema)
        .map_err(|_| internal())?
        .with_descriptor(descriptor)
        .with_total_records(i64::try_from(rows).map_err(|_| internal())?)
        .with_total_bytes(i64::try_from(bytes).map_err(|_| internal())?)
        .with_ordered(true);
    Ok(Materialized {
        info,
        frames: Arc::new(frames),
        receipt,
    })
}

#[tonic::async_trait]
impl FlightService for Gateway {
    type HandshakeStream = ResponseStream<HandshakeResponse>;
    type ListFlightsStream = ResponseStream<FlightInfo>;
    type DoGetStream = ResponseStream<FlightData>;
    type DoPutStream = ResponseStream<PutResult>;
    type DoExchangeStream = ResponseStream<FlightData>;
    type DoActionStream = ResponseStream<arrow_flight::Result>;
    type ListActionsStream = ResponseStream<ActionType>;

    async fn get_flight_info(&self, request: Request<FlightDescriptor>) -> Rpc<FlightInfo> {
        let start = Instant::now();
        let owner = self.session(&request)?;
        let permit = self
            .queries
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("query busy"))?;
        self.ensure_slot()?;
        let descriptor = request.into_inner();
        let command = command(&descriptor)?;
        if let Command::CommandGetSqlInfo(info) = command {
            if info.info.len() > 64 {
                return Err(Status::resource_exhausted("SQL info selector cap"));
            }
            let batch = info
                .into_builder(&self.metadata)
                .build()
                .map_err(|_| internal())?;
            let result = materialize(
                descriptor,
                &batch.schema(),
                &[batch],
                serde_json::json!({"metadata":1}),
            )?;
            return self.publish(owner, result, start + MAX_TIME);
        }
        let bound = self.bind(command, start).await?;
        let deadline = bound.deadline;
        let catalog = Arc::clone(&self.catalog);
        let root = self.root.clone();
        let registry = self.registry.clone();
        let cancellation = CancelOnDrop(CancellationToken::default());
        let token = cancellation.0.clone();
        let task = tokio::task::spawn_blocking(move || {
            let schema = bound.schema();
            let (output, receipt) = bound.execute(&catalog, &root, registry.as_deref(), token)?;
            let result = materialize(descriptor, &schema, &output.batches, receipt)?;
            Ok::<_, Status>((result, permit))
        });
        let (result, _permit) = tokio::time::timeout_at(deadline.into(), task)
            .await
            .map_err(|_| expiry())?
            .map_err(|_| internal())??;
        self.publish(owner, result, deadline)
    }

    async fn get_schema(&self, request: Request<FlightDescriptor>) -> Rpc<SchemaResult> {
        self.session(&request)?;
        let _permit = self
            .queries
            .try_acquire()
            .map_err(|_| Status::resource_exhausted("query busy"))?;
        let command = command(request.get_ref())?;
        let schema = if matches!(command, Command::CommandGetSqlInfo(_)) {
            self.metadata.schema()
        } else {
            self.bind(command, Instant::now()).await?.schema()
        };
        SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map(Response::new)
            .map_err(|_| internal())
    }

    async fn do_get(&self, request: Request<Ticket>) -> Rpc<Self::DoGetStream> {
        let session = self.session(&request)?;
        let permit = self
            .readers
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("reader capacity"))?;
        let any = Any::decode(request.into_inner().ticket).map_err(|_| ticket_missing())?;
        let Command::TicketStatementQuery(ticket) =
            Command::try_from(any).map_err(|_| ticket_missing())?
        else {
            return Err(ticket_missing());
        };
        let id: [u8; 16] = ticket
            .statement_handle
            .as_ref()
            .try_into()
            .map_err(|_| ticket_missing())?;
        let frames = {
            let entries = self.tickets.lock().map_err(|_| internal())?;
            let ticket = entries
                .get(&id)
                .filter(|t| {
                    t.expires > Instant::now()
                        && bool::from(t.owner.as_bytes().ct_eq(session.as_bytes()))
                })
                .ok_or_else(ticket_missing)?;
            Arc::clone(&ticket.frames)
        };
        let read = stream::iter((0..frames.len()).map(move |index| {
            let _permit = &permit;
            Ok(frames[index].clone())
        }));
        Ok(Response::new(Box::pin(read)))
    }

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Rpc<Self::HandshakeStream> {
        Err(unsupported())
    }

    async fn list_flights(&self, _: Request<Criteria>) -> Rpc<Self::ListFlightsStream> {
        Err(unsupported())
    }

    async fn poll_flight_info(&self, _: Request<FlightDescriptor>) -> Rpc<PollInfo> {
        Err(unsupported())
    }

    async fn do_put(&self, _: Request<Streaming<FlightData>>) -> Rpc<Self::DoPutStream> {
        Err(unsupported())
    }

    async fn do_exchange(&self, _: Request<Streaming<FlightData>>) -> Rpc<Self::DoExchangeStream> {
        Err(unsupported())
    }

    async fn do_action(&self, _: Request<Action>) -> Rpc<Self::DoActionStream> {
        Err(unsupported())
    }

    async fn list_actions(&self, _: Request<Empty>) -> Rpc<Self::ListActionsStream> {
        Ok(Response::new(Box::pin(stream::empty())))
    }
}
