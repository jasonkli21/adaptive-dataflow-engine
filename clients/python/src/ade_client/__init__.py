"""Client for ADE's bounded local Flight SQL service."""

import ipaddress
import json
import secrets
from dataclasses import asdict, dataclass

import pyarrow as pa
import pyarrow.flight as flight
from google.protobuf import any_pb2, descriptor_pb2, descriptor_pool, message_factory


def _sql_messages():
    # The subset of Apache Arrow FlightSql.proto used by this client.
    file = descriptor_pb2.FileDescriptorProto(
        name="ade_flightsql_subset.proto", package="arrow.flight.protocol.sql", syntax="proto3"
    )
    statement = file.message_type.add(name="CommandStatementQuery")
    statement.field.add(name="query", number=1, type=9, label=1)
    metadata = file.message_type.add(name="CommandGetSqlInfo")
    metadata.field.add(name="info", number=1, type=13, label=3)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file)
    return tuple(
        message_factory.GetMessageClass(pool.FindMessageTypeByName(f"{file.package}.{name}"))
        for name in ("CommandStatementQuery", "CommandGetSqlInfo")
    )


_Statement, _SqlInfo = _sql_messages()


def _descriptor(message):
    any_message = any_pb2.Any()
    any_message.Pack(message)
    return flight.FlightDescriptor.for_command(any_message.SerializeToString())


@dataclass(frozen=True)
class Function:
    id: str
    abi: int
    digest: str


@dataclass(frozen=True)
class Query:
    operations: tuple
    function: Function | None = None
    timeout_ms: int = 10_000
    table: str = "input"

    def descriptor(self):
        body = asdict(self) | {"version": 1, "target": "local"}
        encoded = json.dumps(body, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
        command = any_pb2.Any(type_url="ade.gateway/QueryRequestV1", value=encoded)
        return flight.FlightDescriptor.for_command(command.SerializeToString())


class QueryError(Exception):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


def _rpc(method, *args):
    try:
        return method(*args)
    except (pa.ArrowException, NotImplementedError, TimeoutError) as exc:
        # Flight's C++ bridge maps some gRPC codes to a general Arrow status.
        # The gateway's stable messages disambiguate those categories.
        message = str(exc)
        if (
            isinstance(exc, (flight.FlightTimedOutError, TimeoutError))
            or "query deadline" in message
            or (isinstance(exc, flight.FlightCancelledError) and "Timeout expired" in message)
        ):
            code = "DEADLINE_EXCEEDED"
        elif isinstance(exc, flight.FlightCancelledError):
            code = "CANCELLED"
        elif isinstance(exc, flight.FlightUnauthenticatedError):
            code = "UNAUTHENTICATED"
        elif isinstance(exc, (NotImplementedError, pa.ArrowNotImplementedError)) or any(
            value in message for value in ("Flight SQL operation unsupported", "query version or target", "function ABI")
        ):
            code = "UNIMPLEMENTED"
        elif any(value in message for value in ("ticket unknown or expired", "unknown table", "unknown function")):
            code = "NOT_FOUND"
        elif any(value in message for value in ("native execution failed", "source verification failed", "source path drift", "function identity mismatch", "function artifact drift")):
            code = "FAILED_PRECONDITION"
        elif any(value in message for value in ("native result cap", "native resource cap", "Flight result cap", "UDF encoded cap", "UDF input cap", "UDF input IPC cap", "query request cap", "SQL byte cap", "ticket capacity", "reader capacity", "query busy", "RESOURCE_EXHAUSTED")):
            code = "RESOURCE_EXHAUSTED"
        elif "UDF " in message:
            code = "FAILED_PRECONDITION"
        elif isinstance(exc, pa.ArrowInvalid) or "invalid query request" in message:
            code = "INVALID_ARGUMENT"
        elif isinstance(exc, flight.FlightUnavailableError):
            code = "UNAVAILABLE"
        else:
            code = "INTERNAL"
        raise QueryError(code, message) from exc


class Client:
    def __init__(self, host, port, token, *, timeout=11.0):
        address = ipaddress.ip_address(host)
        if not address.is_loopback:
            raise ValueError("numeric loopback required")
        self._flight = flight.FlightClient(f"grpc://{'[' + host + ']' if address.version == 6 else host}:{port}")
        self._options = flight.FlightCallOptions(timeout=timeout, headers=[
            (b"authorization", f"Bearer {token}".encode("ascii")),
            (b"x-ade-session", secrets.token_hex(32).encode("ascii")),
        ])

    def sql(self, statement):
        return self.fetch(self.submit_sql(statement))

    def submit_sql(self, statement):
        return self.submit(_descriptor(_Statement(query=statement)))

    def schema(self, statement):
        return _rpc(self._flight.get_schema, _descriptor(_Statement(query=statement)), self._options).schema

    def sql_info(self, ids=()):
        return self.fetch(self.submit(_descriptor(_SqlInfo(info=ids))))[0]

    def submit_plan(self, query):
        return self.submit(query.descriptor())

    def submit(self, descriptor):
        return _rpc(self._flight.get_flight_info, descriptor, self._options)

    def open_ticket(self, ticket):
        """Return PyArrow's FlightStreamReader, which supports cancel()."""
        return _rpc(self._flight.do_get, ticket, self._options)

    def fetch(self, info):
        if len(info.endpoints) != 1:
            raise QueryError("INTERNAL", "expected exactly one Flight endpoint")
        reader = self.open_ticket(info.endpoints[0].ticket)
        result = _rpc(reader.read_all)
        if result.schema != info.schema or result.num_rows != info.total_records:
            raise QueryError("INTERNAL", "result does not match FlightInfo")
        return result, json.loads(info.app_metadata)

    def close(self):
        self._flight.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
