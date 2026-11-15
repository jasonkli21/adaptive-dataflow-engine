# Bounded Flight SQL and Python batch functions

Phase 22 exposes a default-off, read-only Arrow Flight SQL gateway for one
operator-owned local EventV1 Parquet manifest. Apache PyArrow Flight and the
Java Flight SQL client connect to the same service. The gateway compiles
SQL through the ADE frontend and runs ADE's native local executor.
[Flight SQL protocol](https://arrow.apache.org/docs/format/FlightSql.html)

## Start a local gateway

Use an immutable manifest and Parquet files in an operator-controlled directory.
The gateway follows the existing Phase 11 `input` manifest and exact EventV1
schema, including field order, file digests and non-nullability. The manifest
itself must resolve beneath the source root. Register one trusted interpreter
and function file using absolute paths. Omit all three function arguments
to serve plain SQL and native plans only.

```sh
cargo build --locked -p ade-gateway --features flight-sql-v1
python3.10 -m venv /tmp/ade22-demo
. /tmp/ade22-demo/bin/activate
python -m pip install ./clients/python blake3==1.0.8
PYTHONPATH=scripts/phase22 python -c 'from fixtures import fixture; from pathlib import Path; fixture(Path("/tmp/ade22-input"))'
export ADE_GATEWAY_TOKEN="$(python -c 'import secrets; print(secrets.token_hex(32))')"
export ADE22_DIGEST="$(python -c 'import blake3; print(blake3.blake3(open("scripts/phase22/map_secondary.py","rb").read()).hexdigest())')"
target/debug/ade-gateway --listen 127.0.0.1:50051 \
  --root /tmp/ade22-input --manifest /tmp/ade22-input/manifest.json \
  --python /tmp/ade22-demo/bin/python \
  --script "$PWD/scripts/phase22/map_secondary.py" --digest "$ADE22_DIGEST"
```

Run the clients in a shell that has the same environment. Use numeric loopback,
a 64-lowercase-hex operator bearer, and one independent 64-lowercase-hex
session secret per client. Both SDKs generate the latter locally. Keep these
credentials out of logs and process arguments. Plaintext loopback assumes an
operator-trusted host and local network stack.

## Python

```python
import os
from ade_client import Client, Function, Query, QueryError

sql = "SELECT row_id, secondary FROM input WHERE secondary < 3"
operations = (
    {"op": "filter", "column": "secondary", "less_than": "3"},
    {"op": "project", "columns": ["row_id", "secondary"]},
)
function = Function("map_secondary", 1, os.environ["ADE22_DIGEST"])
with Client("127.0.0.1", 50051, os.environ["ADE_GATEWAY_TOKEN"]) as client:
    sql_rows, _ = client.sql(sql)
    native_rows, _ = client.fetch(client.submit_plan(Query(operations)))
    assert sql_rows.equals(native_rows)
    mapped, receipt = client.fetch(client.submit_plan(Query(operations, function)))
    expected = [(2**63 + i, i % 19 % 7) for i in range(521) if i % 19 % 7 < 3]
    assert list(zip(mapped["row_id"].to_pylist(), mapped["secondary"].to_pylist())) == expected
    assert receipt["worker"]["invocations"] == 3
    try:
        client.submit_plan(Query(operations, Function("unknown", 1, function.digest)))
    except QueryError as failure:
        assert failure.code == "NOT_FOUND"
    else:
        raise AssertionError("unknown function succeeded")
```

`Client` accepts a transport timeout. `Query.timeout_ms` limits native plan
execution. `open_ticket` returns PyArrow's cancellable FlightStreamReader.
A timed-out or disconnected submission cancels in-progress execution.
Phase 22 does not implement the Flight SQL CancelQuery action.

## Java

Build and run with OpenJDK 17, Maven 3.9.11 and Apache Arrow Flight SQL
19.0.0. The SDK uses a bounded allocator and closes Arrow streams through
try-with-resources. The example preserves large unsigned integers as decimal
strings and timestamp values as epoch milliseconds in its JSON output.

```sh
mvn -B -f clients/java/pom.xml compile dependency:build-classpath \
  -Dmdep.outputFile=target/classpath.txt
ade22_cp="clients/java/target/classes:$(cat clients/java/target/classpath.txt)"
java --add-opens=java.base/java.nio=ALL-UNNAMED -cp "$ade22_cp" \
  ade.Example 127.0.0.1 50051 sql 'SELECT row_id, secondary FROM input WHERE secondary < 3'
java --add-opens=java.base/java.nio=ALL-UNNAMED -cp "$ade22_cp" \
  ade.Example 127.0.0.1 50051 plan /path/to/request.json
```

## Language-neutral request

Standard Flight SQL uses CMD descriptors with protobuf Any wrapping
`CommandStatementQuery` or `CommandGetSqlInfo`. Statement GetSchema binds
without running the query. GetFlightInfo executes and returns one
`TicketStatementQuery` in its endpoint. DoGet redeems that ticket.
SQL Info item 10001 advertises the local request envelope and whether a
function is registered. Prepared statements, writes, transactions, remote
binds, Substrait, server-side explicit cancellation and generic catalog
enumeration are outside this surface.

For a native request set the Any type URL to `ade.gateway/QueryRequestV1` and
put UTF-8 JSON bytes in its `value` field. For example, replacing the digest
with the registered 64-character BLAKE3 hex string yields this request.

```json
{
  "version": 1,
  "target": "local",
  "table": "input",
  "timeout_ms": 10000,
  "function": {"id": "map_secondary", "abi": 1, "digest": "<registered digest>"},
  "operations": [
    {"op": "filter", "column": "secondary", "less_than": "3"},
    {"op": "project", "columns": ["row_id", "secondary"]}
  ]
}
```

| Field | Contract |
|---|---|
| `version`, `target`, `table` | Required. Exactly `1`, `local`, `input`. |
| `timeout_ms` | Required integer in 1–10,000. SQL uses 10 seconds. |
| `function` | Omit or set null for native execution. Otherwise supply exactly the registered ID, ABI and artifact digest, plus at least one downstream operation. |
| `filter` | A non-null UInt64 column and canonical decimal-string threshold in 0–18,446,744,073,709,551,615. |
| `project` | Ordered, unique existing column names. |

The server rejects unknown or duplicate fields, malformed UTF-8 and invalid
numeric encodings. An ordered schema describes names, native types and
nullability. The EventV1 types are UInt64, UInt32, Int64, Float64, UTF-8 and
timezone-free Timestamp(ms), all non-null. Arrow carries large integers and
timestamps directly. Unordered SQL retains its native bag semantics even if
a fixture happens to produce deterministic scan order. The diagnostics in
FlightInfo application metadata identify the query, native plan, source
manifest, function and execution attempt.

## Trusted ABI 1

The gateway launches one Python process group per pulled native scan batch,
with no shell, a scrubbed environment, isolated Python mode, and one fixed
operator-installed artifact. The registry hashes the Python executable and
checks the configured script digest at startup and query admission. The
bootstrap executes the checked script bytes and imports pinned PyArrow
24.0.0 from the operator-managed environment.

The child reads one request from stdin. Its 88-byte header holds the eight
bytes `ADEUDF01`, a zero-based little-endian UInt64 batch index, a
little-endian UInt32 row count, 64 ASCII hex bytes identifying the query
attempt and function, and a little-endian UInt32 payload length. The child
echoes those fields, sets the response payload length, and inserts a
little-endian UInt64 process high-water RSS observation before its payload.
Each payload contains one Arrow IPC V5 stream with exactly one schema, one
uncompressed record batch and an explicit end marker. No dictionaries,
trailing frames, nullable fields or schema metadata changes are admitted.
The adapter checks the decoded shape, row count, order and values of every
column except `secondary`. Verified output feeds the existing native
operators. There is no automatic retry.

This process boundary assumes trusted Python code and an immutable Python
installation. It provides fault supervision and byte caps. Linux applies
an address-space limit and both supported Unix builds apply a CPU limit.
The worker also watches its gateway parent. On macOS the RSS value is a
measurement, and neither the native managed ledger nor retained wire bytes
is a process RSS cap. Keep the source root immutable while the service runs.
Distributed, streaming, cyclic, transaction and recovery targets are
outside this contract. WASM isolation is deferred.

## Limits and cleanup

| Resource | Bound |
|---|---|
| Gateway connections, HTTP/2 streams per connection | 16, 8 |
| Native queries and active UDF children | One each |
| Admitted source | 32,768 rows and 64 MiB of Parquet files, plus native row-group checks |
| Request and UDF batch | 16 KiB SQL or JSON body, 256 rows, 1 MiB each encoded IPC frame and decoded batch |
| Managed native allocation | 16 MiB, prefetch and spill disabled |
| Completed result | 32,768 rows and 2 MiB native collected and encoded Flight budgets |
| Retention | 16 tickets for 15 seconds, up to 4 active result readers, zero result spool bytes |

The gateway builds the full result and completes source verification before
publishing its ticket. Expired, unknown and cross-session tickets refuse.
Valid repeat reads use the same retained frames. The 15-second expiry applies
to ticket redemption. An active DoGet retains its frames and reader slot until
completion or transport closure. The server configures a 120-second connection
age and 15-second grace period. A dropped DoGet ends that reader. Transport
failures can interrupt delivery of an already verified
result. A process restart discards all ticket state. ENOSPC on TMPDIR cannot
leave a partially published result because the bounded path uses no spool
or spill files. Service startup, file IO, and interpreter calls rely on
ordinary local OS behavior. Worker errors return redacted status messages.

## Reproduce the acceptance campaign

Build the gateway and the direct native probe from the same source. Install the
Python package and compile the Java client as shown above. The full campaign
requires an empty disposable filesystem mounted at a private mountpoint.
It fills up to 64 MiB on that filesystem to reach ENOSPC, then verifies that
the spill-disabled gateway can still serve the source from its separate root.
A 32 MiB Linux tmpfs or a 32 MiB macOS HFS+ disk image is sufficient.

```sh
cargo build --locked -p ade-gateway --features flight-sql-v1 \
  --bin ade-gateway --example phase22-native
PYTHONDONTWRITEBYTECODE=1 python scripts/phase22/campaign.py \
  --binary target/debug/ade-gateway --native target/debug/examples/phase22-native \
  --java "$(command -v java)" --java-classpath "$(cat clients/java/target/classpath.txt)" \
  --full-mount /path/to/disposable-mount --output /tmp/ade22-campaign
ade22_pin="$(python scripts/phase22/inventory.py /tmp/ade22-campaign)"
python scripts/phase22/verify.py --mode current-source --receipt-pin "$ade22_pin" \
  --repo "$PWD" --binary target/debug/ade-gateway --native target/debug/examples/phase22-native \
  /tmp/ade22-campaign
python scripts/phase22/tamper.py --receipt-pin "$ade22_pin" /tmp/ade22-campaign
```

Retain the printed seal digest independently of the candidate directory.
Use `historical-archive` to inspect an extracted archive with its recorded
seal digest. That mode checks the campaign semantics without attributing its
results to local executables. `--interop-only` runs the Linux CI correctness
subset without the release fault/benchmark campaign or ENOSPC mount.
