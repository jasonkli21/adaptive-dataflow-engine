# Phase 14 streaming operator guide

This is a default-off, single-process Kafka source/progress foundation. It shares
ADE's native stateless Arrow filter/projection with bounded execution. It does
not admit a streaming `ade-physical::PlanV1`, run DataFusion, or change batch EOF.
See ADR-0021 and the Phase14 report for the measured support envelope.

## Build and broker contract

Use stable Rust with C/C++, CMake and OpenSSL development libraries:

```sh
# macOS Homebrew; on Linux install the corresponding OpenSSL development package.
export OPENSSL_DIR=/opt/homebrew/opt/openssl@3
export OPENSSL_ROOT_DIR="$OPENSSL_DIR"
cargo build -p ade-stream --features kafka --locked
```

Use one Kafka-compatible cluster and topic with 1–4 fixed partitions. All metadata
brokers must number at most four. The source reads metadata/configuration; it
never creates/alters/deletes a topic. Give it Describe/Read and DescribeConfigs
permissions. Auto topic creation, auto commit/store and offset reset are disabled.
Manual assignment uses exactly the supplied absolute offsets; no group subscribe,
rebalance, committed-offset lookup or implicit reset is performed.

**Resource admission requires `compression.type=uncompressed` and
`max.message.bytes<=32768` from topic creation throughout its entire retained
history.** The adapter verifies current effective settings before assignment and
once per second, but cannot retroactively prove history. Do not point it at a
previously compressed or mutable topic. Kafka consumer fetch/response limits alone
cannot bound native decompression. Trust the broker and operator to maintain this
contract; Byzantine brokers and undetected change-and-restore are excluded.

The expected cluster ID must be obtained independently from the broker's operator
or DescribeCluster API. The topic partition inventory is pinned for the session;
leader changes within that inventory are permitted. This client's public metadata
API does not expose a stable topic UUID/record leader epoch. An unchanged topic
incarnation/history is an operator requirement. Topic recreation or truncation
and regrowth entirely between observations may evade detection; no durable source
identity or recovery promise is made.

## Strict configuration and execution

The config must be a regular file; it is opened nonblocking so a FIFO is refused.
All fields below are required; unknown fields and duplicate JSON fields refuse.
The configuration is capped at16KiB. Partition entries must be unique and sorted.

```json
{
  "version": 1,
  "bootstrap": "127.0.0.1:19092",
  "expected_cluster_id": "REPLACE_WITH_ACTUAL_CLUSTER_ID",
  "topic": "ade-stream-input",
  "starts": [{"partition": 0, "offset": 0}, {"partition": 1, "offset": 0}],
  "security": {"mode": "loopback_dev"},
  "key_less_than": 10,
  "allowance_ms": 10,
  "payload_bytes": 4096,
  "client_queue_kib": 16,
  "sink_stall_ms": 90000,
  "timers": [
    {"kind": "processing", "deadline_ms": 1000},
    {"kind": "event", "deadline_ms": 10000}
  ]
}
```

`loopback_dev` is an explicit plaintext development exception: bootstrap must be
a numeric loopback IP:port and all advertised broker addresses must be loopback.
A separate SASL_SSL configuration accepts only PLAIN with verified CA/hostname:

```json
{"mode":"sasl_ssl","username_env":"ADE_KAFKA_USER","password_env":"ADE_KAFKA_PASSWORD","ca_path":"/absolute/path/to/ca.pem"}
```

The CA path must be absolute and refer to a regular file of at most1MiB.
Credential values are read from the environment, capped at4KiB each and never
serialized in diagnostics. Arbitrary client overrides, insecure TLS and remote
plaintext are refused. Broker error strings and malformed payloads are redacted.
Consult the report for the exact authentication tests actually witnessed.

Every Kafka value is strict bounded JSON:

```json
{"version":1,"key":4,"value_i64":-7,"event_time_ms":1000}
```

No nulls, unknown fields, alternate versions, tombstones, Kafka headers or keys
larger than4KiB are admitted. Invalid input stops the session; nothing is skipped.
Event time is signed epoch milliseconds independent of arrival time/offset. The
native UInt64 key filter retains `key<key_less_than`; the Arrow projection changes
column order to value/key/event time. Output receipts contain transformed values,
source session, partition, offset, event time and lateness. Filtered inputs emit
`projected:null` receipts. This makes the sink-confirmed progress of filters
explicit without inventing output data rows.

```sh
target/debug/ade-stream config.json > receipts.jsonl 2> trace.jsonl
# Explicit finite TEST stop, never Kafka EOF:
target/debug/ade-stream config.json --test-stop-records 100
# Or a test duration including admission:
target/debug/ade-stream config.json --test-stop-ms 5000
```

Without test flags the source remains live until SIGINT or failure. Idle polls
stay Pending and retain every assigned partition in global progress. Processing
timers use elapsed monotonic milliseconds since startup; event timers use signed
event-time deadlines and the published global minimum. Deadline comparisons are
inclusive. Timers can be delayed by a blocked sink; they never fire early and have
no window/state/sink effect. Registration IDs are session-local, ordered and never
reused. Event timers stay pending while any partition has an unknown watermark.

## Acknowledgment, stop and failure

A whole NDJSON receipt written to the output FD acknowledges a record. This is
**not fsync, external consumer acceptance, a durable checkpoint or exactly-once**.
A partial/failed write can leave a truncated line. A `--test-stop-ms` deadline at
a frame boundary stops cleanly; after any frame byte is written it fails as
`partial_output_frame` with byte counts. The unconfirmed record is discarded and
no later acknowledgment occurs. Preserve output/traces for diagnosis and choose
explicit replay offsets yourself. Replaying from a prior offset may duplicate
data. `next_offset_suggestion` is nondurable; losing output can invalidate even a
previously observed suggestion.

Seen/read and forwarded cursors are distinct. Increasing Kafka gaps (including
aborted transaction/control offsets under read_committed) are legal. Backwards
records, start/range drift, changed inventory/cluster, invalid schema, arithmetic
overflow, broker outage, failed output/diagnostics and expired sink stalls fail
closed. V1 never retries/reassigns/replays automatically. Restart a stopped broker
and explicitly start a new reader session; old timers/progress are not restored.

Watermarks are max successfully forwarded event time minus allowance, checked for
overflow and monotone. The frontier is unknown until every assigned partition has
a watermark, then their minimum. These are declared out-of-orderness policy bounds,
not proof Kafka cannot deliver older events. Late events are forwarded and marked.
No idle timeout excludes a partition and no stop emits a completion frontier.

Terminal statuses are STOPPED for explicit operator test limits or FAILED with
failure reason. SIGINT is cancellation/FAILED, not clean source EOF. SIGKILL has
no terminal trace. Success is emitted only after explicit consumer close. No
production service supervisor, remote sink publication or crash adoption exists.

## Bounds and diagnostics

ADE has no downstream channel: one record and one output frame are in flight.
Payload cap128..4096B (oversize detection retains at most cap+1), Arrow accounting
<=8KiB, output frame<=2KiB, conservative simultaneous managed in-flight<=16KiB,
config<=16KiB, diagnostics<=8KiB/frame, <=4 cursors. Timer capacity64 registrations
live/8KiB charged, lifetime1024 registrations; a timer node is conservatively
charged128B. Removing/firing never permits ID reuse or resets lifetime limits.

Client queue target1..1024KiB, queued.min.messages=1, per-partition fetch initial
16KiB, aggregate fetch32KiB, response cap64KiB; at most four brokers/partitions.
Queue targets are soft: separately bounded fetch responses may overshoot them.
Separate partition queues conservatively allow up to4*(target+64KiB) fetched
payload, plus protocol/native overhead. The read-only Admin client is a separate
bounded metadata/control domain; receive/statistics frames cap64KiB. Partition
queues are polled round-robin only with downstream demand. During a sink stall,
ADE stops polling partition data queues, control/statistics continue, the bounded
client prefetch queue fills and broker backlog grows. Client-level pause/resume
is deliberately unused after reproduced native destruction hangs.

Stats/queue peaks, broker backlog, current and confirmed cursors, watermark/frontier,
timers, PID/session, config digest, client version and failure/stop reason appear in
bounded NDJSON traces. No telemetry history is kept in the process. An external
trace file can grow without bound over days: its rotation/storage quota belongs
to the operator. Blocked diagnostics fail closed, rather than buffering logs.

These are managed queue/accounting bounds, **not process RSS**. Allocator, Arrow
metadata, TLS/client stacks, native protocol/control buffers and kernel pipe/TCP
buffers remain distinct. Trusted uncompressed broker batches bound codec inputs;
no arbitrary compression/RSS bound is claimed. Output and diagnostics use safe
nonblocking FDs, retrying output every10ms with a finite stall budget. Poll waiting
is<=100ms; metadata calls each have1000ms request budgets. OS file I/O and native
library scheduling/destruction are not hard real-time. The fault suite measures
local cancellation/teardown and must pass before bounded completion is claimed.

## Reproduce the real broker campaign

Download and verify the official Apache Kafka3.9.1 binary distribution separately;
no service download, paid/cloud provisioning or production mutation is performed
by the harness. It starts an isolated loopback broker, creates uniquely named
private fixture topics and stops only its owned processes. It retains every raw
command, producer ledger, output, progress/failure trace, source snapshot and hash.
It never deletes a topic on an existing broker.

```sh
python3 scripts/phase14/acceptance.py \
  --kafka-home /tmp/kafka_2.13-3.9.1 \
  --java-home /path/to/jdk17 \
  --output benchmarks/results/phase14/fresh-run
```

The harness requires a new output directory and at least30 seconds for the blocked
sink witness. Its Python oracle is independent of ADE's event parser/operators.
Unit tests require no broker; the network campaign is a local correctness gate,
not an historical release speedup or CI performance threshold.

## Verify the preserved and reviewed evidence

The original `docs/evidence/phase14/acceptance.tar.gz` bytes remain unchanged.
Because its old missing-partition config is malformed, verify that archive only
in qualified historical mode:

```sh
python3 scripts/phase14/verify_evidence.py --mode historical
```

For current acceptance, run the fresh campaign above, package its raw files with
`scripts/phase14/package_evidence.py`, then use strict current mode:

```sh
python3 scripts/phase14/package_evidence.py \
  --campaign benchmarks/results/phase14/fresh-run \
  --destination docs/evidence/phase14-review-remediation
python3 scripts/phase14/verify_evidence.py \
  --archive docs/evidence/phase14-review-remediation/acceptance.tar.gz \
  --manifest docs/evidence/phase14-review-remediation/manifest.json \
  --mode current
```

Strict mode checks every named negative case's reached failure class and trace,
poison-record cursors/output, and raw blocked-sink snapshots. It accepts bounded
prefix output for sink cancellation, source drift and process kill, while requiring
those sessions to remain failed or unreported rather than successful. Review
disposition and the source-pinned 2026-10-29 campaign are recorded in
`docs/reports/phase-14-review-remediation-2026-10-29.md`.
