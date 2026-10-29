# ADR-0021 — Fixed Kafka source and progress v1

Status: Accepted; bounded v1 evidence revalidated after independent review remediation, 2026-10-29 Pacific.
Baseline: 9fc6e35643b2d758ed5a06d00c4d4896f8ab2601.

Refines architecture §§3.5,5.3,7–9,18,21–22 and INV1–10. The user's explicit
Phase14 request authorizes this bounded slice. Legacy PlanV1 and batch EOF,
protocols, defaults, oracle and Phase10–13 evidence remain unchanged.

## Ownership and state machines

A real `ade-stream` crate owns unbounded source polling, admission, cursors,
progress, timers and demand. It calls shared ade-exec stateless Arrow operators;
it is not a parallel execution kernel. The new strict `StreamConfigV1` is separate
from bounded physical PlanV1. Only the `kafka` feature enables its CLI/client.

One topic, 1–4 explicitly fixed partitions, absolute offsets, one session UUID.
Manual assign; never subscribe, commit or store offsets. Auto commit/store and
auto topic creation disabled; offset reset is error. Admission verifies cluster,
full topic partition inventory, watermarks and start range before output. Metadata
is rechecked during operation. Partition leaders may move; missing partitions,
changed cluster/inventory and retention/truncation refuse. Kafka client metadata
cannot prove topic incarnation or an undetected truncate-and-regrow between
observations; immutable topic incarnation/history during a session is an explicit
operator assumption. No claim of stable topic UUID or leader-epoch validation.

Lifecycle: admission → Running(Data | Pending) → StoppedByOperator or Failed.
Empty poll is Pending forever. One decoded record/batch and one pending sink frame
are allowed; no downstream channel (depth zero). Stop polling partition data queues while sink demand is absent; resume only
after acknowledgment. Native prefetch is allowed only within its finite queue
and response bounds; main control/statistics polling continues. Client-level
pause/resume was rejected after reproducible native destruction hangs (retained
failed candidates); the demand gate preserves queued messages and clean teardown. On cancellation discard
unacknowledged input; no detached worker or late acknowledgment. Filtered records
emit explicit sink receipts before progress advances, including event time.
Seen cursor advances on source delivery; forwarded cursor advances only after the
whole NDJSON receipt is written. Legal increasing holes are accepted. Backward,
foreign session/partition, overflow and malformed payloads fail. Restart suggestions
are diagnostics; output may be partially written and replayed on manual restart.

## Time and timers

Per partition: published watermark = max forwarded event_time_ms minus nonnegative
allowance, checked before output. Negative times are valid. Global frontier is
unknown until all assigned partitions have a bound, then their minimum. Published
bounds never decrease. Older events are forwarded with late=true; these watermarks
are a declared policy, not proof no older record can arrive. Idle never advances
progress or excludes a partition. No completion frontier is emitted.

Processing deadlines use injectable monotonic elapsed milliseconds; event deadlines
compare inclusively with published global frontier. Registration sequence breaks
ties after deadline and kind. Fixed count/byte and lifetime-registration caps mean
no unbounded fired-ID tombstones. IDs cannot be reused. Cancellation clears timers;
firing is at most once per session. Timer traces have no state/window/sink effect
and no persistence. A blocked sink can delay timer observation, never fire early.

## Resource and cancellation domains

Payload ≤4KiB; one-row Arrow input/filtered/projected batch; one ≤2KiB sink frame;
one input in flight; no channel; bounded config/diagnostic frames and ≤4 cursors.
Kafka topics must have immutable uncompressed history and effective
compression.type=uncompressed/max.message.bytes<=32768; current configuration is
verified via bounded read-only DescribeConfigs before assignment and periodically.
This prevents admitting unbounded native decompression under a compressed response
cap; history/change-and-restore remain trusted operator assumptions.
Kafka fetch queue has explicit KiB and message targets, fetch/response byte caps,
request timeouts and bounded metadata. librdkafka queue targets can overshoot by
one fetched response per broker; admission caps brokers and the receive cap bounds
that overshoot. These domains are documented separately, never as an RSS cap.
Native/client/allocator, compression, TLS and kernel buffers remain distinct.

Poll ≤100ms; metadata RPCs have finite timeouts; output is nonblocking with 10ms
cooperative retries, finite stall deadline and signal token. No polling for data
while sink stalls. Diagnostics are also bounded/nonblocking; inability to emit
telemetry fails closed. Regular filesystem writes and library teardown can retain
OS scheduling/I/O latency; no hard real-time guarantee.

## Supported/refused

Support: real loopback Kafka plaintext only with explicit development flag, or
SASL_SSL (PLAIN) with certificate/hostname verification and environment credentials;
strict version1 events; key<threshold filter; projection reorders key/value/time;
NDJSON receipts; fixed source progress; bounded timers; manual restart.
Refuse: consumer groups/rebalance, other connectors, schema evolution, SQL,
joins/windows/keyed state, checkpoints, automatic recovery, transactional sinks,
exactly-once, durable timers, general frontiers, HA/cloud and physical elasticity.
Non-loopback plaintext is refused. No arbitrary client-property escape hatch.

## Proof obligations

G0–G9 in the Phase14 plan remain mandatory, including real separate broker,
independent producer ledger, legal gap/replay, quiet partition, timers, 30–60s
blocked sink/backlog/resume, fault/security tests and legacy checks. Missing proofs
mean PARTIAL. No timing benefit is required or historical evidence reclassified.
