# ADR-0022 — Bounded keyed state and local checkpoints v1

Status: Accepted for implementation, 2026-10-29. Refines architecture §18,
INV-1/3/4/6/7/8; authorization is the current Phase15 user request.

The default-off `stateful` feature composes the existing `Engine` native Arrow
transform and single-owner demand loop. A separate strict stateful descriptor and
`ade-state-stream` binary leave StreamConfigV1 and PlanV1 unchanged. No frontend
runtime or alternate execution kernel is introduced.

Signed tumbling windows use checked Euclidean alignment. Global min/unknown
watermarks retain every assigned partition. Close when frontier >= end; arrivals
whose end <= the confirmed frontier receive explicit closed-window-late receipts
and never reopen state. This differs from the partition-watermark late diagnostic.
Output order is (window start,key). The logical output ID is job/key/start/end/version;
at-least-once sink replay after crash is expected and visible. Each output also
includes `payload_fingerprint=window-payload-v1:<sum_i64>:<count_u64>`. Kafka does
not define a total order across partitions, so different legal merges can emit the
same logical ID with different sum/count payloads. Consumers must compare the
fingerprint and reconcile conflicts; ID-only deduplication is unsafe. This bounded
contract is correct for the observed delivery trace, not order-independent replay.

One record mutates working state before its receipt, then confirms progress after
the complete receipt. Any error poisons the attempt; no checkpoint is allowed with
input or close output pending. The next demand turn drains one close record at a
time, confirms it and removes its accumulator. Only after closure obligations are
settled can a checkpoint freeze state and confirmed offsets together. No partition data polling occurs during snapshot. A stateful-only owned/joined
control poller (1MiB requested stack) drains native main-queue statistics/errors
throughout synchronous I/O. It never owns partition data, state or output; default
stateless polling remains unchanged. Diagnostics failure aborts before a new cut.

Hot entries occupy a sorted preallocated fixed-capacity vector (32 bytes/entry);
no map node/index allocation is hidden. Sorted fixed-record cold shard files
provide real eviction/read-through under disk quota including rewrite overlap.
Working files have per-record checksums and are private, session-specific and
lossy. They are never selected as recovery state. Stable BLAKE3(key little endian)
low five bits select 32 virtual shards; owner=shard%{1,2,4}. Each owner is a separate store instance with its own preallocated hot vector,
cold inventory and private directory. Restore redistributes shards into fresh owner stores;
this is logical local rescale, never live physical membership.

Full checkpoints stream canonical records into 32 immutable shard files. A bounded
manifest contains codec/hash/operator version, logical fingerprint (excluding
initial/resume offsets, session and resource/topology knobs), offsets/time progress,
late metrics, owner inventory, counts/sizes/digests and prior generation. Files and
directories are synced before generation rename, and before atomic current-pointer
replacement. Prior generations are retained. Current pointer binds manifest hash;
restore never scans for newest files or silently falls back from corrupt current.
Explicit restore may select a retained generation by UUID with its manifest hash.
Checkpoint root has an exclusive process lock; stale locks require operator removal
only after proving the old process dead. Retained generation/total-byte quotas fail
closed; no automatic deletion of prior evidence. Local filesystem sync semantics
are assumed; no disk-loss, remote HA or power-loss hardware guarantee is inferred.

All bytes, ownership, source/config fingerprint and progress are validated before
source assignment/output. Kafka then checks saved offsets against live retained
ranges and the existing trusted immutable/uncompressed topic contract. A new UUID
session is created; logical progress survives; transient processing timers do not.
Unsupported versions, identity drift, corruption, symlinks, unsafe roots, resource
exhaustion and ambiguous publication fail closed. No group offset commit,
exactly-once, automatic recovery, adaptive cadence, SQL or savepoint API.

The supplied plan asks for independent review. The user's stronger instruction
forbids subagents and requires all work in this session; this implementation uses
an explicit same-session self-review and records that independence limitation.

Public API checks were verified against [Rust rename](https://doc.rust-lang.org/std/fs/fn.rename.html),
[File sync_all](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_all) and
[rdkafka0.39 Consumer](https://docs.rs/rdkafka/0.39.0/rdkafka/consumer/trait.Consumer.html).
Atomic replacement and synchronization are separate operations; the local fixture
verifies process-crash visibility, not hardware power-loss behavior.

The native statistics enqueue behavior was inspected in pinned librdkafka2.12.1
`rd_kafka_stats_emit_all`: snapshots enter the application queue each interval.
An owned control pump avoids accumulating them during slow filesystem calls;
thread safety follows [librdkafka threads/callbacks](https://github.com/confluentinc/librdkafka/blob/v2.12.1/INTRODUCTION.md#threads-and-callbacks).

Record/time cadence publishes only after acknowledged input progress has changed
since the last cut. Filtered and closed-window-late receipts count as progress;
unchanged empty or restored-idle cuts do not create generations. The minimum
admitted checkpoint budget is 66,048 bytes (64 KiB staging reserve plus admission
headroom); larger state can still fail closed when retained generations plus a
new staging generation exceed the configured quota.

The shared operator-test time stop is successful only at a frame boundary. If
any bytes of an input or window NDJSON frame were written when the deadline fires,
the attempt fails with `partial_output_frame`, reports written/total byte counts,
and does not acknowledge the receipt/closure or publish a checkpoint. The
stateful terminal reports pending input and pending window output separately.

Kafka preserves per-partition order, not a total cross-partition merge order.
Window/late decisions are deterministic for the observed ordered delivery trace
and its confirmed frontier. A different merge of arbitrarily late records can
change whether a still-open window accepts them; restore does not journal the
uncheckpointed merge order. Stable output identity is not a promise of identical
payloads under different legal source interleavings. The equivalence witnesses
use causally released prefixes and compare the actual trace with an independent
oracle; no universal order-insensitive late-data result is claimed.
