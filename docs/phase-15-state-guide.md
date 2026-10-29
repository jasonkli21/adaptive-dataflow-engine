# Phase 15 local state operator guide

Build the separate default-off stateful binary:

```sh
export OPENSSL_DIR=/opt/homebrew/opt/openssl@3
export OPENSSL_ROOT_DIR="$OPENSSL_DIR"
cargo build -p ade-stream --features stateful --locked
```

The original `ade-stream` CLI/config, PlanV1, batch/ops/remote contracts and all
historical evidence retain their meanings. This path uses the existing native
Arrow filter/projection and single-owner source/sink demand pump.

Create two **canonical absolute**, separate directories with mode0700. On macOS,
resolve `/tmp` to `/private/tmp` before configuring roots. Parent symlinks,
relative paths, `..`, nonprivate roots/files and linked data files refuse.

```json
{
  "version": 1,
  "job_id": "bd53c668-f786-4529-93c2-2fd53d854d04",
  "source": {
    "version": 1,
    "bootstrap": "127.0.0.1:19092",
    "expected_cluster_id": "REPLACE_FROM_OPERATOR",
    "topic": "private-state-input",
    "starts": [{"partition":0,"offset":0},{"partition":1,"offset":0}],
    "security": {"mode":"loopback_dev"},
    "key_less_than": 1000,
    "allowance_ms": 10,
    "payload_bytes": 4096,
    "client_queue_kib": 16,
    "sink_stall_ms": 90000,
    "timers": []
  },
  "topic_partitions": [0,1],
  "operator": "tumbling_sum_count_v1",
  "window_ms": 100,
  "late_policy": "receipt_drop_closed_v1",
  "hash_version": 1,
  "owners": 1,
  "hot_bytes": 384,
  "cold_bytes": 1048576,
  "checkpoint_bytes": 16777216,
  "max_generations": 64,
  "checkpoint_every_records": 10000,
  "checkpoint_every_ms": 60000,
  "working_root": "/private/tmp/ade-state-work",
  "checkpoint_root": "/private/tmp/ade-state-checkpoints"
}
```

All fields are required, unknown/duplicate fields refuse, descriptor<=16KiB.
`topic_partitions` pins the entire topic inventory, including any unassigned
partitions; `source.starts` is the explicitly assigned inventory. Kafka admission
and ongoing checks retain the [Phase14 source/security contract](phase-14-streaming-guide.md):
trusted immutable topic incarnation/history, uncompressed<=32KiB batches, manual
assignment, no group offset commit/reset, bounded client queues. Topic UUID and
undetected truncate-and-regrow remain operator assumptions. SASL_SSL uses the
same verified TLS/environment-credential configuration as Phase14.

Run or manually restore:

```sh
target/debug/ade-state-stream state.json > output.jsonl 2> trace.jsonl
target/debug/ade-state-stream state.json --restore > replay.jsonl 2> restore-trace.jsonl
# Finite fixture stops, cumulative across a checkpoint lineage:
target/debug/ade-state-stream state.json --restore --test-stop-records 200
```

Restore takes **all saved next offsets**, overriding configured initial offsets.
It verifies every selected checkpoint byte, semantic identity, state ownership and
logical progress before rebuilding working state, then verifies live Kafka ranges
and cluster/topic/config before output. A fresh session UUID fences stale input.
Changing owners1/2/4 redistributes stable virtual shards into separate local owner
directories. Owner/resource/cadence/endpoint knobs may change; job, schema,
operator, window, late/hash policy, source cluster/topic/inventory, filter and
allowance must match. This is logical local restore rescale. It does not launch,
move or elastically replace physical workers.

Signed event times are epoch milliseconds. Windows are `[floor(t/width)*width,end)`;
checked overflow refuses. Sum is checked Int64; count is checked UInt64. Source
partitions share the same key/window accumulator. Global frontier is unknown until
every assigned partition has a sink-confirmed watermark, then their minimum.
Quiet/idle partitions never complete time. Close when frontier>=end; output order
is `(window_start,key)`. A later record with end<=frontier emits
`closed_window_late:true` and never reopens a window. Partition-watermark `late`
is a separate diagnostic; out-of-order records in open windows still update.
Filtered inputs retain explicit receipts and time progress.

Output is NDJSON: `type:input` wraps the original receipt and the closed-window
late decision. `type:window` carries sum/count, logical ID
`job:key:start:end:1`, and `payload_fingerprint` in the canonical form
`window-payload-v1:<sum_i64>:<count_u64>`. Whole-frame local FD writes confirm
delivery; this is not an external transactional acknowledgment. A crash after
output but before checkpoint promotion can replay the same ID and fingerprint.

Kafka preserves order within each partition but does not define one total order
across partitions. A different legal merge can therefore close a window before a
late record arrives, producing the same logical ID with different sum/count and
fingerprint values. The contract is trace-relative. Consumers must compare the
fingerprint and reconcile conflicting payloads; deduplicating by `output_id`
alone can silently keep the wrong aggregate. Exactly-once and order-independent
replay are not provided. Equivalence evidence checks each observed delivery order.

`--test-stop-ms` can end successfully only before any byte of the current frame is
written. If time expires after a partial input or window frame, the process fails
with `failure_class: partial_output_frame`; terminal diagnostics include the
written and total frame bytes plus `pending_input` and
`pending_window_output`. The receipt/closure remains unconfirmed and no checkpoint
is published. This shared runtime rule also applies to the stateless Phase 14 CLI.

Checkpoint cadence is fixed by records or elapsed milliseconds, observed only at
quiescent turns after acknowledged input progress changed since the last cut.
Filtered and closed-window-late receipts count as changes. Empty startup and idle
restored cuts are not republished periodically, so unchanged state does not burn
generations. Demand pauses; pending receipt and one-at-a-time window closure
frames finish before serialization. The manifest records confirmed next offsets,
max times/watermarks/min-or-unknown frontier, late counts and open accumulators.
Open entries encode their event-time deadlines; no overdue closure is permitted
in a published generation. No monotonic-clock processing deadline is restored.
Source ranges/config are checked before snapshot and before pointer promotion.
A stateful-only owned control poller drains native statistics/errors while file
I/O blocks the demand owner. It never polls partition data or mutates state; its
requested1MiB stack is a separate native/thread domain. Shutdown joins it before
closing/dropping the consumer. Stateless polling remains unchanged.

Generations contain32 sorted fixed64-byte shard files (32-byte little-endian
record plus BLAKE3 checksum), a strict<=32KiB manifest, and an atomic<=512-byte
`CURRENT` pointer binding the manifest digest. State files, manifest, staging and
root directories and pointer are synced in publication order. Prior generations
remain immutable; partial staging is never selected. A corrupt current generation
fails rather than silently selecting older state. Local filesystem sync semantics
are the durability envelope; disk loss/power-loss hardware behavior, HA and
remote durability are unproved.

A root `LOCK` excludes concurrent writers. SIGKILL can leave this lock plus lossy
session working directories and unpublished staging. Prove the old process is
**dead** before removing its `LOCK` and deleting that attempt’s orphan session
directory under `working_root`; never adopt working files. A nonempty working root
refuses a new attempt, so crash garbage cannot bypass its disk quota. Restore from
`CURRENT`. To deliberately use a retained prior cut, an operator must select its
UUID and manifest hash by replacing `CURRENT` atomically under offline exclusive
control; expect replay duplicates. There is no savepoint/upgrade API. Normal exit
cleans owned working state and removes the lock; cleanup failure may leave private
garbage, never a new recovery selection. The process retains no cleanup queue.

`max_generations`2..64 and `checkpoint_bytes`66,048..256MiB bound retained plus staging
checkpoint storage, including a conservative64KiB metadata/pointer reserve per
promotion. The minimum can publish an empty or one-entry first generation; later
promotions also need room for retained generations and the new staging cut. No
automatic GC deletes recovery history. Exhaustion fails closed;
operators can remove unselected staging/old generations offline while preserving
CURRENT and at least one verified prior generation. An indefinitely running
service needs explicit storage/trace rotation. This bounded research path can
refuse when its finite storage fills.

Hot state preallocates a fixed-capacity sorted vector, charging32B per entry
slot before allocation (total cap32B*owners..16MiB); each owner has its own
separate hot allocation and cold inventory/directory. These vectors never grow
beyond their combined configured capacity. Cold disk cap128B..128MiB includes old+replacement
rewrite overlap. Fixed-record scans have no cardinality-sized index; eviction and
read-through are real, including repeated updates. One close record/frame, no
output queue,<=2KiB frames,<=4 source cursors,4KiB charged I/O scratch and128KiB
charged checkpoint metadata are separate managed domains. Metrics expose charged
peaks, evictions, hot/cold hits, windows and retained checkpoint reservation.
They are not RSS or filesystem-allocation/page-cache caps. Linear cold scans and
full snapshots intentionally favor simple bounded correctness over throughput.

Any source/sink/diagnostic/storage failure poisons the attempt; only explicit
restore of a verified generation may resume. Disk quota/full/partial/corrupt
working I/O never becomes a checkpoint. Polls and sink retries remain cooperative;
SIGINT cancels and fails, SIGKILL gives no terminal success. Local regular-file
I/O/fsync and native teardown are subject to OS latency, not hard real-time bounds.

Reproduce using a separately installed verified Apache Kafka3.9.1/JDK17:

```sh
python3 scripts/phase15/acceptance.py \
  --kafka-home /tmp/kafka_2.13-3.9.1 \
  --java-home /path/to/jdk17 \
  --output benchmarks/results/phase15/new-campaign
python3 scripts/phase15/verify_evidence.py --campaign benchmarks/results/phase15/new-campaign
python3 scripts/phase15/package_evidence.py \
  --campaign benchmarks/results/phase15/new-campaign \
  --destination docs/evidence/phase15-new
python3 scripts/phase15/verify_evidence.py \
  --archive docs/evidence/phase15-new/acceptance.tar.gz \
  --manifest docs/evidence/phase15-new/manifest.json
```

The campaign creates a fresh private loopback broker and only kills its owned
processes. It makes no production/cloud changes. Offline replay needs Python3
standard library only. It verifies outer SHA256 inventories, exact source archive,
producer ledger, ordered native receipts/windows, snapshot state/progress, crash
selection and every named negative contract. Rust/process tests separately check
the inner BLAKE3 checksums. Authored PASS labels are insufficient.

Verify the accepted immutable package and repeat its checksum-consistent attacks:

```sh
python3 scripts/phase15/verify_evidence.py \
  --archive docs/evidence/phase15/acceptance.tar.gz \
  --manifest docs/evidence/phase15/manifest.json
python3 docs/evidence/phase15/tamper-checks.py
```

The tamper command uses temporary copies; it leaves the accepted archive intact.
