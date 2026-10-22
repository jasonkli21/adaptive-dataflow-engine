# ADR-0015: Bounded runtime key-shard repartitioning v1

Status: Implemented candidate; Phase8 empirical exit FAILED on primary two-host gates. See report.
Date: 2026-10-21. Baseline: f66addb815deaa8accb2529124b713a8efd85c5e.

## Scope and architecture reconciliation

Refines architecture §§6,9,11–17,22,28–31: a running prepared job probes real
source data, then commits one immutable routing epoch before exchange. This
implements the PRD Phase 8 bounded-batch interpretation, not mid-exchange epoch
switching. No producer-completion or shuffle-materialization barrier is added.
The existing local partial aggregate is blocking independently of this decision.
The independent initial review accepts this interpretation with that qualification.

48 virtual shards use hash_v1(key)%48. Static ownership shard%N exactly preserves
hash_v1(key)%N for N=2,3,4. Layout v1 binds hash version, worker count and all
48 owners with a domain-separated BLAKE3 digest; active finals are derived from
ownership, never independently trusted. Original physical transfer IDs and N×N
terminals remain. Inactive sinks drain and verify zero data without running an
aggregate or emitting gather output. Gather commits exactly the active set.

## Observation and policy

The bounded prefix probe reads at most four files, 4096 rows/file, 16384 rows per
producer, counting distinct sampled keys per producer/shard. It is a biased
post-partial-output proxy, not raw hot-key frequency or full cardinality. A
single occupied virtual shard may contain multiple distinct keys; v1 does not
refine that fixed 48-shard ownership domain. Sample quality affects speed only.
Defaults are static; sampled_v1 is explicit. A deterministic descending-load
greedy assignment splits a heavy original hash bucket only with >=20% predicted
maximum-load gain and >=256 sampled distinct contributions. Tiny total
contribution <=64 coalesces to one real final task only when every assigned source
row is observed; otherwise the planner retains static ownership and records
`insufficient_sample_coverage`. A single occupied shard is reported separately
from a tiny-key coalesce; this reason does not claim that the shard contains one
distinct key.
Sample limits and thresholds are typed and strictly validated. Metadata is
O(48*N + capped sampled keys + files), never per segment.

Prepare -> bounded probe RPCs -> pure decision -> layout ACKs from all endpoints
-> producers and active finals plus empty-sink verifiers concurrently. Every
worker rejects producer/final/exchange before layout installation. Duplicate
probe/commit and stale identities fail closed. One shared coordinator stage
deadline bounds RPCs; cancellation propagates to probes between local reads.
Normal local filesystem reads share the existing scan's OS I/O limitations;
these are not claimed to be interruptible provider operations or an RSS cap.
Probe rereads and elapsed time are reported separately, with decoded rows and
logical file-size proxy distinguished from physical device traffic.

## Identity and recovery

One layout remains pinned through original production, terminal manifests,
complete-copy facts/receipts, recovery plan and replay. Digest fencing supplements,
never replaces, execution UUID, logical IDs, attempts and content fingerprints.
Recovery remains one post-producer final-worker loss, one quiescent surviving host,
all original terminals, full immutable source closures, exact fingerprint replay,
and attempt-safe gather. Inactive sink failure, producer loss, multiple loss,
partial commit, drift, poisoned storage and episode failure cancel the job.
All original retention and cost policies remain independently selectable.
The initial Phase 8 control schema advanced ADETCP06 -> ADETCP07; the review
remediation adds source-coverage evidence and advances the current control magic
to ADETCP08. Exchange wire v2/hash v1/result schema v1 remain. Complete object
namespace/receipt version remain unchanged with an added optional topology digest
in the private control schema.

## Review remediation — source coverage and bounded shard semantics

The observation now carries `source_complete`, set only when the probe has
processed every assigned file and its row count matches all row groups in those
files. The planner checks complete coverage before applying the tiny coalesce
rule. This fixes the branch-order failure for one-key and empty inputs, while
preventing a short prefix from funneling a large unseen suffix to one final.
Incomplete tiny observations produce `insufficient_sample_coverage` and retain
the static mapping.

The fixed 48-shard Phase 8 boundary is retained. When sampled load occupies one
shard, the decision reason is `one_occupied_shard`; it does not infer a single
distinct key. The known keys 38, 103, 160, 175, 209, 363, 481, 537, 575, 634, 656,
and 666 all occupy shard zero and remain together for 2–4 workers. This remains a
documented supported-domain limitation pending a separately planned finer
routing domain. The source-prefix probe is pre-exchange planning: original
partial aggregation begins after layout commitment, and no mid-execution
topology transition is claimed.

## Predeclared empirical exit

Before measurement: seed 42, four files, 1,000,000 rows, 262144 distinct key domain,
8192-row groups, uniform, engineered distinct-key hash-bucket skew and raw hot-key
negative control. 2 workers primary; 4-worker skew confirmation. 32 MiB task,
1 MiB exchange buffer, prefetch 1, spill delivery, one warmup and three isolated
measured iterations. Compare identical fixtures/config except partition_policy.
Every child must match independent oracle and dataset identity. Severe-skew
median maximum exclusive final operator work must improve >=20%, median job
latency must not increase; uniform latency <=1.30x static. Retain failures and
negative controls; never change gates after measuring. Operator work excludes
source pulls (aggregate build starts its work timer after next_batch), includes
spill processing; record wall and source waiting separately. Clean release source
identity and raw artifacts must be recorded. Timing stays outside CI.

No Phase 9, hot-key salting, worker elasticity or broader recovery is authorized.

## Compatibility and failure matrix

| Topology | Hosts | Delivery | Snapshot | Normal | One active final loss after producers |
|---|---|---|---|---|---|
| static | 2/3/4 | all five static modes, threshold_v1 | optional | supported, unchanged routing | replay with replayable snapshot |
| static | 2/3/4 | expected_cost_v1 | required replayable | supported | verified restore/recompute |
| sampled_v1 | 2/3/4 | all five static modes, threshold_v1 | optional | supported, split/coalesce/no-benefit | pinned-layout replay with replayable snapshot |
| sampled_v1 | 2/3/4 | expected_cost_v1 | required replayable | supported | pinned receipt restore/recompute |

Normal tests cover every delivery mechanism and host count; real dynamic loss
coverage is explicitly reported separately. No snapshot/nonreplayable snapshot
refuses recovery. Inactive loss, producer loss, multiple losses, unsupported
attempts, invalid/stale layout or transfer, partial ACK and poisoned replay fail
the entire job. No automatic static fallback. Research loss targeting an inactive
destination is rejected before dispatch. Zero-data transfers still complete at
every physical destination. The recovery host may be an inactive destination
whose empty-input verification and producer are both complete; it runs only the
lost logical final under attempt 1. Reused partitions list only successful active
finals, not idle sinks.

Probe provenance is assigned snapshot paths and sizes before observation, then
full immutable fingerprints before original exchange production and replay.
Verifying full files before planning would introduce an unbounded source read;
this is intentionally avoided. Metered Parquet reads cap all metadata and key
column traffic at 8 MiB/producer (configurable up to 16 MiB), decoded row/file
limits remain independent. API read bytes include cache-served reads and are not
physical device bytes. Selected nonempty row groups are explicit and recorded;
metadata lists remain subject to the byte cap. Bytes exceeding the cap fail
closed. Source-pull wall time includes waiting, decoding and I/O and is labelled
accordingly, not claimed to be pure waiting or CPU work. Final completion uses
the job execution epoch, including recovered attempts.


Final boundedness review added a 2 MiB uncompressed selected key-column chunk
cap before Parquet reader construction, independently of compressed API read
bytes and returned row count. Primitive UInt64 projection is checked before
decode. A large compressed key chunk with a one-row sample is explicitly refused
in a regression test. This is a conservative supported-source restriction,
preventing a small sample limit from hiding a large legitimate page decode.


## Separate supported-domain experiment — 2026-10-21, after original failure

The original 262144-key release experiment failed static producer correctness
with SpillNoProgress. The existing local aggregate caps its table at 65536 slots
(32768 live keys) irrespective of a larger budget, and spills into 16 buckets
using hash_v1 low bits. This fixture's hash%4=0 keys occupy four spill buckets,
allowing at most 4*32768=131072 domain keys. The original declared suite remains
FAILED; its two completed uniform runs and failed skew children are preserved.
No severe-skew speed result was available when choosing this correction.

Before timing a separately named revised experiment, predeclare 131072 keys in
**all** three regimes; retain 1000000 rows, seed42, four files, budgets, modes,
repetitions and all three numeric performance gates unchanged. This is the
analytically derived supported-domain ceiling, not a size selected for a timing
win. Revised raw paths default to phase8-acceptance-v2. A pass, if obtained, is
limited to this supported workload; it cannot relabel the original failed suite.
Independent review accepts this separate experiment with those qualifications.
The baseline spill algorithm/cap is not changed as Phase 8 cleanup.

## Destination frame packing — separate implementation experiment

The supported-domain un-packed experiment passed every correctness/identity
check but failed primary timing: exclusive-work ratio0.8063446422, job ratio
1.0692664741; uniform ratio1.0272618494 passed. Preserve it as FAILED. The 4-host
confirmation ratios were0.0592951646(work),0.7171119652(job); these do not override
the primary 2-host gate.

Before measuring new code, retain exactly that supported-domain workload/options
and all gates, but pack sampled destination contributions into <=1024-row frames.
Rows retain per-destination order and values; no reduction, reordering, full-stage
buffer or routing change. Pending buffers are bounded by N*1024 rows (24 bytes
per contribution, <=96KiB for four hosts), separately accounted with peak/capacity
row metrics. One emitted Arrow frame, one selected batch and bounded encoder
transients are additional, not hidden inside the task ledger or an RSS claim.
Final leftovers flush before terminals. Targeted replay uses the identical
per-destination stream/boundaries independent of skipped destinations, preserving
original terminal fingerprint equality within each execution.

Static defaults remain byte-for-byte batching behavior. Explicit **static_packed**
is a static diagnostic baseline using the same packing and unchanged hash%N;
it performs no probe/layout adaptation and runs all finals. It is not labelled
adaptive. Add one 2-host skew static_packed arm (one warmup, three measured) to
separate batching from ownership effects. Gates still compare the original static
arm to sampled_v1; disclose the diagnostic comparison and do not attribute all
observed gains to repartitioning alone. New artifact paths use
phase8-acceptance-packed. Independent review required this attribution control.


Final packed suite (source3cb19f9, runtime tree71dd3c427eedfabd3716cc451ce3da61bc32e544)
passed all39 child correctness checks but FAILED primary gates: work ratio
0.8362201473, job ratio1.0850558768; uniform1.0820259475 passed. Four-host ratios
0.0704644508(work),0.7258686201(job) are limited confirmation only. Phase8 remains
incomplete. The report preserves all three experiments and exact source identity.
