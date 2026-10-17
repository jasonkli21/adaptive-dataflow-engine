# ADR-0014: Expected-cost complete-transfer materialization v1

**Status:** Accepted — predeclared release exit passed; see Phase 7 report
**Date:** 2026-10-16

## Scope and architecture impact

Implements PRD Phase 7 and architecture sections 12–17, 22, 27–29; preserves
INV-1 through INV-10. Refines ADR-0012/0013's no-cost/no-restore boundary only for
the existing single final-worker-loss episode. Static modes and default behavior
remain; explicit `adaptive_policy=threshold_v1` remains independent. Phase 8
partition topology, broader retry and production cloud deployment are excluded.

## Economics and provenance

One decision per complete logical producer-to-final transfer, consumer count 1.
Whole-producer source closure is charged for that transfer's recomputation:
current recovery cannot replay a smaller source subset. Operator work is the sum
of exclusive `ade-exec` work durations measured before sender finalization, not
CPU billing. Wall elapsed is recorded separately. Source file sizes/counts and
encoded output bytes/segments are execution facts. File snapshot verification
hashing, network/serialization/final merge work and idle/control billing are not
included in the production compute proxy.

Prices are configured integer nano-USD per work ns, source/read/write byte and
request, and capacity byte-ms; probability is configured ppm (not worker health).
Throughputs are configured bytes/s; slack/retention and linear SLO penalty use ms.
Time conversions round upward. Monetary products/sums use checked u64; probability
numerators use u128, preserving exact ties without probability rounding. Invalid
probability/zero throughput, config values above 10^12, overflowing capacity rates,
and overflowing observed calculations fail explicitly. No saturating policy math;
only lateness clamps negative slack usage to zero.

Let `R` be measured work price + whole-source reread byte/request prices + estimated
recompute lateness penalty. Let `D` be object read byte/request prices + estimated
restore lateness penalty. Let `W` be write byte/request + expected capacity price +
estimated write lateness penalty. Recompute time is measured operator work plus
estimated source read time; restore/write times use encoded payload and configured
throughputs. Restore performs HEAD + range GET, so it prices **two** requests per
segment. Writes use one PUT per segment. Consumer count must equal one; unsupported
fanout is rejected rather than projected into this topology.

Persist iff `1_000_000*W + p*D + 1_000_000*minimum_margin < p*R`.
Ties skip. `cost_mode=always|never` are explicitly labelled full-copy research
baselines using the same staging mechanism. Recovery compares direct `D < R`,
without applying probability again; ties recompute. Deadline is an estimated
penalty objective, not an admission guarantee/hard deadline scheduler.

## Producer staging and complete-copy facts

Deviation from the recommended destination staging/terminal-ACK extension:
the producer stages the exact encoded frames in one sequential temporary log per
transfer while delivery continues. The existing destination terminal ACK validates
normal receipt; after that ACK, the producer verifies staged counts/schema/hash
against its attempt-0 terminal and uploads a selected full copy. Only then does
its task RPC return the decision and optional receipt. The coordinator's existing
all-producers failure gate therefore sees receipts before it permits process loss.
This avoids changing ACK payloads and gives measured work directly to the decision
without another observation message. Loss before producer completion remains
outside the supported envelope.

Staging reuses ADR-0011's blocking-I/O owner, cancellation, deadline and poisoning
machinery. One outstanding operation and at most a bounded encoded frame are held
per staging log; no segment index/key vector is retained. Output disk volume is
not quota-managed. Staging payload bytes/read/write elapsed are separate from
normal delivery spill/object bytes. Object PUT wait elapsed is measured separately;
it includes client/thread/runtime overhead. Restore elapsed includes validation
and sending into the recovery store, not just provider time.

Expected-cost delivery uses the existing direct/buffer/spill threshold mechanism,
with remote suffix escalation disabled. Complete copies use `complete-v1/job-J/`
plus exchange/partition/producer/original-attempt and sequence; legacy delivery
objects cannot yield a receipt. Job IDs increase on reused workers; each cluster
owns its own backend root. Receipt version 1 binds original manifest, backend,
failure domain, layout version, write bytes/requests and elapsed. No object-key
list is sent. Coordinator validates decision reconstruction, coverage and receipts
before recovery. Workers validate restored logical identity, all sequences,
schema/counts/content fingerprint and attempt-1 equality against attempt 0.

## Failure domain, restore and cleanup

`LocalCluster::spawn_with_materialization` owns one shared filesystem backend
outside all worker-local stores. Its caller installs the same provider-neutral
`ObjectStore` in children. Capability is `worker_process_surviving`: no host,
AZ or production cloud durability is claimed. The default ephemeral backend is
ineligible. General supplied backends remain supported through the public worker
backend constructor, subject to matching explicit capability assertions.

Recovery may restore and recompute different producer transfers. Both use the
same strict encoded-frame Sender/receiver and existing local-materialized recovery
store. All transfer manifests validate before final aggregation/gather commit.
Missing/corrupt objects fail the episode cleanly, with no partial output and no
fallback after partial restore. Existing attempt fencing and one episode remain.

The coordinator requests job-prefix cleanup from a reachable survivor on success
and failure, even if task cancellation already removed worker job state. Cleanup
validates the worker's job watermark and streams object enumeration/deletion;
metadata is independent of segment count. Failure is logged and invalidates an
otherwise successful execution; failed jobs preserve their original error. A
poisoned/unresponsive provider can prevent timely deletion; parent-owned fixture
teardown after children die is the final safety net. Normal cleanup after real
writer loss is tested before cluster teardown. No persistent catalog or cross-job
reuse is added.

## Protocol and bounds

Control framing `ADETCP04` -> `ADETCP05` rejects the prior version before payload
decode. Transfer header/frame/terminal/ACK shape remains exchange wire v2; hash
and benchmark result schema remain v1. The control framing also envelopes exchange
headers/ACKs. No v3 transfer payload is needed because observations/receipts travel
in producer control replies. Control cap remains 1 MiB. Plans/facts/decisions are
O(workers² + source files), N=2–4; encoded frames remain <=1 MiB. Task budgets can
overlap, exchange/staging allocations are separate, and none is a process RSS cap.

## Predeclared release acceptance

Before release measurement: fixed 200,000-row synthetic GroupBySum, 4 files,
32,768 keys, seed 42, 2 workers, 32 MiB task budget, 1 MiB exchange buffer,
prefetch 1. Independent oracle and child isolation; two samples per success and
real-final-loss arm, no best-sample selection. Compare expected, always complete
copy, never complete copy and unchanged threshold. Regimes (ppm/source-byte-price/
write-byte-price/read-byte-price): low=(1000/0/1000/1), high=(900000/1000/1/1),
expensive-restore=(500000/1/1/100000). Other prices are recorded defaults.

Empirical scores charge actual original/replayed operator work, source bytes,
actual complete-copy PUT bytes/requests, restored bytes and HEAD+GET requests,
plus observed staging elapsed at the compute proxy rate. Legacy object delivery
I/O is charged if present. Staging time includes waits, deliberately conservative;
this is a declared proxy, not cloud billing. Pair arms as `(1-p)*success+p*failure`.
Actual query wall times separately measure normal overhead; no model alternative
is used for empirical scoring. Zero capacity/SLO prices in this suite are explicit;
unit tests prove their modeled paths and deadline/throughput decision changes.

Exit requires oracle/identity/integrity checks for every sample, expected skip in
low/expensive-restore and persist+real restore in high; always-copy in expensive-
restore must recompute despite receipts. Aggregate expected score must beat always,
never and threshold. Each regime's mean normal expected query time must be <=5x
threshold's mean. This generous bound is for a local research fixture with
synchronous per-object owners, not a production overhead target. If it fails,
report failed exit without moving the bound. Scripts retain raw inputs, outcomes,
reconstructed policy alternatives and independently calculated empirical scores.

Empty-transfer economics preserve the original measured production observation but
price replay compute/source/time as zero, matching Phase 6's synthesized empty
terminal. An additive work-time plus source-throughput estimate is deliberately
conservative: measured operator work can already include scan I/O waits. It is
not a claim of independently measured CPU and I/O service times.
