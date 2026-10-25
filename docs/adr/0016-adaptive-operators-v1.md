# ADR-0016 — Bounded adaptive operators v1

Status: Accepted bounded v1 (2026-10-24). Original frozen evidence records 118 successful Phase9 children/95 measured samples,3 expected negatives, all8 fixed guards and Phase6–8 regressions. Follow-up review fixes pass the 139-test all-features workspace suite; the historical timing evidence is unchanged. See the Phase9 implementation report for source identities and limits.
Date: 2026-10-22. Baseline: user branch c87d2baa5a2489994ddaa32103d03bbf7c696ede,
plus completed Phase8 remediation matching clean source ab7404b/runtime432cc7dd.
The Phase8 report and immutable release record closure before Phase9 source work.
The user requests one final commit, all work in this session, no subagents.

## Authority and outcome

Implements PRD Phase9; refines architecture §§7–9,14,16–17,22,28–29 without
changing INV-1..10 or introducing a frontend dependency. Keep ade-bench ->
ade-distributed -> ade-exec. No Phase10 scheduler or Phase11 general IR/SQL work.
Static GroupBySum/hash aggregation remains exactly the default.

The join is a non-null UInt64-key/Int64-value INNER equijoin. Both sources have
independent versioned file identities. Output is (key,left_value,right_value),
including EVERY matching row pair and identical duplicate multiplicity. Build
side changes never reverse column semantics. Order is unspecified. A separate
DataFusion SQL context supplies the oracle; canonical integer MULTISETS retain
every duplicate across batches. Validation and execution have explicit output
bounds; a bound refusal is not a successful benchmark. Existing oracles stay.

## Physical mechanism and bounds

Use a narrow two-source execution helper rather than a generalized optimizer/IR.
Core hash state uses pre-reserved fixed primitive slots and duplicate-row lists;
no unreserved growable map or one task per row. A configured hash-row capacity
is an admitted state constraint. When actual build rows exhaust it, BEFORE any
output, evacuate every buffered row and all subsequent build/probe rows into
32 grace partitions using deeper hash bits independent of wire hash%N. Release
the old hash reservation after evacuation. Process one partition at a time with
the same bounded table. An oversized partition, including an unsplittable duplicate
key, gets a typed skew refusal; no truncation, unlimited recursion or OOM fallback.
Forced hash-only and grace are diagnostic controls; only auto pressure counts as
an observed hash-to-grace transition.

Spill headers identify format/version, side and partition. Checked row/byte
counts and checksums detect corrupt/truncated files. Files are complete before
reading; bounded metadata tracks at most64 paths/counts, not per-row locations.
One bounded I/O buffer and a bounded number of open files are used at a time.
A per-execution temporary root is removed on success/cancel/error. Track source
batches, decode chunks, metadata, table/output reservations and spill/FD peaks
separately. The ledger is not process RSS. Checks occur between source pulls,
spill chunks, partition builds and pair output. Ordinary OS file calls retain
the pre-existing non-preemptibility limitation; distributed deadlines/cancellation
and child ownership bound the supported process fixture, not kernel syscall time.

Projected Parquet sources validate primitive types, nulls, metadata/read limits
and uncompressed selected-column chunk limits before decoding. Row/byte/file
caps and deadlines apply independently; a tiny compressed file is not proof of
a small decoded side. Runtime inputs and output have explicit row/byte bounds.

## Observation, decision and distribution

Join observations are TWO-sided and independent of Phase8's one-source sample.
A bounded real-source probe reports assigned files/rows, actual API/decode bytes,
coverage and both source identities. Only proven complete bounded build input
may broadcast. Incomplete prefixes never prove smallness. Arithmetic/fanout and
per-destination state upper bounds are checked; forced broadcast obeys the same
proof. Otherwise observed policy repartitions both sides, with a deterministic
build-side fallback. Forced distribution/build controls use identical semantics.

A typed immutable strategy binds job/execution namespace, both source versions/
fingerprints, workers, hash1, stages, selected distribution/build side and probe
facts into a digest. Every worker checks its own saved observation and ACKs that
exact decision before exchange. Duplicate/stale/missing/partial decisions fail
closed. Full immutable file fingerprints validate before original emission;
source immutability remains required, not transactional filesystem isolation.

Use existing Sender/Store/retention machinery with explicit typed schemas:
legacy aggregate partials remain schema1; join source rows and join output pairs
have separate schema versions. Left, right and gather have disjoint exchange
IDs. Join headers fence strategy/execution identity; forged stages/sides/schema
or identities cannot enter another store. Frame wire remains v2, old aggregate
IDs and result schema1 remain. Private control advances from Phase8 ADETCP10.

V1 join supports spill-retained delivery only. Reject other retention modes,
expected-cost copies/recovery, Phase8 sampled layout, slow-consumer hooks and
unsupported prefetch combinations at admission. This is explicit bounded v1
compatibility, not coercion. For each source side all N*N terminal sessions
complete, including empties. Broadcast deliberately replicates build N times;
probe is routed once. Repartition routes both by the SAME hash1%N. Spill accepts
bounded ingress without waiting for a reader, so sequential source-side production
cannot deadlock a final consuming the opposite build side. Per-store ingress
row/byte/frame caps bound unconsumed sides. Metadata/receiver slots stay bounded.

Final output uses attempt-isolated internal gather and commits only after EVERY
required producer/final terminal and conservation check, including the global
output bound. Join process loss always fails and cancels both source sides; no
aggregate-only replay assumption is reused. Partial gather never becomes a
returned result. Dead-worker local files are removed by the parent-owned fixture
on child teardown; no independent-host/cloud durability claim is made.

## Producer partial aggregation

Keep fixed_hash as default. Explicit adaptive_v1 ingests a bounded ORIGINAL
prefix into hash state, observes actual distinct reduction/pressure, then either
continues the same hash state or flushes accumulated contributions exactly once,
releases it and streams the unconsumed raw tail as (key,value,1). Forced raw is
a diagnostic baseline. Sampling is not a second read or duplicate row emission.
Checked sum/count arithmetic and the existing final merge/route/manifest format
remain. Prefix sums that need multiple Int64 contributions must conserve both
sum and count; an overflow is not forgiven by the oracle.

Phase8 sampled routing is independent and must retain prefix/ACK and digest
fences for admitted combinations. Adaptive replay is admitted only after exact
original/replay manifest tests; otherwise reject recovery/copy combinations
upfront while retaining all default fixed-hash Phase6–8 recovery. Every supported
and refused combination will be listed in the report, not silently generalized.

## Evidence and deferred work

Before timing, freeze a versioned Phase9 experiment: tiny complete build,
balanced/large sides, pressure join, low/high-cardinality aggregation and skew/
output-bound negatives, true forced controls, one warmup and >=3 isolated
measurements, primary2 workers and limited4-worker confirmation. Keep proposed
1.15/1.20 guards unless explicitly revised BEFORE the corresponding measurement.
Use host readiness declared before timing, preserve failures and raw observations,
and retain actual row/network amplification, source/probe/ACK, table/spill/cleanup
and loss evidence. CI is correctness-only. Phase6/7/8 regressions remain separate.

Sort-merge, arbitrary joins/keys/nulls, mid-output side reversal, unbounded inputs,
recursive unsplittable-key salting, worker movement and a general optimizer are
deferred: they add different semantics/ownership without being required for
this bounded mechanism/policy experiment. A plan conflict or unmet required
outcome must be reported explicitly; implementation alone is not completion.

## Accepted bounded-v1 refusal and review clarifications (2026-10-24)

Grace uses one fixed set of 32 deeper-hash partitions. After all partition files
are sealed and validated, but before the first output, it returns `JoinSkew` if
any partition has at least one probe row and more build rows than `hash_rows`.
The check is conservative: a partition containing two distinct build keys and a
different probe key can be refused even though its true join result is empty.
That collision refusal is a supported v1 failure mode; callers must not treat
it as successful empty output. Refinement, recursive repartition and sort-merge
rescue remain deferred. The refusal and cleanup are covered by a focused test.

Hash-only exhaustion now returns `JoinHashCapacityExceeded` even when spilling
is enabled. `SpillDisabled` is reserved for a configuration that actually
disables spilling. The remote error carries the accurate typed message while
retaining fail-closed cancellation and cleanup.

The independent join oracle now checks the exact `key UInt64 nonnull`,
`left_value Int64 nonnull`, `right_value Int64 nonnull` schema on each batch,
including zero-row batches, before comparing duplicate-aware bags. Benchmark
result schema v1 is unchanged. Macrobenchmark `run` emits version-2 diagnostic
sidecars with the existing run ID and warmup/iteration slot plus child PID;
standalone `validate` without a result identity retains sidecar v1. Verifier v2
binds the slot to the run's effective config and both manifest-derived source inventories. Phase 9
evidence includes a source archive SHA-256 tied to the commit and runtime tree,
and bundled verification checks it against the included Git bundle. The
immutable historical release and its verifier-v1 artifacts remain unchanged;
they do not acquire the new arm binding retroactively.

The benchmark admits the new query through a separate `ade-distributed-join`
adapter. This keeps the aggregate adapter's options and defaults unambiguous;
both adapters share the existing child protocol, schema-v1 results, worker
binary and exchange implementation. The two-source dataset extends the manifest
with an optional omitted-when-absent `join` generator contract, preserving old
manifest serialization and physical identity. A separate side name/version and
file fingerprint set enter each source identity. DataFusion registers exactly
those two manifest lists and validates sorted bags under an explicit output cap.

Concrete compatibility: non-fixed partial modes reject source snapshots, replay,
complete copies and failure hooks at admission. All five static exchange modes,
threshold retention and sampled48-shard normal execution are tested. This refuses
unproven byte-identical adaptive replay while preserving the fixed-hash graph.
Private control is ADETCP11. Typed wire-v2 schemas2/3 add the strategy/execution
context and a terminal BLAKE3 over logical transfer, context, sequence and encoded
frames; aggregate schema1 bytes/ACKs remain unchanged. Stage2=left,3=right,4=gather.
The pure diagnostic verifier recomputes decision and source identities and checks
actual source/receive/output/retention facts; it never substitutes for the SQL bag
oracle. Join queues are separately bounded at4MiB per store (two sides plus bounded
gather stores); encoded ingress caps include all accepted frames before retention.
