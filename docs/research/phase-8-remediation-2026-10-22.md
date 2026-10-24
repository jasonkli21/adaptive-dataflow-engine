# Phase 8 remediation experiment — 2026-10-22

Baseline: c87d2baa5a2489994ddaa32103d03bbf7c696ede. Phase 8 remains open.
The user authorized fixing Phase 8 before Phase 9 on 2026-10-22. This does
not waive any performance, interpretation or evidence gate.

Before collecting new timing, retain the supported-domain release matrix in
ADR-0015 and scripts/phase8-acceptance.sh without changing any workload, seed,
size, memory, repetition, comparator or numeric threshold. Use a fresh named
results directory; preserve all earlier failed experiments. Report unsuccessful
new experiments too. The primary two-worker skew gates are >=20% exclusive
final work improvement AND no median job-latency increase; uniform <=1.30x.

Candidate mechanism: replace repeated per-destination Arrow take followed by a
second copy into packed primitive vectors with one row-routing pass into the
same bounded destination buffers. Preserve per-destination ordering, exact
1024-row boundaries, signed values, skipped replay destinations and terminals.
Static remains unchanged. static_packed uses the same new packing for attribution.
No aggregate hash/table/spill algorithm, layout, sample or recovery change is
part of this experiment. Add separate route/packing and send/encoding wall
observations, measured on all producer paths, so pipeline overhead is visible.
Parallel layout ACK RPCs use the same shared deadline and all-worker success
requirement. They change no admission or commit fence. Protocol advances for the
additional private metrics; wire v2/hash v1/result schema v1 remain unchanged.

These are implementation hypotheses, not established causes of the old latency
regression. Compare producer work, route time, send time, probe/ACK and final
work on every measured sample before making an attribution claim. Correctness
and the original three empirical gates remain separate. Timing stays local.
The original pre-exchange transition interpretation and external evidence
availability also require explicit resolution before Phase 9 can start.

## Original-production prefix (separate candidate, before measurement)

To resolve the open transition interpretation with execution evidence, prepare
and ingest one original scan/project/hash-aggregate batch during the probe RPC,
then retain that exact task state until the all-worker layout ACK. Resume the
same scan and aggregate at RunProducer: the prefix is neither replayed nor
emitted twice. This starts original partial aggregation before the topology
commit without waiting for producer completion or exposing exchange traffic.
The independent bounded key probe remains separately metered. Empty sources
have zero prefix rows. Bound retained state by the existing single producer
budget and prefetch depth; cancel/drop shuts down scan/prefetch and removes
spill. Record original rows ingested before commitment and preparation time.
Static defaults bypass this preparation. Table capacity, spill bits, output
ordering, manifests, replay and arithmetic must be byte-for-byte unchanged.
This tightens the Phase 8 pre-exchange window; it still does not claim in-flight
exchange epoch changes or finer than 48-shard routing.

Full source fingerprint verification remains at producer resume, before any
exchange output. The prefix is speculative until that verification succeeds;
no full-source hashing is added to the bounded planning probe. The immutable
source contract remains. The original scan/decode/prefetch bounds are separate
from the independently capped key-only probe; prefix preparation is reported
separately and is included in the coordinator's probe-stage wall cost.

## Listener-dispatch candidate (predeclared after prefix failure)

The original-prefix snapshot2123887 passed all39 correctness checks, skew work
ratio0.6411084192 and uniform1.0578900517, but FAILED skew job latency1.0137313513.
Preserve its failed bundle and every raw sample. Do not rerun it for a lucky pass.
Its sampled primary probe+preparation took4.50–5.24ms and ACK1.65–2.58ms;
producer route/pack was1.21–1.33ms per producer. Three static job times were
275.327,250.722,248.912ms; sampled254.165,259.686,251.723ms. The static outlier
included17.66ms route wall on one producer, illustrating scheduling variation.

Before measuring a new source snapshot, replace idle nonblocking listener
polling/sleep with blocking accept and an explicit localhost wake connection
on shutdown, in both control and shared exchange receivers. Keep8 control and4
exchange handler slots, socket/whole-RPC deadlines, cancellation/poisoning and
normal handler joining. Polling only while every handler slot is occupied is
unchanged. Recheck stop immediately after accept so the wake socket never starts
a reader that could delay shutdown. Add idle and all-slots-busy shutdown tests.
No payload/protocol, buffer, topology, workload, repetition or numeric-gate change.
This targets dispatch latency, not a new adaptive algorithm or a static slowdown.
Run the full original release matrix once for the new source, preserving its
outcome regardless of pass/fail. Complete the old Phase6 regression as separate
compatibility evidence; it does not waive the failed timing gate.
