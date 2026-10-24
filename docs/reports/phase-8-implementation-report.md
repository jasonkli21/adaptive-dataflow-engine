# Phase 8 implementation report — bounded runtime repartitioning v1

Date: 2026-10-21. Original baseline: `f66addb815deaa8accb2529124b713a8efd85c5e`.
At the original implementation start, HEAD matched the supplied baseline and no
baseline code changes were present. The Phase 8 plan was included in that
implementation. This review remediation is a follow-up source revision; its
identity and evidence are recorded separately below. The unrelated untracked
bootstrap prompt remains outside the change.

**Current exit status: COMPLETE for bounded Phase 8 v1 (2026-10-22).**
The final source-bound closure below passes the original numeric gates in both
predeclared confirmation suites and across all nine pooled primary samples,
with immutable public raw/source evidence. The ADETCP08 and subsequent failed
experiments below remain failed historical outcomes; none was relabelled.
The original release evidence records its measured source commit and exact
`HEAD:crates` runtime tree. It must not be attributed to the review-remediation
source revision.

## Implemented contract and reconciliation

ADR-0015 records the bounded interpretation of the current candidate: workers
and stores are prepared, bounded source-prefix probes run, then all workers
commit/ack one immutable layout before original producers and final consumers
start. The original partial aggregation has not begun when the layout is chosen.
This adds no producer-completion or full-shuffle planning barrier, but it is
pre-exchange runtime-informed planning, not a topology change observed during
original production. It does **not** implement arbitrary mid-exchange epoch
switching or the architecture's future post-production splitting example. Phase
8 remains incomplete; no claim is made that this narrower behavior fulfills the
broader PRD dynamic-transition outcome. Existing blocking local partial
aggregation is unchanged.

The pure planner uses hash v1, 48 virtual shards divisible by 2/3/4, deterministic
load ordering/ties, a relative imbalance threshold of 1.5x mean, minimum sample
work 256 and predicted maximum-load gain >=20%. Tiny distinct-contribution total
<=64 coalesces to one final only when every assigned source row is observed; an
incomplete tiny prefix retains static ownership. One occupied virtual shard can
contain many distinct keys, and the fixed 48-shard planner cannot refine it. No
hot-key salting or duplicate final keys are introduced. `static` bypasses probes,
ACKs and shard routing. Static hash vectors are unchanged. Threshold and
expected-cost retention policies remain independent.

Layout v1 binds hash version, physical host count and all shard owners with a
domain-separated BLAKE3 digest. Active finals derive from mapping, never a trusted
independent bitmap. All physical producer/destination terminals remain N×N,
including empties. Only active final RPCs execute merge aggregation; inactive
RPCs drain/verify zero frames and never send gather output. Gather requires the
exact active set while preserving original TaskId=N+destination and attempt
quarantine. Receiver validates every key's committed owner before retaining it.
A split changes key ownership/final load with up to N existing finals; coalescing
changes actual final count to one. Split alone cannot increase beyond N hosts.

## Provenance, bounds, protocol and lifecycle

The sample is an exact capped distinct-key set *per producer* over assigned file
prefixes, so repeated raw keys do not fabricate partial-output load. It estimates
sampled distinct contributions, not full cardinality or encoded bytes. Up to four
files, 4096 rows/file, 16384 decoded rows and 8 MiB source API read bytes/producer
(defaults); 48 counters. Config caps are strict (maximum byte budget 16 MiB).
Metered reads include metadata and key-column reads; row groups are explicitly
selected, bounded and recorded. File-size proxy and actual API reads have
separate fields; neither is physical disk/cache-miss traffic. Prefix bias and
underestimation can hurt speed, never ownership/correctness.

Probe verifies prepared snapshot assignment and opened-file sizes, then normal
production verifies full immutable fingerprints before any data emission. This
avoids full-source hashing before layout choice. A same-size mutation can distort
the sample but cannot escape original production/replay verification. Local
filesystem syscalls share existing scan limitations; cancellation/deadlines are
checked between reads/batches, not a claim that a blocked OS read is interruptible.
Probe owns the producer-busy guard, preventing a cancelled probe from overlapping a
new job. Coordinator RPCs share a stage deadline, worker probes carry explicit
bounded deadlines. Duplicate probe/commit, early tasks/exchange, stale jobs,
malformed ownership/version/digest and partial commitment fail closed.

The measured candidate used control protocol **ADETCP07**, replacing ADETCP06;
exchange frame/header wire v2, hash v1 and result schema v1 remained. Review
remediation adds source-coverage evidence and advances the current control magic
to **ADETCP08**; the earlier release evidence remains bound to ADETCP07 and does
not measure this source revision. Topology diagnostics stay in the existing
optional sidecar. `ade-worker --verify-topology` reconstructs the pure decision
and digest for offline evidence validation, with the same 1 MiB input bound.
Source/observation metadata remains O(files + 48*N + bounded sampled keys),
exchange and task budgets remain separate, and none is a process RSS quota.
No per-segment coordinator history or global key registry was added.

## Supported and refused matrix

| Topology | Hosts | Delivery / policy | Normal | Recovery |
|---|---|---|---|---|
| static | 2–4 | direct/buffered/spill/local/object/threshold | unchanged | existing one-final replay |
| static | 2–4 | expected_cost_v1 | unchanged | existing restore/recompute |
| sampled_v1 | 2–4 | direct/buffered/spill/local/object/threshold | split/covered coalesce/no-benefit/coverage fallback | exact-layout one-active-final replay |
| sampled_v1 | 2–4 | expected_cost_v1 | same topology options | exact-layout full-copy restore/recompute |

Recovery requires all original producer terminals and replayable immutable source
lineage, even if a complete copy exists. Normal execution without a snapshot is
supported; failure then returns no result. Existing producer/multiple-worker-loss,
poisoning, nonreplayable/missing snapshots, source drift, corrupt/missing copy,
recovery failure and ambiguous gather attempts remain unsupported. Inactive
failure targets are refused. A quiescent inactive host may recover a lost active
final after its own producer and empty verification finish. Only successful active
final partitions are described as reused; an idle host is not a reused aggregate.
Real dynamic process-loss tests exercise 2 hosts; normal topology tests cover
2/3/4. This is not a broader failure-domain claim.

Layout digest supplements original logical IDs, attempt, execution UUID and
terminal fingerprint in producer/final metrics, complete-copy facts/receipts,
recovery plan and replay. Recovery restores/recomputes only the lost physical
partition under its pinned mapping, validates original manifests exactly and
commits only attempt 1; surviving active attempts stay 0. Shared complete-copy
cleanup remains explicit on success/failure, including after writer death.
Object namespace `complete-v1/{execution-uuid}/job-J` and receipt version 1 retain
meaning, with an added optional topology digest in the private control shape.

## Validation and independent review

Focused development: planner determinism/overflow/invalid layout, receiver
misrouting, probe byte cap, duplicate/partial/stale commit, early task/transfer,
biased sample, assignment mismatch, source size drift, inactive failure target,
no-snapshot dynamic failure, and real split/coalesce across all six delivery
choices and 2/3/4 workers. Real loss exercises split/coalesced replay and split/
coalesced complete-copy restoration, original manifest equality, one attempt
commit and cleanup before cluster teardown. Existing Phase 6/7 tests remain.

Independent review identified and led to source-assignment/size validation without
unbounded preplanning hashing, byte-bound observation validation, explicit probe
deadline/busy ownership, job-epoch recovered completion times, compatibility
matrix, and stronger evidence reconstruction/conservation. Initial sandboxed
process tests failed on localhost bind permission, then passed with authorized
socket access. Build/lint errors during development were fixed; logs preserved in
`/tmp/phase8-*.log`, including failed attempts.

Executed validation status is finalized below after the release run. Workspace
format/check/clippy, full tests, debug build/smoke and default-feature check are
required. Clean release command: `cargo build --release --workspace --locked
--offline`; acceptance: `bash scripts/phase8-acceptance.sh`, followed by focused
`phase6-acceptance.sh` and `phase7-acceptance.sh`. No unavailable command is counted
as passing. CI contains correctness only; local timing gates stay in scripts.

## Predeclared release evidence and limitations

ADR/script declare seed 42; 1,000,000 rows, 4 files, 8192-row groups, 262144-key
domain; uniform, engineered distinct-key hash-bucket skew and 100% single raw hot
key. Primary 2-host controls plus limited 4-host skew; 32 MiB task, 1 MiB exchange,
prefetch 1, spill delivery, one warmup and three isolated measured iterations.
Every child is checked against the independent DataFusion oracle. Tiny workload
proves a real one-final set; additional genuine loss cases exercise replay/restore.

Gates remain >=20% median maximum exclusive final operator-work improvement on
severe skew, no median end-to-end skew latency increase, <=1.30x uniform latency.
Exclusive work includes spill processing, excludes source pulls; source-pull wall
includes waiting/decoding/I/O, not pure waiting. Last-final completion uses the job
epoch; final wall elapsed remains context. Decision/load/digest reconstruction,
actual rows/bytes and complete transfer conservation are checked separately from
sample predictions. Negative hot-key results are retained. Failed or inconclusive
performance gates leave Phase 8 **incomplete**, not redefined as success.

No mid-exchange epoch change, generalized salting, multi-task-per-host execution,
worker elasticity, scheduler work stealing, SQL/joins, streaming, cross-job state
or Phase 9 is included. Bad samples and source-read limits can reject or slow jobs;
this heuristic is a small research implementation, not a calibrated cost model.


Final boundedness review added a 2 MiB uncompressed selected key-column chunk
cap before Parquet reader construction, independently of compressed API read
bytes and returned row count. Primitive UInt64 projection is checked before
decode. A large compressed key chunk with a one-row sample is explicitly refused
in a regression test. This is a conservative supported-source restriction,
preventing a small sample limit from hiding a large legitimate page decode.

## Final correctness gate and source freeze

The original measured candidate is `4ed3f625e5a8b4672e2925476ad815237b389645`, with
runtime tree `8821caae863a4335bfeb0575fdd6a4f540c9bde5` (`HEAD:crates`). The clean
local checkout is `/tmp/ade-phase8-release`. Revised experiment declarations and later report/status documentation
amendments of that experiment retain that exact runtime tree; raw data must not be relabelled as
having been generated from an amended documentation commit.

| Command | Result / raw log |
|---|---|
| `cargo fmt --all --check` | PASS; `/tmp/phase8-final-fmt3.log` |
| `cargo check --workspace --all-targets --locked --offline` | PASS; `/tmp/phase8-final-check2.log` |
| `cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings` | PASS; `/tmp/phase8-final-clippy4.log` |
| `cargo test --workspace --all-features --locked --offline` | PASS, 108 tests + doc tests before final decode guard; `/tmp/phase8-final-tests.log` |
| `cargo test -p ade-distributed --all-features --locked --offline` | PASS, 51 unit + 10 process tests including final decode guard; `/tmp/phase8-final-probe-tests.log` |
| final focused planner tests | PASS after row/file consistency guard; `/tmp/phase8-final-planner-tests.log` |
| `cargo check -p ade-distributed --no-default-features --locked --offline` | PASS; `/tmp/phase8-default-check.log` |
| `cargo build --workspace --locked --offline` | PASS; `/tmp/phase8-debug-build.log` |
| `bash scripts/smoke.sh` | PASS; `/tmp/phase8-smoke.log`; smoke timings are not release evidence |
| shell syntax, Python AST, `git diff --check` | PASS |

The normal Apple debug linker emitted its existing oversized unwind-section
warning while successfully linking. Initial release invocation from `/tmp`
failed because that directory has no Cargo.toml; corrected working directory is
the clean checkout above. Preparatory optimized builds were superseded by the
final review fixes before any timing measurement. Those logs remain
`/tmp/phase8-release-build*.log`; no preparatory build is called acceptance.

Changed implementation files: distributed `topology.rs`, `probe.rs`, `partition.rs`,
`protocol.rs`, `worker.rs`, `coordinator.rs`, `gather.rs`, `exchange.rs`,
`metrics.rs`, `materialization.rs`, `recovery.rs`, `lib.rs` and worker binary;
distributed process tests; benchmark adapter and versioned dataset generator;
Cargo manifest/lock; the three Phase 8 scripts; ADR/plan/report, README, AGENTS,
architecture exchange refinement and benchmark methodology. No execution core
frontend dependency or local aggregate rewrite was introduced.


## Preserved original failed experiment and separate revision

Original source `4ed3f62`, runtime tree above: uniform static measured query ms
332.757041,339.479167,329.094833; uniform sampled measured query ms
341.846250,343.715291,463.763833. These are raw context, not a passing suite.
All four skew-static children (one warmup, three measured) exited1 with producer
SpillNoProgress. No valid skew comparison or empirical exit exists for the original
262144-key declaration. The script stopped before the remaining regimes;
verification explicitly fails, and interrupted evidence can still be bundled.
Original raw results are retained under `phase8-acceptance` in the clean checkout
and copied to `benchmarks/results/phase8-acceptance-initial-failed`; bundle
`benchmarks/results/phase8-initial-failed-bundle.tar.gz` includes its source archive.
Logs: `/tmp/phase8-release-acceptance.log`, `/tmp/phase8-initial-verifier.log`.

The baseline aggregate table has a 32768-key capacity and 16 low-hash spill
buckets. Engineered hash%4=0 data uses only four of them; 262144/4 exceeds capacity,
while 131072/4 is the supported ceiling. Increasing the task budget cannot change
the hard table cap. No local aggregate rewrite is introduced. Independent review
accepted a **separate**, predeclared supported-domain experiment: all regimes now
use 131072 keys, other workload/options/repetitions and numeric gates unchanged.
This revision happened after original uniform measurements and before any revised
measurements; it is not represented as an untouched original predeclaration.
Any success is limited to that explicit supported workload. Original failure stays
failed. Revised artifacts use `phase8-acceptance-v2`.


## Preserved supported-domain timing failure and bounded packing experiment

Source `3e9e425676d2f102eab60b71a52a76503c3aa73d`, runtime tree 8821caae above:
all 35 children passed oracle/layout/recovery verification; the suite FAILED
primary performance gates. Sample/static median ratios were0.8063446422 for
maximum exclusive final work (19.37% improvement, short of20%),1.0692664741 for
job wall latency, and1.0272618494 for uniform latency (passes1.30x). Four-host skew
ratios were 0.0592951646(work) and 0.7171119652(job), a limited confirmation that does
not replace the failed primary gate. Raw results remain phase8-acceptance-v2.

A separately source-bound packing implementation now accumulates <=1024 rows per
destination, reducing fragmentation/encoding/spill calls introduced by routing.
This changes batching as well as topology; static_packed is an explicit static
attribution control, with identical packing and no repartitioning. Original
static behavior and the three performance gates are unchanged. Pending capacity
is N*1024 contributions (<=96KiB for four hosts), plus bounded selected/emitted
Arrow batches and encoder transient; peak/capacity rows are diagnostic facts.
Packing does not aggregate values, reorder a destination, or delay it past a full
frame. Final leftovers flush before terminals. Original/replay packing is identical
for each destination, including selective replay. Tests include511/512/1023/1024/
1025/2049 boundaries, signed values, idle destinations, spill, actual process loss,
complete copies and original manifest equality. New evidence uses
phase8-acceptance-packed; the failed v2 result is never relabelled passing.


## Final measured outcome — Phase 8 remains incomplete

Measured source: **3cb19f9cee174b9dd4c9d139faf9d994ededeca7**. Final runtime tree:
**71dd3c427eedfabd3716cc451ce3da61bc32e544**. All subsequent changes are report/
status documentation only; the single enclosing implementation commit is the
final handoff identity. Raw results retain the literal measured source SHA,
not a rewritten claim about the later documentation amendment.

All39 isolated children passed oracle, source/config identity, reconstructed
layout/digest, active gather, full transfer/byte conservation, bounded packing,
real replay/restore and attempt-fingerprint checks. This includes nine warmups,
30 measured children and the explicit static_packed attribution arm. Tiny input
ran exactly one aggregate and one idle verifier on two hosts, with one gather
commit; split input moved distinct key ownership to both/all four active finals.
All N×N physical terminal sessions remained complete. Primary performance FAILED:

| Regime / hosts | Static median job ms | Sampled median job ms | Static max-exclusive-work median ms | Sampled median ms |
|---|---:|---:|---:|---:|
| Uniform / 2 |305.359|330.407|34.262|35.093|
| Engineered skew / 2 |288.813|313.378|48.684|40.710|
| Single raw hot key / 2 |72.842|72.682|0.527|0.480|
| Engineered skew / 4 |289.344|210.026|106.440|7.500|

Primary skew work improved **16.38%**, below the fixed 20% requirement; job latency
increased **8.51%**, failing no-increase. Uniform latency increased 8.20%, passing
the 1.30x limit. The four-host confirmation improved exclusive work 92.95% and job
latency 27.41%, but cannot replace the failed primary gate. Single-hot-key routing
remained unsplittable; its small work timings are not evidence of legal hot-key
parallelization or a meaningful speed win.

The static_packed diagnostic medians were 337.467ms(job) and 64.978ms(max work).
Sampled ratios against that diagnostic were 0.92862 and 0.62652, respectively.
These expose batching effects and local variation; they do not supersede the
original static comparator. No causal claim that packing improved primary job
latency is made: its final suite still failed. Packing is bounded and avoids
fragmentation while retaining the deterministic replay/copy contract.

For accounting illustration, the first measured packed two-host skew sample sent
256408 partial rows in 252 frames /6381792 payload bytes; static sent the same
rows in 256 frames /6384448 bytes. Probe reads were 424256 source API bytes,
probe wall 3.468ms, ACK wall 3.162ms. Each producer's pending peak was 2046 rows,
below its 2048-row capacity. These single-sample facts are context, not chosen
best timings; gates use all three measured iterations and their medians. Full
per-final work/source-pull/completion/rows/bytes/spill/memory facts and every raw
iteration remain in verification.json and child sidecars.

### Final source-bound checks

- Full packing revision workspace tests: **111 tests + doc tests PASS**,
  `/tmp/phase8-packing-workspace-tests.log`.
- Workspace all-target check and all-feature clippy with warnings denied: PASS,
  `/tmp/phase8-packing-check.log`, `/tmp/phase8-packing-final-clippy.log`.
- Formatting, shell/Python syntax and whitespace checks: PASS.
- Debug workspace build and smoke: PASS, `/tmp/phase8-packing-debug-build.log`,
  `/tmp/phase8-packing-smoke.log` (usual Apple debug unwind warning only).
- Clean optimized build: PASS, `/tmp/phase8-release-build-packed.log`.
- Phase8 packed acceptance: **correctness PASS; empirical gate FAIL, exit1**,
  `/tmp/phase8-release-acceptance-packed.log`.
- Final clean Phase6 release regression: PASS, eight positive static/adaptive/
  recovered cases and explicit no-snapshot failure,
  `/tmp/phase8-packed-phase6-regression.log`.
- Final clean Phase7 economic acceptance: PASS,
  `/tmp/phase8-packed-phase7-regression.log`; expected score 5000576124.7375 vs
  always 6584081255.1325, never 9389592729.6415, threshold 9381958728.9 nano-USD
  proxy. These are declared research prices, not provider billing.
- Copied-artifact forged layout digest and row-conserving payload-byte mutation
  were both rejected by the evidence verifier with correctness_passed=false;
  preserved under phase8-verifier-negative-layout and phase8-verifier-negative-bytes.

### Raw evidence and provenance

All paths below are ignored, relative to the repository:

- `benchmarks/results/phase8-acceptance-initial-failed`: unsupported 262144-key
  original experiment, including its valid uniform runs and invalid skew runs.
- `benchmarks/results/phase8-acceptance-v2-failed`: all 35 children correct;
  original un-packed supported-domain performance failure.
- `benchmarks/results/phase8-acceptance-packed`: final39-child suite,
  `verification.json` explicitly has correctness_passed=true and passed=false.
- `benchmarks/results/phase8-packed-phase6-regression` and
  `benchmarks/results/phase8-packed-phase7-regression`: final runtime regressions;
  earlier 4ed3f62 regressions remain in phase8-phase6/phase7-regression.
- Matching generated data remains under
  `/tmp/ade-phase8-release/benchmarks/datasets/`; seeds/configs/manifests/commands,
  compiler/host identity and source archives are captured for reproduction.

Source/checksummed archives include all raw child outcomes and are intentionally
usable for failed experiments:

| Bundle under benchmarks/results | SHA-256 |
|---|---|
|phase8-initial-failed-bundle.tar.gz|f84f58bb443e23e593bad133d004feb0410fe7a633bbde3b7ca8ad032dd07e5e|
|phase8-v2-failed-bundle.tar.gz|18a4471a79744ef614203ab3548e7440a58787eb7d39f709cee8c9a3ed640e20|
|phase8-packed-failed-bundle.tar.gz|bb121a00c88f10cc473e0437d6e1a5a4dddbe90a258b2261b233d639c2b66f92|
|phase8-review-ade08-failed-bundle.tar.gz|062334443fa45a0a810229393f91e1c38b4ffa8c9bb1db7a852c2c614cc47740|

The unresolved exit is performance on the primary supported two-host workload,
not routing or recovery correctness. Probe/control overhead, producer work,
packing/copying and final spill behavior need further evidence-led work within
Phase 8. The baseline aggregate's finite spill-domain capacity remains a separate
input limitation. No thresholds were weakened, no failed run replaced by a lucky
iteration, no four-worker result substituted for the primary gate, and no Phase 9
work was begun. This is a tested Phase 8 candidate with an explicitly **unmet
empirical exit**, not a completed research milestone.

## Independent review remediation — 2026-10-21

The review's one-key/empty coalesce bug and prefix-underestimation case are fixed
by making exact assigned-source coverage a precondition for a one-final coalesce.
The coverage fact is computed from all assigned files and Parquet row-group row
counts, validated through the pure planner, and included in the observation
diagnostics. The control protocol is now ADETCP08. Regressions cover complete
single-key, all-empty and multi-key inputs; a two-row prefix followed by 1022
distinct keys keeps both static finals active and verifies the full oracle result.

The review's distinct-key/same-virtual-shard counterexample is retained as an
explicit negative test for 2, 3 and 4 workers. The decision label is now
`one_occupied_shard`, which does not imply a single key. The fixed 48-shard v1
boundary is preserved by the active Phase 8 instructions; finer refinement is a
future plan change, not silently introduced here.

Focused distributed validation passes with 53 unit and 14 process tests. The
same-shard process case verifies 1000 distinct keys, oracle equality, unchanged
single-destination ownership and actual active/gather task counts for 2–4
workers. The tiny-prefix test verifies its full 1024-key result while keeping
both finals active. The complete-source cases verify one-key, empty and tiny
multi-key coalescing with one gather transfer.

The raw Phase 8 tarballs and checksums listed above exist in the local ignored
`benchmarks/results/` directory. They are not published artifacts and are not
part of this commit, consistent with the repository rule that raw benchmark
results stay out of Git. The independent-auditability finding remains open until
those bundles are made available through an immutable external artifact.

No causal performance change is claimed from the old packed metrics: they do not
contain separate routing/Arrow-take/encoding timings, so they cannot attribute
the two-host latency regression. The numeric release gates are unchanged. The
ADETCP08 release run below confirms that the final-work and uniform gates pass,
but the primary end-to-end skew gate still fails.

The available ADETCP07 sidecars do give a limited paired-run profile. The three
sampled two-host skew runs had 3.16–3.47 ms probe time and 1.06–3.25 ms layout
commit time (4.22–6.50 ms combined). Their producer aggregate-work sums were
393.45–436.02 ms across the two concurrent producers, versus 391.53–400.51 ms
for static. Both policies emitted 256408 rows; sampled packing emitted 252 frames
versus 256 static frames, and both spilled 27868192 producer bytes. Sampled
maximum final work ranged 32.56–91.93 ms, versus 47.45–49.05 ms static. These
three-run ranges show that probe/commit alone does not account for the full
24.565 ms median job-latency increase, but they do not establish the cause: the
sidecars lack per-stage route selection, Arrow take, packer and encoding CPU
timings, and final work varies substantially. No routing rewrite or performance
claim is based on this incomplete profile.

### Review-remediation source checks

- `cargo fmt --all --check`: PASS.
- `cargo check --workspace --all-targets --locked --offline`: PASS.
- `cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings`: PASS.
- `cargo test --workspace --all-features --locked --offline`: PASS, including 53 distributed unit tests and 14 distributed process tests.
- `cargo build --release --workspace --locked --offline`: PASS.
- `bash scripts/smoke.sh`: PASS with oracle validation; the result metadata notes the expected dirty source tree because this review remediation was not committed at smoke time.

ADETCP08 clean-source release regressions all used source commit
`73c40cd74e3dcf9f6c2c1d06f98156fa824d873f`, runtime tree
`5dabbdeb0ae8bc1d322f0f01c87b758fdb0ba00b`. The only item excluded from the
clean-status check was the unrelated pre-existing untracked bootstrap prompt;
it was not edited or committed.

- Phase 6 acceptance: **PASS**. Five static delivery modes, normal and recovered adaptive cases, 2/4-worker runs, and the unrecoverable missing-snapshot negative case validated against the oracle. Raw output: `benchmarks/results/phase8-review-ade08-phase6/`.
- Phase 7 acceptance: **PASS**. Expected-cost total was 4976223355.916 nano-USD proxy, below always (6590782186.486), never (9399179712.8695) and threshold (9381977304.6025); all declared regimes passed. Raw output: `benchmarks/results/phase8-review-ade08-phase7/verification.json`.
- Phase 8 ADETCP08 acceptance: **correctness PASS; empirical gates FAIL**. All 39 isolated children passed oracle, identity, layout, active-gather, transfer-conservation and replay/restore checks. The two-host skew median max-exclusive-work ratio was **0.7727966488** (22.72% better, passes >=20%); median total job latency was **1.1524198889x** static (15.24% slower, fails no-increase); uniform latency was **1.2172439569x** (passes <=1.30). The 4-host skew confirmation ratios were 0.0710745459 work and 0.5992444103 job latency, and do not replace the failed primary gate. Raw output is `benchmarks/results/phase8-review-ade08/verification.json`; checksummed bundle is `benchmarks/results/phase8-review-ade08-failed-bundle.tar.gz` with SHA-256 `062334443fa45a0a810229393f91e1c38b4ffa8c9bb1db7a852c2c614cc47740`.

The release source commit above includes this report before the outcome amendment;
the amendment changes documentation only, and `HEAD:crates` remains the measured
runtime tree. None of these compatibility checks closes the Phase 8 primary
performance gate or broadens the transition/recovery claims.

Final default-feature library check also passed: `/tmp/phase8-packed-default-check.log`.


## User-authorized closure remediation — 2026-10-22 (final evidence pending)

Baseline HEAD c87d2baa5a2489994ddaa32103d03bbf7c696ede. The pre-existing untracked
Phase9 plan and bootstrap prompt are preserved. The user explicitly authorized
fixing Phase8 before continuing Phase9, without subagents. All failed experiments
and thresholds above remain unchanged. This amendment does not relabel them.

Routing-only snapshot ed3720a7fe159ded28fa261034b3de5c104ef993, clean optimized
build, retained identical supported-domain inputs, all39 process-isolated children
and independent oracles. All gates passed: primary skew exclusive-work ratio
0.6358535417 (36.41% improvement), skew job0.9948873723 (0.51% improvement),
uniform job1.0415432114. Four-host work0.0693004892, job0.6928247024 are limited
confirmation. The small job margin and local variation do not establish a robust
production speedup or prove the cause of the earlier regression.

Raw/source archive: phase8-route-2026-10-22-bundle.tar.gz, SHA256
65c2377a4ccb7bc52d431937f172afede33c3b74cea22eba87a6d8fe33d3905c, currently
under /tmp/ade-phase8-remediation/benchmarks/results. The final evidence release
will also preserve this candidate and the prior failed source-bound bundles.

The subsequent original-prefix candidate is specified before timing in
docs/research/phase-8-remediation-2026-10-22.md and ADR-0015. It retains the
original algorithm and32,768-key table/16 low-hash-bit spill-domain limitation;
extracting incremental ingestion does not extend that domain. A local prepared
task holds one original input batch already aggregated, its exact source cursor,
reservation ledger and bounded prefetch until the all-worker ACK. Resume uses
that same state; replay starts fresh with identical row-order/batching. Full
source fingerprints are still verified before any exchange production. The
independent key-only probe keeps its own read/decode limits; original prefix
preparation has the original task's scan/memory bounds and a separate time/row
observation. Probe-stage wall includes both. The final source must independently
pass the same release matrix before closure can be recorded.

Focused checks: exact packed-frame equivalence across2/3/4 workers including
selective replay/signed limits/sink failure passed. Prefix resume output/spill
identity, exactly-once ingestion, cancellation/sink error/drop reservation
release passed for prefetch0/1. All20 local unit tests passed. Distributed tests
passed54 unit and14 process cases. Initial sandbox tests failed18 socket-binding
cases with Operation not permitted; rerunning with authorized localhost access
passed. A pre-existing default-feature test-helper cfg error was also corrected.
Intermediate clippy diagnostics were fixed; final command results follow below.

Original-prefix snapshot **2123887ad20816a381d296876ca07785a15d0197**, runtime
**e831adcb3f672dd4e3a055801a4c0b92427cb2bc**, passed all39 correctness children.
It FAILED the unchanged primary skew job gate: ratio1.0137313513 (1.37% slower).
Skew work0.6411084192 and uniform1.0578900517 passed; four-host work0.0527355890,
job0.7150198688 do not substitute. Preserve the source/raw failed bundle
phase8-prefix-2026-10-22-failed-bundle.tar.gz, SHA256
fe0a1dd39b6623e6c4d64bb80f43fba292751a983acadca01050f5d5a0871ae8.
Its Phase6 release regression passed all declared static/normal/recovered and
unrecoverable cases, but does not close this candidate's failed timing exit.

The next listener-dispatch candidate is predeclared separately in the research
record before timing: blocking accept with bounded handler slots and explicit
shutdown wakeup replaces periodic idle sleeps. No threshold, input, memory,
protocol or comparator changes. Focused tests cover idle wildcard-bound accept,
saturated4-slot cancellation, delayed/stalled I/O and existing process recovery.
Final acceptance remains pending below; no Phase9 source change has been made.


## Formal bounded-v1 closure — 2026-10-22, before Phase 9 source work

**COMPLETE within ADR-0015's fixed48-shard/2–4-process boundary.** The user
explicitly authorized remediation before continuing Phase9. Main remains at
baseline c87d2baa5a2489994ddaa32103d03bbf7c696ede until the requested single final
commit; all code/evidence snapshots were committed separately in temporary
clean repositories, never presented as commits on the user's branch. The closing
runtime source is **ab7404b1c7095ce3e8b60d79f944a78cd9787c34**, crates tree
**432cc7dd212ec41ad6fdec93bc97f957df5a514d**. Documentation/evidence-packaging
amendments after this measurement do not change that runtime identity.

The first listener suite A passed all39 correctness checks but FAILED timing:
work1.3799937472, job1.0982230575. Producer work rose to0.79–2.65seconds versus
prior0.17–0.19seconds, then returned near normal. Post-run host load was
18.32/51.02/39.29 with substantial compressed memory. This does not prove the
cause or excuse A. A's failed bundle is preserved with SHA256
bb802b4b88048c92f43a081c596c830eb81e74ea0090fd8418e98d7d5f1ab581.

Before any further samples, the supplemental stability confirmation was saved
and SHA256-pinned as **19caad75efcdfb4264ca95e017e8d193c416f1648ae4f515f0db9eff08b64269**.
It declared exactly two additional full unchanged39-child suites B/C, readiness
criteria before each, independent passes in BOTH B and C, and all-nine pooled
primary medians INCLUDING A. There was no selection of favorable iterations.
The original matrix/configs/controls/numeric gates were already committed in
ab7404b. **Deviation:** the supplemental declaration was saved in this session
before B/C, separately from the frozen runtime commit, rather than claiming a
new commit identity for an already-built binary. Its literal text, helper,
telemetry, failed readiness attempt and all raw outcomes are in the bundle.

| Suite | 2-host skew max-work ratio | 2-host skew whole-job ratio | Uniform job ratio | Outcome |
|---|---:|---:|---:|---|
| A |1.379994|1.098223|1.005904|FAIL, preserved|
| B |0.661829|0.955266|1.023503|PASS|
| C |0.224558|0.723303|1.031348|PASS|
| All9 measured samples per primary arm |0.228727|0.741231|1.026599|PASS|

All117 A/B/C child processes passed the independent oracle, exact source/build/
dataset/config identity, decision/digest reconstruction, actual active/gather
counts, row/byte/terminal conservation, prefix-state observations and genuine
pinned replay/restore checks. Every warmup/measurement remains. The combined
verifier reruns each suite verifier, checks matching cross-suite identities and
117 distinct child PIDs, requires nine primary measurements, BOTH B/C passes
and the original three pooled numeric gates. Source/count drift cannot pass.

Variation remains substantial: pooled ratios must not be attributed entirely to
listener dispatch, routing or skew adaptation. B's smaller measured job gain
(4.47%) is also retained. These are local supported-workload acceptance results,
not a statistically significant or production speed claim. Four-worker pooled
work0.070215/job0.689292 is confirmation only; raw-hot-key pooled job1.081184
exposes overhead and is not evidence of legally splitting one key.

### Final validation and artifact access

| Command/evidence on the closing code | Result |
|---|---|
| cargo fmt --all --check | PASS, clean ab7404b checkout |
| cargo check --workspace --all-targets --locked --offline | PASS; ade-p8-ab7404-check.log |
| cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings | PASS; ade-p8-ab7404-clippy.log |
| cargo test --workspace --all-features --locked --offline | PASS,118 tests plus doc tests:55 distributed unit,14 process,20 local and29 benchmark/integration |
| no-default-feature distributed library check | PASS on identical closing code |
| cargo build --release --workspace --locked --offline | PASS;4m57s final rebuild |
| bash scripts/smoke.sh (closing release binaries) | PASS, independent oracles; not timing evidence |
| Phase6 acceptance | PASS, five static modes, normal/recovered2/4-worker cases and explicit missing-snapshot negative |
| Phase7 empirical economic acceptance | PASS; expected4971026589.579 vs always6692075354.036, never9388127475.849499 and threshold9379011544.653 nano-USD proxy |
| Phase8 original matrix A/B/C plus combined verifier | all117 correct; A failed, B/C and all9 pooled criteria PASS |
| Copied-artifact forged original-prefix count and layout digest | both rejected with correctness_passed=false |
| Source bundle/inventory audit | every SHA256 matched; Git clone reproduced exact clean source SHA/crates tree |

[Immutable public evidence release](https://github.com/jasonkli21/adaptive-dataflow-engine/releases/tag/phase8-evidence-2026-10-22).
[Closure bundle](https://github.com/jasonkli21/adaptive-dataflow-engine/releases/download/phase8-evidence-2026-10-22/phase8-closure-2026-10-22-bundle.tar.gz)
SHA256 **53db547ab743666afb379f12fdbbd75b79e9c2ad0556f7747d86e7a5b7bb41d9**.
GitHub reports draft=false, immutable=true and the matching server-side digest.
The release tag points to planning baseline c87d2ba, deliberately NOT the measured
source. The bundle contains exact Git objects/source for ab7404b, all raw A/B/C
results, commands, host/readiness telemetry, oracles, negative evidence, Phase6/7
regressions and quality/smoke logs. Clone source.bundle and follow REPRODUCE.md.
The original four failed bundles, the routing-only experiment, prefix failure
and listener-A failure are separately published with their original checksums.
This closes the previously open external-auditability finding.

### Transition and boundary reconciliation

Original partial aggregation now really ingests one batch before the layout
commitment, retains its exact state/cursor and resumes after the all-worker ACK.
The independent bounded key probe remains separately metered. This is an
observed transition after original operator work, not a full-stage planning
barrier or a mid-exchange epoch switch. Tests prove identical resumed frames,
spill bytes and counts, source/fingerprint checks before exchange, selective
replay equality, sink/cancellation/drop cleanup and idle/saturated listener
shutdown. The table capacity,16 low-bit aggregate spill buckets,48 ownership
shards, hash1, wire2, result1, snapshot identity, failure window and one-episode
attempt-safe gather are unchanged. Current private control framing is ADETCP10.

Many distinct keys in a single virtual shard remain unsplittable in v1. The
262144-key engineered fixture remains outside the original aggregate spill
capacity; no broader key domain, hot-key salting, process RSS cap, independent
network-host durability, scheduler, SQL or streaming claim is made. Phase9 is
now authorized as the next separate capability boundary by the user's request;
Phase8 fixes above are not counted as Phase9 implementation.
