# Phase 7 implementation report — Expected-cost materialization v1

Date: 2026-10-16. Baseline main: `ae5d0f03d9532320e7644d1d8fa34849947fe8c5`.
The supplied plan matched main exactly; no intervening commits were present.
This report is completed with measured release evidence below before handoff.
Final commit identity is provided by the single implementation commit containing
this report and embedded verbatim in final acceptance `verification.json`.

## Independent review evidence note — 2026-10-21

The original acceptance figures below describe implementation commit
`dc1796ed6b04821f032336142e0417190df270fa`. Its final results directory is
gitignored and was not available in the independent review, so those reported
numeric claims cannot be independently audited from this source tree alone. They
are historical evidence for that commit, not evidence for the remediation in this
report. A clean committed run can be packaged with
`python3 scripts/phase7-bundle.py RESULTS_DIR OUTPUT.tar.gz`; the archive contains
the raw results, verifier output, source commit, benchmark identity and checksums.
The generated bundle remains a local ignored artifact until someone attaches or
publishes it for other reviewers.

## Implemented contract

`cost.rs` implements a pure checked integer model, `materialization.rs` complete
staging/receipts/strict restore, and existing worker/coordinator/recovery modules
propagate and validate facts. The benchmark adapter preserves independent oracle
ownership, strict options and static/threshold defaults. No Phase 8 topology work
or broader recovery was introduced.

Let R=recompute work + whole-source byte/request cost + `Penalty(recompute_ms)`;
D=restore byte/HEAD+GET request cost + `Penalty(write_ms + restore_ms)`;
W=PUT byte/request + expected capacity, without SLO penalty. Persist iff
`1_000_000*W + (1_000_000-p)*Penalty(write_ms) + p*D + 1_000_000*margin < p*R`
(p in ppm). The sequential failure path gets one deadline clamp. Ties skip.
Recovery compares D<R directly, ties recompute. `cost_mode=always|never` are
explicit baselines, not policy changes disguised as expected-cost decisions.

After process loss, recovery carries elapsed time across its sequential closures,
starting with the slowest producer's measured sum of complete-copy upload times.
Producer overlap before loss is approximated by that slowest producer path.

Measured: sum of exclusive producer operator work, producer wall time (separate),
whole-producer source file count/byte sizes, encoded output counts/bytes,
staging byte/read/write elapsed facts, actual object PUT bytes/requests/elapsed,
restored counts/bytes/read requests/elapsed, replayed work/source facts and query
wall time. Configured: nano-USD prices, ppm probability, source/object throughputs,
capacity retention horizon, slack/linear penalty and savings margin. Topology:
consumer_count=1, one complete transfer per producer/destination. Derived: time
estimates, alternative prices/probability numerators and actions. Unsupported
fanout is rejected. Probability is not observed health; throughputs are not
claimed to be calibrated. Snapshot hashing/verification, encoding/network waits,
final merge/control/idle time and cloud billing are excluded from the producer
work proxy. Source accounting charges one logical file reopen per replay, not
filesystem page-cache misses or extra snapshot-verification reads.

## Mechanism, protocols and lifecycle

Producer staging is the meaningful plan deviation: it avoids an observation/ACK
schema extension because local execution already yields work before terminal
finalization. Normal delivery ACK validates schema/sequence/terminal reception;
selected staged bytes are verified against the original manifest and uploaded
before the producer task returns. The coordinator receives the compact fact before
the existing all-producer gate permits injected loss. Staging reuses contained
spool I/O; one outstanding frame per log, topology-bounded decisions/receipts,
no per-segment coordinator records or key arrays. Normal-case staging is charged
and reported, not hidden in delivery metrics. Full copies use
`complete-v1/{execution-uuid}/job-J/`; the coordinator-created UUID is included
in receipts and cleanup requests without changing logical IDs. Full copies use
that isolated prefix; threshold suffix objects cannot be restore candidates.

The parent LocalCluster fixture root survives an actual child worker exit and is
shared by all workers through the provider-neutral object-store boundary.
Failure domain is strictly worker_process_surviving; default ephemeral local
objects are ineligible. No host/AZ/cloud durability, remote shuffle deployment,
persistent coordinator/catalog or cross-job reuse is claimed.

Receipts bind original logical ID/attempt, counts/fingerprint, backend/domain,
layout/version and write facts. Restore uses bounded encoded-frame decoding and
sequence validation through the same Sender/receiver path, reconstructs an
attempt-1 manifest, and validates equality against attempt 0 before the existing
final merge/gather commit. Missing/corrupt objects fail the sole episode without
partial output or fallback after contamination. Recompute still uses the original
whole-producer closure and only missing destination. Mixed actions are supported.

Cleanup streams job-prefix deletion from a reachable survivor even after writer
loss or earlier task cancellation; validates job watermark and is observable.
Cleanup failure invalidates otherwise successful execution; original failures are
preserved and logged. Parent teardown after kill/wait is a final safety net for
poisoned/unresponsive I/O. Tests assert object files are gone before cluster drop.
Disk volume has no quota; local task budgets can overlap; exchange/staging/codec
memory is separate and is not a process RSS cap. Non-cooperative I/O retains its
owner and poisons the worker using existing containment semantics.

Control framing is ADETCP06 (prior ADETCP04/ADETCP05 rejected before decoding); the
exchange header/data/terminal/ACK shapes stay wire v2; hash and benchmark result
schema remain v1. Control payloads remain <=1 MiB. Source/plan facts are bounded
by source file count and N² logical transfers for static N=2–4 workers.

## Predeclared experiment and criterion

Before release measurement ADR-0014 and scripts fixed 200,000 rows/four files,
32,768 keys, seed 42, 2 workers, 32 MiB task budget, 1 MiB exchange buffer,
prefetch 1. Two isolated samples per success and real-loss arm, independent oracle.
Low (ppm/source/write/read byte prices)=(1000/0/1000/1),
high=(900000/1000/1/1), expensive-restore=(500000/1/1/100000).
Expected, always complete copy, never complete copy and legacy threshold are
compared; all other prices/defaults are retained in artifacts.

Empirical score uses actual original/replayed work/source bytes, observed
staging elapsed charged conservatively at the work-price rate, PUT bytes/requests,
restored bytes and HEAD+GET requests, plus any legacy delivery object I/O. It
never scores the policy's own alternative estimates. Pair scores using
(1-p)*success+p*failure. These are declared proxies, not production cloud billing.
Capacity and SLO prices are explicitly zero in the suite; focused tests establish
their modeled arithmetic and deadline/throughput decision changes.

Fixed exit criterion: all oracle/identity/integrity checks pass; expected skips in
low/expensive-restore and persists/restores in high; valid always copies recompute
in expensive-restore; aggregate expected score below always, never and threshold;
each regime's mean normal expected wall time <=5x threshold. No criterion moves
after measurement. This is a local research bound, not a production overhead claim.

## Development checks and failed experiments

The initial compile exposed two baseline Phase 6 remediation compile defects:
the empty replay branch referred to an undefined manifest after discarding its
return value; the research exit hook did not map distributed errors into the
local sink error type. Both were fixed and regression tested.

Sandboxed socket tests failed with Operation not permitted; rerun with localhost
networking enabled passed. An SLO unit test initially kept source reread slower
than writes while expecting skip; the fixture now varies the intended throughput.
The new process test's generic adaptive helper initially assumed every final used
adaptive delivery, including recovered local-materialized finals; it now checks
the actual committed attempt. First lint runs identified enum growth, missing
error/documentation details and cohesive functions exceeding the line heuristic.
Cost configuration is boxed to avoid inflating benchmark enums; remaining narrow
lint exceptions state their architectural reason. Logs preserve failed checks.

Focused distributed tests pass, including new full/empty copies, writer-session
independence, forged/suffix/ephemeral rejection, corrupt/missing object clean
failure, staging cancellation, mixed recovery planning and real process loss
restore/recompute/never plus post-loss cleanup. Legacy static modes, threshold,
Phase 6 recovery and default-off research hooks remain tested.

The review also moved all full-copy uploads after every delivery terminal ACK,
so slow object I/O cannot leave another delivery socket idle before its terminal.
An explicit `ADE_DISTRIBUTED_METRICS_FILE` benchmark sidecar appends one bounded
JSON diagnostic per successful execution (including child PID); acceptance sets
it only for isolated measured runs. The existing harness captures successful
child stderr, so this preserves actual measured-child observations without
changing result schema v1 or substituting diagnostic-run estimates. Query wall
times include this uniform instrumentation overhead. Sidecar failure fails the
benchmark rather than silently losing evidence.

## Final validation and release evidence

The final workspace gate passed formatting, workspace all-targets check/clippy
(all features, warnings denied), no-default-features distributed check, workspace
all-features tests, debug workspace build, smoke and diff whitespace. It passed
93 tests plus doc tests; subsequent empty-transfer cost truthfulness added one
focused test, and the final distributed suite passed 42 unit + 5 process tests.
The final clippy rerun passed after those additions. The existing Apple debug
linker emits an oversized unwind-section warning while linking successfully.
Smoke's single-iteration cross-engine timing is not used as economic evidence.

Commands actually run (CARGO_HOME=/tmp/ade-cargo, RUSTUP_HOME=/tmp/ade-rustup,
PATH including /tmp/ade-cargo/bin; stable Rust 1.99.0 on macOS/aarch64):

```text
cargo fmt --all --check
cargo check --workspace --all-targets --locked --offline
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
cargo check -p ade-distributed --no-default-features --locked --offline
cargo test --workspace --all-features --locked --offline
cargo build --workspace --locked --offline
bash scripts/smoke.sh
cargo test -p ade-distributed --all-features --locked --offline
git diff --check
bash -n scripts/phase7-acceptance.sh
python3 (in-memory AST parse of scripts/phase7-verify.py)
```

Release build/acceptance results are appended below after measurement; no unrun
release check is claimed passed.
Raw local logs use /tmp/ade-phase7-*.log; release artifacts use ignored
benchmarks/results/phase7-acceptance and phase7-phase6-regression directories in
the clean temporary checkout, copied back to the working repository for handoff.

## Limitations and Phase 8 handoff

Transfer-level decisions, whole-producer source closure, configured objectives and
throughputs, one post-producer final-worker loss, one quiescent survivor/episode,
static 2–4 topology and shared-local fixture only. Staging always writes locally,
even when skipping persistence; no adaptive disk quota or shared RSS budget.
Missing/corrupt copies cleanly fail rather than retry. Expected cost is a narrow
linear proxy; no price discovery, failure estimator or global deadline scheduler.
Phase 8 can consume explicit complete-copy facts and pure replaceable economics
while focusing on topology/skew only after new authorization.

Empty-transfer economics preserve the original measured production observation but
price replay compute/source/time as zero, matching Phase 6's synthesized empty
terminal. An additive work-time plus source-throughput estimate is deliberately
conservative: measured operator work can already include scan I/O waits. It is
not a claim of independently measured CPU and I/O service times.

## Measured clean release acceptance

The first clean release build passed in 5m 31s at
`853bdb6c495458a21a843bf50b2b41b89440d828`. Runtime source tree (`HEAD:crates`)
is `a2439c5d5995a3844c222813b48bd9f19e48ef32`. All 48 isolated measured children
(two samples in each success/failure arm) and 24 extra diagnostic validations
matched the independent oracle. The complete predeclared verifier passed.

| Aggregate empirical score (nano-USD proxy) | Value |
|---|---:|
| expected | 4,971,476,314.01 |
| always complete copy | 6,583,150,528.156 |
| never complete copy | 9,389,915,146.569 |
| legacy threshold | 9,380,630,089.3465 |

Expected's aggregate score was 24.48% below always, 47.06% below never and
47.00% below threshold. This is a suite result under declared synthetic prices,
not a claim that expected wins every regime or predicts production cloud costs.
Threshold was cheaper in the low/expensive-restore regimes because it avoids
staging; expected's high-failure savings outweighed that real overhead. Expected
and always are effectively the same choice in high; small local work differences
are noise, not an additional policy win.

| Regime | Expected normal mean query ms | Threshold mean ms | Ratio |
|---|---:|---:|---:|
| low failure / expensive writes | 53.700 | 46.444 | 1.156x |
| high failure / expensive reread | 87.193 | 45.534 | 1.915x |
| expensive restore | 54.271 | 44.967 | 1.207x |

All ratios passed the fixed 5x local research bound. Even skipped copies staged
1,606,624 payload bytes per job. The first low normal sample spent 3,095,914 ns
in staging writes and performed no complete-copy reads/PUTs. The first high normal
sample staged/read 1,606,624 bytes and PUT 124 objects with the same payload bytes;
staging write/read waits were 3,334,372/1,831,285 ns. These are observations of
one sample for accounting illustration; suite scores/timings use both samples.

High-failure expected runs restored 62 segments / 801,200 bytes through 124
HEAD+GET requests after the real target process exited, with zero source replay.
Expensive-restore always-copy runs retained complete receipts yet recomputed all
62 missing segments from four source morsels, with zero object reads. Low and
expensive-restore expected runs skipped all complete copies. Every recovered
manifest matched attempt 0 after only the attempt change, and gather committed
partition 0 at attempt 1 while reusing partition 1 at attempt 0.

A copied-artifact mutation forged a retained receipt fingerprint while keeping
counts unchanged; the verifier rejected it (exit 1), retained at
`/tmp/ade-phase7-verifier-forgery` with log `/tmp/ade-phase7-verifier-forgery.log`.
Runtime negative coverage passed for ephemeral/suffix/forged receipts, missing or
corrupt objects, invalid/overflowing prices, old control version and cancellation.
The process test verifies shared objects drain before cluster teardown after
writer process loss; parent teardown removes its root.

The clean Phase 6 release regression also passed: eight positive isolated runs
plus diagnostic validations cover all five static modes, recovered local and
threshold adaptive, normal four-worker adaptive, unchanged oracle/conservation,
partial-gather fencing and original logical identity. The explicit no-snapshot
negative validation/child both failed as intended after real process exit;
there was no partial successful outcome.

Actual additional commands:

```text
cargo build --release --workspace --locked --offline
bash scripts/phase7-acceptance.sh
bash scripts/phase6-acceptance.sh  # phase7-phase6-regression output/data paths
python3 scripts/phase7-verify.py /tmp/ade-phase7-verifier-forgery  # expected refusal
```

Initial clean evidence is preserved under
`benchmarks/results/phase7-acceptance-initial` and
`benchmarks/results/phase7-phase6-regression-initial`, with matching dataset
folders. Logs: `/tmp/ade-phase7-release-build.log`,
`/tmp/ade-phase7-release-acceptance.log`, `/tmp/ade-phase7-phase6-regression.log`.
At the time, the single commit was amended to include this measured report and
README wording corrections; runtime source was unchanged. After that amendment,
commit-bound release acceptance was rerun from the clean temporary checkout into
distinct final paths:
`benchmarks/results/phase7-acceptance-final` and
`benchmarks/results/phase7-phase6-regression-final`. Their `verification.json`
files embed the literal final SHA and final pass status. The final handoff states
those results; initial measurements above are not relabelled as final-SHA data.
Phase 7's predeclared exit passed; Phase 8 remains unauthorized.

Source byte accounting is the sum of physical file **sizes** in the reopened
producer closure, an explicit logical reread proxy. It is not a measured Parquet
column/range read counter, OS device traffic or cache-miss count. Object requests
are provider API operations in the shared filesystem fixture, not measured HTTP
billing. The decision and empirical score expose `put_head_get_proxy_v1`; cleanup
LIST/DELETE request prices remain excluded, so this is not a full provider
lifecycle request-cost objective. Copy-verification CPU and final aggregation
work also remain outside the monetary scores; their wall overhead remains in the
reported query time. No byte/request field is presented as a cloud bill.

## Review remediation evidence workflow

For a clean checkout of the remediation commit, run the release suite into a
distinct ignored directory, then package its passing verifier result:

```bash
ADE_PHASE7_DATA=benchmarks/datasets/phase7-review-remediation \
ADE_PHASE7_RESULTS=benchmarks/results/phase7-review-remediation-final \
bash scripts/phase7-acceptance.sh
python3 scripts/phase7-bundle.py \
  benchmarks/results/phase7-review-remediation-final \
  benchmarks/results/phase7-review-remediation-bundle.tar.gz
```

The bundle records the source commit and benchmark identity, includes raw child
results and verifier stdout, and carries per-file and archive SHA-256 checksums.
It is intentionally ignored by Git; it must be attached or published separately
before another reviewer can download it.
