# Repository readiness and Phase 10 remediation — 2026-10-26

**Current outcome: all six unchanged release timing guards PASS on continued source
`ee9f8da`, with 62 successful children, 49 measured samples and three verified
reached failure stages.** The final workspace gate passes 192 Rust and 18 Python
tests. Static remains the default. Phase 10 is still PARTIAL because physical
membership is fixed. Uniform4 has only a 0.378ms margin to its 1.15 guard in this
local campaign. Historical failures and the superseded stopping decision below
remain explicit; later success does not reclassify their sources or samples.

## Identity, authority and scope

The user requested the attached readiness/remediation handoff and a few local
logical commits. No subagents, push, PR, history rewrite or Phase 12 work was used.
The reviewed and actual baseline was
`bcfdb05f1b944b6c9c44317e561a4e81d2211b14`, crates tree
`36c7bac4d05e288f03b9321620f955ee2b80edd6`. The only existing untracked file,
`sol-medium-phase-0-bootstrap-implementation-prompt.md`, was preserved and excluded.

The first measured implementation candidate is
`fcc325ec4364fe75a4af7df0b61485ad6bf82b4e`, crates tree
`f0f8d45fc068e6a573ce24b6c76fee8db2a1390a`. Both releases were built from the same
clean temporary checkout at their respective commits, with
`CARGO_TARGET_DIR=/Users/jasonkli/projects/adaptive-dataflow-engine/target cargo build --release --workspace --all-features --locked --offline`.
Captured binaries are preserved separately. The final handoff commit contains
this report/documentation; the correctness-only Python CI step is in ee9f8da; it does not relabel measurements as a different
source. Resolve it with `git log -1 --format='%H %s' -- docs/reports/repo-wide-readiness-2026-10-26.md`.

Host: Darwin 27.0.0 arm64, Apple M3, 8 cores, 24GiB RAM; Rust/cargo 1.99.1 stable,
system allocator, locked cached dependencies, release opt3/thin LTO/codegen1/debug1.
Localhost processes share one filesystem and failure domain. Readiness is a
premeasurement condition, not proof of continuous host isolation.

## Corrections and maintained contracts

**R3, evidence integrity:** release-evidence schema v2 requires exactly three
referenced macOS arm64 readings with exact top/vm_stat/uptime/ps commands,
host/kernel/session binding, positive monotonic and wall times, at least 10s
between samples, consistent clocks, each idle >=70%, load1 <=8, nondecreasing
swapouts with delta <=1000 pages, and no compiler/benchmark/worker process.
Missing/malformed evidence or a legacy bundle is UNVERIFIED. Darwin bare daemon,
login-shell and parenthesized exiting-process names are parsed explicitly; hidden
competitor suffixes refuse. Ten seconds of settling after source/binary identity
I/O precedes capture; the three samples and every numeric threshold are unchanged.

A noncircular SHA256 inventory binds readiness/postflight, source, exact commands,
statuses, manifests/oracles, raw logs, metrics, results and unique run IDs. Offline
verification checks every reference, command time window, result source/build,
run/slot/PID, original sample inventory and source/lease/terminal conservation.
The outer closure inventory additionally covers derived verification and attacks.
These hashes detect copied-artifact changes; they are **not signed host
attestation** and cannot prove an entirely fabricated, consistently resealed host.
Original bundles remain unchanged and use their archived verifier; they cannot
be promoted into v2 evidence by the new verifier.

Expected negatives now require structured research-hook stderr v1 emitted from
the actual lease mechanism: worker0 sent a nonempty exchange frame before exit;
a worker0 lease with heartbeats disabled actually expired; or a spot notice on
a live worker0 lease reached its one-millisecond grace deadline after a completion.
The records bind execution/fence, descriptor, nonce, worker, process and stage
facts to the exact captured command/manifest. Expiry/notice require the matching
failure cause; every negative must fail with empty stdout. Parse errors, missing
worker binaries and other preflight errors cannot substitute for reached faults.

**R4, reproduced liveness coupling:** the old serial service blocked healthy
20ms renewals behind a partial accepted peer for longer than an 80ms lease.
`stall-before-retry.log` preserves the focused failing reproduction. The new
single-thread service polls at most `2*N+1` nonblocking connections, one bounded
read/write per peer per turn, and retains one serialized ledger. For N<=4 this is
at most nine admitted sockets/frames, each with a 500ms absolute budget and 1MiB
payload cap plus framing/allocation/JSON transients. Saturation aborts and closes
the peer set. No handler threads, unbounded user-space queue or reassignment were
added. One stalled peer cannot monopolize renewal service; malformed/truncated or
timed-out peers still fail the job. This does not promise availability against
multiple stalled peers, OS scheduling stalls or malicious traffic.

**R1/R2, one bounded performance candidate:** full catalog admission hashing and
both per-lease content passes remain. One reusable 256KiB scratch buffer per
verification call replaces 8KiB reads; cancellation/deadline checks run between
every chunk. This reduces read/dispatch calls without substituting metadata for
BLAKE3 identity. Tests exercise cancellation/deadline between chunks, same-size
replacement and symlink target drift between verified passes. File immutability
remains required: neither version provides transactional isolation or detects an
adversarial change-and-restore during decoding.

New optional diagnostic counters separate coordinator source admission,
ping/prepare, gather collection, scan/open/decode pulls and aggregate work.
Routing/synchronous encoding-write time, hash verification, acquire/wait, ACK,
final source pull, producer completion, spill/managed peaks, frames and harness
child RSS remain separate. Scan timing combines open/decode pulls; eager adapter
metadata and terminal/commit/control overhead can remain in residual wall time.
External `/usr/bin/time -l` logs include the whole command and its validation;
their CPU utilization/RSS are not per-query or per-stage CPU measurements.

Static defaults, five retention modes, immutable manifests, independent
DataFusion oracle, benchmark schema1/child protocol and ADETCP12/wire2/hash1 are
unchanged. One manifest file remains one lease; exclusive ownership, truthful
per-file output, ACK after provisional sending, i128-to-Int64 contribution
splitting, spill accounting and N×N terminal fingerprints remain. No coalescer,
coarse scheduled unit or hidden worker hash table was introduced. Recovery still
permits only the established single final-worker episode after original
producers finish, with original IDs, lost-range/file closure, fingerprint
validation and attempt-safe gather. Leased replay, producer/multiple loss,
poisoning and recovery failure cancel survivors. Task/exchange budgets overlap;
control/hash scratch and decoder/kernel/allocator transients are not a process
RSS cap. Physical membership remains fixed at 2–4 processes.

## Correctness evidence

Logs are under ignored `benchmarks/results/phase10-remediation-20261026/`.
The baseline full gate/smoke passed with 178 Rust tests. The final candidate passed:

- `cargo fmt --all -- --check`.
- `cargo check --workspace --all-targets --locked --offline`.
- `cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings`.
- `cargo build --workspace --all-features --locked --offline`.
- `cargo test --workspace --all-features --locked --offline`: **183 passed**, no failures or doc-test failures.
- `bash scripts/smoke.sh`: passed; its one-sample engine comparison is not timing acceptance.
- `PYTHONPYCACHEPREFIX=/private/tmp/ade-phase10-pycache python3 -m unittest discover -s scripts/tests -p 'test_*.py'`: **17 passed**; also passed after the final process-disguise check.
- `git diff --check`; exact 76-file runtime inventory equality with final-quality-3.

The final gate continued after fixing Clippy documentation/test-style findings
and a macOS test setup error: configuring a timeout on an already reset socket
returned EINVAL. The timeout now precedes the send; teardown asserts EOF/reset,
not a timeout. All failed logs remain. The first reproduction's exact-name
filter selected zero tests; only the subsequent real failing run is cited.
An initial release-build cwd error, Python development indentation failure and
inherited home-cache permission failure are also preserved. `/private/tmp` cache
and loopback-enabled execution were used thereafter. No failed correctness path
was reclassified as passing.

Selected legacy coverage in that workspace gate includes local oracle/spill/sort;
all five static modes, poisoning and cancellation; real single-final replay,
strict-prefix gather isolation and source-drift refusal; complete-copy
restore/recompute/cleanup and cost units; 2/3/4-worker sampled split/coalesce,
incomplete-prefix refusal and pinned replay/restore; duplicate-preserving joins,
strategy/context/refusal/pressure and adaptive partial exactness; real leased
2/3/4 coverage, activation/drain, stale ownership, wide sums, death/expiry/notice;
and Phase 11 native IR, SQL/CLI, distributed aggregate/join and output-cap refusal.
The new Python CI step runs correctness tests only, with bytecode writing disabled.
Ubuntu/MSRV CI was not executed locally. No Phase 6–9 historical timing campaign was replayed. Their original source
identities, failed experiments and modeled-cost (not cloud billing) qualifications
remain unchanged. Phase 11 remains **COMPLETE bounded v1** on correctness evidence.

## Controlled diagnostic

`baseline-diagnostic-6` records one complete controlled campaign at bcfdb05: the
frozen 32-file/4m-row layouts plus deterministic 32k-row fixtures, static and
leased_v1 at 2/4 workers, one warmup and three samples per arm. All 64 children
and 48 diagnostic measured samples remain; only 32 children use the full fixture.
This is a diagnostic inventory, not the 13-arm release inventory. It uses the
same spec, memory/batch/exchange/recovery/hash settings and manifests paired
within arms. The candidate diagnostic reuses those exact manifest bytes.

| Baseline diagnostic (ms) | Static job | Leased job | Static last producer | Leased last producer |
|---|---:|---:|---:|---:|
| Imbalanced, 2 | 181.192 | 368.409 | 165.856 | 326.900 |
| Imbalanced, 4 | 186.867 | 278.409 | 158.010 | 225.027 |
| Uniform, 2 | 149.447 | 377.579 | 133.066 | 336.305 |
| Uniform, 4 | 79.581 | 278.399 | 56.533 | 224.025 |

For one measured uniform/4 child, per-worker operator work is 54.7–55.7ms,
source hashing 44.1–44.3ms, acquire/wait about 12.2ms, ACK 6.2–6.9ms. Do not sum
those four workers into job latency or add final merge wall time to producer
wall time: the tasks overlap. There are 32,768 leased partial rows/128 frames
versus 4,096 static rows/16 frames at four workers; at two workers, 32,768/64
versus 2,048/4. New-source diagnostics below separate coordinator admission from
worker critical-path costs, and report bounds as optimistic estimates rather
than an invented CPU attribution.

All premeasurement attempts remain: #1 exceeded load; #2 passed readiness but
failed data generation because the parent directory was missing; #3 and #5 had
initial idle below 70%; #4 refused a real Darwin `(git)` process name before the
parser correction. Each took **zero measured samples**. #6 passed after identity
I/O settling. The parent-path/parser fixes and settling period changed no layout,
sample selection, timing guard or runtime baseline. Original Phase 10 failed
bundles were neither edited nor removed.

## First candidate: diagnosis and superseded stopping decision

Candidate diagnostic #1 exceeded load (10.32/8.89/7.98); #2 had a 67.67% idle
reading. Both took zero measurements. #3 passed and retains all 64 children/
48 diagnostic samples. It reuses the **identical** baseline manifest bytes.

| Candidate diagnostic (ms) | Static job | Leased job | Leased last producer |
|---|---:|---:|---:|
| Imbalanced, 2 | 179.202 | 323.258 | 281.604 |
| Imbalanced, 4 | 182.488 | 249.193 | 199.267 |
| Uniform, 2 | 111.634 | 321.265 | 280.817 |
| Uniform, 4 | 79.409 | 243.584 | 193.918 |

Absolute leased job times decreased relative to the earlier diagnostic; static
uniform/2 also varied substantially (149.447 to 111.634ms). This before/after
comparison does not establish a portable causal speedup. The new release below
compares each leased arm with its own exact source/config static control.

Illustrative uniform/4 child: admission 78.501ms, prepare 4.373ms, gather collection
0.189ms; worker hash passes 38.18–38.58ms, scan/decode pulls 42.51–42.99ms,
aggregate work 9.01–9.16ms, acquire/wait 5.30–5.64ms, ACK 3.65–4.34ms. The
producer/final operator and I/O timings are exclusive wall components, not CPU.
Frames/partial rows remain unchanged; task managed/spill and child RSS observations
are in every diagnostic sample. Whole-command time logs for uniform/2 leased are
2.97s real/2.90 user/0.74 sys before and 2.77/2.88/0.62 after; uniform/4 are
2.64/3.02/0.89 and 2.51/2.99/0.76. These imply about 1.23/1.26 and 1.48/1.49
cores for the full command envelope, including validation, not query CPU rates.

An optimistic bound **holding the measured hash/scan work fixed** removes ALL
aggregate, routing, send/control, final and gather costs. Admission precedes
production, and each producer's hashes and scan pulls execute sequentially.
Thus admission + max_worker(hash + scan) still has median 236.346ms (range
235.790–237.180) at 2 workers, and 160.422ms (160.068–166.052) at 4. The diagnostic
uniform limits are only 128.379/91.320ms. This uses the maximum producer, never
the sum of parallel producers or overlapping final times. Merely eliminating
partial amplification/control cannot clear these guards with the measured source
path. Verified decoder-pass sharing/immutable buffering or a different scheduling
contract needs separately scoped research. Coarser work would be a new variant;
content checks, file ownership and ACK truth must not be dropped to obtain a win.
Stop after **one measured candidate**, rather than spend the second permitted
iteration on a profile-inadequate coalescer or tune until a favorable median.

## Unchanged 13-arm release: FAILED

`release-1` passed readiness: idle 81.31/80.12/81.30%, load1 3.05/3.21/2.95,
swapouts 1506142/1506142/1506142 pages; no competing compiler/benchmark/worker.
All three readings, postflight, command time windows and kernel/host binding
passed independent offline v2 verification. Bench and worker paths were explicitly
pinned to the captured sibling binaries; see `release-launch.json`.

All **13 arms, 62 successful children, 49 measured samples and 3 reached-stage
negatives** are retained. The original one-warmup + five measured primary/two
worker and one + three confirmation/four worker counts are unchanged. No sample,
arm, threshold or failed attempt was removed. Source and binary identities:

- Source archive SHA256: `4bb31465cc11a6b6b039b9fe1eae7950181ce3196162e336b1e7a8e229cf3c82`.
- ade-bench SHA256: `a07fa0de415fec9ed06670ba88ec60df017358cbdc2c8c5bcf77c5ab31950ec2`.
- ade-worker SHA256: `6f89b911da9aef7c9e8301306fee4aa75596dd439ad75f2ac18868067085d5ea`.
- Manifest fingerprints: imbalanced `57717502af0ffb5e84caf1c45c81e20336cca434a2ee6e5a321f9121d7e2e96b`; uniform `91661e8edf1585cf59dbe78e1308cc18e68faad03bd6f28cd4499fa6237cefa2`.
- Both logical fingerprints remain `a568675dab5518ae407fb3601b3f8057fb14156d79ca3ad4703d29813821e139`; physical bytes 83539241/83626120. Diagnostic/release manifests compare exactly equal.

Every child passed the independent oracle, source/config identity, pure schedule
reconstruction, full unique file/nonce/completion coverage, N×N terminals and
row/byte/frame/fingerprint/context conservation before gather. The verifier returns
`correctness_passed=true`, `passed=false`, `status=FAILED`. Runner/verifier exit1
is the expected scientific timing failure, not a successful release or query
correctness failure.

| Guard | Workers | Historical f32da29 ratio | New fcc325e ratio | Frozen limit | New outcome |
|---|---:|---:|---:|---:|---|
| Imbalanced job | 2 | 2.074546 | 1.857053 | 0.90 | FAIL |
| Imbalanced last producer | 2 | 2.008443 | 1.700962 | 0.90 | FAIL |
| Uniform job | 2 | 3.400814 | 2.861890 | 1.15 | FAIL |
| Imbalanced job | 4 | 1.556017 | 1.350122 | 0.90 | FAIL |
| Imbalanced last producer | 4 | 1.415445 | 1.249904 | 0.90 | FAIL |
| Uniform job | 4 | 3.493694 | 3.022507 | 1.15 | FAIL |

Historical figures remain attached to source f32da29/runtime96b0a4e; the reviewed
bcfdb05 diagnosis, new fcc325e measurements and final report commit are distinct.

| Release arm | Median job ms | Job min–max ms | Median last producer ms |
|---|---:|---:|---:|
| Imbalanced 2 static | 179.851 | 179.512–181.342 | 164.762 |
| Imbalanced 2 no-steal | 454.920 | 446.719–478.405 | 407.090 |
| Imbalanced 2 leased | 333.994 | 320.613–335.710 | 280.255 |
| Uniform 2 static | 113.350 | 112.183–116.127 | 96.365 |
| Uniform 2 leased | 324.397 | 322.756–335.400 | 281.628 |
| Imbalanced 4 static | 182.457 | 179.467–190.753 | 156.673 |
| Imbalanced 4 no-steal | 437.149 | 431.005–441.916 | 377.481 |
| Imbalanced 4 leased | 246.339 | 242.918–264.071 | 195.827 |
| Uniform 4 static | 81.027 | 79.273–98.588 | 55.843 |
| Uniform 4 leased | 244.905 | 237.480–259.673 | 193.208 |
| Capacity unweighted | 381.973 | 368.810–385.476 | 319.048 |
| Capacity weighted | 378.685 | 371.507–383.031 | 319.628 |
| Participation | 1171.784 | 1162.921–1171.955 | 1106.578 |

Stealing is proved by off-preference grants and reconstruction; no-steal retains
preferred owners. Weighted/unweighted controls both have file shares 2/10/10/10
in all measured samples, with slightly different byte assignments; their small
elapsed difference does not prove a capacity-policy advantage or hardware model.
Participation has actual shares 1/16/15/0, activates worker2, notices/drains worker0
while it owns a lease, keeps parked3 empty, and commits all four original finals
and 16 producer terminals. It is logical participation, not physical autoscaling.

The exit negative emitted one nonempty frame (516 rows) then exited. Expiry had
a live nonce2 lease granted at4ms, no renewals, deadline84ms, failed at84ms. Notice
had a live nonce1 lease granted at4ms, notice at19ms after one completion,
one-millisecond deadline20ms, failed at20ms. Each has the execution fence and
manifest descriptor in `verification.json`, nonzero exit and **empty stdout**.
No unrelated preflight failure counted.

## First candidate: adversarial evidence and historical disposition

Eight existing copied attacks reject: run, slot, source inventory, effective
config, PID, nonce, terminal rows and best-only samples. Nine additional copies
reject altered/missing readiness, altered postflight, and consistently resealed
stale time, fabricated process line, foreign host/kernel, preflight-only negative,
and a valid best-only result/sidecar. The final attack passes check-result but
fails the frozen repetition inventory even after resealing. Originals remain
unchanged. Unit tests also cover malformed command/time/platform fields and
positive threshold/stage boundaries. Every attack and rejection is preserved.

| Finding | Disposition after fcc325e |
|---|---|
| P0 | None independently confirmed in this bounded stabilization; not proof of absence |
| R1, P1 acceptance | **OPEN / FAILED** on new clean source; no scheduler promotion |
| R2, P1 cost | Attributed and bounded remediation measured; mandatory source path still dominates; further design deferred |
| R3, P1 verifier | Fixed for new release-v2 evidence; legacy archived interpretation preserved |
| R4, P2 liveness | Reproduced and fixed within bounded one-peer isolation; saturation/malformed traffic remains fail-closed |
| R5, P2 confidence | Current correctness gate passes; historical timing identities and modeled-cost limits preserved |
| R6, P2 handoff | Current authorization/status reconciled; historical declarations retained |
| Deferred | Physical worker addition/removal, migration, leased failover, verified-pass/coalescer redesign, broader SQL and Phase 12 |

**Decision: conditional GO for the existing bounded research execution paths;
NO-GO for promoting leased scheduling or claiming Phase 10 empirical acceptance.**
Phase 10 remains PARTIAL and its opt-in scheduler explicitly experimental.
Static is the supported research baseline. Phase 11 remains COMPLETE bounded v1.
Physical elasticity is deferred. Phase 12 was not started or authorized.

## Continued reassessment — working evidence

The user subsequently required continued remediation, overriding the initial
stopping decision above. All earlier outcomes and the original six gates remain
unchanged. The following native diagnostics guide implementation; they are not
process-isolated benchmark acceptance and do not replace a fresh 13-arm campaign.
Each uses the exact frozen full fixtures, one warmup plus three samples per
static/leased 2/4-worker regime, fresh worker processes, and an independent direct
Parquet/i128 oracle. All 32 jobs per diagnostic passed correctness. Raw records,
logs and source-file SHA256 inventories are retained as continued-native-N.

Native diagnostic 1 added projected key/value decoding, bounded scoped parallel
full-content hashes, and buffered complete sidecars (including flush/errors in
the original timed interval for both controls and leased arms). The 4-worker
imbalanced guards passed diagnostically, but both 2-worker imbalanced guards and
both uniform guards remained above their limits. Diagnostic 2 added persistent
fully fenced control frames, independent renewal sessions and scoped parallel
leased preparation; five comparisons passed, but uniform4 remained 1.3883 versus
1.15. Diagnostic 3 tested 4MiB reusable scratch and bounded acquire backoff;
uniform4 regressed to 86.090ms versus 61.029ms static. This negative experiment
remains recorded; larger reads are not automatically faster.

An isolated full-file hash matrix (hash-probe-source and hash-probe-uniform.jsonl)
checks every canonical fingerprint and retains five repetitions for each
2/4-worker, serial/scoped-thread and 256KiB–4MiB chunk setting. It confirms the
benefit of parallel BLAKE3 on this machine but provides no replacement acceptance
claim. Reusable 1MiB scratch retains the existing bounded pool and every
admission/pre/post pass. Further measured costs are file-local setup: duplicate
first-footer reads, unused spill directories, and sorting before an unordered raw
partial drain. Their bounded changes and safety conditions are in ADR-0017; the
next native diagnostic will assess them before freezing another release source.

Native diagnostic 4 removed unused raw-partial sorting, empty spill-directory
creation and duplicate first-footer decoding, keeping all content passes and
exact contributions. Uniform4 improved to 72.048ms versus 59.291ms static, still
failing 1.15. Diagnostic 5 used serial-per-file catalog work stealing with
256KiB scratch and a shorter control sleep; uniform4 remained 70.265ms versus
58.719ms. Diagnostic 6 packed exact rows into existing bounded frames and used
vectored writes. Frames fell from 32 to 10 per representative uniform4 producer,
but uniform4 was 75.636ms versus 59.010ms: lower network overhead did not establish
an end-to-end win. Hash wall time also rose in that run, so timing improvements
are not inferred solely from frame counts. All six diagnostics' raw samples and
comparisons are retained; no samples were removed or best-iteration selected.

The distributed correctness run overlapped thin-LTO compilation and hit two
80ms minimum-lease expiry cancellations (continued-distributed-tests.log).
After compilation stopped, the complete identical distributed suite passed
(continued-distributed-tests-isolated.log), including packed ACK accounting and
actual fault hooks. This is an environment-sensitive deadline test, not a claim
that expired leases can succeed. Final validation must avoid concurrent compiler
load; the failure remains preserved. The next source uses the original 100us
service sleep and a compact positive-count hash-table sentinel, retaining bucket
capacity, load/spill thresholds, checked updates and the legacy sorted output.

Native diagnostic 7 retains packing/vectored writes, restores the original
100us control sleep and replaces padded Option slots with positive-count compact
slots, while preserving bucket count, half-full spill threshold, checked updates
and ordinary sorted output. All 32 oracle-checked jobs pass. Its six diagnostic
ratios are 0.5915/0.5841 for imbalanced2 whole/tail, 0.4278/0.4055 for imbalanced4,
0.9185 uniform2 and 1.1001 uniform4; unchanged limits are 0.90 and 1.15. Uniform4
medians are 59.469ms static / 65.421ms leased. This is the first continued native
diagnostic with all comparisons passing; a clean full release still determines
acceptance. The compact layout is 32 rather than 48 bytes per slot on this host,
charged at actual storage size to the original task ledger. Zero counts are
rejected; zero keys/sums remain valid. Tests cover failed-update atomicity,
spilled/unspilled contributions and cleanup. The shared change applies to both
static and leased controls; no artificial baseline delay is introduced.

## Frozen continued implementation

The continued source is `ee9f8da63b3910bfa99276f71e5ee72a19f3c021`, crates tree
`4b780320d99cfa421be380f15f110d951cd384b5`. Its sequential final gate passes
192 Rust tests (one ignored local diagnostic), all 18 Python tests, fmt, workspace
all-target checks, strict all-feature Clippy, a workspace debug build and smoke.
`candidate-2/quality-source-equivalence.json` matches all 84 quality input files to
that commit. New correctness CI includes the Python evidence tests; timing remains
local release work. Linker diagnostic text, intermediate Clippy fixes, the
compiler-contention test failures and all unsuccessful diagnostic timings remain
in raw logs; no claim that every development command passed is made.

The new clean build uses the same temporary checkout at this commit and
`CARGO_TARGET_DIR=/Users/jasonkli/projects/adaptive-dataflow-engine/target cargo build --workspace --all-features --release --locked --offline`, matching the
baseline/fcc325e target-cache convention. An initial private-target build was
interrupted (exit130) to avoid recompiling unchanged upstream dependencies; its
log is retained. `candidate-2/build-command.json` records both attempts and the
exact source, cwd, flags and target directory. Captured binaries will be kept in
candidate-2/bin. The completed build and release outcomes are recorded below.

The first source's archive was independently extracted and its archived verifier
rerun with its own captured fcc325e binaries. `offline-release-1.json` is exactly
equal to its original verification: correctness passes, all six guards fail.
The verifier was called without rewriting the original artifacts. The new
resource-bound checks are source-specific and must not be used to reinterpret
that older release; archived verifiers preserve each source's evidence contract.

## Continued release acceptance — ee9f8da

The completed source-bound campaign is `release-2-ready-3`. It uses the unchanged
13 arms, 62 successful isolated children, 49 measured samples, one warmup per arm,
five measured repetitions at two workers and three at four. The same full fixture
manifest bytes match release-1 exactly; both independent DataFusion oracles pass.
No best sample was selected and no failed timing criterion was relaxed.

| Original guard | Ratio leased/static | Limit | Result |
|---|---:|---:|---|
| imbalanced_job-2 | 0.660238 | 0.90 | PASS |
| imbalanced_tail-2 | 0.642063 | 0.90 | PASS |
| uniform_job-2 | 0.833954 | 1.15 | PASS |
| imbalanced_job-4 | 0.431919 | 0.90 | PASS |
| imbalanced_tail-4 | 0.415751 | 0.90 | PASS |
| uniform_job-4 | 1.143644 | 1.15 | PASS |

| Regime / workers | Static whole / last producer (ms) | Leased whole / last producer (ms) |
|---|---:|---:|
| imbalanced / 2 | 166.777 / 162.499 | 110.112 / 104.335 |
| imbalanced / 4 | 166.819 / 160.881 | 72.052 / 66.886 |
| uniform / 2 | 101.675 / 96.864 | 84.792 / 80.484 |
| uniform / 4 | 59.510 / 54.101 | 68.059 / 61.973 |

The uniform4 median is 68.059ms against 59.510ms static; the allowed median is
68.437ms. Its 0.378ms margin is narrow and machine-local. This closes the declared
campaign criterion, not a cross-machine speed guarantee or a noisy CI timing gate.
All raw ranges and individual samples remain in verification.json and results.
No historical Phase6–9 campaign was rerun or relabelled as this source's timing.

Readiness passed at idle 80.64/83.57/82.14%, load1 2.77/3.11/2.79, with swapouts
unchanged at 1,803,137 pages and no competing compiler/benchmark/worker. Two prior
new-source attempts are preserved with zero measurements: `release-2` rejected
post-build load 22.39/19.18/15.70; `release-2-ready-2` rejected an initial 40.40%
idle reading. The successful attempt follows the same capture/seal rules. Final
postflight, all commands/statuses, source/binary identities, run/slot/PIDs and
negative stages are bound by the release-v2 inventory, independently rechecked.

Expected failures reached their intended points, with empty successful stdout:
worker0 wrote a 516-row frame at 4ms before exit; its disabled-heartbeat lease
expired at 82ms with zero renewals; the spot deadline reached 7ms after a 6ms
notice while the lease was still incomplete. Each carries actual source, nonce
and schedule fences. These are failure tests passing, not recovered leased jobs.
Participation retains producer shares [1,16,15,0], all four final owners and all
16 terminals. Capacity controls complete correctly (132.238/132.346ms medians);
no capacity speed advantage is claimed from that pair.

For the median uniform4 leased child, coordinator admission is 13.427ms and
preparation 1.468ms. Per-producer full pre/post hashing is 25.494–26.471ms,
scan/decode 7.937–8.013ms, aggregate work 4.770–5.080ms, acquire 1.482–2.029ms,
ACK 1.105–1.416ms, and sending 0.854–1.357ms. These overlap across workers and
must not be summed into query CPU/latency. The mandatory content path remains a
major cost. There are still 32,768 exact leased rows versus 4,096 static rows;
packing reduces leased frames to 40, from 128 before packing. It does no keyed
coalescing. Final row/byte/frame/hash conservation remains independently checked.

In that representative child, each producer's managed peak is 2,130,944 bytes,
hash scratch is 1MiB and pending routing peak is 3,915/4,096 rows (96KiB capacity),
plus a transient frame and existing bounded encoding. Coordinator catalog scratch
is capped at 2MiB; hash thread concurrency is capped at eight per job stage.
These domains are separate from receiver buffers and do not constitute an RSS
cap. Harness child peak RSS is 18.58MiB leased / 17.20MiB static for the selected
median children. The external whole-campaign time record is 79.72s real,
42.73s user/13.81s system, max RSS 145,096,704 bytes; this includes setup, validation
and all commands, not per-query CPU. Result cpu_time_ns remains unset.

The archived ee9f8da verifier and its own captured binaries reproduce the complete
PASSED result exactly in `offline-release-2.json`, without rewriting originals.
Eight existing copied attacks and twelve v2 copies all reject, including resealed
best-only samples, preflight-only fault logs, stale/foreign readiness, and forged
catalog/thread/provisional-buffer bounds. Attack drivers, retained forged payloads and precise
rejection logs are kept. The first archived verifier separately reproduces its
original FAILED outcome; new-source checks do not reinterpret legacy bundles.

Source archive SHA256: `c2dfffa8b175bcc3629eaa3c3dab6f2ddce34d709eabc5a040e80345a9a3b8a5`.
Captured ade-bench SHA256: `65d044d1fa22c564227ce75ac5331cd4f1b5ac4b7ba586f89a44cbfc1d050f23`;
ade-worker: `ed99d7664db8bfaab7599cab56a8bd641a8ba603aa3af631bf324bc947ba283f`.
The final documentation commit retains this crates tree and all 84 tested code
inputs; measurements continue to belong to ee9f8da, not to a relabelled HEAD.

## Final disposition

| Finding | Current disposition |
|---|---|
| R1 acceptance | Closed for the bounded local campaign: all six unchanged guards pass at ee9f8da |
| R2 cost | Full-content hashing, decode, file setup, control and framing reduced without dropping a content pass, row or per-file ACK |
| R3 verifier | Release-v2 integrity and frozen inventory checks pass; 20 copied attacks reject; archived outcomes reproduce |
| R4 liveness | Stalled peer isolation and persistent renewal tests pass; malformed/capped/expired control still cancels |
| R5 confidence | 192 Rust/18 Python tests, full gate/smoke and selected Phase6–11 correctness paths pass; historical timing stays source-bound |
| R6 handoff | Current scope/status reconciled, both failed sources retained, local commits and closure evidence supplied |
| Deferred | Physical process add/remove, migration, leased failover, broader SQL, cross-machine performance and Phase12 |

**GO for the bounded opt-in leased research path on this local evidence.** Static
remains the supported default. Phase10 remains PARTIAL for physical membership;
Phase11 bounded v1 stays COMPLETE. No recovery, frontend or phase boundary widened.

## Local closure artifact and reproduction

The local closure is
`benchmarks/results/phase10-remediation-20261026-bundle.tar.gz`.
Its SHA256 is
`850b166a48b062558f32c617ce59f536d9081ab349d478ad6e8317a23b823877`.
The adjacent `phase10-remediation-20261026-bundle-check.json` records extraction,
733 verified inventory entries, and fresh archived-source/binary verifier replays
of both release-1 FAILED and release-2-ready-3 PASSED. Replays leave the original
artifacts unchanged. The archive includes raw commands/results/sidecars, fault
witnesses, host readings/seals, all stabilization attempts, source bundles/archives,
captured binaries, quality logs, diagnostic/hash probes, copied attacks,
source-equivalence proofs, closure notes and REPRODUCE.md.

Verify the outer checksum, extract into a fresh folder, verify all sha256.json
entries, then follow REPRODUCE.md with each source's own archived verifier and
sibling captured binaries. Full fixture files remain in the original ignored
benchmarks/datasets directories; the declarations and per-file descriptors are
in the results. Offline evidence verification does not rerun timing queries.
The archive/report are local evidence, with no public hosting, production or
cross-host claim. The final Git change after ee9f8da updates documentation only.


## Remediation re-review follow-up (2026-10-26)

The [separate validation note](phase-10-rereview-validation-2026-10-26.md) records
read-only replay of all733 historical archive entries using each release's own
verifier/binaries, preserving fcc325e FAIL and ee9f8da PASS exactly. Clean snapshot
b4a2a70 adds a static_projected per-file diagnostic, a matched16-arm campaign
(80 children/64 measured) and a separate original13-arm confirmation(62/49).
All six matched and all six original confirmation comparisons pass locally.
The narrow credited scheduler-policy benefit is imbalanced work: uniform samples
record no steals. Independent host/day repeatability remains UNVERIFIED.
These are supplementary new-source observations, not rewritten historical gates.
Static stays default, physical membership stays fixed, Phase10 remains PARTIAL,
Phase11 bounded v1 stays COMPLETE, and Phase12 is not authorized.
