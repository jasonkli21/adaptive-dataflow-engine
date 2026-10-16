# Phase 6 implementation report — lineage recovery v1

Date: 2026-10-16. Baseline: `602cb3aab1a97360c15b6db61965a5b64c325e79`
(`Address Phase 5 independent review findings`). The checkout matched the plan's
reviewed baseline; there were no intervening commits to reconcile. The supplied
Phase 6 plan was untracked and is included. The unrelated untracked
`sol-medium-phase-0-bootstrap-implementation-prompt.md` is preserved and excluded.

Final identity is the single implementation commit containing this report:
`git log -1 --format='%H %s' -- docs/reports/phase-6-implementation-report.md`.
A commit cannot embed its own SHA. The final handoff records its literal SHA and
clean commit-bound acceptance result. Clean release acceptance runs after the
commit from a temporary checkout, preserving the unrelated user file in place.

## Supported outcome

One worker may disappear after **every original producer RPC successfully
completed its acknowledged exchange outputs**, while its logical final has no
successful gather terminal. Every other final must succeed and validate its
gather terminal. The coordinator identifies the one missing partition, derives
its original terminal segment ranges, plans file lineage replay, confirms worker
unavailability and runs one bounded episode on the lowest successful survivor.
Only the missing destination is emitted; successful final partitions are reused.
No new logical JobId or whole-job restart occurs.

The demonstrated failure is an actual `process::exit(86)` after consuming complete
final input and writing partial gather batches, before the terminal. This proves
incomplete-attempt isolation. The coordinator releases the hook only after
receiving every original producer result, eliminating a producer-ACK/exit race.
Other failure windows are not claimed. Producers, multiple losses, live-worker
task errors, timeouts, missing/invalid snapshots, non-replayable lineage, replay
mismatch, poisoned hosts, ambiguous completed gather or recovery failure still
fail/cancel the job with no partial output.

## Files and contracts

- `ids.rs`: explicit LogicalTransferId, SegmentId, MorselId and attempt conversion.
- `lineage.rs`: versioned source snapshot, whole-file descriptors, bounded-memory
  BLAKE3 file hashing and terminal manifest replay comparison.
- `recovery.rs`: pure producer-evidence validation, lost ranges, deterministic
  minimal supported closure/host planning and compact recovery diagnostics.
- `gather.rs`: separate partition/attempt stores, promotion validation, duplicate
  commit rejection and cancellation/quarantine of incomplete original output.
- `exchange.rs`: incremental sender fingerprints and manifests exposed only after
  matching terminal ACK; existing framing/sequence/retention behavior retained.
- `worker.rs`: shared targeted producer/final execution, one combined recovery RPC,
  fresh local exchange and research-only gated process exit.
- `coordinator.rs`: conservative task-loss candidate collection, survivor commit,
  pure planning, one episode, replay verification and exact final partition set.
- Benchmark adapter: verified manifest/file descriptors, effective recovery/hook
  options, existing independent oracle and result schema v1.
- Process/unit tests, Phase 6 acceptance/verifier, README, AGENTS and ADR-0013.

One direct `blake3` dependency was added using an already locked workspace package;
no package upgrades. Crate direction remains `ade-bench -> ade-distributed ->
ade-exec`. Core execution has no frontend dependency. Control framing is now
`ADETCP03`; a test rejects `ADETCP02` before incompatible payload decoding.
Exchange wire v2, hash v1, schema v1, child protocol and benchmark result schema v1
are unchanged. Worker/coordinator builds must match.

## Identity, snapshot and closure truthfulness

Logical transfer identity is job + exchange + destination + logical producer;
segment identity adds sequence. Attempt 0/1 changes only execution identity.
Morsel IDs are stable manifest ordinals scoped to the source snapshot. Source
paths, process/socket IDs, clocks, attempts and temporary roots are not logical
identities. Snapshot v1 denotes the existing physical aggregate graph/hash/schema;
the prepared job retains the typed original task configuration used for replay.

The adapter binds the existing manifest identity and schema/logical fingerprints
to file sizes and fingerprints. Workers check exact source assignment and hash
physical files before original production and replay. Missing source identity or
an unsupported version refuses recovery. Sources must remain immutable during
execution: filesystem checks do not provide transactional snapshot isolation or
protection against adversarial concurrent file mutation.

The graph has implicit deterministic edges from ordered morsels to round-robin
producer tasks and from each producer to each hash destination. Partial aggregate
state spans every file assigned to its producer. Therefore each missing transfer
requires that producer's complete file closure, while replay enables **only** the
missing destination. There is no row-group, source-range or individual-segment
replay primitive. Empty original transfers retain explicit zero-count coverage.

Original successful producer evidence must contain exactly N destinations and N
manifests, matching the job/exchange/producer/destination, attempt 0, counts and
aggregate send totals. Lost coverage is one ordered `[0, terminal_segments)`
range per producer for the single missing partition. Other partitions are not
marked lost. Planner output contains the source closures and successful reused
partitions, with no file/network side effects or Phase 7 economics.

## Manifest integrity and execution

The sender incrementally hashes a version/domain and logical identity, then each
sequence, encoded payload length and IPC payload, followed by terminal version
and count. BLAKE3 state is constant-sized. The manifest is returned only after
matching destination ACK. It carries logical ID, attempt, segment/row/byte counts
and fingerprint. Identical replay must match every field after replacing only
attempt 0 with attempt 1. Tests cover identical/empty replay, unexpected attempts,
changed content/segment count and the existing duplicate/gap/terminal rejection.

The host executes one `RecoverFinal` RPC. It rejects poisoning, active original
roles and duplicate recovery. A fresh local-materialized Store/receiver expects
original logical producer IDs, the original JobId, lost destination and attempt
1. Closures execute sequentially through the same spillable partial aggregate;
only the target partition opens a sender. Source/range identity is checked, then
each acknowledged replay manifest is compared before the existing final merge
aggregate runs. Coordinator validation repeats the manifest comparison before
promoting the recovered gather output.

This is a deliberate deviation from several recovery-session RPCs: a single
owner bounds state/concurrency, uses the original job cancellation token and
avoids distributed session cleanup bookkeeping. The plan explicitly permits
sequential replay. No alternate aggregate, dynamic membership, replacement worker
or scheduler was added.

## Gather, lifecycle and metrics

Each original logical final has a distinct local-materialized Store expecting
attempt 0. Its task reply and gathered terminal counters must agree before
promotion. An incomplete failed store is cancelled/quarantined; an already
validated old terminal refuses replacement as ambiguous. A distinct recovery
receiver/store expects only that final's attempt 1. Old traffic cannot reach the
new store. Duplicate promotion fails, and collection requires exactly N committed
logical partitions. A failed episode returns no collected output. This is an
internal batch gather commit boundary, not an external transactional sink.

Recovery uses the original job cancellation token and RPC deadline. Outer cleanup
cancels survivors on success and failure; receiver owners stop/join existing
bounded handlers and temporary stores are removed. Recovery does not use object
or spill retention; original tasks preserve ADR-0011 containment/poisoning. The
recovery host rejects both worker/session poisoning and a cancelled job. A lost
or timed-out recovery host fails the job without another episode. An OS file
operation itself is not made preemptible by this phase.

Diagnostics include real loss detection, one plan/attempt, exact lost ranges,
source closure/morsels, replay manifests, host, recomputed counts/bytes, elapsed
time, restart avoidance and one committed attempt per partition. Network
conservation compares original production with surviving/recomputed final input;
replay traffic is reported separately instead of double-counting original sends.
For an adaptive job, the summary includes surviving original-final observations.
The lost worker's tier history is unavailable; the recovered final truthfully
reports local materialization. No adaptive history is fabricated.

Metadata is bounded by source file count and topology (N<=4, N² original
manifests, N lost ranges/closures, one episode, N gather commits). There is no
per-segment history/map, per-segment RPC, or unbounded recovery channel. The 1 MiB
control cap rejects oversized snapshots/plans. Hashing uses an 8 KiB scratch
buffer. Sequential replay uses one task budget followed by one final budget;
original producer/final budgets may overlap. Exchange accounting is separate,
disk volume has no quota, and managed memory is not a process RSS cap.

Local RAM/disk is lossy. The default object store remains the execution-scoped
ephemeral filesystem backend; no durable restore or cloud durability is claimed.

## Focused and workspace validation

Toolchain: `CARGO_HOME=/tmp/ade-cargo RUSTUP_HOME=/tmp/ade-rustup
/tmp/ade-cargo/bin/cargo`, stable Rust 1.99.0 on macOS/aarch64.

Focused distributed validation passed 33 unit tests and four process tests,
including two new process cases. Coverage includes stable IDs/morsels, snapshot
mismatch, minimal closure and invalid ranges, planner refusal, terminal hashes,
partial/stale/duplicate gather attempts, unexpected task attempts, poisoned host,
five static modes, adaptive mixed tiers, sequence/terminal validation, retention
cancellation/deadline and worker reuse/watermark. The primary process case checks
exact fixture sums/counts, one real lost worker, original logical IDs, target-only
replay, manifest equality, attempts `{0:1,1:0}` and unchanged JobId. The negative
non-replayable case checks structured refusal and survivor reuse after cleanup.

Initial sandboxed socket tests failed with `Operation not permitted`; the same
suite passed with localhost networking enabled. Python bytecode compilation
initially hit the system cache permission boundary; rerunning with
`PYTHONPYCACHEPREFIX=/tmp/ade-phase6-pycache` passed. A new poisoning assertion
initially expected the word `poisoned`, but session poisoning cancels its token
and can reject earlier as cancellation; the assertion now verifies refusal.
Lint findings (checked conversions, default constructors, test size) were fixed.
These failed development checks are retained in the session logs.

The workspace gate passed 83 tests across all targets (plus doc tests). Formatting,
workspace check/clippy, the default-library check, debug build and smoke passed.
The hook reply-order review fix was followed by focused process/distributed
regression checks; the gate publishes its reply before allowing process exit.
Final release results are recorded in the final handoff, with
full test/smoke logs at `/tmp/ade-phase6-workspace-test-final.log` and
`/tmp/ade-phase6-smoke.log`:

```text
cargo fmt --all --check
cargo check --workspace --all-targets --locked --offline
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
cargo check -p ade-distributed --no-default-features --locked --offline
cargo test --workspace --all-features --locked --offline
cargo build --workspace --locked --offline
bash scripts/smoke.sh
cargo build --release --workspace --locked --offline
git diff --check
```

The no-default-features check verifies the library without research-hook API.
The Apple debug linker emits the existing oversized unwind-section warning while
successfully linking. Smoke's cross-engine single-iteration comparison is not
comparable performance evidence and is not used as a recovery speed claim.

## Development evidence and clean release procedure

Raw precommit adapter checks are preserved under
`benchmarks/results/phase6-development-20261016`; the associated generated dataset
is under `benchmarks/datasets/phase6-development-20261016` (both ignored).
These are dirty debug-build development evidence, not clean release acceptance.
Dataset: 200,000 rows, four files, uniform 32,768-key domain, seed 42, row groups
8,192; 32 MiB task budget. Normal/recovered results both independently validate
fingerprint `8a07d922c1a426eadb0672c9618b42841066456b699e0b866235f88c3fe9a5f8`.

Normal history: both finals commit attempt 0. Recovered history: original JobId 0,
partition 0 lost, host 1, partition 1 reused at attempt 0, partition 0 committed at
attempt 1. Original producer tasks 0/1 own morsels `[0,2]` / `[1,3]`. Lost ranges
are each `[0,31)` for destination 0: 62 segments, 31,104 partial rows, 801,200
payload bytes. Replay matches both original fingerprints/counts. Gather contains
two committed transfers, 32 batches, 32,694 result rows, 813,824 payload bytes.
Development normal query time was 579.540 ms; recovered 2,091.730 ms, including
1,347 ms recovery. These are single diagnostic observations, not performance
claims. The missing-snapshot adapter case logs a real exit and fails explicitly
with `unrecoverable ... missing source snapshot` without successful output.

`scripts/phase6-acceptance.sh` requires a clean source tree and a release binary.
It fixes the same deterministic dataset/workload, 32 MiB task budget, 1 MiB
exchange buffer, prefetch 1 and one isolated measured child (zero warmups) per
case. It checks normal and recovered local-materialized jobs, direct/buffered/
spill/object static refresh, recovered adaptive with two workers and normal
adaptive with four workers. A ninth negative case uses the same exit hook but
`recovery_enabled=false`, proving the planner refuses missing lineage; its invalid
result and diagnostic stderr are retained. No timing threshold is used.

The verifier checks source/build/environment/dataset/workload identity, independent
oracle fingerprints, real exit evidence, all original terminals, exact missing
ranges/closures/morsels, target-only replay, manifest equality, original JobId,
committed attempts, survivor reuse, conservation, drained retention, bounded
adaptive transitions and the negative error classification. Final clean-commit
artifacts are stored under `benchmarks/results/phase6-acceptance-20261016`, with
verifier output `verification.json` there. The final handoff states the run result;
source SHA is embedded in artifacts rather than inferred from a dirty development
binary. Unrelated user files are not hidden or ignored to satisfy provenance.

## Limitations and Phase 7 handoff

Recovery is one final-worker-loss episode with complete original producers.
There is no arbitrary producer-stage recovery, multi-loss recovery, persistent
coordinator/catalog, dynamic scheduler, worker replacement, durable object
restore, expected-cost model, transactional external sink, streaming/state or
cross-job reuse. An engine instance with a dead owned worker cannot execute a
subsequent job; the benchmark's existing process-isolated iterations each own a
fresh cluster. Successful survivor reuse after failure cleanup is tested.

Phase 7 receives explicit replayability, stable logical data identity, source
closures, verified terminal fingerprints and measured recovery counts/timing.
Those facts are available for a future cost policy; this phase selects only by
capability and correctness. A reviewer should scrutinize the gather quarantine/
promotion boundary, conservative failure classification, immutable-source
assumption and truthful omission of lost adaptive history.

## Measured release acceptance and final commit binding

The first clean release build completed successfully (8m 21s including build-lock
waiting). It used clean source commit
`9da50396996bda2fb8d532afdcea797d195cc5a3`. All eight positive diagnostic validations
and eight isolated measured children passed the independent oracle. The negative
validation/isolated child both failed as expected with a real exit and explicit
missing-source-snapshot refusal; no partial outcome was returned.

The initial verifier reached its last assertion and falsely matched the error
prefix `ADE distributed execution` as a success log. The assertion was narrowed
to the actual successful `ADE distributed execution metrics=` diagnostic; the
corrected verifier passed all retained artifacts. The initial failure remains in
`/tmp/ade-phase6-acceptance.log`. A separate copied-artifact mutation replacing a
replay fingerprint while leaving counts unchanged was correctly rejected.

Raw isolated measured query times follow (one iteration each; no timing claim):

| Case | Query ms |
|---|---:|
| 2-normal-buffered | 35.485 |
| 2-normal-direct | 187.119 |
| 2-normal-local_materialized | 38.132 |
| 2-normal-object_store | 75.586 |
| 2-normal-spill | 42.947 |
| 2-recover-adaptive | 93.480 |
| 2-recover-local_materialized | 76.816 |
| 4-normal-adaptive | 52.125 |

Both recovered diagnostic jobs replayed two producer tasks/four morsels for
partition zero: 62 segments and 801,200 payload bytes. The static diagnostic
recovery took 36 ms; adaptive took 39 ms. Both committed `{0:1,1:0}` and reused
partition one without restarting it. Four-worker adaptive committed all four
original attempts with recovery dormant. All cases shared the oracle fingerprint
reported above, clean source/build/environment identity and the deterministic
manifest. Raw logs, invalid negative result and verifier summary remain under
`benchmarks/results/phase6-acceptance-20261016`.

The single implementation commit was amended to include this measured report,
the verifier correction and stronger equal-count content/order checksum tests.
Those focused tests and final clippy passed. Final commit-bound acceptance is
rerun from the clean temporary checkout after the amendment, using distinct
`benchmarks/results/phase6-acceptance-20261016-final` and matching dataset paths.
Its `verification.json` records the final source SHA and pass result; the final
handoff provides the literal SHA and final verifier outcome. The first evidence
is preserved rather than overwritten or relabeled as final-commit evidence.

A final whole-commit whitespace check (beyond the working-tree diff) caught the
supplied plan's five Markdown trailing-space hard breaks and extra EOF blank
line. They were normalized to explicit backslash hard breaks/one EOF newline.
The redundant release build for the pre-formatting SHA was interrupted so final
acceptance could bind to the corrected commit. Whole-commit whitespace checking
then passed; this documentation-only correction does not change runtime behavior.
