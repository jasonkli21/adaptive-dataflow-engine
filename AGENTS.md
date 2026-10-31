# AGENTS.md — Development Guidance for Coding Agents

This repository is designed to be developed phase-by-phase with substantial assistance from coding agents. Treat this file as the first operational instruction for repository work.

## 1. Read order and source of truth

Before changing code, read:

1. `docs/product/prd.md`
2. `docs/architecture/architecture.md`
3. the active plan under `docs/plans/`
4. relevant ADRs under `docs/adr/`
5. the latest implementation report under `docs/reports/`, if one exists
6. the code/tests relevant to the task

Precedence is:

```text
PRD scope/outcomes
  > architecture invariants
  > active phase plan details
  > accepted ADR decisions
  > implementation report/current code reality
```

An ADR can intentionally refine an architectural recommendation, but it must record the affected section/invariant and rationale. If code and docs differ accidentally, do not silently choose one: reconcile them and document the resolution.

An accepted ADR does not silently widen the active phase plan. When a concrete decision changes a phase deliverable or boundary, reconcile the active plan explicitly while preserving the PRD outcome and architecture invariants.

## 2. Phase discipline

The active phase is the hard scope boundary.

Phase 8 bounded v1 is accepted at source ab7404b/runtime432cc7dd, with immutable
public evidence in its implementation report. Historical failed experiments and
fixed48-shard/aggregate-spill-domain limitations remain explicit. Source-bound
B/C confirmations and all-nine pooled original gates pass; this is local evidence.
Phase 9 bounded v1 remains accepted with review fixes at 3e89edf. Its frozen
measurements belong to 9b0330a/runtime fab08a0, not this revision. Phase 10 was
explicitly authorized on 2026-10-24. Phase10 remains partial: the original and
fcc325e sources failed all six timing guards; ee9f8da now passes all six unchanged
local release guards and correctness. Physical membership stays fixed. Phase 10 remains PARTIAL under docs/plans/phase-10-implementation-plan.md and ADR-0017:
file-morsel leasing, stealing and elastic producer participation in a fixed
2–4-process registry. Preserve static defaults and accepted Phase6–9 contracts.
The user explicitly authorized Phase 11 bounded v1 on 2026-10-25 despite Phase 10 debt.
Phase 11 bounded v1 is COMPLETE under docs/plans/phase-11-implementation-plan.md
and ADR-0018. On 2026-10-26 the user authorized repo-wide readiness and bounded
Phase 10 remediation, without subagents. After the first measured candidate
failed, the user explicitly instructed continued reassessment and remediation
until the failures are addressed, superseding the earlier two-candidate time box. This stabilization was the scope before the 2026-10-28 Phase 12 authorization; it does not widen recovery,
frontend admission or physical membership. Preserve historical source identities
and failed gates. Static remains the supported default; leased scheduling remains
an opt-in bounded research path with local acceptance at ee9f8da. The user explicitly authorized Phase 12 implementation on 2026-10-28, without
subagents and with one local commit. The continuation completes Phase 12 bounded v1
under ADR-0019 and `docs/reports/phase-12-implementation-report.md`: opt-in durable
local operations for static aggregate and bounded HashOnly join, fixed externally
supervised 2–4 managed workers, proved drain before fresh whole-job retry, atomic
local output, cancellation and byte budgets. Operator spill is disabled in this
path; ambiguous/poisoned/changed workers require an operator. Full production HA,
replication, worker replacement and broader sinks remain deferred. No Phase 13
authorization is inferred. Preserve the initial foundation evidence at fcffd77b
and the separately measured completion source; do not relabel old timing evidence.
On 2026-10-28 the user explicitly authorized Phase13 in this session, without
subagents and with one final project commit. Phase13 is COMPLETE_BOUNDED_REMOTE_V1
under ADR-0020 and docs/reports/phase-13-implementation-report.md after independent
G0 safety proof at 474f8c9 and clean acceptance at ead9de6. The opt-in remote-s3
path uses a separate S3-compatible service, static GroupBySum, fixed 2–4 logical
producers/hash partitions and fresh stage-boundary reader processes. Its G0–G8
proofs are local; preserve every failed/premeasurement candidate and all samples.
Defaults, legacy wire/result/manifest v1 contracts and ade-batch local operations
remain unchanged. Full cloud/HA/cross-host/AZ durability, in-flight membership,
remote operations, general SQL/joins and serverless remain deferred. Phase10
physical elasticity and Phase12 production HA remain separate debt. No Phase14
authorization is inferred.

On 2026-10-28 the user explicitly authorized Phase14 bounded streaming v1,
without implementation subagents and with one implementation commit, followed
by independent Sol High review and Luna XHigh remediation in one fix commit.
Phase14 is COMPLETE_BOUNDED_STREAMING_V1 under ADR-0021 and
`docs/reports/phase-14-implementation-report.md`: default-off real Kafka input,
fixed explicit offsets/partitions, shared native stateless Arrow kernels,
sink-confirmed progress, bounded timers and source-demand backpressure. All
G0–G9 are local evidence; preserve failed candidates and frozen source identities.
Topics require trusted immutable uncompressed <=32KiB batch history. Offset
suggestions/timers are nondurable; no consumer-group recovery, windows/state,
exactly-once, HA/cloud or changed PlanV1/batch EOF/default contracts are inferred.
No Phase15 authorization is inferred. During the requested review/fix agents,
the parent stays idle; any resumed work stays in that same subagent.

On 2026-10-29 the user explicitly authorized Phase15 implementation in this
session, without subagents and with one local project commit. Phase15 is
COMPLETE_BOUNDED_STATE_V1 under ADR-0022 and
`docs/reports/phase-15-implementation-report.md`. The default-off stateful Kafka
path adds checked keyed tumbling windows, separate bounded hot/cold owner stores,
matched quiescent local checkpoints, manual restore and logical1→2→4→1 owner
redistribution. All G0–G11 proofs are local/source-bound at candidate8; preserve
its immutable inventory and every earlier candidate/qualification. The owned
control-only Kafka poller drains statistics through synchronous I/O and never
owns partition data/state/output. Defaults, StreamConfigV1/PlanV1, old protocols,
stateless timers and Phase10–14 historical evidence remain unchanged. Output is
at least once. Physical distributed rescale, remote/disk-loss HA, transactions,
savepoints and adaptive checkpoints remain deferred; no Phase16 authorization is
inferred. The user's no-subagent requirement overrides the proposed independent
review step; same-session self-review and independent producer-ledger oracle
proofs are disclosed without claiming an independent reviewer. The additive
2026-10-29 review remediation is recorded in
`docs/reports/phase-15-review-remediation-2026-10-29.md`: bounded replay semantics
are trace-relative, repeated logical IDs carry a payload fingerprint, ID-only
deduplication is unsafe across legal Kafka interleavings, unchanged cuts do not
consume checkpoint generations, the minimum checkpoint budget is 66,048 bytes,
and time-stop interruption after a partial output frame is a failed run.

The 2026-10-26 stabilization is recorded in docs/reports/repo-wide-readiness-2026-10-26.md:
fcc325e passes correctness but fails all six timing guards. Continued source/scan
and contribution remediation at ee9f8da passes all 62 children/49 measured samples,
three reached negative stages, and all six unchanged gates. The uniform4 ratio
is 1.1436 against 1.15, a narrow local margin. Preserve every failed candidate and
all six original gates; do not infer physical elasticity or Phase 12 authorization.

The remediation re-review follow-up is recorded in
`docs/reports/phase-10-rereview-validation-2026-10-26.md`: immutable historical
archive replay passes; clean snapshot b4a2a70 passes all six supplementary
static_projected comparisons and all six original confirmation guards. The new
static_projected pipeline is a diagnostic only; static defaults remain unchanged.
Only local imbalanced scheduler-policy benefit is credited; uniform runs had no
steals and independent host/day repeatability is unverified. Preserve all samples,
refused readiness attempts, source identities and fixed membership boundaries.

- Keep `ade-bench -> ade-distributed -> ade-exec` and `ade-frontend -> ade-physical`;
  native runtimes may depend on ade-physical, never on the frontend/DataFusion.
- Preserve the independent oracle, child protocol, manifest inputs and result schema v1.
- Reuse the local spillable aggregate through partial merge, IPC scan/live source and streaming sink.
- Only the documented single final-worker loss can recover. Producer/multiple
  losses, replay mismatch, poisoning and recovery failure cancel/clean survivors.
  Preserve one episode, original logical IDs and attempt-safe gather commit.
- Exchange buffers are separately bounded; producer/final task budgets may overlap.
  The local ledger is not a process RSS cap.
- Preserve the pure policy/mechanism boundary and static defaults. Scheduling is limited to Phase10 leased fixed-hash aggregation; no streaming/state; the bounded Phase 11 frontend uses native adapters. Joins are limited to the explicit Phase9 bounded inner-equijoin contract.
- For Phase11, run focused new tests, one final workspace gate/smoke and selected
  Phase6–10 correctness regressions. Existing five-mode static, adaptive2/4-worker,
  normal/recovered/unrecoverable, economic, split/coalesce, pinned recovery and
  skew/uniform release gates retain their original criteria and source identities.
  Their historical local timing evidence is separate from Phase11 correctness;
  do not claim an unrun campaign passed or reclassify failed Phase10 guards.
  Keep CI correctness-only; timing guards remain local release evidence.

A small coherent implementation is preferred to speculative architecture.

## 3. Make reasonable decisions without unnecessary prompting

When a detail is underspecified:

1. prefer the simplest choice consistent with the phase plan and architecture;
2. inspect current upstream public APIs/documentation when dependency behavior matters;
3. record meaningful design decisions in an ADR;
4. continue implementation unless genuinely blocked.

Do not stop for cosmetic choices or choices that can be safely made and documented.

## 4. Code quality

- Use stable Rust and idiomatic ownership/error handling.
- Prefer explicit types at architectural boundaries.
- Keep library errors structured; `anyhow` is acceptable primarily at binary/tooling boundaries.
- Avoid `unsafe` unless a benchmark-backed requirement justifies it. Any `unsafe` requires a documented invariant and dedicated tests.
- Avoid global mutable state.
- Keep I/O and CPU responsibilities clear.
- Keep engine-independent benchmark semantics separate from engine adapters.
- Do not depend on private/internal APIs of upstream engines when a public API is available.
- Keep dependency features narrow.

## 5. Tests and validation

A feature is incomplete until its correctness path is tested.

For Phase 0:

- schema/config parsing gets unit tests;
- dataset generation gets determinism/read-back tests;
- workload validation gets exact or tolerance-aware correctness tests;
- child-process orchestration gets success/failure protocol tests;
- result comparison gets incompatibility/regression tests;
- CI performs only correctness/smoke benchmark work, not noisy absolute performance gating.

Before finishing a substantial change, run the repository checks available in the environment, typically:

```bash
cargo fmt --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

If a command cannot run because of the environment, record that fact in the implementation report instead of claiming success.

## 6. Benchmark integrity

Never trade correctness for a faster number.

- Required correctness validation must pass before a benchmark is classified as valid.
- Record environment, source, engine, configuration, dataset, and workload identity.
- Do not compare semantically incompatible runs.
- Do not report a single best iteration as the result.
- Keep microbenchmarks separate from process-isolated macrobenchmarks.
- Treat small local-machine deltas cautiously.
- Generated benchmark data/results stay out of Git except intentional small examples.

## 7. Documentation responsibilities

Update docs when the implementation changes a real contract.

Use:

- `docs/adr/` for decisions;
- `docs/research/` for experiments/questions;
- `docs/reports/` for completed phase state;
- code comments for local invariants that are not architectural decisions.

Avoid rewriting the PRD merely to mirror code. Product scope changes should be explicit.

## 8. Phase completion

Each phase ends with an implementation report. The current Phase15 reports are
`docs/reports/phase-15-implementation-report.md` and its additive review follow-up
`docs/reports/phase-15-review-remediation-2026-10-29.md` (COMPLETE bounded state v1; full
distributed rescale/production HA and exactly-once deferred). The Phase14 report is
`docs/reports/phase-14-implementation-report.md` (COMPLETE bounded streaming v1;
checkpointing/exactly-once and full streaming SQL deferred). The Phase12 report is
`docs/reports/phase-12-implementation-report.md` (COMPLETE bounded v1; full HA deferred). The Phase11 report is
`docs/reports/phase-11-implementation-report.md`. The Phase10 debt report is
`docs/reports/phase-10-implementation-report.md`; Phase8 prerequisite closure remains recorded in:

`docs/reports/phase-8-implementation-report.md`

Include baseline/final identity, supported failure envelope, source snapshot and
file morsel truthfulness, lost ranges/closure, terminal fingerprint validation,
attempt-safe gather, bounded metadata, cancellation/poisoning, protocol version,
commands/tests, raw acceptance evidence, negative evidence, deviations and Phase 9
boundary. Preserve failed experiments. Preserve Phase 7 decision provenance, complete-copy identity, explicit cleanup and
predeclared empirical criteria. Do not broaden recovery or hide Phase10 tuning in Phase11 cleanup. The current
Phase10 remediation is separately authorized; its dated readiness report must
distinguish new-source evidence from historical phase acceptance.

## Phase16 authorization — 2026-10-29

The user explicitly authorized Phase16 adaptive checkpoint/state placement in this
session, without subagents and with one local commit. Follow ADR-0023 and the
Phase16 plan/report. Keep the fixed v1/default paths and all earlier evidence;
completion requires all G0–G12 on a matched runtime/executable with a real separate
S3 service. No Phase17 authorization, production HA, physical rescale, automatic
recovery/GC or exactly-once is inferred. Source replay remains trace-relative;
remote budget ledger loss/rollback requires a fresh namespace before new writes.

Phase16 is COMPLETE_BOUNDED_ADAPTIVE_STATE_V1 under ADR-0023 and
`docs/reports/phase-16-implementation-report.md`:117 real Kafka/MinIO cases,
136 matched cuts, bounded policy/full/incremental/local/remote/two-copy and
explicit guarded replay/restore. Preserve candidate5 runtime/executable identities
and all four failed predecessors. Static defaults and v1 remain; same-host service
proof is not physical HA. No Phase17 authorization is inferred.

## Phase17 authorization and completion — 2026-10-30

The user explicitly authorized Phase17 implementation in this session without
subagents and with one final local project commit. Phase17 is
COMPLETE_BOUNDED_TRANSACTIONAL_STATE_V1 under ADR-0024 and
`docs/reports/phase-17-implementation-report.md`. Candidate5 has121 transactional
cases,144 committed-reader observations/376 matched cuts/65 sessions, five fresh
real Kafka/MinIO Phase16 post-fix proofs, and a matched actual-ENOSPC supplement.
All required workspace checks pass (301 Rust tests/10 existing ignored,54 Python
checks); fresh stateless30-case and fixed-state70-case regressions pass. Preserve
its immutable source/executable/archive identities and all four predecessors.

The default-off v3 path selects complete state+source progress+output through one
CURRENT3 pointer, with kernel writer fencing, poisoned ambiguous attempts,
self-contained exact-digest savepoints, durable pause/resume, real codec1→2
migration and local logical1→2→4→1 owner redistribution. Exactly once is the
committed-visible window prefix within one pinned local job lineage through the
versioned reader. Source replay remains trace-relative. Trusted immutable Kafka
history and single-local-filesystem rename/fsync assumptions are explicit.
No external Kafka/HTTP/S3 exactly once, cross-service2PC, consumer-group coupling,
automatic recovery/GC, disk-loss/cloud/production HA, arbitrary rolling upgrade or
physical state/Kafka rescale is inferred. Imported continuations require one active
branch under operator control; no global cross-filesystem writer registry exists.
Defaults, v1/v2 formats and prior evidence remain; old modes refuse v3 roots.

The additive `docs/reports/phase-16-postfix-reacceptance-2026-10-30.md` closes the
five decisive review paths on the matched current runtime. It does not relabel the
historical117-case campaign or rewrite the earlier PARTIAL_VERIFICATION report.
Earlier Phase10–16 broader debt remains separate. No Phase18 authorization is inferred.
