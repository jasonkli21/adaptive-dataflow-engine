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

## 2. Phase discipline

The active phase is the hard scope boundary.

Phase 5 is the active/completed boundary, defined by
`docs/plans/phase-5-implementation-plan.md` and its implementation report.
Phases 2–4 remain accepted. Phase 5 adds explicit deterministic `threshold_v1`
adaptive exchange alongside the five static mechanisms. Destination partitions
escalate monotonically at segment boundaries with at most three transition
records; mixed-tier sources preserve strict completion and bounded retention.
Object-store acceptance uses an ephemeral filesystem backend, not proof of cloud
durability. Do not begin Phase 6 without a new explicit instruction.

- Keep `ade-bench -> ade-distributed -> ade-exec`; core execution has no frontend dependency.
- Preserve the independent oracle, child protocol, manifest inputs and result schema v1.
- Reuse the local spillable aggregate through partial merge, IPC scan/live source and streaming sink.
- Worker failure fails the job; cancel survivors and clean up. No retry/recovery.
- Exchange buffers are separately bounded; producer/final task budgets may overlap.
  The local ledger is not a process RSS cap.
- Preserve the pure policy/mechanism boundary and static defaults. No lineage,
  recovery, adaptive scheduling, joins, SQL frontend or streaming/state.
- Run focused tests during development, one final workspace gate/smoke and focused
  five-mode static regression and focused adaptive 2-worker/limited 4-worker release
  acceptance evidence. Keep CI correctness-only; timing guards remain local release evidence.

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

Each phase ends with an implementation report. Phase 5 is recorded in:

`docs/reports/phase-5-implementation-report.md`

Include baseline/final identity, policy/configuration, observation truthfulness,
transition/extent bounds, mixed-tier lifecycle, failure/cancellation and backend
durability statements, deviations, ADRs, commands/tests, raw normal/adversarial
release evidence, actual partition histories, limitations and Phase 6 handoff.
Preserve failed experiments as well as final evidence. Do not implement Phase 6
lineage, retry or recovery as cleanup.
