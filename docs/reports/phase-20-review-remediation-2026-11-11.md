# Phase 20 independent review remediation — 2026-11-11

## Status

**`PENDING_INDEPENDENT_REACCEPTANCE`**. This additive report supersedes the
historical all-PASS status in the original implementation report and support
matrix. It does not replace the original archive, criteria, fixture lock,
implementation report or attempts. The detached original archive is immutable
historical evidence; no current-source G0–G15 result is inferred from it.

The independent handoff is
[`phase-20-independent-review-and-remediation.md`](phase-20-independent-review-and-remediation.md).
This remediation preserves its findings and stop rule. The current claim is
limited to the closed, specialized Expand → Reduce → Delay template for the two
implemented graph algorithms. The physical node/arc description is not a
general user-composable operator API. Arbitrary cyclic regions, nested or
multiple loops, distributed progress, and general iterative ML remain
unproved and deferred. No Phase 21 work is authorized.

## Changes in this remediation source

- Added `scripts/phase20/case-contract.json`, a frozen specification for all
  375 process cases. It binds each case name to canonical request identity;
  only machine-specific source/root parent paths are normalized, while fixture
  basenames and every semantic/limit field stay bound. Malformed JSON requests
  are hashed as exact raw bytes. This permits fresh isolated temp directories
  without allowing another plan or input. The contract also binds exact
  operation, exit disposition, required diagnostic, stdout policy, complete
  case inventory and primary-root inventory.
- Added `case_contract.py` and integrated it before the archived semantic
  checks. A case cannot be omitted, added, converted from required success to
  failure, assigned a different request, or turn a refusal into unexpected
  stdout. The one expected stalled-output case must be a prefix of the
  independently derived terminal stream and cannot contain a completion
  footer. Root inventory checks catch unexpected or missing root side effects.
- Kept exact timing samples, medians, work calculations, and frozen threshold
  predicates. Only the serialized population standard deviation comparison
  now allows `rel_tol=1e-12` and `abs_tol=1e-9` nanoseconds, addressing the
  reported one-ULP platform variance without relaxing a guard.
- Added focused standard-library tests for success-to-failure mutation,
  wrong refusal reason, unexpected refusal output, changed request, omitted
  cases/roots, changed failure stage, and invalid stalled-output prefix.
- Reconciled ADR-0027, the Phase 20 plan, operator guide, README and repository
  instructions to state the closed-template limitation and pending status.

## Validation and evidence boundary

This remediation has **not** regenerated or edited
`docs/evidence/phase20/acceptance.tar.gz`, its manifest, criteria, fixture lock,
attempts, or any Phase 17–19 evidence. `case-contract.json` is a new checked-in
verifier input. The attached review's two fully rehashed mutation witnesses
are represented in the focused contract tests and the preserved rehashed
attacks in
[`docs/evidence/phase20-review-remediation/`](../evidence/phase20-review-remediation/).
The detached verifier replay passed locally on the unchanged historical
archive: 380 process observations, 33,978 rounds, 1,099,948 feedback rows,
146 selected cuts, 77 roots and 15 measured pairs. Eleven complete rehashed
mutations were refused for their intended reasons, including success-to-failure,
wrong refusal reason, unexpected stdout, changed request, omitted case,
selected receipt forgery, process reuse, round trace forgery, and material
standard-deviation forgery.

The replay used macOS 27.0.1 arm64, Python 3.10.20 and PyArrow 24.0.0 with a
temporary adapter calling the cached Rust BLAKE3 1.8.7 implementation. Network
access prevented installing the pinned Python packages. This establishes one
local replay, not the handoff's required Linux/Python 3.12 portability replay.
The verifier run was detached: the archived provenance self-hash and binary
identity are intact, 179 of the original 180 source inputs still match the
checkout, and the sole changed original input is the verifier under repair.
The pristine replay and mutation suite passed on that one environment. G14
remains `NOT_RUN` for the handoff's full source-matched portable acceptance
because the second-platform replay and fresh campaign have not run.

The archived native run, its process/fault traces, actual HFS+ ENOSPC result,
and timing samples remain historical-source evidence. Fresh native fault,
ENOSPC, source/executable matching and final-source G0–G15 campaigns are not
credited by this report unless an exact command, environment and result are
added below. Phase 18 remains `PARTIAL_PHASE18_UNIFIED_ADMISSION_V1`.

## Current gate matrix

| Gate | Current status | Basis |
| --- | --- | --- |
| G0 | NOT_RUN | No final remediation campaign provenance archive yet. |
| G1–G12 | NOT_RUN | Historical implementation evidence exists, but this remediation has not freshly source-bound the complete gate set. |
| G13 | PASS | `scripts/phase20/checks.sh` passed on this remediation checkout, including the workspace, smoke, Python and both historical Phase 19 checks; logs and exits are retained in the evidence directory. |
| G14 | NOT_RUN | Detached pristine replay and eleven rehashed attacks passed locally; cross-platform and current-source acceptance remain outstanding. |
| G15 | NOT_RUN | Final-source multi-process and report/support acceptance remains outstanding. |

Focused standard-library tests: **8 passed**. Python regression suite:
**72 passed** using the available PyArrow 24 environment. The consolidated
`scripts/phase20/checks.sh` gate passed all 15 steps, including `cargo fmt`,
workspace `cargo check`/Clippy/tests, no-default feature checks, worker smoke,
Python regressions and both historical Phase 19 archive verifiers. Logs and
the exact exit manifest are retained. The release executable still hashes to the archived
measurement binary `ae2de07d49ca896df7dd58ec559cd532d0a86f0fee49ca3c24e956d94c25fad2`;
it was not rebuilt or used for a new native campaign. Exact verifier output and
attack archives are retained with the evidence.

The fresh Phase 20 campaign and the Linux/Python 3.12 portability replay remain
unrun. A campaign attempt using available PyArrow 24 stopped at fixture-lock
comparison before any native case ran; generated fixtures and its failure
trace are preserved in
[`attempts/pyarrow24-fixture-lock-mismatch/`](../evidence/phase20-review-remediation/attempts/pyarrow24-fixture-lock-mismatch/).
The pinned PyArrow 21 wheel could not be installed because the environment has
no network access. G13 is PASS; the other G0–G12 and G14–G15 remain NOT_RUN for full
current-source reacceptance, and `PENDING_INDEPENDENT_REACCEPTANCE` stays in
force. Preserve the work counters
and wall-time observations as originally reported; no general or end-to-end
speedup claim is made.
