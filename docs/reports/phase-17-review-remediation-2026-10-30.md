# Phase 17 independent review remediation — 2026-10-30

Status: **PARTIAL_VERIFICATION**. The blocking and lifecycle findings have
implementation fixes and local regression coverage. Candidate5 and its G0–G14
archive remain historical and unchanged. Fresh Kafka/MinIO G15 acceptance and a
matched final-source service campaign have not run in this environment, so the
Phase 17 completion claim remains withheld.

## Findings addressed

| Finding | Change | Local witness / remaining boundary |
|---|---|---|
| F17-01 missing selector | Added synced `ACTIVATED3`, a monotonic refusal marker that never selects an epoch. Selector-less admission is limited to an initialized root containing only `ROOT3` and `WRITER3`; any marker, stage, orphan, selector temp or import artifact fails closed. Both `Lease::acquire` and the committed reader use this predicate. The commit still uses only atomic `CURRENT3` replacement. | Rust tests remove `CURRENT3` after a 40-output commit and after an empty PAUSED commit; `read`, `run`-mode open and `resume`-mode open refuse without root mutation. Restoring exact selector bytes restores the original 40 outputs and offsets. An interrupted first import also refuses as an empty root. New real-service G15 cases are declared in `scripts/phase17/criteria.json`, but NOT_RUN. |
| F17-02 stale savepoint continuation | Savepoint import now requires the retired predecessor's authoritative selected-head digest and checks it against the saved selected head before target effects. The CLI and guide state that independent imported roots require one active continuation and must not be merged. | Rust test passes the newer source head for an older savepoint and observes refusal; the live campaign has a stale continuation refusal case. This is operator attestation, not global cross-root fencing, and the campaign is NOT_RUN. |
| F17-03 PAUSED handle | A successful `PAUSED` commit terminates that `Transaction` handle. `accept` and other commits refuse; unchanged repeated PAUSED is idempotent. Public `Transition` commits refuse; codec/owner changes go through the validated transition entry point. | Focused Rust tests cover terminal calls, repeated PAUSED, direct Transition refusal, and fresh resume from the selected PAUSED cut. |
| F17-04 savepoint overwrite race | Export publication uses `rustix::fs::renameat_with(..., RenameFlags::NOREPLACE)`, mapping to the platform's atomic no-replace operation. Unsupported behavior fails closed. | Rust test creates an empty label directory after the friendly collision check and confirms the export fails while preserving the destination. |
| F17-05 evidence identity | Candidate5's source, executable, archive and seven-file source-inventory difference remain recorded as historical identities. Criteria v4 starts at `bd5d1b9` and adds G15; the offline verifier accepts v3 historical archives with their original strict checks and has a separate v4 G15 contract. | No new service archive or campaign executable identity is claimed. A fresh source-bound campaign is required after build. |

`ACTIVATED3` is a refusal witness only. A missing selector on an activated root is
never reconstructed from directory names and never treated as empty. If an
operator must recover, use an independently verified exact selector or import a
verified savepoint into a fresh root. The operator-supplied retired-head digest
cannot prove that every other independent root is stopped; cross-root global
fencing remains deferred.

## Local verification

The following review-remediation checks passed on this worktree:

```text
cargo fmt --all -- --check
passed

cargo check --workspace --all-targets --locked
passed

cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
passed

cargo check -p ade-stream --no-default-features --locked
cargo check -p ade-stream --no-default-features --features kafka --locked
cargo check -p ade-stream --no-default-features --features stateful --locked
cargo check -p ade-stream --no-default-features --features transactional-state --locked
all four feature configurations passed; stateful-only checks retain an existing unused-import warning for `Start`

cargo test --workspace --all-features --locked -- --test-threads=1
306 passed, 0 failed, 10 ignored

cargo test -p ade-stream --features transactional-state --test transactional --locked -- --test-threads=1
13 passed, 0 failed

PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p 'test_*.py'
57 tests passed

PYTHONPYCACHEPREFIX=/private/tmp/ade-pycache python3 -m py_compile \
  scripts/phase17/acceptance.py scripts/phase17/verify_evidence.py \
  scripts/phase17/remediation.py scripts/tests/test_phase17_remediation.py
passed

python3 -m json.tool scripts/phase17/criteria.json
passed

python3 scripts/phase17/acceptance.py --help
passed

bash scripts/smoke.sh
passed; one child validated; single-sample comparisons are descriptive only

cargo build -p ade-stream --features transactional-state --locked --bin ade-state-stream-txn
passed; executable SHA256: edf934a11ce446ee2e2d308cf387b1bd4fae647947aad743383af8e1c8e8e1f0
```

The first full-workspace rerun had one transient failure in the existing
`ade-distributed` join integration test: its source-drift case observed an
exchange read error. That test passed in isolation and the complete suite passed
on the next run. The fresh Kafka/MinIO acceptance is **NOT_RUN**:
the environment has Java, but no Docker or MinIO executable and no Kafka home was
provided. The remediation runner now records missing-selector refusal before
source assignment, empty PAUSED selector loss, incomplete import refusal and
stale predecessor-head refusal. It must be run with disposable owned services and
a newly built final-source executable before G15 or current bounded completion can
be promoted.

The smoke script emitted a one-sample cross-engine comparison classified as a
78.37% timing regression while also reporting differing run configuration and
engine version plus a dirty source tree. The smoke validated its child and result
integrity, but that timing is not comparable acceptance evidence and is not used
to change any benchmark gate.

## Preserved evidence and next gate

The candidate5 archive, its manifest, all four failed predecessors, Phase16
post-fix evidence, Phase14/15 regression archives and every earlier phase archive
were not edited. Offline verification passed for candidate5 (121 cases, 144 reader
observations, 376 matched cuts, 65 sessions), the Phase15 fixed-state archive
(70 cases), and the Phase14 stateless archive (30 cases). Candidate5 continues to
prove that result only for its frozen source/executable pair; it does not cover
these remediation changes. The SHA256 above identifies a local diagnostic build
only; it is not included in a fresh source-bound service archive.

Current status remains `PARTIAL_VERIFICATION`. Local workspace and focused Rust
gates pass, and the historical legacy archives verify offline. Reacceptance still
requires freezing the committed remediation source and executable identities,
then running the version-4 criteria against fresh disposable Kafka/MinIO services
and packaging a new immutable evidence bundle. If live services remain
unavailable, retain this status and the explicit NOT_RUN result. No Phase18 work
is authorized.
