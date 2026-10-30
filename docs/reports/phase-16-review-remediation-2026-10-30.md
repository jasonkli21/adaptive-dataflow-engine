# Phase 16 Independent Review Remediation — 2026-10-30

## Result

**Status: `PARTIAL_VERIFICATION`.** The two findings from the independent review
handoff are addressed in code and covered by focused Rust and Python regressions.
The required fresh Kafka/MinIO campaign was not run, so this is not independent
reacceptance of Phase 16 and does not change the historical G0–G12 result.

This additive record is for reviewed base `2662c8d55bf81486aac40885f3efd87bb22465b6`.
The existing Phase 16 implementation report, acceptance archive, source inventory,
measurements, and failed candidates remain unchanged and describe their original
source. The attached handoff was treated as review evidence and task guidance, not
as proof that the requested changes or tests had passed. The work was completed
without subagents, in one local commit, with no push or external service use.

## Findings and changes

| Finding | Remediation |
|---|---|
| F16-01: a low-cost replay could be selected even when Kafka had expired its original offsets, stranding a still-usable verified checkpoint. | Recovery now verifies the selected checkpoint, then performs unassigned read-only broker identity and low/high checks for both the original replay vector and saved checkpoint vector before ranking. An ineligible candidate receives no recovery cost. If replay is expired and saved offsets remain retained, restore is chosen with an explicit replay refusal reason. If neither candidate is eligible, recovery fails before assignment/output and reports both reasons. `KafkaSource::connect` repeats the selected-vector range check immediately before assignment; a race fails closed without a post-decision fallback. |
| F16-02: the snapshot writer could change a scored incremental action to full after policy admission. | Snapshot planning now computes which placements can reuse the fully verified prior closure, including placement, remote identity, depth, copy-floor, and progress checks. Policy scores only executable actions, so full capacity and SLO constraints are applied before staging or remote probes. New decision traces record the reusable placement set, and the independent Python oracle checks those effective candidates while retaining replay support for historical traces. |

The first published cut after full replay is also forced self-contained even if
replay has advanced beyond the old checkpoint's record count. That force-full flag
is cleared only after successful publication.

The broker checks establish live ranges and configured source identity. They do
not establish topic UUID/incarnation or prove immutable history; the existing
trusted operator attestation and trace-relative, at-least-once contract remain.
No checkpoint format, selector, legacy v1, or default feature behavior changed.

## Validation

Passed on the remediation worktree:

- `cargo fmt --all -- --check`
- `cargo check --workspace --all-targets --locked`
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `cargo test -p ade-stream --all-features --locked --test adaptive_checkpoint` — 12 tests passed.
- `cargo test --workspace --all-features --locked -- --test-threads=1` — passed. Serial execution also avoids contention in the existing `ade-ops` staging fixture.
- `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p 'test_*.py'` — 51 tests passed.
- `python3 -m json.tool docs/reports/phase-16-support.json` and `git diff --check` — passed.
- `bash scripts/smoke.sh` — exited successfully through the local correctness smoke. Its one-sample cross-engine timing and configuration/dirty-tree diagnostics are not comparable performance evidence.
- Historical verifier: `PYTHONDONTWRITEBYTECODE=1 python3 scripts/phase16/verify_evidence.py --archive docs/evidence/phase16/acceptance.tar.gz --manifest docs/evidence/phase16/manifest.json` — passed with 117 cases, 104 sessions, 136 matched cuts, and 295 source files.

The historical archive replay is a baseline regression check only. It is not
evidence for the remediation source. The acceptance harness now defines positive
retained-checkpoint restore after replay-origin expiry and a both-vectors-expired
refusal; its verifier independently checks new recovery eligibility fields and
still replays the old archive schema. The placement-switch source campaign also
checks the effective full decision. Those new real-service cases were not run,
and there is no new source-bound acceptance archive or campaign identity. Local
unit/oracle regressions cover eligible recovery fallback and full-action capacity
admission.

## Unrun evidence and limits

No fresh real Kafka `deleteRecords` recovery run or remote MinIO placement campaign
was available. Docker, Podman, `kafka-topics`, and MinIO executables are not
installed in this environment. Therefore the handoff's decisive cases—especially
restoring from retained 20/20 offsets after expiring one replay origin, the
preflight/final-connect race, and live remote full-capacity refusal before PUT—
remain unverified against services. No G0–G12 gate is newly claimed as passed,
and Phase 16 is not independently reaccepted by this follow-up.

The prior Phase 16 source identities and all failed/premeasurement candidates
remain preserved. No production HA, cross-host/AZ durability, automatic recovery,
physical rescale, exactly-once delivery, or provider-cost claim is added.
