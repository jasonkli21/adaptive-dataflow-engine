# Phase 15 Independent Review Remediation — 2026-10-29

## Result

**Verdict: `COMPLETE_BOUNDED_STATE_V1` with trace-relative output semantics.** The review findings below are addressed for the bounded local contract. This does not complete the broader PRD Phase 15 outcome: physical distributed rescale remains partial. The stateful Kafka path does not promise that arbitrary legal cross-partition replay orders produce identical window payloads, exactly-once external delivery, disk-loss recovery, topic-incarnation attestation, or production HA.

This is an additive remediation record for the review of implementation commit `7f838db7bfb0275f6f8e7c65aab8da90e4d6bd79`. It does not rewrite that commit's report or candidate8 evidence. The work was performed in one session without subagents. The independent Python model is a separate executable oracle, but this remediation does not claim a separate human or agent review.

## Findings and disposition

| Finding | Disposition |
|---|---|
| F15-01: legal partition merge orders can emit different aggregates under the same logical window ID | Retained the stable logical ID and added a canonical payload fingerprint (`window-payload-v1:<sum_i64>:<count_u64>`). Public docs now define outputs as trace-relative, warn that ID-only deduplication can discard a conflicting aggregate, and require consumers to compare fingerprints and reconcile conflicts. Rust and Python oracle tests cover both legal orders; the fresh Kafka campaign verifies the conflicting ID and payloads. |
| F15-02: idle time-only checkpoint cadence consumes finite generations | Checkpoint due-ness now requires newly forwarded receipt progress and then the configured count or elapsed-time trigger. Empty startup, unchanged idle state and idle restore do not publish repeated generations. Filtered and closed-window-late receipt progress still qualifies; changing state still checkpoints. Focused state tests cover these paths. |
| F15-03: the admitted minimum checkpoint budget could not stage an empty cut | Added a named minimum of 66,048 bytes and reject smaller limits during configuration admission with a specific error. Boundary tests cover empty and one-entry cuts, retained generation plus staging, and impossible admission. Support documentation reports the same limit. |
| F15-04: test stop inside an NDJSON frame could exit successfully | A stop at a zero-byte frame boundary remains graceful. Once bytes have been written, the runtime returns the distinct `partial_output_frame` error; it does not acknowledge the input or confirm the window close or publish a checkpoint. Terminal diagnostics expose pending input/window output and partial byte counts. Fake-clock tests cover stateless input and stateful window frames. The shared Phase 14 guide and ADR-0021 record this additive behavior. |
| F15-05: fresh execution evidence was missing | Ran the Rust and Python gates and a new source-bound Kafka 3.9.1 campaign on the final implementation source. Packaged and re-verified the resulting evidence at `docs/evidence/phase15-review-remediation/`. The original candidate8 evidence remains unchanged. |

## Contract clarification

`WindowOutput.output_id` remains a logical key (`job:key:start:end:1`), not a content identity. Different legal Kafka partition interleavings can advance the minimum watermark at different points and close a window with different values. A consumer must not assume that equal IDs imply equal payloads across arbitrary replay histories. Compare `payload_fingerprint` where present and apply an explicit reconciliation policy. The fingerprint detects differing sum/count payloads; it does not make replay deterministic or provide transactional sink semantics. Legacy output records without the additive token remain accepted by the Python verifier.

The selected behavior preserves the bounded single-owner state machine and its existing watermark policy. It does not add unbounded buffering or claim a global Kafka order. This is the qualified bounded result allowed by the review handoff; users requiring merge-independent deterministic results need a future contract and mechanism.

## Evidence identity

- Reviewed base: `7f838db7bfb0275f6f8e7c65aab8da90e4d6bd79`.
- Source-bound campaign: `/private/tmp/ade15-review-remediation-2026-10-29-c` (raw local campaign retained outside the repository).
- Campaign verifier: **PASS**, 70 cases, 55 sessions, 81 windows, 274 source files; queue plateau 13,317 bytes; recorded stall 34,267 ms.
- Platform/toolchain: macOS Darwin 27, arm64; `rustc 1.99.1`, `cargo 1.99.1`, Kafka 3.9.1 and JDK 17.
- Final source archive SHA-256: `b423453af6c9ce0c8cec17e3ef5179b33e75b2ea9943deee1f72c31402a53937`.
- Source inventory SHA-256: `778c6f8400801a8d617bd13512d734e0a92385a54bac5c666d646ae9719ee946`.
- Campaign binary SHA-256: `3cda500287aeaa254eaf2cb45183e2d674b33dbe550807b874a334451565b9f3`.
- Added evidence archive: `docs/evidence/phase15-review-remediation/acceptance.tar.gz`; SHA-256 `0e6b38b50b025ac38c4df8e4f57997254416eb3d658f22a859c19626a82c202e` (10,801 evidence files; 2,693,670 bytes).
- Added evidence manifest SHA-256: `afa6d5bebe4bfeaea11aae702a99c0b0a1152508a75f9672bf00d991ae50d23f`.
- The added package's offline verifier passed with the same 70-case/55-session/81-window result. Its symlink attack fixtures point shard paths at `/etc/passwd` and are included as refusal evidence.
- The adversarial case emits the same logical ID, `00000000-0000-0000-0000-00000000000f:1:0:100:1`, with `(sum=12,count=3)` versus `(sum=5,count=2)` and distinct payload fingerprints under the two legal interleavings.
- One early campaign attempt (`...-a`) exposed an acceptance-fixture job-ID mismatch and was not treated as a pass. Its raw directory and the pre-final campaign (`...-b`) remain under `/private/tmp`; only the final source-bound `...-c` campaign is packaged here.

The final source inventory excludes this report and evidence artifacts by design, so adding this record does not change the source identity proven by the campaign. The immutable Phase 15 candidate8 archive and report retain their original identities and contents.

## Validation

Passed on the final implementation source:

- `cargo fmt --all -- --check`
- `cargo check --workspace --all-targets --locked`
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `cargo test -p ade-stream --all-features --locked` — 28 tests passed.
- `cargo test --workspace --all-features --locked` — 280 passed, 0 failed, 10 ignored. The first sandboxed attempt could not bind loopback sockets (`Operation not permitted`); the authorized loopback rerun passed. The ignored tests are existing fixtures, timing, and S3 cases.
- `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p 'test_*.py'` — 44 passed.
- `cargo build -p ade-stream --all-features --locked --bin ade-state-stream`
- Fresh campaign and packaged evidence verification described above.
- Historical candidate8 archive verification: 66 cases, 51 sessions, 81 windows passed. Its archive hash remains the original `434003467c113e574e1e5d0cb8eacf978be833a07e49cec74726875bdb84747a`.

`bash scripts/smoke.sh` was run and exited 1 at its local benchmark comparison. The data generation and correctness checks reached that comparison; it reported a one-sample 186.4% regression together with mismatched run configuration/verification policy, engine configuration, and dirty source tree. The script stopped before later spill/distributed/join stages. This is not comparable performance evidence and is recorded as a diagnostic limitation, not as a passing smoke gate or a product timing result. No timing acceptance claim is made from it.

## Remaining limits

- Outputs are trace-relative across legal Kafka partition interleavings. The new fingerprint makes conflicts detectable, not merge-independent.
- Delivery to the local file descriptor remains at least once; external output is not transactional.
- Checkpoints are local and require retained trusted Kafka history. Disk-loss HA, physical distributed rescale, worker replacement, topic-incarnation attestation, savepoints, and adaptive checkpoint policy remain outside this bounded result.
- The smoke script's non-comparable benchmark comparison prevented its later stages from running in this remediation session; those stages are not represented as newly passing here.
- Historical Phase 14 and Phase 15 acceptance records remain separate evidence with their original source identities.
