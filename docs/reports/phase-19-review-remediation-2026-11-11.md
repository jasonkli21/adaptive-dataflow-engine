# Phase 19 independent review remediation — 2026-11-11

## Disposition

The review findings F19-01 through F19-04 are repaired and verified on the matched remediation source. Phase 19 remains `COMPLETE_BOUNDED_INCREMENTAL_MAINTENANCE_V1` within ADR-0026's local finite-epoch contract. F19-05's native build, fault and actual OS ENOSPC validation was completed here. This does not close the separate Phase 18 G14–G16 or current-source Phase 17/18 Kafka, MinIO and worker acceptance debt. The explicit Phase 18 waiver remains authorization for Phase 19 only.

The original Phase 19 report, frozen criteria, archive and manifest remain unchanged. The original evidence remains bound to source inventory `3612e92e03b911acfaaf11aa7ed1b5a4389fe6eda84270d794e199a8ff15e346`. Additive source-bound evidence is in [`docs/evidence/phase19-review-remediation/`](../evidence/phase19-review-remediation/).

## Repairs and witnesses

| Finding | Repair | New evidence |
| --- | --- | --- |
| F19-01 supplied baseline downgraded to absent | `Baseline::open` now returns an error on every missing or invalid named file. Only omission at the request layer represents parent-only continuation. Input admission completes before root initialization and policy selection. | Rust regression plus first, middle and last missing-file cases against an existing epoch-0 head; refusal checks for `apply`, `explain` and forced restore; each refusal is followed by an epoch-0 read. Omitted baseline with an exact tail then completes an incremental epoch-1 update. |
| F19-02 output budget excluded the footer | CLI `read`, `read-view` and `verify` now account for exact serialized stdout bytes, including the footer and newline, in the output writer. An over-cap operation exits nonzero; consumers require both success and a complete footer. The generation no longer uses a 128-byte-per-row estimate as a read cap. Library callback consumers own serialization budgets. | Empty, one-row and many-row outputs exercised at caps 1, 127, 128, one byte below exact, and exact. Failure cases verify the byte ceiling and reject a complete success footer. |
| F19-03 route estimates omitted mandatory verification | `Work` exposes shared predecision `admission_bytes` and additional route-specific `route_verify_bytes`. Restore, incremental and full replay include known mandatory source/checkpoint/shard reads. The independent Python oracle recomputes both fields and candidate rankings. | 54 paired samples and all three route regimes verified by the independent oracle. Original thresholds and workload definitions were unchanged. The proxy remains a deterministic work estimate, not a wall-time or storage-latency model. |
| F19-04 named view consumers were not independently addressable | Added `read_view` and CLI `read-view` for `all_v1` and `filtered_v1`, sharing the same committed root and arrangement. `defined_views` identifies stored definitions; `ReadOutcome.views_read` reports one or two selected views. Existing combined `read` remains supported. | Separate concurrent reader processes selected each named view from one committed generation. The independent oracle matched each bag and both readers reported the same manifest hash and result identity. Unknown views refused before output. |
| F19-05 reviewer could not execute native/fault/ENOSPC checks | Ran the native toolchain, workspace checks and source-matched acceptance on this host. A separate 32 MiB HFS+ image was filled to zero available KiB; a real epoch apply returned OS error 28. | [`actual-enospc.json`](../evidence/phase19-review-remediation/actual-enospc.json) records fill errors, apply exit/stderr/stdout, selected generation, manifest hash and post-recovery epoch-0 readback. This is local process/storage evidence, not power-loss or disk-loss proof. |

## Matched source and evidence identities

| Item | Identity |
| --- | --- |
| Review baseline commit | `6cc1d19d713dcb544eb481d1c5a2d87935643e44` |
| Baseline tree | `73951b53a3ccecfe4160ee52e37d7b3e9b060a3c` |
| Remediation source inventory SHA-256 | `5c941dd614e52ca57811fd9f6e776abc3d7b88d55481e8f03c55af88e3cd2c67` |
| `ade-maintain` SHA-256 | `a160dbd536c1f082dd963b65a3049d2652fa87984528cd8bc4d7b87e2004d6d5` |
| Fixture encoder SHA-256 | `9126cf2e9fdabdfcc34c0dff84dabaa2ad0db9f954fc99ed043ec0588b64b0f5` |
| Frozen criteria SHA-256 | `0cd0af66dab3770afe4c926004a7df6df46b7b6424ba024cdd413d31b60226ac` |
| Remediation acceptance archive SHA-256 | `0aac3c3286396ab6a409167b221da7a1abe81c64d697c59377b95558bd172d4a` |
| Remediation archive manifest SHA-256 | `6036ac0fd8b157cb5a6bf439990ed61750f30b1faac04e2b9ba4523959963959` |
| Actual ENOSPC supplement SHA-256 | `cac8a461c9b47e802b8796fcc63f01bfefd90719acb4e36dea51f58212fddf67` |

The source inventory was computed from the modified worktree rooted at the review baseline; the acceptance campaign matched every Rust/Cargo and Phase 19 script input to its inventory, and the final remediation commit includes those inputs. The archive contains 78 functional cases, 54 paired samples, 13 snapshots and the independent verifier. All three rehashed semantic attacks were refused. The original archive was independently reverified offline using its original verifier; neither old archive was rewritten.

## Acceptance results

| Gate | Remediation result | Evidence / qualification |
| --- | --- | --- |
| G0 | PASS, waiver-qualified | The Phase 18 waiver is recorded. Phase 18 remains partial. |
| G1–G2 | PASS | Existing identities and independent signed-bag replay retain their contracts; original evidence stays immutable. |
| G3 | PASS | Explicit incomplete baselines fail closed; omission remains the only parent-only path. |
| G4 | PASS | Native full and incremental routes and both view bags matched the independent oracle. |
| G5 | PASS | Separate named readers share one generation/manifest; one arrangement defines both views. |
| G6 | PASS, local filesystem scope | Process-kill boundaries and actual OS ENOSPC preserve generation 0; retry/reader checks pass. No arbitrary power-loss guarantee is claimed. |
| G7 | PASS | Original retry, replay and lineage checks remain valid in the new campaign. |
| G8 | PASS | Independent policy oracle matched candidate work fields and selections. |
| G9 | PASS, CLI stream scope | Exact serialized CLI output cap includes footer and newline. Callback API serialization is caller-accounted. Existing decoder, allocator and RSS exclusions remain. |
| G10 | PASS, host-local | Sparse incremental/full median ratio 0.12023; dense/adaptive-to-fastest-static ratio 0.99415; restore/adaptive-to-static ratio 1.05194. Each remains within the frozen criteria. |
| G11 | PASS | Default-off path and legacy selectors remain unchanged. Current-source Phase 18 Kafka/worker acceptance is separate and still open. |
| G12 | PASS | Workspace format/check/clippy/tests, no-default feature check, logical tests, Python suite and smoke completed. One earlier full run had a transient unrelated join source-drift assertion; the isolated rerun and final full run passed. |
| G13 | PASS | Matched inventory, offline independent archive verification and three rehashed tamper refusals. |
| G14 | PASS | ADR, plan, guide, support and this report now state the reviewed contracts and remaining scope. |
| G15 | PASS | Existing separate-process lifecycle plus two named-view reader processes verified the committed target. |

Fresh supplementary medians were:

| Regime | Adaptive median | Static comparison | Ratio |
| --- | ---: | ---: | ---: |
| Sparse | 68,363,375 ns | Full replay: 564,990,750 ns | 0.12023 incremental/full |
| Dense | 466,614,334 ns | Fastest static full replay: 469,357,792 ns | 0.99415 |
| Restore | 34,385,375 ns | Static restore: 32,687,667 ns | 1.05194 |

These are host-local observations from five measured samples per policy/regime after warmup. They do not establish cross-host repeatability or cost savings.

## Commands run

All commands below completed successfully on the final remediation source except the first sandboxed workspace test attempt and one transient full-run assertion described above.

```text
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo check -p ade-incremental --no-default-features --locked
cargo test -p ade-incremental --all-features --locked
cargo test -p ade-logical --locked
python3 -B -m unittest discover -s scripts/tests -p 'test_*.py'
bash scripts/smoke.sh
python3 -B scripts/phase19/verify_evidence.py --archive docs/evidence/phase19-review-remediation/acceptance.tar.gz --manifest docs/evidence/phase19-review-remediation/manifest.json --repo . --bin target/release/ade-maintain --encoder target/release/examples/baseline --tamper
```

The sandboxed workspace test attempt initially failed loopback socket tests with `PermissionDenied`. The authorized run with local socket access passed except for one existing timing-sensitive source-drift assertion; its isolated rerun passed, and a subsequent full workspace run passed. `scripts/smoke.sh` exited successfully; it reported a one-iteration smoke measurement and a configuration-difference regression warning, so no performance conclusion is drawn from smoke.

## Preserved unsuccessful attempts and remaining limits

Intermediate harness and verifier failures are retained under [`attempts/`](../evidence/phase19-review-remediation/attempts/), including their raw cases, sample, performance and provenance files where available. The final archive supersedes those attempts without deleting them. Original Phase 19 samples, archive and manifest remain untouched.

Phase 18 remains `PARTIAL_PHASE18_UNIFIED_ADMISSION_V1`; its current-source Kafka semantic parity, worker join and quota campaign remain unverified. Current-source Phase 17 Kafka/MinIO reacceptance was not rerun. Local HFS+ ENOSPC and process interruption do not prove power-loss behavior, hostile filesystem safety, disk-loss recovery, remote durability or production HA. The output cap applies to the CLI serializer; library callbacks must enforce their own byte budgets.
