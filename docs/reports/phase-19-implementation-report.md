# Phase 19 implementation report

## Outcome

`COMPLETE_BOUNDED_INCREMENTAL_MAINTENANCE_V1` under ADR-0026. The opt-in `ade-maintain` runtime publishes finite signed epochs from verified local Parquet and immutable delta files. It maintains two keyed sum/count views over one indexed arrangement. A fresh process can restore an exact committed target, update its compatible parent or fully replay the same history through the native ADE scan.

On 2026-11-11 the user authorized Phase 19 and one local commit, then explicitly waived current-source Phase 18 acceptance. G0 records that decision. Phase 18 remains `PARTIAL_PHASE18_UNIFIED_ADMISSION_V1`. Fresh Phase 17 and Phase 18 Kafka/MinIO service acceptance was not run under that waiver. The historical Phase 17 and Phase 18 archives passed offline verification and remain bound to their original runtimes. This implementation and its review ran in one session without subagents.

## Provenance

| Item | Identity |
| --- | --- |
| Starting commit | `2af859a26ba1ed559f383dcb129858c4d0ad31f5` |
| Starting tree | `f2495895585f42b36bb6380badc74fefcedaec1b` |
| Measured source inventory SHA-256 | `3612e92e03b911acfaaf11aa7ed1b5a4389fe6eda84270d794e199a8ff15e346` |
| Release `ade-maintain` SHA-256 | `a0f1a01c5e820281e4c7498bae02794a6a356493ba90a92c93ae8d7aa7b3d927` |
| Release baseline fixture encoder SHA-256 | `9126cf2e9fdabdfcc34c0dff84dabaa2ad0db9f954fc99ed043ec0588b64b0f5` |
| Frozen criteria SHA-256 | `0cd0af66dab3770afe4c926004a7df6df46b7b6424ba024cdd413d31b60226ac` |
| Platform | macOS 27.0.1 arm64, Apple M3, 8 logical CPUs, 24 GiB RAM, local system data volume |
| Toolchain | `rustc 1.99.1 (58cf8f9c3 2026-10-13)` and `cargo 1.99.1 (5f94df478 2026-08-27)` |
| Release features | `ade-incremental/maintained-v1,fault-injection`, LTO disabled, 16 codegen units |

The source inventory hashes workspace Rust and Cargo inputs and the acceptance and verifier code. The final commit carries those inputs and the evidence. The unrelated untracked bootstrap prompt was excluded. The acceptance runner recorded filesystem identity, source manifest details, immutable epoch bytes, exact plans, policy candidates, readers, crash steps and raw timings. Hardware details are supplemented by an authorized read-only host probe.

## Acceptance gates

| Gate | Result | Witness |
| --- | --- | --- |
| G0 Provenance | PASS with authorized waiver | Matched source and release executables, frozen criteria, environment and unchanged historical archives. Current-source Phase 18 acceptance was waived. |
| G1 Semantic identity | PASS | Independently hashed plans and chains. Duplicate fields, unknown fields, overlong and deeply nested shapes, altered thresholds and wrong target chains refuse. |
| G2 Weighted bag | PASS | Exact Python replay covers duplicates, cancellation, threshold crossings, signed minima and maxima, retractions, zero-count removal and signed replacement. Negative multiplicity and count/sum overflow refuse. |
| G3 Finite input | PASS | Native Parquet preflight and postflight, independent PLAIN fixture decoder, signed delta file inventory, schema refusals, wrong predecessor and reordered delta refusals, in-flight source mutation and restored-byte identity mutation. |
| G4 Native execution | PASS | Baseline scan, full replay and differential update produced the independent result in both views across all four fixtures. |
| G5 Arrangement reuse | PASS | Both consumers share one indexed arrangement. Sparse update touched four keys and four shards and decoded zero baseline rows. Exact reused objects and both views were verified after restart. |
| G6 Atomic publication | PASS | Six reached SIGKILL boundaries, exact witness recovery, exclusive writers, no provisional reader, short-write injection, stage and object tamper, root clone, lease replacement and selector rollback refusals. |
| G7 Idempotence | PASS | Exact retries restored without applying a delta. Divergent retry, fork and gap refused. Restarted signed continuation matched uninterrupted and full replay. |
| G8 Optimizer | PASS | Independently reconstructed all eligible candidate estimates, costs and selected routes. Three adaptive winners executed. Forced routes and disabled full replay exercised. Unsafe forced routes refused. |
| G9 Resources | PASS | Exact small limits passed. Hot, disk, state and output quotas refused when insufficient. Deadline and cancellation stopped execution before selection. |
| G10 Performance | PASS | 45 valid timed children, nine warmups, frozen thresholds and raw matched samples. All three gates passed on this host. |
| G11 Compatibility | PASS | Default-off runtime with an empty default normal dependency tree. Workspace correctness, native worker smoke and focused Phase 11 and 14–18 coverage passed. Existing formats and source identities retain their contracts. |
| G12 Toolchain | PASS | Format, locked workspace check, all-feature Clippy/tests, worker smoke, default feature checks and Python regressions passed. |
| G13 Independent evidence | PASS | Offline Parquet and epoch replay reconstructed both views, all committed snapshots, route scores and medians. Three semantic mutations with recomputed outer checksums refused. |
| G14 Documentation | PASS | ADR, reconciled plan, operator guide, support matrix, report and repository handoff describe the bounded contract and waiver. |
| G15 Separate-process lifecycle | PASS | Bootstrap, signed apply, read, six separate killed/resumed attempts, neutral and signed continuation and final reads. Full replay, restore and incremental routes independently agreed on result identities. |

G6 used an independently observed short write after a physical shard prefix and a returned write error. The Phase 19 campaign did not provision a full filesystem for actual OS ENOSPC. Process kill coverage exercises local visibility and restart on an honest filesystem. It does not measure power-loss behavior.

## Local performance and resources

Each regime used five valid measured children per policy after one discarded warmup, with rotated and reversed order. Bootstrap or target preparation happened before the timer. Each measured process used the same input, caps and target as its controls. An untimed fresh reader validated both committed views after each process. The sample clock includes child launch, admission, native work and publication or restoration.

| Regime | Adaptive route | Adaptive median | Static controls | Required ratio | Observed |
| --- | --- | ---: | --- | ---: | ---: |
| Sparse, 750,000 rows and 4 changed keys | Incremental update | 67.818 ms | Incremental 66.082 ms, full 530.708 ms | Incremental/full ≤0.80 | 0.1245 |
| Dense, 4,096 rows and 4,096 changed keys | Full recompute | 486.665 ms | Full 497.183 ms, incremental 477.374 ms | Adaptive/fastest ≤1.25 | 1.0195 |
| Already committed target | Restore | 31.166 ms | Restore 33.565 ms, full 89.410 ms | Adaptive/fastest ≤1.25 | 0.9285 |

Sparse incremental execution decoded no baseline rows, applied eight signed delta rows, visited both views for four keys, and wrote 24,192 shard bytes. The matched full route decoded 750,000 rows and wrote 772,160 shard bytes. Source attestation remained active because the paired requests supplied the baseline. Sparse incremental verified 24,022,186 baseline bytes across admission and postflight. Target-only continuation also ran without a baseline binding.

The maximum observed hot arrangement ledger in the timed campaign was 40,980 entries, the decoded batch peak was 16,576 bytes, the largest selected arrangement was 772,160 bytes and the largest root ledger was 1,613,528 bytes. The bounds cover managed logical entries, encoded state, file bytes and batch data. Allocator overhead, filesystem metadata, OS cache and RSS are outside these ledgers. Timing and fault evidence describe one local machine.

## Reproduce and inspect

```sh
CARGO_PROFILE_RELEASE_LTO=false CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 \
  cargo build --release --locked --offline -p ade-incremental \
  --features maintained-v1,fault-injection --bin ade-maintain --example baseline
python3 -B scripts/phase19/acceptance.py --out /private/tmp/ade19-reproduction
python3 -B scripts/phase19/package_evidence.py \
  --run /private/tmp/ade19-reproduction --output /private/tmp/ade19-packaged
python3 -B scripts/phase19/verify_evidence.py \
  --archive docs/evidence/phase19/acceptance.tar.gz \
  --manifest docs/evidence/phase19/manifest.json --repo . \
  --bin target/release/ade-maintain --encoder target/release/examples/baseline --tamper
```

The packaged final run records 55 functional observations, 54 child samples including nine warmups, 13 native snapshots and representative raw reader ledgers. The detached verifier can omit `--repo`, `--bin` and `--encoder` when replaying archived semantics without a local checkout or build. The archive SHA-256 is `54512c4f5c0985b3daea78d28f65f16713af296d157df8ffbe8d42ff97cc5d36`. Its manifest pins all 1,853 member hashes. The `attempts/` entries preserve a premeasurement fault-hook omission, the subsequent functional rehearsal, a premeasurement host-query refusal, and the previous measured campaign with all 54 child samples and its lint failure. The previous campaign used the same criteria. The final campaign repeats the measurement after a lint-only CLI edit and a packaging import fix. Its release executable hashes match the previous campaign, while its source inventory is distinct.

The workspace suite passed 330 Rust tests. Ten pre-existing remote-service and leased diagnostic tests retained their ignored status. All 64 Python regressions and the real-worker smoke passed. Compatibility commands used Homebrew OpenSSL for Kafka-feature compilation and local socket access for real workers.

```sh
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
bash scripts/smoke.sh
cargo check -p ade-incremental --no-default-features --locked
cargo tree -p ade-incremental --no-default-features -e normal --locked
cargo tree -p ade-incremental --features maintained-v1 -e normal --locked
cargo test -p ade-stream --no-default-features --lib --locked
python3 -B -m unittest discover -s scripts/tests -p 'test_*.py'
python3 -B scripts/phase19/verify_evidence.py --help
```

## Supported envelope

Supported use is one operator-owned local lineage with bounded immutable Parquet and epoch files, an independently retained target chain, and single-host filesystem commit. The current reader serves the exact selected head. Older immutable generations remain referenced for integrity and reuse. A fresh empty root can replay a target without adopting or overwriting a damaged root.

General SQL maintenance, differential joins, live Kafka CDC, remote or distributed arrangements, physical rescale, automatic garbage collection, production HA and external transactions remain deferred. Earlier phase debt remains separate. Phase 20 has no authorization in this work.

[Operator guide](../phase-19-maintained-dataflows-guide.md) · [ADR-0026](../adr/0026-incremental-maintained-dataflows-v1.md) · [Support matrix](phase-19-support.json) · [Evidence manifest](../evidence/phase19/manifest.json)

## Independent review remediation

The 2026-11-11 independent review findings F19-01–F19-05 are addressed in the additive [review remediation report](phase-19-review-remediation-2026-11-11.md). It records the strict optional baseline binding, exact CLI stream output cap, route read accounting, independently addressable views, a fresh matched acceptance archive and a real disposable-filesystem ENOSPC result. This implementation report's original source identity, samples and evidence archive remain unchanged; use the addendum for current-source qualification.
