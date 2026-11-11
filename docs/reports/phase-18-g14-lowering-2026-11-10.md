# Phase 18 native stream lowering remediation — 2026-11-10

## Outcome

**`PARTIAL_PHASE18_UNIFIED_ADMISSION_V1` remains in force.** The validator-only G14 implementation gap is addressed in code: an admitted logical Kafka plan now compiles into a private `NativeStreamSpecV1`, and the existing Kafka source, stateless Engine, state/adaptive paths, and pure-live v3 transaction consume that spec or configs lowered from it. G14 is not closed because real Kafka semantic-mutation and parity evidence was not run. G0–G13 current-source reacceptance, G15 worker execution, and G16 quota evidence are also incomplete. No Phase 19 work is authorized.

## Source and worktree

- Baseline commit: `510671b2b72272d25495a719ab45c168b38c54f7` (`Address Phase 18 independent review`).
- Baseline tree: `6a165ab7ad6e5df594b182719ff748709323f413`.
- The baseline worktree had one unrelated untracked file, `sol-medium-phase-0-bootstrap-implementation-prompt.md`. It was preserved and excluded from the change.
- Phase 17 and candidate5 Phase 18 evidence archives were not changed. They remain bound to their original source/runtime and do not establish acceptance of this change.

## Implementation

`NativeStreamSpecV1` is compiled only from strict `LogicalPlanV1` admission. It has private fields, no public constructor, and no deserializer. Its deterministic hash is emitted once with unified admission telemetry; it is not added to per-record trace entries.

The spec holds the pinned Kafka identity, partition inventory and original starts, history/mode/cut, native filter and projection, event-time allowance, timers, optional keyed-window/late policy, sink delivery and semantic job identity. Binding checks legacy semantic config as a compatibility assertion before source/root/lease effects, then lowers effective runtime config from the spec. Endpoint, credentials, resource quotas, roots, checkpoint policy and mutable recovery cursor remain operational inputs. Restore progress must retain the plan's original start. The existing source, Engine, state/checkpoint and CURRENT3 implementations remain in use; no second stream kernel or selector format was added. Transactional hybrid still refuses before effects.

Focused tests cover stale config refusal, a consistent filter/time/timer mutation changing Engine output, unsupported projection refusal, source/state lowering and identity, and delivery binding. These are code-level tests, not live-broker evidence.

## Verification

The focused Rust checks passed on the final code before documentation updates. The `rdkafka-sys` CMake build initially failed to locate OpenSSL. Pointing it at the installed Homebrew OpenSSL 3 paths made the feature build and tests pass:

```bash
OPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3 \
OPENSSL_LIB_DIR=/opt/homebrew/opt/openssl@3/lib \
OPENSSL_INCLUDE_DIR=/opt/homebrew/opt/openssl@3/include \
cargo test -p ade-stream --all-features --test unified --locked
# 5 passed

cargo test -p ade-stream --no-default-features --lib --locked
# passed; 0 unit tests

cargo test -p ade-logical --locked
# 8 contract tests passed

cargo test -p ade-frontend --features unified-planner --test queries \
  unified_join_binds_without_a_physical_target_and_cli_refuses_pre_output \
  --locked -- --nocapture
# 1 passed; no-worker refusal reached before output

cargo check --workspace --all-targets --locked
OPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3 \
OPENSSL_LIB_DIR=/opt/homebrew/opt/openssl@3/lib \
OPENSSL_INCLUDE_DIR=/opt/homebrew/opt/openssl@3/include \
cargo check --workspace --all-targets --all-features --locked
OPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3 \
OPENSSL_LIB_DIR=/opt/homebrew/opt/openssl@3/lib \
OPENSSL_INCLUDE_DIR=/opt/homebrew/opt/openssl@3/include \
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
OPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3 \
OPENSSL_LIB_DIR=/opt/homebrew/opt/openssl@3/lib \
OPENSSL_INCLUDE_DIR=/opt/homebrew/opt/openssl@3/include \
cargo build --workspace --all-features --bins --examples --locked
# all four workspace check/lint/build commands passed

PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s scripts/tests -p 'test_*.py'
# 64 passed

cargo fmt --all -- --check
git diff --check
# passed
```

The broad all-features test was attempted in the normal sandbox with its build output under `/private/tmp`:

```bash
CARGO_TARGET_DIR=/private/tmp/ade-phase18-all-feature-tests \
OPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3 \
OPENSSL_LIB_DIR=/opt/homebrew/opt/openssl@3/lib \
OPENSSL_INCLUDE_DIR=/opt/homebrew/opt/openssl@3/include \
cargo test --workspace --all-features --locked
```

Compilation completed. The run stopped in `ade-distributed`: **67 tests passed and 29 tests failed** while opening local sockets with `PermissionDenied (Operation not permitted)`. Later workspace test targets therefore did not run. The elevated attempt was canceled during compilation before any tests ran; no tests were run with elevated socket access.

`bash scripts/smoke.sh` also exited 1 at its real worker/network path. A small isolated sandbox probe confirmed the exact boundary:

```text
ade-worker: I/O: Operation not permitted (os error 1)
ade-bench: ADE distributed execution: I/O: failed to fill whole buffer
```

This is recorded as blocked worker execution, not as product acceptance. The only frontend join result is the passing no-worker refusal test above.

Both immutable archives still verify offline, and their recorded SHA-256 values match:

```text
Phase 18 candidate5: fa38f4dbc0922d20b0d0075cd85dc54ee3f82b73c36ae7defb3a5c8beb2f2794
  verifier: 38 cases, 2,061 records, 1,897 classifications, 64 windows, 12 measurements
Phase 17 reacceptance: ace36b8fa54ef0a279b4050e04481c18a9d7da39764aecee29eed1db4cf99e05
  verifier: 137 cases, 152 reader observations, 387 matched cuts
```

The archive checks were:

```bash
python3 -B scripts/phase18/verify_evidence.py \
  --archive docs/evidence/phase18/acceptance.tar.gz \
  --manifest docs/evidence/phase18/manifest.json
python3 -B scripts/phase17/verify_evidence.py \
  --archive docs/evidence/phase18-phase17-reacceptance/acceptance.tar.gz \
  --manifest docs/evidence/phase18-phase17-reacceptance/manifest.json
```

These counts and hashes are historical archive replay, not current-source passes.

## Environment and remaining acceptance

Rust is `1.99.1 (58cf8f9c3 2026-10-13)`; Cargo is `1.99.1 (5f94df478 2026-08-27)`. OpenJDK `17.0.20.1` is installed at `/opt/homebrew/Cellar/openjdk@17/17.0.20.1/libexec/openjdk.jdk/Contents/Home`. Kafka and MinIO service binaries were not found. No broker campaign, worker-backed join, current-source Phase 17 reacceptance, or fresh evidence archive was created.

| Gate | Current result | Evidence / remaining work |
| --- | --- | --- |
| G0 | BLOCKED | No fresh Kafka/MinIO run on this source; Phase 17 prerequisite reacceptance is not current-source evidence. |
| G1 | PARTIAL | Strict plan admission is used for compilation; complete current-source mutation/tamper campaign not run. |
| G2 | HISTORICAL_ONLY | Existing optimizer evidence remains tied to its archived source. |
| G3 | PARTIAL | Target-neutral binding and pre-output no-worker refusal test pass; worker execution and independent join oracle do not. |
| G4 | BLOCKED | No fresh Kafka stateless/stateful/v3 parity. |
| G5 | HISTORICAL_ONLY | Prior hybrid/cut archive is unchanged; no current-source source-bound run. |
| G6 | HISTORICAL_ONLY | Phase 17 lifecycle evidence remains bound to its archived runtime. |
| G7 | PARTIAL | Stale semantic config refusal is covered; full current-source refusal campaign is not. |
| G8 | NOT_RUN | Current-source whole-resource and near-minimum quota campaign not run. |
| G9 | HISTORICAL_ONLY | No new optimizer measurement campaign. |
| G10 | PARTIAL | All-feature compile/check passes; full compatibility suite was blocked by local socket permissions. |
| G11 | PARTIAL | Format, checks, lint, focused tests and Python tests pass; workspace tests stop at socket-denied tests. |
| G12 | HISTORICAL_ONLY | Both historical archives verify; no versioned current-source archive, manifest, G14–G16 verifier or tamper campaign was produced. |
| G13 | PASS | README, active instructions, guide, ADR, plan, support matrix and this report state the qualified outcome and blockers. |
| G14 | BLOCKED | Plan-derived native spec and 5 focused tests pass; real Kafka semantic mutation/parity remains unverified. |
| G15 | BLOCKED | No-worker refusal passes; worker bind is denied by the sandbox before actual join execution. |
| G16 | BLOCKED | Paired 8 KiB/16 KiB V2 broker quota campaign was not run. |

The next acceptance environment needs disposable local Kafka and MinIO services plus a permitted local worker/socket runtime. Then add/run the current-source versioned G14–G16 criteria, fixture runner, independent verifier and rehashed tamper campaign; run Phase 17 and Phase 18 acceptance on the frozen final source; preserve raw attempts and archive hashes; and update this matrix only from verified evidence. Until that work passes, Phase 18 remains partial and Phase 19 stays unauthorized.
