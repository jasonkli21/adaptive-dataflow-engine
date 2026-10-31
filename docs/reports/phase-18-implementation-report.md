# Phase18 implementation report — 2026-10-31

Status: **COMPLETE_BOUNDED_UNIFIED_PLANNER_V1**. All G0–G13 pass on the frozen
runtime/executables and real owned services identified below. Work began on
2026-10-30 and concluded in this session without subagents. One local project
commit delivers the implementation; its final SHA is returned with this report.
No push, PR or production service mutation occurred. No Phase19 is authorized.

## Baseline, challenge and final identity

Baseline HEAD: `fbe0e2b7739fd7d61c45c58706faa56b63a945be`. Initial tracked diff was
empty. The supplied untracked Phase18 plan was preserved and reconciled in place;
the unrelated bootstrap prompt remains untouched and excluded. Phase17 initially
had no fresh G15 service acceptance. That prerequisite is now independently
reaccepted in the [additive report](phase-17-reacceptance-2026-10-31.md).

[ADR-0025](../adr/0025-unified-bounded-unbounded-planning-v1.md) records the
same-session plan challenge before implementation: termination is distinct from
frontier/time; batch groups differ from event-time windows; cuts are per-partition
offsets, not watermarks or atomic snapshots; source incarnation needs attestation;
stream filters cannot cross receipt/state/time boundaries; physical decisions and
resource constraints cannot change semantic identity; v3 hybrid cannot acquire an
unauthoritative sidecar. KafkaRange is explicitly reserved/refused in this v1,
because native finite Kafka EOF/progress is not introduced. Transactional hybrid
is refused before effects. These decisions reconcile the preserved plan.

The final source/executable campaign is candidate5. Its public
[manifest](../evidence/phase18/manifest.json) binds1,848 files and the
[portable archive](../evidence/phase18/acceptance.tar.gz). Both final campaigns use
exactly the same 147 Rust/Cargo inputs, still byte-identical in the delivered tree.
Documentation and independent-verifier strengthening after measurement have their
own [verification inventory](../evidence/phase18-verification/manifest.json).
No runtime, acceptance threshold, dataset or executable changed after measurement.

| Identity | SHA256 / value |
|---|---|
| Phase18 source inventory,352 files | `53e53f53e7c963a8ed74915eb1bedaaff38a9a918b0f21421d625feb164aa646` |
| Phase18 raw runtime inventory | `f3819bf56a757d93b1c758a4c6c033a75fc3f04647360f5bd7b1137e1fd91f80` |
| Canonical147-input set shared with Phase17 | `3b916fb9744a5aa66ef347497af2e034e160d75aaff0ce86619d45f2731a4b91` |
| Phase18 portable archive | `fa38f4dbc0922d20b0d0075cd85dc54ee3f82b73c36ae7defb3a5c8beb2f2794` |
| ade-plan | `70dd174ed477a064606626bc49ad61ca25ec2e1d1dd7e50c2ae1b2031f2b0d43` |
| ade-query | `11ee28e09d97a78065d9c1e7d9d9dc78dd609767a4ed4fbff629f417cb39c0d1` |
| ade-stream | `25c3810dc36c0a797c1c14e6051a7fd1478dbe2de108fb16e3ac09525d6af96d` |
| ade-state-stream | `d1b6d9d93c0b4ceecfb6b59ab2d7adda7b26aa9ca3e525ff3e556116f156d598` |
| ade-state-stream-txn | `1b81e47a33781e3aa6b41f3c716ba3baddf37cedb09168f23a95a8c450fc0b7f` |
| ade-worker | `df3ab563d3add4c37a4cfad49bfda57b4c397ed478266495f35e426f04206eb8` |
| Separate DataFusion reference | `6f52c139f9c16a2cf33628a6f7a274b7bf0c9afee8f5dfe68d176a5bd88a4655` |

Environment: macOS Darwin 27 arm64, Rust 1.99.1/LLVM 23.1.1, Kafka 3.9.1/JDK 17.0.20.1,
rdkafka 0.39.0/librdkafka 2.12.1. The actual MinIO binary SHA256 is
`5c5445919b1060311618aa4efe5450fadc29580ef86f2ddc34e931859eda02c0`
(Go module commit07c3a429bfed). Kafka JAR hashes, Java executable hash/version
stderr, owned broker configuration, service PIDs/ports, producer ledgers and all
commands are retained. The legacy service helper's `java_version` stdout field is
empty because Java prints its version to stderr; that actual stderr is preserved.
Services were separate processes on this host, not physical HA/AZ evidence.

## Implemented behavior and refusals

A real neutral `ade-logical` crate owns strict LogicalPlanV1, derived schema,
termination/phase/time/frontier/replay/order/distribution/delivery properties,
semantic hashing and one deterministic legality-first optimizer. It depends on the
portable ade-physical types and finite schema inference, not Kafka, DataFusion or
runtime code. Its explicit ade-plan utility explains version1 plans; execution
routes remain behind default-off unified-planner features and explicit flags.

SQL and typed DataFrame binding reuse established semantic operator construction,
discard physical properties/target, then regenerate unchanged PlanV1 from the
shared optimizer's actual choice. Native execution consumes that chosen graph.
The independent verifier reconstructs the complete executed PlanV1 and hash,
checks native entrypoint/workers/exchange metrics, actual Parquet fingerprints and
separately executed DataFusion bags. This is an executed route, not a printed
wrapper over an unrelated hand-authored query switch.

Kafka descriptors use the same optimizer/admission before native effects and
validate their source, transforms, timers, state and sink against the unchanged
native configurations. The existing Arrow Engine, source demand/ACK, keyed stores,
checkpoint policy and transactional functions execute. No second stream kernel,
filter rewrite, delivery downgrade or checkpoint-mechanism cost substitution exists.
The compiled admission's decision view is read-only; a compile-fail test prevents
Rust callers from laundering derived properties through mutation.

| Axis | Supported bounded result | Explicit refusal / boundary |
|---|---|---|
| Batch | Phase11 SQL/Rust operator families, schemas/aliases, bags/ordered sort, local unary and existing fixed-worker aggregate/join | General SQL and unsupported native graph shapes remain refused; no DataFusion fallback. |
| Optimizer | Local vs distributed unfiltered GroupSumCount; deterministic integer proxy; hard forced target and static disable | No price/latency calibration, learned policy or guaranteed performance benefit. |
| Stream | EventV1 filter/project and native diagnostic timers; tumbling sum/count with receipt_drop_closed_v1; v1/v2/v3 modes | No stream SQL/joins, unbounded sort/global LIMIT/collect, UDFs or alternate kernels. |
| History/live | One pinned Kafka topic/full inventory, fixed nonempty assigned subset, explicit exclusive cut per assigned partition | No global atomic snapshot, Parquet/CDC bridge, multi-source join or finite KafkaRange EOF. |
| v1/v2 identity | All semantic fields, origin and cut bind through a derived128-bit native job UUID; manual exact restore | Fresh unified lineage required; no implicit adoption/migration of old v1/v2 jobs. |
| v3 | Pure live through unchanged authoritative lineage/reader/fences/PAUSED/savepoints/transitions | Transactional hybrid and committed-to-stdout downgrade refuse before root/broker effects. |
| Operations | Native fixed membership, bounded quotas, cancellation and explicit restore | No physical rescale, automatic recovery/GC, production HA/cloud/external transactions or Phase19 deltas. |

Semantic identity excludes endpoints, credentials, roots, physical target and
mutable resources. Finite output ceilings are normalized only for semantic hashing;
their actual values remain hard execution constraints and part of the native plan
hash. Shared v1/v2 state uses the derived job identity; pure-live v3 retains its
existing full native job/original-start/history fingerprint. Existing native jobs,
formats and defaults continue through their original entrypoints.

## Gate results and witnesses

| Gate | Result | Executed witness |
|---|---|---|
| G0 | PASS | Pinned baseline/dirty inventory,147 matched inputs/executable hashes; all 385 historical evidence files match baseline Git objects; fresh Phase17 G0–G15 plus real Kafka/MinIO prerequisite and actual ENOSPC. |
| G1 | PASS | Six neutral Rust contract tests, read-only authority compile-fail test, independent inference/canonical digest; unknown/duplicate/forged fields, cycles, oversized metadata/plan, bad time/cut/sink and resource identity checks. |
| G2 | PASS | Independent candidate arithmetic/legality/order; deterministic ties/static/forced controls; both actually executed aggregate targets and native PlanV1 hash reconstruction. |
| G3 | PASS | Full Phase11 positive SQL/Rust family matrix through new and legacy routes vs DataFusion; real2/4-worker aggregate/join; signed overflow, caps, deadline, source binding/drift and no-result refusals. |
| G4 | PASS | Real Kafka old/new stateless timer/cursor parity, stateful parity, v2 local/both restore, v3 separate committed reader; every receipt/ACK/frontier/filter/late/window checked against original producer identities. |
| G5 | PASS | Two/four partitions with actual read-only low/high cut capture; same session across prefix/tail; legal aborted/control holes, empty prefix, asynchronous/idle tail, assigned subset, causal closed-late drop,30s stall/resume, cold restore and actual checkpoint SIGKILL; changed/rebound cut/history and expired origin refuse. |
| G6 | PASS | Fresh137-case Phase17 lifecycle/poison/selector/savepoint/pause/codec/owner campaign; pure-live v3 output prefix; hybrid/downgrade pre-effects refusal; old selected v1 cut survives SIGKILL before pointer publication. |
| G7 | PASS | Exact failure-stage/reason/input predicates, no stdout/assignment on admission failures, unchanged roots for cut/history refusal; schema/source/manifest/path/selector/capability negatives in focused/full suites and fresh Phase17. |
| G8 | PASS | Fixed plan/search/trace and native resource caps; programmatic metadata caps before copies; actual tiny hot/cold/checkpoint/client peaks; blocked-source demand holds polls/ACK fixed, cancellation and quota/OS-error tests. |
| G9 | PASS | All 12 predeclared matched static/optimized samples retained; independent DataFusion bags; fixed proxy estimates labeled, actual losses/outliers disclosed below; no speed threshold or billed-cost claim. |
| G10 | PASS | Default/relevant-feature matrix, existing APIs/CLIs/protocol tests and historical replay; PlanV1, StreamConfigV1, CURRENT/CURRENT2/CURRENT3, old benchmark/result/child schemas unchanged. |
| G11 | PASS | Locked fmt/check/strict Clippy,319 Rust passes/0 failures/10 existing ignores,63 final Python passes, smoke and10 feature checks; focused prior-phase regressions included. |
| G12 | PASS | Portable source/executable/service/producer/output/state evidence; independent complete native-plan reconstruction and raw state/output oracle; eight semantic attacks refused after recomputing outer hashes, including state checksums/manifest/selector hashes. |
| G13 | PASS | Reconciled plan/ADR, guide, this report, additive Phase17 closure, machine support matrix, README/AGENTS, preserved negatives and explicit next-phase boundary. |

The primary campaign verifies38 process cases,2,061 admitted records,1,897 explicit
historical/live classifications,64 windows and12 batch measurements. A historical
cut never seeks, resets state, forces a watermark/window close or converts idle to
EOF. Actual receipt offsets, session, attempt start, every ACK/cursor/frontier and
selected state are checked; offset density is never assumed. Causally released
late events prove closed windows do not reopen. V1/v2 output remains trace-relative
at least once. Restart reproduces the exact selected cut, not arbitrary merge order.

Current-source Phase17 independently verifies137 cases,152 reader observations,
387 cuts,70 sessions and4 exports. Its five Kafka/MinIO prerequisite probes include
848 signed requests/172 remote objects. Actual ENOSPC preserves80 records/80 state
entries and reader output. CURRENT3 remains the sole visibility selector;
ACTIVATED3 remains a refusal witness. No stale savepoint/root or PAUSED handle is
silently treated as a new lineage.

Measured peaks: hot128 bytes, cold disk1,216 bytes, checkpoint ledger140,279 bytes,
Kafka queue13,356 bytes. Native checkpoint metadata has a separate128KiB domain.
Plan<=256KiB/64 nodes/depth; candidates<=3; decision<=16KiB; one native input/frame,
zero downstream channel, sink frame<=2KiB. Kafka queue targets can overshoot by one
<=64KiB response per admitted broker (<=4). Plan/diagnostic metadata, decoder,
allocator/client, OS/page-cache and RSS are separate; these ledgers are not a total
process-memory guarantee. No hidden historical-record buffer was added.

## Controlled science: all samples, including losses

Times are whole-process fixture elapsed milliseconds, including startup/logging;
no warmup, fixed static-then-optimized order, three samples, local debug builds.
The declared proxy is local `4*rows`, distributed `20000+2*rows`, tie local. It is
not calibrated latency or dollars. Both targets have exactly matching bags.

| Rows / chosen optimized target | Static samples ms | Optimized samples ms | Median optimized/static |
|---|---|---|---|
|128 / local |4833.340,40.784,41.138 |41.984,41.129,40.601 |0.9998 |
|12000 / distributed aggregate |103.891,102.596,103.491 |1646.325,153.389,154.015 |1.4882 |

The first-process outliers are retained. Distribution was slower here; **no
performance advantage is claimed**. The gate required truthful controlled
measurements and two executable legal choices, not a speedup. Prices, SLO models,
physical elasticity and future calibrated optimization are not inferred.

## Commands and portable reproduction

All required checks ran successfully; raw logs and exact argument vectors are in
the verification bundle. Individual stateful-only builds retain the preexisting
unused Start import warning. Large macOS debug links retain the existing compact-
unwind warning. Neither is claimed as a new runtime failure or hidden skipped gate.
Smoke's mixed-engine one-sample configuration/timing warnings are descriptive, not
Phase10 release evidence. Ten existing ignored service/large/timing tests retain
their original status; no new test was ignored.

```sh
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked -- --test-threads=1
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p 'test_*.py'
bash scripts/smoke.sh
```

Feature checks: ade-stream no-default, kafka, stateful, transactional-state,
adaptive-checkpoint, remote-checkpoint, unified-planner, and unified-planner with
transactional-state/remote-checkpoint; ade-frontend no-default and unified-planner.
Final binaries/examples were built together with all features and --locked for
ade-stream/ade-logical/ade-frontend/ade-distributed/ade-bench.

```sh
python3 scripts/phase18/acceptance.py --output NEW_ABSOLUTE_DIRECTORY \
  --kafka-home KAFKA_HOME --java-home JAVA_HOME --minio MINIO_BINARY \
  --service-python PYTHON_WITH_BOTO3
python3 scripts/phase17/acceptance.py --criteria scripts/phase18/phase17-criteria.json \
  --output ANOTHER_NEW_ABSOLUTE_DIRECTORY --kafka-home KAFKA_HOME \
  --java-home JAVA_HOME --minio MINIO_BINARY --service-python PYTHON_WITH_BOTO3
python3 scripts/phase17/enospc.py --campaign CAMPAIGN --output CAMPAIGN/supplemental/enospc
python3 scripts/phase18/verify_evidence.py \
  --archive docs/evidence/phase18/acceptance.tar.gz \
  --manifest docs/evidence/phase18/manifest.json
python3 scripts/phase17/verify_evidence.py \
  --archive docs/evidence/phase18-phase17-reacceptance/acceptance.tar.gz \
  --manifest docs/evidence/phase18-phase17-reacceptance/manifest.json
python3 scripts/phase18/tamper_checks.py \
  --archive docs/evidence/phase18/acceptance.tar.gz \
  --manifest docs/evidence/phase18/manifest.json --output NEW_TAMPER_DIRECTORY
```

The final Phase17 package includes its matched-runtime/required-checks inventory,
workspace logs and supplemental ENOSPC proof; see its package/verification tools
for packaging a new raw campaign. The operator [guide](../phase-18-unified-planner-guide.md)
and [support matrix](phase-18-support.json) give exact API/identity/refusal rules.
No local service or proprietary Python module is needed for portable verification.

## Preserved negatives and limits

[Development evidence](../evidence/phase18-development/inventory.json) retains all
four earlier candidates: wrong v1 fault-hook fixture; leading-dash Kafka cluster-ID
setup failure; correct native sink-deadline refusal; and an admin timeout while
heavy workspace tests ran. Baseline socket refusal/preliminary Phase17 evidence,
compiler/lint iterations, verifier variable/log-name repairs and all raw diagnostics
are retained. Candidate5 alone qualifies Phase18. Later verifier hardening adds
strict negative-input/type checks, native config/source binding, native-plan hash
reconstruction and rehashed semantic attacks; it does not change runtime inputs or
acceptance thresholds. Every earlier phase archive remains byte-identical.

Source history must be operator-attested immutable, uncompressed and <=32KiB
batches. Metadata cannot prove topic incarnation or an undetected truncate/regrow;
per-partition cuts are not a globally atomic snapshot. Unified restore
conservatively requires the original history still retained, even when a saved
state alone could otherwise resume. V1/v2 replay can repeat/conflict in output;
payload fingerprints detect but do not reconcile that. V3 is one local committed
prefix, not Kafka/HTTP/S3 exactly once or cross-service2PC. Imported continuations
require one active branch under operator control; there is no global cross-root
registry. Local rename/fsync/process-crash proof is not disk-loss or hardware
power-loss/HA proof. Physical rescale, automatic recovery/GC, general SQL/stream
joins, Parquet CDC continuity and Phase19 maintained deltas remain deferred.

The user forbade subagents. Same-session adversarial review and independently
implemented producer/property/state/output oracles were used; no separate human
or agent reviewer is claimed. Earlier Phase10 physical elasticity, Phase12
production HA and broader Phase13–17 debt remain separate and unchanged.
