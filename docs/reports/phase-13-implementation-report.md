# Phase 13 implementation report — bounded remote disaggregated batch v1

Verdict: **COMPLETE_BOUNDED_REMOTE_V1**, 2026-10-28. Full PRD Phase13/cloud
production completeness is not claimed. Every mandatory G0–G8 gate passes for
the bounded local S3-compatible service and process-turnover envelope.

The G0–G8 measurements, counts and source identity below are the original
acceptance record for `ead9de6c0bbd3494b88110e50aa825f651be58cc`; they remain
historical and unchanged. The independent-review remediation and its fresh
source-bound local checks are appended at the end of this report.

## Authority and source identity

The user's explicit implementation request authorizes this bounded Phase13
continuation, all work in this session, no subagents and one final project
commit. Baseline is `474f8c92ff13ffb59c63d8fd2edd3be4ce6019bb`.
Initial status contained only the supplied untracked Phase13 plan and unrelated
`sol-medium-phase-0-bootstrap-implementation-prompt.md`. The plan is included;
the unrelated prompt is preserved untouched/untracked. No push, PR, paid cloud,
production bucket or remote permission change occurred.

The measured/validated clean source snapshot is
`ead9de6c0bbd3494b88110e50aa825f651be58cc`, tree
`dcdedb24c5b1ab1fd8c5d3ea791e64ab9df3f3f2`, in a temporary evidence repository,
not an extra commit on the user's project branch. Its source tar SHA256 is
`8d44f7d6ee580c041c0728b7a287a80afd7c0683a80813e2b091f770b993ad6c`;
the142-input inventory SHA256 is
`b39273ba3761397be8ae2047e6da35eb096a44c6ac61d2a799e9f96c7f53706e`.
Final documentation/evidence updates are separate from that runtime snapshot.
The final project commit SHA is returned in the handoff and obtained with
`git rev-parse HEAD`, avoiding a self-referential report hash. The small tracked
[phase-13-evidence.json](phase-13-evidence.json) pins the source, binaries, all
commands/log hashes, samples and raw bundle. Large raw artifacts are ignored at
`benchmarks/results/phase13/`.

A first clean candidate `7fcdbc147bc787c10d3bd8f450ba0eaefe4b5348` was superseded
before ANY timing campaign: its harness reused the coordinator test process.
The replacement gives every sample a fresh coordinator process, worker cohort
and scratch. Its source archive, command logs and explicit disposition remain;
its successful premeasurement checks are not relabeled as final acceptance.

Toolchain: rustc1.99.1 (58cf8f9c3), cargo1.99.1, aarch64-apple-darwin/macOS27,
Apple M3,8 logical cores/24GiB, debug profile. Acceptance builds use workspace
all-features (`remote-s3`, native `research-hooks`, operations `fault-injection`)
and locked dependencies. Default features remain off for the new path. Existing
299 locked packages retain their versions/checksums;67 packages are added for
optional S3/HTTP support. `object_store` stays0.13.2. Dependencies were inspected
through their installed public source, including conditional-create, HTTP
connector, body limit and retry semantics. The separate fixture is Moto5.1.16,
Python3.9, requests2.32.5; full installed versions are retained in raw evidence.
MinIO's distribution URL returned410, so no MinIO run is claimed.

## Independent prerequisite G0

G0 PASS on the baseline BEFORE Phase13 source changes. Fresh target and clean
archived baseline build, matching worker, and
`cargo test -p ade-ops --all-features --locked -- --nocapture`:44 tests passed
(43 baseline tests plus one independently added real-process witness). The
additional test is retained in final `ade-ops/tests/native_crash.rs` and checks
changed compatibility after actual native prepare AND after staging, with
attempt/engine UUID unchanged, NEEDS_OPERATOR and no marker/replay.

F12-01's two real processes overlap a prepared/running native job with reached
actual hash-read pauses during submit and verify-sources. Hash operations took
12036/12020ms; A succeeded with exact canonical output and cancel_requested=false.
F12-02 also verifies committed-marker reconciliation across compatibility drift
without changing marker bytes. F12-03 checks NOT_CHECKED list rows, VERIFIED
single-job status, and refusal of corrupt/missing markers. Native coordinator
SIGKILL tests cover claim/prepare/gather/staged/prepublish/link/post-link across
aggregate and join; real producer/gather-prefix witnesses prove drain before
fresh whole-job retry and no partial visible result. Independent scalar/bag
oracles verify recovered values, not just rowcounts.

Baseline worker SHA256:
`5e8c164e7fed815c04fc2834f57f20924d5001b4fbbefc6d21140adb5da3c239`;
baseline ade-batch SHA256:
`8027d9a0b0cc6de779ef10e481849eb442912dded7cc7b04327134c908d9a0cb`.
Native runtime2eb78ca479e60813d2294ef527621ca8546f8d53291a9d8ae1c5f0a2827799ed;
coordinator compatibility90103c47b4c0be45332588f2003ff5667d711a9a43304a83eb72f7c1e0934592.
G0 stdout SHA256:
`a211c05c5cbe60c3709470e2b8d9fc54e379008afd66f6e16cc143862388db73`.
An initial sandbox-only run failed at loopback bind; the actual process gate
passed with local socket permission. This was an environment refusal, not a
passed run. There is no unresolved Phase12 P0/P1 prerequisite finding.

## Architecture and supported/refused contract

[ADR-0020](../adr/0020-remote-disaggregated-shuffle-v1.md) reconciles architecture
§§7,9–10,12,15,17,22–24,27–31 and INV1–10. A default-off `remote-s3` feature
owns an explicit `remote_object` path, not a change to the old exchange selector.
`remote::coordinator` drives real worker processes; `remote::worker` reuses native
HashAggregate, hash1 routing, bounded IPC decoding, merge source and fingerprinted
attempt-safe gather. Core remains frontend/DataFusion independent. A small config
helper uses the existing frontend IR and removes only a composed identity
projection from its fixed query; native admission validates the result.

| Surface | Supported bounded contract | Refused/deferred |
|---|---|---|
| Computation | Original static GroupBySum/Count, IR1, immutable local manifest-listed Parquet,2–4 fixed logical producers/hash partitions | Other SQL/projections/DAGs, join, remote inputs, adaptive/leased/static_projected/sampled/operator-spill paths |
| Remote provider | Explicit signed S3-compatible endpoint/bucket/prefix/region, private never-reused UUID, TLS default and explicit loopback dev HTTP | Anonymous/local FS fallback, redirects, ambient proxies, arbitrary keys/bucket creation |
| Publication | Conditional-create bounded segments, HEAD/GET/BLAKE3 verification, exact N×N terminals then global manifest, independently pinned eligibility hash | LIST completeness, atomic rename fiction, multi-object transaction or partial-result success |
| Turnover | All producers successfully exit/reap, scratch removed; fresh boot/epoch1 reader grants, width1..N over same N partitions | In-flight add/remove/replacement, retry/adopt old attempts, dynamic final ownership |
| Failure | Kill/reap owned cohort, poison ambiguous I/O, bounded read retry, discard private gather on any required failure | Automatic whole-job/lineage/copy recovery, coordinator restart/HA or external exactly-once sink |
| Cleanup | Default dry-run, exact keys, private post-reap capability; cannot retarget through mutable displayed receipt | Live/failed/crashed-prefix GC, LIST/bucket delete, background TTL/version reclamation |
| Cost | CONFIGURED prices/placement, MEASURED requests/body bytes/retries/latency, checked nano-USD arithmetic | PROVIDER_BILLED or inferred real AZ cost, automatic storage/capacity optimizer |

The full machine-readable boundary is in
[phase-13-unsupported.json](phase-13-unsupported.json). Phase10 physical elasticity
and Phase12 production HA remain deferred; no Phase14 is authorized.

## Integrity, bounds, cancellation and retention

Logical `(job,exchange,partition,producer)` identity is separate from execution
UUID/attempt0 and reader epoch1/boot. Global eligibility binds source/plan/config/
provider/static topology, every ordered terminal and its hash. Each terminal
binds exact segment sequences, rows/bytes/BLAKE3 and original transfer hash chain.
Empty partitions/producers close explicitly. Readers validate full terminal
coverage/budgets before pulling bounded batches; every payload checks schema,
positive counts, hash destination and chain completion. Final sent/received
fingerprints and N-partition gather commit, count conservation, output limits and
source postflight all precede returned batches. CLI emits no success JSON on error.

Resources:64 files/1m source rows/64MiB physical input,1MiB manifest/footer and
16MiB decoded row group;32MiB task state with spill disabled;<=1024 rows/1MiB IPC
frame;1MiB control/global JSON;64KiB each terminal/1MiB total terminal metadata;
per producer<=256 segments and per actor<=16MiB cumulative attempted object
bodies/272 creates/4096 requests;<=5s request and120s run;0–2 explicit bounded read
retries (default1), no ambiguous PUT retry. At most4 worker actors, one provider
request each; the stages do not overlap. HTTP connector rejects redirects/proxies,
clamps SDK response allocation hints and limits response bodies to1MiB each and
64MiB cumulative admitted bytes/actor, including errors. Shared gather write
budget<=64MiB; output<=1m rows/64MiB Arrow-accounted estimate. These domains do not
claim RSS, allocator/kernel/codec/physical-network/free-space caps. Control writer
and reader threads are bounded, joined, and unblocked by owned-child kill/reap.
OS syscalls retain non-preemptible teardown limitations.

The real delayed-PUT witness proves a service can finish an object AFTER client
cancellation. It remains an orphan with no global manifest/new grant/output.
Failed/crashed namespaces are quarantined, not auto-adopted. Applying successful
cleanup needs a nonserializable capability anchored to immutable private context,
not public receipt fields; a concurrent other namespace survives. The service
restart witness is allowed to lose its in-memory fixture objects and must refuse
output. Process/store separation does not prove service/host/AZ durability.

## G0–G8 acceptance and release commands

| Gate | Outcome / actual evidence |
|---|---|
| G0 Phase12 safety | PASS — baseline fresh build,44 operations tests, real contention/drift/marker/crash/drain witnesses above |
| G1 actual remote provider | PASS — separately running Moto endpoint and signed public S3 client; conditional-create412 proof;32-sample traffic/body conservation; no FS fallback |
| G2 exact aggregation | PASS — real2/4-worker fixtures, empty/singleton/skew/duplicate/signed results, row/byte/object/request/segment/deadline refusals; unchanged independent oracle for all32 campaign executions |
| G3 compute/storage separation | PASS — producers exited/reaped and scratch absent before new boot/epoch1 readers;4 producers/2 readers retain the same4 hash partitions and exact result |
| G4 faults | PASS — missing/corrupt/truncated/old objects, source drift, schema/row/count/hash-owner/terminal-chain errors, stale boot/epoch/attempt/backend/duplicate grant, absent credentials, real producer/reader death, cancellation,500/truncation retry, outage, redirects/oversized bodies, poisoned/late PUT and actual service restart all fail closed or retry within bounds |
| G5 bounds and GC | PASS —8 focused remote unit tests plus real budget failures; exact-key dry-run/apply; immutable private GC authority preserves another live namespace; no LIST/broad-delete/background GC |
| G6 prior contracts | PASS —251 Rust tests,0 failures,9 explicitly ignored tests;22 Python tests; fmt/check/strict Clippy/build/smoke/default dependency guard. Workspace covers selected Phase4–12 modes/recovery/topology/join/leased/frontend/operations correctness; no old timed campaign is relabeled |
| G7 provenance | PASS — clean ead9de6 source archive/bundle,142-file final input equivalence, toolchain/features, source/binary/test-executable/log/traffic hashes, all commands and samples;8 copied evidence attacks refuse; fixtures stopped |
| G8 cost/latency truth | PASS —32 fresh-coordinator executions,24 measured samples; all same-data local/remote comparisons and configured estimates reconstructed from actual service traffic. Remote overhead is reported, not hidden |

The seven external-service integration tests all pass when explicitly selected:
six in the main S3/fault suite, plus the final real service-restart test. The9
ordinary workspace ignores comprise those7, the paired campaign test and one
pre-existing local diagnostic; required external tests were run separately,
not counted as passed by being ignored. All8 focused remote unit tests also
pass. The documented config-helper/CLI path separately succeeds with exact-key
cleanup and successful source-bound receipt verification. Source is clean before/
after the final command sequence; all142 final compiler/test/script inputs match.


Required commands are run from the clean source with feature-matched binaries:
`cargo fmt --all -- --check`;
`cargo check --workspace --all-targets --locked`;
`cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`;
`cargo build --workspace --all-features --locked`;
`cargo test --workspace --all-features --locked`;
`bash scripts/smoke.sh`;
`python3 -m unittest discover -s scripts/tests -p 'test_*.py'`.
The default dependency tree must exclude cloud/frontend/DataFusion from native
execution. External-service tests are explicitly ignored in ordinary CI and run
separately with the private fixture and matching features. Timing is never CI
pass/fail. Full argv/cwd/status/duration and raw stdout SHA256 are recorded.

## Predeclared paired observations

| Dataset / workers | Local median [min,max] ms | Remote median [min,max] ms | Remote/local | Configured object/egress USD per run |
|---|---:|---:|---:|---:|
|uniform / 2|481.508 [432.730,618.863]|2901.535 [2627.372,2957.454]|6.026x|0.000600941|
|uniform / 4|445.488 [405.025,467.069]|3616.288 [3138.578,5449.219]|8.118x|0.001613303|
|hot-key / 2|327.747 [327.714,343.740]|2125.989 [2003.298,2127.918]|6.487x|0.000145745|
|hot-key / 4|314.630 [308.028,316.958]|2743.271 [2735.343,3198.060]|8.719x|0.000448479|

All three measured values, warmups, cleanup times and counters remain in the
tracked evidence summary/raw32-sample bundle. The estimates above are identical
across repetitions because encoded data and request counts are identical; time
varies. Remote is about6.0–8.7x slower in this debug loopback prototype. It
demonstrates survival of completed shuffle across producer exit and independent
reader-stage width, not a local speed/cost win. The estimate excludes compute
and local-device prices; it is not a total dollar/job comparison or provider bill.

The [declaration](../research/phase-13-experiment.md) fixes100k rows/4files,
seed42,4096-key domain,8192-row groups,Snappy/payload16, uniform versus99% hot-key,
2/4 workers, one warmup and three measured runs for each local/remote arm.
Every sample has a fresh coordinator process, workers and scratch and passes the
unchanged independent synthetic oracle. Native wall includes worker startup/
teardown, verification, exchange/gather and postflight; oracle/serialization and
coordinator test-process startup are outside. Remote has two sequential cohorts
and extra source/manifest verification, so this is whole-path overhead rather
than an isolated network comparison. Cleanup is separately timed after quiescence.
All warmups, measured times, errors and raw API traffic remain; no best selection.

Configured illustrative rates, not a provider tariff: PUT5000,HEAD400,GET400,
DELETE0 nano-USD/request; egress1 nano-USD per object read byte, other object-byte/
storage rates0, retention60000ms. Labels are explicitly loopback-compute/store.
Delete means one-key S3 DeleteObjects POST, not necessarily HTTP DELETE. Object
bodies and admitted HTTP bodies have separate counters; framing/TLS and rejected/
dropped bytes are not a provider traffic bill. Request traces independently match
successful sample counters and reconstruct the configured estimates. No actual
cross-AZ, cloud bill, performance win, host/day replication or provider durability
is inferred. Historical Phase6–10 timing/source identities remain untouched.

## Negative evidence, limitations and runbook

Preserved development negatives: sandbox TCP/DNS refusal, MinIO410; required
scratch0700 fixture refusal; initial zero-count source-decoder test failure (now
explicitly rejected before merge); the config helper initially refused the
frontend's composed identity projections (fixed without widening native shape);
strict-Clippy/style iterations; staged diff review caught and removed four
Markdown trailing-space lines in the supplied plan; and the explicitly superseded premeasurement
snapshot. No failed run is promoted or hidden. Intermediate development logs are
unfrozen diagnostics; acceptance sources are the immutable snapshots above.

No unresolved P0/P1/P2 correctness finding remains in the admitted envelope.
Unverified/deferred: Linux/CI and MSRV1.94 execution, real authenticated TLS cloud
service, cross-host/AZ survival, provider billing, independent host/day performance,
media/power-loss durability, production service security/HA and failed-orphan
operator automation. Known tradeoffs are high per-object/control overhead,
repeated bounded source/manifest verification, disabled operator spill and manual
quarantine of failed/crashed/late-write namespaces. A Generic provider/transport
read error may receive a finite repeat because the public SDK classification is
coarse; known permanent variants and hash/size/schema errors do not relax proof.
OS blocking/DNS/teardown transients are not a process-level hard deadline or RSS
promise. Final fixture checks confirm both loopback listeners are stopped.


Operational instructions, fixture setup, cleanup/quarantine and replay commands
are in the [Phase13 guide](../phase-13-operations-guide.md). Preserve failed
namespaces and traces, fence old actors and prove provider I/O closure before
manual exact-key removal. Never use bucket-wide cleanup or force-resume as a
substitute for an absent global proof. The next bounded step would independently
validate a real authenticated TLS/cross-host provider and service persistence;
physical elasticity and replicated coordinator/sink reliability remain separate
Phase10/12 work. This report grants no further phase.

## Exact project change scope

- `AGENTS.md`
- `Cargo.lock`
- `README.md`
- `benchmarks/README.md`
- `crates/ade-bench/Cargo.toml`
- `crates/ade-bench/tests/phase13.rs`
- `crates/ade-distributed/Cargo.toml`
- `crates/ade-distributed/build.rs`
- `crates/ade-distributed/src/bin/ade-remote.rs`
- `crates/ade-distributed/src/bin/ade-worker.rs`
- `crates/ade-distributed/src/lib.rs`
- `crates/ade-distributed/src/remote/coordinator.rs`
- `crates/ade-distributed/src/remote/http.rs`
- `crates/ade-distributed/src/remote/mod.rs`
- `crates/ade-distributed/src/remote/store.rs`
- `crates/ade-distributed/src/remote/tests.rs`
- `crates/ade-distributed/src/remote/wire.rs`
- `crates/ade-distributed/src/remote/worker.rs`
- `crates/ade-distributed/tests/remote.rs`
- `crates/ade-ops/tests/native_crash.rs`
- `docs/adr/0020-remote-disaggregated-shuffle-v1.md`
- `docs/adr/README.md`
- `docs/phase-12-operations-guide.md`
- `docs/phase-13-operations-guide.md`
- `docs/plans/phase-13-implementation-plan.md`
- `docs/reports/README.md`
- `docs/reports/phase-13-evidence.json`
- `docs/reports/phase-13-implementation-report.md`
- `docs/reports/phase-13-unsupported.json`
- `docs/research/phase-13-experiment.md`
- `scripts/phase13-config.py`
- `scripts/phase13-service.py`
- `scripts/phase13-verify.py`
- `scripts/tests/test_phase13_verify.py`

One final project commit includes these task files only. The measured source is
separately pinned above; final documentation does not relabel its timing.

Raw local bundle SHA256: `16ed3a08f7707e5fe4c6bda1629689e393b5aed7bf1d3e920ecd7775a0b269d2`; 40649832 bytes.

The initial raw container included macOS AppleDouble metadata. The final portable
container suppresses that metadata and verifies all 142 payload hashes; the first
container remains preserved separately. No source, sample or runtime input changed.

## Independent review remediation addendum — 2026-10-28

The independent review targeted `d8ffa492e34fd785f3d05447f14dffb65e36e503`.
This addendum records bounded follow-up changes; it does not rewrite the original
`ead9de6` evidence, its prices, samples or verdict.

| Finding | Disposition |
|---|---|
| F13-01 lifecycle cost | Receipt v1 keeps its pre-cleanup `estimated_nano_usd` field, now explicitly an execution proxy that excludes retained storage. Attempted PUT bytes are not priced as occupancy. After a fully successful exact-key cleanup, `Cleanup` reports successful DELETE counters and a checked lifecycle proxy using the exact verified segment, terminal and global-manifest bytes with the configured assumed duration. Partial cleanup returns a fixed redacted count summary and no completed lifecycle estimate. The supplementary evidence-version-2 campaign uses nonzero DELETE and storage rates; the original evidence-version-1 matrix is unchanged. |
| F13-02 failure diagnosis | The private one-shot envelope is now `ADEOBJ02`/wire v2. Workers return a bounded enum class for admission, source, credential, provider, timeout, cancellation, limit, integrity, stage and child-crash failures. Raw provider errors and stderr remain suppressed. Legacy `ADETCP12`, `ADEOPS01`, manifest v1 and receipt v1 are unchanged. |
| F13-03 orphan disposition | Added `scripts/phase13-orphan-inventory.py` and a runbook recipe for an exact UUID `ListObjectsV2` response. It enforces the key grammar and v1 bounds, rejects truncation/unknown keys and cannot delete. Its result explicitly says cleanup eligibility is not established. Versioned-object and multipart inventory remain operator responsibilities. A late-PUT witness left a manifest-free namespace; a read-only inventory classified its exact five current objects and did not delete them. A subsequent successful execution used a different UUID; the original orphan remained intact. |
| F13-04 evidence accessibility | A tracked machine-readable remediation summary source-fingerprints all committed inputs except its own self-referential JSON and records command/log/sample hashes. The source-bound Moto service run and paired campaign artifacts remain local ignored files; no artifact destination was authorized. The summary explicitly records `EXTERNAL_EVIDENCE_NOT_PUBLICLY_REPRODUCIBLE`, gives the local artifact path and replay commands, and does not claim repository-only independent reproduction. |
| F13-05 architecture wording | Plan, ADR, README, unsupported matrix and benchmark docs now call this a dedicated static-aggregate remote-object executor. A reusable common-exchange plugin and physical elasticity remain deferred; reader-width variation is not credited as Phase 10 elasticity. |

Fresh local verification was run on the remediation source with the pinned Moto
5.1.16 loopback fixture, signed S3 client and bounded request-trace proxy. The
exact status, ignored-test accounting, all 32 supplementary samples, HTTP
conservation, source-input fingerprint, executable hashes, replay commands and
raw-artifact access limit are recorded in
[phase-13-review-remediation-evidence.json](phase-13-review-remediation-evidence.json).
These local results support the bounded local acceptance. They do not make the
raw evidence publicly reproducible or establish a real cloud bill, cross-AZ
durability, or general remote exchange support.

The supplemental v2 campaign again showed local loopback overhead: remote/local
median wall ratios were 6.60× uniform/2 workers, 9.77× uniform/4, 6.22×
hot-key/2, and 9.38× hot-key/4. This does not change the historical acceptance
measurements or claim a speedup. Final serialized workspace tests passed 252
cases with 10 intentionally ignored; the seven selected remote integration
tests and the service-restart witness passed separately.
