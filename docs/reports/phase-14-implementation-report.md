# Phase 14 implementation report — bounded streaming v1

Verdict: **COMPLETE_BOUNDED_STREAMING_V1**, 2026-10-29 Pacific. All mandatory
G0–G9 pass for the explicitly bounded local Kafka foundation. Full PRD streaming,
production deployment, checkpointing and exactly-once are not claimed.

## Authority, source and environment

The 2026-10-28 user request authorized implementation in this chat without
implementation subagents, one local implementation commit, then independent Sol
High review and Luna XHigh remediation in one separate fix commit. No push, PR,
paid service, production topic or external resource mutation occurred. The only
pre-existing untracked files were the supplied Phase14 plan (included) and the
unrelated bootstrap prompt (preserved untouched/untracked).

Baseline/main: `9fc6e35643b2d758ed5a06d00c4d4896f8ab2601`, matching preparation.
Baseline ade-exec/ade-physical tests:40 passed,0 failed. No shared P0/P1 blocker was
found. Phase10 physical membership, Phase12 production HA and Phase13 production
cloud debt and their historical sources/failures/measurements remain unchanged.

Final network acceptance is **candidate-7**, captured from the dirty implementation
on that parent. Exact253-file source inventory SHA256:
`2ea359e8ff1f4e1703aaf89dc9ec92269fca43d05ebcbbe5c2728fcd38e78d7f`.
Source archive SHA256:
`6ab2cec907053db69fdbfe0146265689f51881d192fdf974368ab684b8cbb251`.
Canonical SHA256 of the sorted119-file crate/Cargo input inventory:
`b399925d7749060972412040e58c475f41f4c6d0af6401e881b0a751e52b3590`.
Every current crate/Cargo input was compared with this inventory after acceptance;
all match. Subsequent edits are documentation/evidence only; no runtime input changes. The measured/current ade-stream executable SHA256 is
`704663608ca323538684702011f31488888256337da6eb5574c3f0d89ba66ae5`.
Obtain the final project commit with `git rev-parse HEAD`; it is deliberately not a
self-referential source hash in this report. No extra project commits were used
for evidence freezing, and no timing speedup is asserted.

Environment: rustc1.99.1(58cf8f9c3), cargo1.99.1, aarch64-apple-darwin/macOS27,
Java17.0.20.1(Homebrew), Python3.9.6, debug build; `kafka` feature explicitly enabled.
Network acceptance uses the standalone `cargo build -p ade-stream --all-features
--locked --offline` feature graph; workspace feature unification can produce a
different binary hash. The standalone build is restored and its measured hash
checked after the workspace gate. Client: pinned rdkafka0.39.0 / rdkafka-sys4.10.0 / librdkafka2.12.1, CMake static
native build with OpenSSL3. Kafka: separate Apache Kafka3.9.1 KRaft process using
the official binary distribution, SHA512 verified against Apache's published file:
`1ea204ba73411737a275429ca976d440f007ff0957b90b19be41dc5a4bae52617267769be9f0b5791714d0b3c4c760605bd426faea39edd90763585523fa2cfe`.
The final private broker cluster ID is `dXrWhPJdT1i2d9h2kyb4Og`. Each reader uses a
fresh PID and UUID; the30 cases have distinct reader PIDs. Broker restart preserves
the same fixture cluster and is followed by a fresh explicit reader session.

## Implementation and architectural decision

[ADR-0021](../adr/0021-unbounded-source-and-progress-v1.md) refines architecture
§§3.5,5.3,7–9,18,21–22 and INV1–10. The new `ade-stream` crate has real ownership
of admission, explicit Data/Pending/Stopped polling, Kafka source identity,
read/forwarded vectors, policy watermarks/frontier, bounded timers and demand.
`kafka` is default-off; default dependency-tree evidence contains no rdkafka.
The Kafka data path depends on ade-exec/ade-physical and contains no DataFusion.
Bounded `BatchSource::None`, PlanV1 grammar/hashes, wire/result/manifest v1,
benchmark oracle and existing CLI defaults retain their contracts.

The only shared native change extracts the existing UInt64 less-than filter and
Arrow projection into `ade-exec::stateless`; the old pull operator calls the same
kernels. A strict EventV1 becomes a one-row Arrow batch, runs the native filter,
and projects value/key/event-time. The receipt serializes values obtained from
the projected Arrow result. A filtered input emits `projected:null`, permitting an
explicit sink receipt for progress without inventing a transformed data row.
Independent Python producer-ledger oracles check every value, filtering result,
per-partition order, duplicate identity, lateness, watermark and global frontier.

One owner holds one unconfirmed record/frame. Whole-frame write success, followed
by cancellation checking, precedes acknowledgment. Read/seen can lead forwarded;
filtered and late records follow the same acknowledgment rule. Legal Kafka holes
are accepted, including real aborted transaction/control offsets under
read_committed. Invalid/oversized input, backward/session/partition mismatch and
arithmetic overflow stop without forwarding that input. Restart suggestions are
nondurable diagnostics: crash, output loss or manual replay can duplicate output.
No offsets are stored/committed to Kafka.

Published per-partition watermark is checked max successfully forwarded event time
minus nonnegative allowance, never regressing. Global frontier is unknown until
every assigned partition has a bound, then their minimum. A quiet partition is
never excluded. Negative/out-of-order times are supported; older records are
forwarded with late diagnostics. The bound is a declared out-of-orderness policy,
not an assertion Kafka can never deliver an older record or a general Timely
completeness proof. No idle/stop emits source EOF or a completion frontier.

Timers use injectable monotonic elapsed milliseconds or published global event
progress, inclusive deadlines, ordered deadline/kind/registration-ID ties, bounded
live/byte/lifetime registration and removal on fire/cancel. Timer traces include
observed processing time/frontier. Firing is at most once per session and has no
keyed/window/sink effect or persistence. The real quiet source fires a future
processing timer while its event timer remains blocked by an unknown frontier.

## Supported and refused envelope

The machine-readable [support/refusal matrix](phase-14-support.json) and
[operator guide](../phase-14-streaming-guide.md) specify admission and commands.

| Area | Supported bounded v1 | Refused/deferred |
|---|---|---|
| Source | One real trusted cluster/topic;1–4 fixed explicit partition IDs/absolute offsets; manual assignment | Group subscribe/rebalance, implicit committed offsets/reset, other connectors, automatic retry/recovery |
| Identity/history | Expected cluster ID, pinned topic partition inventory and current range/config checks | Topic incarnation/record leader-epoch attestation; undetected recreate/truncate-and-regrow/change-and-restore |
| Data/native | Strict version1 UInt64 key/Int64 value/time; shared native filter/project and bounded NDJSON receipts | Schema evolution/registry, tombstones, headers, oversized keys/payloads, SQL, joins/windows/keyed aggregates |
| Time | Monotone policy watermarks; all-assigned min/unknown frontier; bounded local event/processing timers | Idle completeness, nested/cyclic frontiers, durable timers or checkpoint/window semantics |
| Security | Explicit loopback plaintext development mode; verified SASL_SSL PLAIN using environment credentials | Remote plaintext, insecure TLS, arbitrary client-property overrides or cloud provisioning |
| Delivery | Whole-frame local FD acknowledgment, cancellation and honest manual replay suggestions | Fsync/durable publication, external consumer acknowledgment, no-duplicate recovery or exactly-once |
| Execution | One local Unix reader, bounded queues, demand backpressure and operator/test stops | Multi-worker streaming, elastic membership, HA/cloud, streaming PlanV1 or optimizer integration |

**Required broker assumption:** immutable topic incarnation/history, configured
uncompressed with max.message.bytes<=32768 since creation. Current settings are
checked through read-only DescribeConfigs before assignment and periodically.
Fetch response limits alone cannot bound decompression; this restriction closes
that hole. Historical compressed batches/configuration changes entirely between
checks cannot be attested by this client and are outside the admitted contract.
This bounded admission refinement is reconciled in the plan/ADR rather than hidden.

## Resource, failure and cancellation proof

Managed caps: config16KiB; payload128..4096B (oversize detection retains at most
cap+1); one Arrow input/filter/project chain<=8KiB accounted; frame<=2KiB;
conservative simultaneous in-flight<=16KiB; downstream channel depth0;
diagnostics<=8KiB/frame; <=4 cursors. Timers<=64 live/8KiB charged,1024 lifetime
registrations,128B conservative node charge. Output/diagnostics safely duplicate inherited FDs (no /dev/stdout/stderr
reopening). Config opens nonblocking and refuses nonregular files; CA input is an absolute regular file<=1MiB. Credentials are
bounded environment values, absent from diagnostics, source descriptors and hashes.
Fixture credentials/keys are temporary and excluded; a scan rejects their presence
in retained text artifacts.

Client queue target1..1024KiB, minimum-message target1, initial per-partition
fetch16KiB, aggregate fetch32KiB, receive response64KiB, at most4 brokers/partitions.
Separate queues conservatively allow4*(target+64KiB) fetched payload plus native
protocol/control/allocator overhead. Read-only Admin and stats frames are separate
bounded control domains. ADE stops polling partition queues during a blocked sink;
bounded prefetch can fill, while main control/stats still run. Native pause/resume
is deliberately unused after the retained destruction-hang experiments below.

Candidate7 blocks a real output pipe for **35.8386s**, while an independent producer
adds200 records to the initial1400. Forwarded vectors stay exactly fixed in all
blocked snapshots, broker backlog stays above500 records per partition, and the
last ten measured client high-water samples plateau at **7937B**, below the
conservative queue allowance. Draining the pipe resumes and produces all1600
receipts, with exact independent contents/order/progress checks. Terminal managed
peaks and one-in-flight/frame caps are checked by both live and offline verifiers.
The test is a local correctness witness; it ran alongside compilation/regressions
and is not an isolated throughput/RSS/timing benchmark.

Idle sleeps<=100ms. Output retries use10ms cooperative waits and a finite stall
budget<=120s. Metadata/config/range RPCs each have1s timeouts; a full pass has a
finite sum across assigned partitions. SIGINT cancels during idle poll or blocked
output; real process tests require exit within3s after signal on this fixture.
EPIPE, bad schema/oversize/overflow, broker outage and metadata/config/range drift
return exit1/FAILED with no acknowledgment past accepted output. SIGKILL returns
-9 and no terminal trace; explicit replay demonstrates duplicates. Consumer close
precedes terminal status and all owned processes are stopped/joined by the harness.

These are queue/accounting domains, **not RSS**, native allocator/compression/TLS
or kernel-buffer caps. Trusted uncompressed history bounds admitted codec inputs.
OS filesystem calls and library scheduling are not hard real-time guarantees.
External trace files require operator rotation/storage quotas if left running for
days. Timer lifetime/counter caps refuse rather than grow forever. Local output
and in-process progress remain lossy across a crash.

## G0–G9 acceptance

Portable raw artifacts, source snapshots and failed candidates are in
[acceptance.tar.gz](../evidence/phase14/acceptance.tar.gz), authenticated by
[manifest.json](../evidence/phase14/manifest.json). Large broker data and temporary
private keys/credentials are excluded. Archive paths below are relative to it.
The complete original local runs remain under `benchmarks/results/phase14/`.

| Gate | Status | Actual witness |
|---|---|---|
| G0 baseline/defaults | PASS | Baseline40 tests; default/Kafka dependency trees; bounded-contract/legacy tests; scoped source diff |
| G1 real Kafka | PASS | candidate-7/environment.json, broker properties, cluster/producer commands, manual-assignment source and admitted traces; real SASL_SSL also passes |
| G2 offsets/replay | PASS | native/middle-replay/legal-gap/kill/kill-replay ledgers and traces; real aborted transaction holes; seen/forwarded divergence under stall/poison |
| G3 time/frontier | PASS | Seven deterministic core tests plus native/quiet/late traces, independent max/min/unknown reconstruction, checked negative/overflow cases |
| G4 timers | PASS | Deterministic deadline/tie/cap/no-refire/cancel tests; quiet real-source future processing timer, withheld event timer; firing processing/frontier traces |
| G5 native semantics | PASS | Shared ade-exec kernels, Arrow extraction; exact independent producer-ledger receipts and partition-order oracle for both plaintext/TLS and replay |
| G6 backpressure | PASS | candidate-7/backpressure.trace.jsonl/output.jsonl and producer ledgers:35.8386s blockage, fixed offsets,7937B plateau, broker backlog, full resume; poll/stall cancellation |
| G7 faults/security | PASS | Actual broker restart/outage, bad credentials/TLS, malformed/oversize/overflow, EPIPE, SIGINT/kill, offset/retention/partition/config drift, timer cap, FIFO/nested-security refusal; exact exit statuses |
| G8 legacy regressions | PASS | Locked fmt/check/Clippy/workspace test logs, smoke, Phase10/11/12 process/frontend regressions and three separately run real S3 correctness/fault tests |
| G9 reproducibility | PASS |253-file source snapshot,119-input runtime identity, exact binary/versions/commands/seed/PIDs, archive hashes, offline independent oracle and tamper/-O refusal |

The offline verifier checks archive coverage/hashes, every final source archive
member,30 fresh reader PIDs/exits, independently reconstructs complete producer
coverage/filter/project/order/lateness/frontiers for all successful output cases,
and validates timer timing and managed peaks. It never executes the ADE parser or
transformation to build expected results. A one-byte archive mutation refuses;
Python -O refuses so assertions cannot silently disappear.

## Commands and counts

Build environment on this host: `OPENSSL_DIR=/opt/homebrew/opt/openssl@3` and
`OPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3`. Public client APIs/configuration were
verified against installed rdkafka0.39.0 and bundled librdkafka2.12.1, including
assign/split queues/poll, queue overshoot, absolute offsets and disabled commit/store.

| Command | Actual result |
|---|---|
| `cargo test -p ade-exec -p ade-physical --locked --offline` before edits |40 passed,0 failed |
| `cargo fmt --all -- --check` |PASS |
| `cargo check --workspace --all-targets --locked --offline` |PASS, including final source |
| `cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings` |PASS, including final source |
| `cargo build --workspace --all-features --locked --offline` |PASS; existing macOS compact-unwind linker warning retained |
| `cargo test --workspace --all-features --locked --offline` |263 passed,0 failed,10 intentionally ignored |
| `cargo test -p ade-stream --all-features --locked --offline` after final edits |11 passed,0 failed,0 ignored (7 core +4 demand/cancel tests), repeated from workspace |
| `bash scripts/smoke.sh` with freshly built worker |PASS correctness/schema/process validation; one-iteration mixed-engine comparison warnings retained, not performance evidence |
| `python3 -m unittest discover -s scripts/tests -p 'test_*.py'` |26 passed,0 failed |
| `python3 scripts/phase14/acceptance.py --kafka-home /tmp/kafka_2.13-3.9.1 --java-home /opt/homebrew/opt/openjdk@17 --output benchmarks/results/phase14/candidate-7` |30/30 process cases PASS; real separate Kafka; exact source/binary and raw inputs retained |
| Selected ignored `ade-distributed --test remote` tests against fresh private Moto5.1.16 |3 passed,0 failed: genuine turnover/cleanup, caps+producer death, fresh-boot/stale-epoch/foreign-grant refusal |
| `python3 scripts/phase14/verify_evidence.py` |PASS source/hash/oracle validation; mutated archive and optimized Python separately refuse |

The10 workspace ignores are the historical Phase13 timing/cost campaign, leased
full-fixture diagnostic and eight remote fixture/proxy tests. Three remote tests
were separately executed above. The remaining seven ignores are not represented
as passing fresh campaigns. In particular no Phase6–10 historical timing guard or
Phase13 cost campaign was rerun/promoted. CI remains correctness-focused; this
phase adds no timing threshold or production service prerequisite to default builds.

## Preserved negatives and self-review

- Initial sandbox networking was unavailable; authorized local socket/download
  runs used explicit execution permission. An early tar extraction raced the
  incomplete download and failed; the completed distribution verifies SHA512.
- Initial CMake could not discover OpenSSL. Setting both documented OpenSSL root
  variables fixes the build. Early strict-Clippy attempts found the moved test-only
  UInt64 import, documentation/style and long-function annotations; all were fixed.
- Candidate1 emitted correct small output but hung in consumer destruction. It
  predates full source capture; its exact dirty source/binary identity is **not
  claimed**, and its logs remain negative evidence on the recorded parent.
- Candidates2/3 captured exact source archives and correct1600-record output after
  sustained stalls, but timed out in native destruction. Queue cleanup/explicit
  consumer close alone did not solve it. An owned-process sample pins the wait in
  native rd_kafka_destroy. A controlled probe removing native pause/resume exits;
  final demand gating retains bounded prefetch and passes every teardown case.
- Candidate4's verified-TLS attempt failed with the300ms metadata budget. A bounded
  1s budget permits the fresh handshake; candidate5 passes28 cases. Candidate5 is
  preserved as pre-final positive evidence, not relabeled for later timer/admission
  edits. Candidate6 passes29 pre-final cases. Final self-review then confirmed nested
  unit-variant security fields were accepted and /dev/stderr reopening lost pipe
  diagnostics. An empty strict struct variant and safe inherited-FD duplication
  fix both; candidate7 freshly passes30 cases on final runtime inputs.
  Pre-final COMPLETE wording in frozen draft documents is superseded here; only
  candidate7 is the accepted release source.
- Adversarial self-review added immutable uncompressed-topic admission, bounded
  config/CA regular-file checks, processing-time/frontier firing metadata and
  explicit discarded-input/failure classes. It checked ack-before-progress,
  conservative quiet-partition handling, legal gaps, one-owner cancellation,
  bounded native/Arrow/output/timer domains, source identity caveats and default
  isolation. No unresolved verified P0/P1 is carried into bounded completion.

The native shutdown observations justify the chosen demand mechanism; they are
not a claim of a generally diagnosed/upstream-fixed Kafka defect. Failed artifacts
and original samples/source archives remain intact. Public evidence is a local
reproducible witness, not signed host attestation or independent-host repeatability.

## Next independent review and remaining roadmap

The independent reviewer must derive obligations from PRD/architecture before
reading the Phase14 plan/claims, inspect the actual committed range and follow the
provided review prompt. It writes one self-contained
`phase-14-independent-review-handoff.md` without implementation edits. A separate
Luna XHigh agent reads that handoff, fixes valid findings, validates and commits
once. Neither stage uses implementation subagents; the parent remains idle while
it runs and resumption remains in that same agent.

Deferred: Phase10 physical membership; Phase12 replicated/cross-host HA; Phase13
production cloud/durability; Phase15 keyed state/checkpoints; Phase16 state placement;
Phase17 exactly-once/savepoints/transactional sinks; Phase18 unified optimizer;
consumer groups, more connectors and general streaming SQL. This phase does not
silently authorize any of those outcomes.
