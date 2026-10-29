# Phase 14 independent review and remediation handoff

Review date: 2026-10-29 Pacific. Reviewer: the user-requested independent Sol High agent, resumed in the same agent after the usage-limit pause. Repository: `/Users/jasonkli/projects/adaptive-dataflow-engine`.

## 1. Executive verdict

**`ACCEPT_WITH_P2_DEBT` for bounded runtime v1 at `fbafd01e781a5e3a034896fbdef02cadf1adb532`.** The reviewed range is exactly `9fc6e35643b2d758ed5a06d00c4d4896f8ab2601..fbafd01e781a5e3a034896fbdef02cadf1adb532`, one implementation commit on `main`. HEAD and tracked source remained unchanged through review and resumption.

No P0/P1 runtime defect was confirmed. Real Kafka, native transformation, acknowledged progress, legal offset gaps, explicit replay, sustained backpressure, cancellation, timers and selected legacy paths survived source inspection and independent execution. Two **confirmed P2 evidence defects** need a focused correction: the canonical missing-partition test never reaches partition admission, and the portable verifier does not independently check negative-case semantics. These do not establish a runtime data-loss defect. A correctly typed independent missing-partition test reached the expected source refusal.

This acceptance applies only to the advertised one-process, fixed-partition, trusted-history, nondurable slice. It is **not** `FULL_PRD_PHASE14_COMPLETE`, production Kafka readiness, a general completeness frontier, checkpoint recovery or exactly-once delivery. Keep the two evidence qualifications beside any renewed all-gates completion claim until remediation is recorded.

The only repository file written by this review is this handoff. No implementation fix, commit, push, PR or subagent was used. The unrelated untracked `sol-medium-phase-0-bootstrap-implementation-prompt.md` was preserved; its SHA256 is `5a45a881e06e314263d1836c11a4794cfae711f938a0d5d88f4a8b964fc90c76`.

## 2. Independent requirements baseline and plan critique

I first reconstructed obligations from AGENTS, PRD Phase 14 and its later dependencies, architecture §§3.5/5.3/17.3/18 and INV-1–10, the old `BatchSource`/native stateless operators and bounded `PlanV1`, and Phase 11–13 decisions/contracts. I then treated the Phase 14 plan, implementation prompt, ADR-0021, report and support matrix as claims to challenge. The implementation and review prompts are at `/Users/jasonkli/Downloads/phase-14-implementation-prompt.md` and `/Users/jasonkli/Downloads/phase-14-review-prompt.md`; the latter has SHA256 `329904c4ea3855718d384d076a73a9efd86262798fd35eeaef4fcad5f5a7c068`.

| Independently derived obligation | Standard applied and result |
|---|---|
| A real unbounded source | Kafka supplies records; explicit manual assignment and absolute starts; idle remains Pending; no batch-EOF adaptation or fixture fallback. Met in code and real-service runs. |
| Source/replay identity | Cluster/topic/partition/start/config/session identity; increasing delivered offsets may have gaps; no implicit offset-store/reset/commit; changed observable metadata/ranges fail closed. Met within the documented immutable-incarnation/history assumption. |
| Honest sink boundary | Seen may lead forwarded. Only a complete FD write permits acknowledgment. Cancellation or partial failure must not advance the restart suggestion. Met; partial-prefix and last-byte cancellation probes passed. FD acceptance is not durability or consumer acceptance. |
| Actual native ownership | Arrow conversion and the same ADE-owned filter/project kernels used by bounded execution; core has no DataFusion/frontend dependency. Met. |
| Time/progress | Signed event milliseconds separate from offsets and monotonic processing time; watermark only from acknowledged records; min over every assigned partition, unknown until all observed; late records forwarded. Met. This is a declared policy, not proof that older events cannot arrive. |
| Timers | Finite inventory/bytes/lifetime registrations, inclusive deadlines, deterministic ties, cancellation, no refire within session; event timers use published global progress. Met. Processing observation can be delayed by sink blockage. |
| Long-lived boundedness | Bound retention at source/client/Arrow/sink/timer/telemetry layers, stop data demand under pressure, preserve broker backlog and resume; distinguish accounting from RSS. Met for the trusted uncompressed-history envelope; local sustained witness is not a days-long/RSS proof. |
| Failure/security | Explicit pre-effect admission, redaction, TLS verification for authenticated non-dev mode, bounded cooperative shutdown and no late ack. Inspected and challenged with local faults. See evidence and remaining limits below. |
| Legacy compatibility | No PlanV1 grammar/hash/default/EOF/wire/result/oracle change; static and existing batch operation contracts preserved. Diff and current regressions support this. |
| Evidence integrity | Match source/binary/config/process/ledger and independently check positive **and negative** outcomes. Positive record/progress evidence is substantial; F14-01/F14-02 expose holes in negative evidence validation. |

### Where the plan was under-specified or narrower than the product

1. The initial plan's client-fetch limits were insufficient by themselves to bound decompression. The implemented restriction to trusted immutable **uncompressed retained history with batches <=32KiB**, current DescribeConfigs checks and a hard response cap is a meaningful redesign, not a generic Kafka memory proof. The amended plan/ADR/report disclose it. Current settings cannot attest old compressed batches or change-and-restore; do not relax that trust boundary without a new bounded decoder/proof.
2. Cluster ID, topic name and partition inventory are not a topic-incarnation or record-leader-epoch proof. Recreate/truncate-and-regrow may evade observations. The plan permits documenting the limitation, and the final documents do so. This is acceptable for bounded v1, insufficient for durable replay/checkpointing.
3. “Timer firing” lacked a latency service level. The implementation serializes source, sink and timers; a blocked sink delays processing timer observation until ack/error/stop. ADR/guide disclose this. It is safe for observational timers, unsuitable as a future independent liveness/window guarantee without redesign.
4. A new streaming admission descriptor instead of widening `PlanV1` is a justified compatibility choice. Reusing native stateless kernels preserves the execution foundation, but a general unified IR/optimizer is still absent. Phase 18, not this bounded acceptance, must address that remaining split.
5. The plan required named fault cases but did not require an explicit **reached failure stage** for each negative witness. Exit 1 alone can certify the wrong rejection. This is realized in F14-01 and should be corrected in both live and offline acceptance methodology.
6. Full PRD breadth still needs stronger incarnation/replay contracts, operationally managed offsets/sinks, durable state/checkpoints and broader graph integration. These are not silently achieved by bounded completion. The report's qualified vocabulary is appropriate; no PRD rewrite is needed to hide the gap.

## 3. Prioritized actionable findings

| ID | Severity / confidence | Location | Finding | Release effect |
|---|---|---|---|---|
| F14-01 | **P2 CONFIRMED** | `scripts/phase14/acceptance.py:130–135,284–287` | Missing-partition case constructs invalid offset JSON and passes on parser refusal. | Canonical G7 regression/evidence does not prove its named fault; independent correctly typed witness passed. |
| F14-02 | **P2 CONFIRMED** | `scripts/phase14/verify_evidence.py:37–47,47–89` | Portable verification trusts negative labels/exits without checking negative output, trace stage or cursor invariants. | Checksum-consistent semantically invalid negative evidence is reported PASS. |

### F14-01 — Exercise a real missing partition, not an invalid offset type

**Violated obligation:** G7 source/partition failure proof and AGENTS benchmark/evidence integrity. A negative test must reach its claimed boundary.

`descriptor(topic, starts=(0,0), **changes)` enumerates a sequence of offset scalars into `{"partition":p,"offset":s}`. At line 284, `descriptor(native, starts=[dict(partition=3,offset=0)])` supplies a descriptor object as an offset scalar. The generated value is:

```json
"starts": [{"partition":0,"offset":{"partition":3,"offset":0}}]
```

This is rejected by `StreamConfigV1::parse` as `stream admission: strict config JSON`. The assertions only require exit 1, no stdout and no fixture-secret substring. They therefore count a parser failure as the missing-partition witness. The checked-in archive contains this same malformed configuration, and the fresh canonical runs reproduced it.

**Measured proof:** `/tmp/ade-phase14-independent-review/kafka-pinned-campaign/missing-partition.config.json` and `.trace.jsonl`; trace is a single admission-class failure. The earlier authorized campaign also recorded the same behavior.

**Independent runtime countercheck:** `/tmp/ade-phase14-independent-review/independent-kafka/review-missing-partition.config.json` instead uses `"starts":[{"partition":3,"offset":0}]` against a real two-partition topic. It exits 1 with zero output and `source_identity_or_offset` / `assigned partition missing`, reaching `crates/ade-stream/src/kafka.rs:401–403`. Thus this finding is about missing regression proof, not an observed broken refusal implementation.

**Minimal fix:** Give the descriptor builder an unambiguous explicit-starts interface, or override `c['starts']` after building the descriptor. Validate fixture shape before launch. Require the expected source failure class/reason and zero output for this case; do not accept generic parser/network failure. Avoid broad Rust changes.

**Regression and acceptance:** A local helper test asserts the exact typed partition-3 descriptor. A real-broker test reaches the source refusal above. Deliberately changing its offset to an object must fail the missing-partition evidence check. Preserve the old malformed fixture/history and append corrected source-bound evidence rather than rewriting it as a successful historical partition witness.

### F14-02 — Verify semantic invariants for negative evidence

**Violated obligation:** G9 independent evidence reconstruction, G7 no output/progress for the first poison record, and the review requirement to reject hardcoded success metadata.

The verifier validates archive/member hashes, required labels and numeric exits. Its record/trace oracle then visits only eight successful cases. It never checks the oversized/bad-schema/underflow output files or the failure traces/cursors of the negative cases. Named negative cases can therefore have incompatible behavior while verification still says PASS.

**Measured reproduction on copies only:** `/tmp/ade-phase14-independent-review/evidence-negative-probe.py` copies the public archive, injects one emitted NDJSON row into the originally empty `candidate-7/oversize.output.jsonl`, updates that member digest and the copied archive digest in a copied manifest, and runs the existing verifier. It returns exit 0 and:

```json
{"status":"PASS","cases":30,"files":688,"source_files":253}
```

Artifacts: `negative-semantic-forgery.tar.gz`, `negative-semantic-forgery-manifest.json`, and `negative-semantic-forgery.log` under the review directory. The original tracked archive/manifest were unchanged. This is **not** a bypass of the unchanged original cryptographic manifest and not evidence of runtime poison output. It demonstrates that the semantic verifier accepts internally checksummed evidence violating its negative contract, as a publisher/verifier regression could produce.

**Minimal fix:** Introduce explicit per-case negative evidence contracts, separate from ADE's runtime implementation. Check exact failure stage/class, required/forbidden admitted/terminal events, terminal FAILED where a session was admitted, no successful completion, poison-record seen-versus-forwarded invariants, and empty output for cases that must reject before emission. For EPIPE/cancellation/drift/kill, allow truthful prior/prefix output and assert the relevant no-ack/no-success invariants; do not incorrectly demand all negative stdout be empty. Also validate live backpressure snapshots, plateau/backlog and output conservation from raw traces rather than relying only on the result's summary fields.

**Regression and acceptance:** Keep a positive historical archive check, cryptographic-tamper refusal, and Python-optimization refusal. Add checksum-consistent semantic mutation tests: poison output inserted; forwarded cursor advanced on poison; missing-partition replaced with parser refusal; terminal changed to STOPPED; required failure trace removed; blocked snapshot frontier/cursor altered. Each must be rejected for the relevant semantic reason. Share evidence-contract helpers between the live script and offline verifier if useful, while keeping the expected transformation oracle independent of ADE code.

**Historical compatibility:** F14-01 means the existing archive cannot honestly pass a new strict check of the original missing-partition claim. Do not weaken the new validator to hide this. Preserve old bytes, identify that historical limitation explicitly, and bind a corrected supplemental/current campaign to its actual source. A historical integrity/positive-oracle mode may report a qualified result; it must not print unqualified full G0–G9 PASS for the old malformed case.

## 4. Source and execution reconstruction

The diff contains 88 changed/added files, mostly evidence/docs. Runtime changes are the new default-off `ade-stream` crate and extraction of the old native UInt64 filter/Arrow projection into `ade-exec::stateless`. No Phase 10–13 runtime, benchmark oracle, old physical parser or wire/result schema was edited.

Actual path traced:

1. Strict <=16KiB regular-file configuration; `StreamConfigV1::validate` checks version, numeric endpoint/security, topic/cluster strings, fixed sorted starts, caps and timer inventory.
2. `KafkaSource::connect` sets manual assignment, absolute `Offset::Offset`, auto-commit/store false, reset error and no topic auto-create. Cluster/config/inventory/ranges are verified before output. Separate partition queues are acquired after assign.
3. `KafkaSource::poll` serves control callbacks, rotates across fixed queues, retains only cap+1 payload bytes for oversize detection, and rejects headers/oversized keys through poison admission. No offsets-for-times, implicit seek, subscribe or committed-offset adoption exists.
4. `Engine::prepare` separates session/partition/offset checks and seen from forwarded, strictly decodes EventV1, checks arithmetic before output, creates Arrow, calls the shared native filter/project and holds one pending receipt.
5. `runtime::run` owns one sink frame with no downstream channel. A blocked FD stops partition-data polling; periodic source verification services control/statistics. Complete write plus cancellation check precedes `Engine::acknowledge` (`runtime.rs:154–191`).
6. Acknowledgment advances only that pending receipt, recomputes partition max-minus-allowance and all-assigned min/unknown. Timer firing follows published progress; no stop/idle emits completion.
7. CLI cancellation clears pending/timers, explicitly closes the consumer before terminal reporting and reports discarded input/nondurable suggestions. Drop destroys partition queues while the consumer owner remains alive. No detached source/writer/timer task can ack later.

Installed rdkafka 0.39.0 and bundled librdkafka 2.12.1 public source were inspected for assignment, partition queues, callbacks, close and fetch limits. The public configuration reference was also checked: [librdkafka 2.12.1 configuration](https://github.com/confluentinc/librdkafka/blob/v2.12.1/CONFIGURATION.md). It documents per-partition soft queue targets for split queues and growing initial fetch size; the implementation's separate response cap/trusted batch restriction matters. The version-specific docs.rs page was unavailable to the web tool; installed source supplied that API evidence.

## 5. G0–G9 independent evidence matrix

All paths below are under `/tmp/ade-phase14-independent-review/` unless otherwise stated. These are actual witnessed runs, not copied report claims.

| Gate | Independent result | Evidence / limits |
|---|---|---|
| G0 authority/defaults | **PASS** | Exact HEAD/range/status/diff, old PRD/architecture/contracts, current workspace and smoke; `default-tree.log` has no rdkafka, `kafka-tree.log` has no DataFusion/frontend. No broader authorization inferred. |
| G1 real Kafka | **PASS** | `kafka-pinned-campaign/`: actual separate Apache Kafka 3.9.1 KRaft broker, fresh loopback cluster `HreVS3XtT6-v3nqRMaAsRA`, producer/admin ledgers and per-reader PID/session; no fake source substitution. |
| G2 offsets/replay | **PASS** | Canonical native/middle/legal aborted-transaction gaps/kill/replay and explicit broker restart; review four-partition middle starts `{0:1,1:5,2:10,3:15}` produce exactly 161 expected receipts, per-partition order preserved. Partial-prefix/last-byte cancellation probes leave offsets unacknowledged. |
| G3 time/frontier | **PASS** | Reviewer seed 14141029: 192 four-partition records with duplicate keys, UInt64 extremes, Int64 value extremes, negative/near-min/max event times, filter boundaries and an independently computed per-ack vector/min. A never-seen fourth partition preserves unknown and withholds its event timer. Canonical underflow refuses before emission. |
| G4 timers | **PASS** | Workspace 7 core +4 demand tests; reviewer independent timer inventory/cancellation/tie/deadline oracle; real quiet-source future processing timer and global-frontier event traces. No durable timer claim. |
| G5 native semantics | **PASS** | Shared-kernel source trace; canonical producer-ledger oracle plus reviewer corpus and separate scalar Python oracle. Four-partition full run 192 receipts and middle run 161; every projected value/filter/null receipt/lateness/order checked. |
| G6 backpressure | **PASS local bounded envelope** | Immutable-binary campaign stalls a real output pipe **35.892246042s**, source forwarded vectors remain fixed, native fetch high-water plateaus at **7937 bytes**, backlog stays broker-side, then all 1600 expected receipts resume exactly. One record/frame, no ADE downstream queue. No RSS/days-long claim. |
| G7 faults/security | **PASS runtime, P2 canonical evidence debt** | Canonical 30 cases; reviewer header, >4KiB key, tombstone, correctly typed missing partition and frozen-broker signal cases. Frozen broker plus SIGINT exits FAILED after **0.806801625s** from signal with no forwarded record. F14-01/F14-02 qualify reusable negative evidence claims. TLS-specific supplemental results are recorded below. |
| G8 regressions | **PASS for executed scope** | fmt/check/strict all-feature Clippy/build; workspace **263 passed,0 failed,10 intentionally ignored**; smoke; Python **26 passed**. Three selected real-S3 ignored tests separately pass against a new private Moto fixture; no historical timing campaign rerun/reclassification. |
| G9 reproducibility | **PASS source/positive reconstruction, P2 negative-validator debt** | Public archive hashes/oracle reconstruction pass: **30 cases,688 files,253 source files**. All **119 crate/Cargo inputs** match the historical accepted runtime inventory. New immutable-binary campaigns and raw hashes retained. F14-02's copied semantic forgery is wrongly accepted by current verifier. |

### Actual commands and setup

Rust commands ran from the repository with cached locked dependencies, `OPENSSL_DIR=/opt/homebrew/opt/openssl@3` and `OPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3` where relevant:

```sh
cargo fmt --check
cargo check --workspace --all-targets --locked --offline
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
cargo build --workspace --all-features --locked --offline
cargo test --workspace --all-features --locked --offline
bash scripts/smoke.sh
python3 -m unittest discover -s scripts/tests -p 'test_*.py'
python3 scripts/phase14/verify_evidence.py
cargo tree -p ade-stream --locked --offline
cargo tree -p ade-stream --all-features --locked --offline
```

Each completed successfully after permitting authorized local networking for process tests. The first sandboxed workspace attempt stopped at `ade-distributed` with **67 passed/29 failed**, with socket `Operation not permitted`; this is preserved in `tests-sandbox.log`, not called a product regression or pass. The first Kafka attempt similarly failed binding a local port before broker launch (`kafka-campaign.log`). Authorized reruns passed. Build/test logs retain macOS linker unwind-table warnings; strict Clippy passed.

Broker toolchain: `/tmp/kafka_2.13-3.9.1`, Java `/opt/homebrew/opt/openjdk@17`, Python 3.9.6; Rust 1.99.1 `(58cf8f9c3)`, Cargo 1.99.1, aarch64-apple-darwin. The client reports rdkafka 0.39.0 / librdkafka 2.12.1. Private brokers/topics were created only by owned fixtures, never on a production endpoint; all owned processes were stopped/reaped.

The first authorized 30-case campaign passed while Cargo could replace `target/debug/ade-stream`; it is retained as supplementary evidence only. The definitive repeat used an immutable copied all-feature workspace binary, SHA256 **`9614eedc385a69f04479c3feff63293c0056b3a98e8cad108cdda847ac05e2f6`**:

```sh
python3 scripts/phase14/acceptance.py \
  --kafka-home /tmp/kafka_2.13-3.9.1 \
  --java-home /opt/homebrew/opt/openjdk@17 \
  --binary /tmp/ade-phase14-independent-review/ade-stream-pinned \
  --output /tmp/ade-phase14-independent-review/kafka-pinned-campaign
```

That binary differs from the original standalone-build acceptance hash; source is identical and workspace feature unification/build graph differs. Do not relabel the original hash/timing evidence. `kafka-pinned-campaign/commands.json`, `environment.json`, `source-inventory.json`, raw ledgers/traces/output and `inventory.json` record the fresh run.

Reviewer-authored supplemental tests:

```sh
python3 /tmp/ade-phase14-independent-review/independent-kafka.py \
  --kafka-home /tmp/kafka_2.13-3.9.1 --java-home /opt/homebrew/opt/openjdk@17 \
  --binary /tmp/ade-phase14-independent-review/ade-stream-pinned \
  --output /tmp/ade-phase14-independent-review/independent-kafka
# 8 cases passed; reviewer Java producer/admin and Python oracle retained.

CARGO_TARGET_DIR=/Users/jasonkli/projects/adaptive-dataflow-engine/target \
  cargo test --manifest-path /tmp/ade-phase14-independent-review/runtime-probes/Cargo.toml --offline
# 3 reviewer tests passed: every partial frame failure boundary, cancellation
# immediately after the final newline, and a separate timer inventory oracle.

python3 /tmp/ade-phase14-independent-review/remote-regressions.py
# 3 previously ignored real remote tests passed on a fresh private Moto service.

python3 /tmp/ade-phase14-independent-review/evidence-negative-probe.py
# CONFIRMED F14-02: semantically invalid copied negative evidence accepted.
```

The temporary Rust probe is an isolated package with its own retained Cargo.lock; its cached dependency resolution is supplemental, not a substitute for the locked workspace gate. It imports the reviewed path sources. The independent Kafka script reuses the implementation harness's private-service setup/launch helpers only; its four-partition corpus, Java producer extensions and record/progress oracle were written independently and do not call the implementation oracle.

Selected remote commands were `cargo test -p ade-distributed --all-features --locked --offline --test remote NAME -- --ignored --nocapture --test-threads=1` for:

- `genuine_remote_static_turnover_and_scoped_cleanup`: 1 passed,7 filtered.
- `real_remote_caps_and_producer_death_cancel_fail_without_result`: 1 passed,7 filtered.
- `fresh_boot_stale_epoch_attempt_backend_duplicate_and_foreign_grants_refuse_before_io`: 1 passed,7 filtered.

The fixture used Moto 5.1.16 at a new loopback port, a new private `ade-phase13-test` bucket and explicit throwaway credentials. Five other remote fixture tests and the benchmark full-matrix/real-worker fixture tests remained ignored in the workspace result; do not claim their rerun. The 3 selected tests are separately counted, not added to or substituted into the original 263/10 result.

### Evidence accessibility and hashes

The tracked historical archive is portable to a fresh checkout:

- `docs/evidence/phase14/acceptance.tar.gz`: SHA256 `d65c824568e092a8dbf8b632f3e420a79ea56dcc01b20399abc2d9338f1faad5`.
- `docs/evidence/phase14/manifest.json` authenticates its member inventory. Untouched verification returns 30 cases/688 files/253 source files. This proves historical byte identity and the implemented positive oracle, subject to F14-01/F14-02.

New review evidence is **local only**, not available from a fresh checkout by itself. Raw files and reviewer source are at `/tmp/ade-phase14-independent-review/`; the local review bundle and its final hash are recorded in the addendum below. Broker data, the large executable and temporary private credentials/keys are excluded. The executable digest, source inventories, commands and outputs remain. Preserve these files before temporary storage is reclaimed if portable review evidence is desired.

| Local artifact | SHA256 |
|---|---|
| `tests-authorized.log` | `09648a5912435c48de106fb2b5bacb9ac5a49c9d9d2b72367416dc55a7e2513f` |
| `smoke.log` | `57c1796e4fe096cc397a6634299b4ecd59e5d0bc0d24032e70ccbbd60f3116fb` |
| `runtime-probes.log` | `19e39837364648bf05fd802f061137903ace48f2ced2814908b8c62c8a8c0620` |
| `kafka-pinned-campaign/result.json` | `647da66874783b86141f3c4a361f5666a0eabea11fc7cb307d8f2f2dab110ec8` |
| `kafka-pinned-campaign/inventory.json` | `9f6a2cf246a92dd41a69103483908f5b46a3f72a775246c623cc0cd5bd67c083` |
| `independent-kafka/review-result.json` | `b586aad9dc87fb71788b509e4a270247b1ab160670a215c74ace895992a26bd9` |
| `independent-kafka/inventory.json` | `ec6b2d5b0da224b8efbf1bd0ba1c0d36f8c04fb3f72ef9eee47ab0aa31abd61c` |
| `remote-results.json` | `464075105e2bb1ccb16e6ab49060917e0e883807bfc80547f09015aa3d87ac0c` |

## 6. Concrete instructions for the fix session

Continue as the user-authorized **single Luna XHigh fixer**, without implementation subagents, in this repository. Make **one scoped local fix commit**, no push or PR. Re-read AGENTS and discover actual HEAD/status before edits. Preserve the unrelated bootstrap prompt, this review handoff and every historical failed/premeasurement candidate. Do not treat this review as authorization for Phase 15 or broader runtime work.

1. Reproduce F14-01 and F14-02 from the paths and minimal examples above. If temporary evidence is unavailable, reconstruct F14-01 from the descriptor helper and public archive; reconstruct F14-02 using only copies of the archive/manifest and an inserted poison output line with updated copy hashes.
2. Fix the descriptor interface/test and require the reached source failure stage. Keep strict malformed-config admission as its own distinct case.
3. Add negative semantic validation to the live/offline evidence path and focused Python regression tests, likely in `scripts/tests/test_phase14_evidence.py`. Preserve independent expected-event semantics. Use explicit case contracts; avoid accepting any exit-1 result under every fault label.
4. Reconcile historical evidence honestly. The old raw bytes contain the bad missing-partition fixture; retain them. Add a corrected source-bound supplemental/current witness and update the report/evidence JSON with an additive review-remediation section. Distinguish historical byte verification from strict current acceptance. Correct the ADR's stale “acceptance pending” header if touching its status; this is minor documentation consistency, not a runtime blocker.
5. Run the focused new helper/verifier mutation tests, the corrected real missing-partition case and a fresh canonical broker campaign against an immutable feature-pinned executable. No existing broker/topic deletion, paid service, cloud provisioning or production credentials/resources. Use new private fixture paths and unique topics; preserve failures.
6. Run final fmt/check/strict Clippy, locked workspace tests, Python tests and correctness smoke with a matching worker, as appropriate to the final changed scope. The locked runtime is unchanged if only evidence/scripts/docs change; demonstrate that equality instead of claiming old measurements belong to new code. Do not rerun historical timing campaigns or infer speedup.
7. Record actual passed/failed/ignored/unverified counts, source/input/binary hashes, commands and raw accessibility. One scoped commit may include the corrected test helpers, focused tests, additive docs and small sanitized supplemental evidence; exclude unrelated files and generated broker data/binaries. Do not silently overwrite historical archives to make a stricter verifier green.

Acceptance criteria: both findings have executable regressions; a typed missing-partition case proves source admission; malformed offset objects cannot satisfy it; checksum-consistent invalid negative artifacts are rejected; genuine partial-output failures remain representable; corrected fresh live/portable evidence agrees; defaults/legacy contracts remain unchanged. If permissions or infrastructure block a required witness, stop honestly with the missing proof labeled UNVERIFIED. Do not claim a failed or unrun gate passed.

No Rust runtime refactor is required by the confirmed findings. If the fixer discovers a separate runtime defect, reproduce/classify it and keep any correction minimal and within this bounded scope.

## 7. Residual risk, blind spots and roadmap

**If left running for days:** ADE's retained state remains one pending receipt, fixed cursors, bounded timers and bounded client fetch domains under the admitted history assumption. External NDJSON output/trace files can exhaust disk without operator rotation/quotas; they are not in-process queues or durable checkpoints. Native allocator/TLS/kernel overhead is not an RSS cap. Counters/timer registration lifetime have finite refusal behavior. No multi-day soak or production-host memory audit was performed.

**If source identity/history drifts:** observable cluster/inventory/config/range contradictions fail closed, but undetected recreate/truncate-and-regrow or historical compression/change-and-restore remain outside the trust contract. Current metadata checks cannot become a durable replay identity. Broker failover across real hosts and repeated broker-cohort churn were not independently exercised.

**If the downstream consumer blocks:** only bounded native prefetch may continue, data polling stops, broker backlog grows, and a finite stall budget eventually fails the session. Timers can be delayed. A pipe write means kernel acceptance, not consumption. Regular-file I/O and library scheduling are not hard real-time; O_NONBLOCK does not make arbitrary filesystem writes preemptible. Partial output and last-byte cancellation can require manual replay and duplicates; the independent ambiguity probe confirms this honest behavior.

Additional unverified/deferred areas: full CI on Linux, declared MSRV 1.94, cross-host/AZ durability, production authentication/authorization deployments, malicious-broker historical codec behavior, exhaustive native teardown failure injection, and every ignored remote/proxy test. These are not passed claims. The actual local service and selected failure envelope were tested; absence of a discovered bug is not a general correctness proof.

Preserve separate roadmap debt: Phase 10 physical membership/elasticity; Phase 12 replicated metadata/production HA and worker replacement; Phase 13 reusable remote exchange/production cloud and cross-AZ behavior; Phase 15 keyed/window state, durable checkpoints and restore; Phase 16 adaptive checkpoint/state placement; Phase 17 transactional/exactly-once sinks, savepoints and migration; Phase 18 unified bounded/unbounded optimizer. Consumer groups, more connectors and general streaming SQL remain optional future scope, not inferred authorization.

## 8. Final security and artifact addendum

The final supplemental `tls-probes.py` run passed **3 cases** on another owned private Kafka broker and the same immutable review binary: a valid SASL_SSL control emitted exactly two records; an unrelated regular-file CA refused with broker-class failure and zero output; after replacing the broker certificate, a certificate trusted by the supplied CA but identifying only `127.0.0.2` refused a `127.0.0.1` connection, also with zero output. This challenges actual certificate/hostname verification, beyond the canonical nonexistent-CA-path refusal. Fixtures used temporary keys/credentials, scanned retained text for secret values and removed private security material.

Reproduction: `python3 /tmp/ade-phase14-independent-review/tls-probes.py --kafka-home /tmp/kafka_2.13-3.9.1 --java-home /opt/homebrew/opt/openjdk@17 --binary /tmp/ade-phase14-independent-review/ade-stream-pinned --output NEW_PRIVATE_OUTPUT_DIRECTORY`. Raw commands/traces are in `tls-probes/`; `review-result.json` SHA256 is `6f98fe6b3b9dbc00a75fcb333fddb5854f10ee269ae60526e536221b53a1888a`.

Final local review bundle: `/tmp/ade-phase14-independent-review-evidence.tar.gz`, **11545257 bytes**, **463 files**, SHA256 **`385e6c2fe88b463e7fb84b3dbac22a55a66493d2ce527373bf866cb831d2459a`**. It contains the raw review logs, source/config/ledger inventories, reviewer probes and copied semantic-forgery reproduction, with per-file hashes in `review-file-inventory.json`. It is not tracked or externally published; a fresh checkout cannot retrieve it by itself. The original public archive and unrelated bootstrap prompt still have the unchanged hashes recorded above. No tracked implementation file changed.
