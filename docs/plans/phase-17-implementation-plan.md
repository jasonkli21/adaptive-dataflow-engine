# Adaptive Dataflow Engine — Phase 17 Implementation Plan

## Exactly-once-capable output and savepoint-class operations (bounded v1)

**Prepared:** 2026-10-30 (America/Los_Angeles)
**Repository:** https://github.com/jasonkli21/adaptive-dataflow-engine
**Inspected `main`:** [`46a976d41d8642d54fe0829bd7944c9a1ddd39d1`](https://github.com/jasonkli21/adaptive-dataflow-engine/commit/46a976d41d8642d54fe0829bd7944c9a1ddd39d1), “Fix Phase 16 recovery and admission review findings.” Recheck HEAD, worktree, relevant docs and any subsequent changes before implementation.
**Status:** **COMPLETE_BOUNDED_TRANSACTIONAL_STATE_V1**. Explicitly authorized by the user's submitted implementation request; qualified local G0–G14 evidence is recorded in the implementation report. A planning document alone supplies no authorization.
**Authority:** PRD product outcome > architecture invariants > reconciled active plan > accepted ADRs > reports/current code; follow `AGENTS.md` for procedure. Record any intentional refinement in an ADR, and reconcile the plan rather than silently changing product scope.

## 0. Executive decision and rationale

PRD Phase 17 (`docs/product/prd.md`, Phase Roadmap) lists **exactly-once-capable sinks, transactional commit integration, savepoints, pause/resume, upgrade, rescale and state-migration hooks**. This is a demanding correctness milestone, not a collection of UI switches. The smallest defensible implementation is **an opt-in, single-active-writer, real-Kafka-input, manifest-committed local transactional output path**, in which **state, consumed offsets, output visibility and lineage advance in one atomic publication**. Add independent verified savepoints, a clean pause/resume protocol, one real versioned state migration, and logical owner rescale 1→2→4→1, all using that same commit discipline.

This **does not mean** Kafka transactional output, distributed two-phase commit, global exactly-once semantics across arbitrary sources/sinks, cross-host failover, physical worker rescale or production rolling upgrades. Do not claim those outcomes. An atomic local manifest sink is a legitimate first exactly-once-capable sink only if an independent reader consumes **committed** output, real process failures demonstrate no duplicate visible records, and the state and sink truly share a single authoritative commit point. Simply assigning `output_id`s, deduplicating after the fact, using `rdkafka` idempotence or keeping the old stdout sink is **not** enough.

**Proposed status on success:** `COMPLETE_BOUNDED_TRANSACTIONAL_STATE_V1`, with an explicit `PARTIAL` qualification against the broader PRD's external-sink/physical-rescale aspirations. If any required bounded product gate fails or cannot be witnessed, say `PARTIAL`; never relabel an untested branch as accepted.

### 0.1 Read-first map and source reality

Read, in order:

1. `AGENTS.md`; `docs/product/prd.md` §§7–9, 11, 15–17 (especially Phase 17 and its neighbors); `docs/architecture/architecture.md` §§3, 7–10, 14–18, 21–22, 27–31 and INV-1–10.
2. `docs/adr/0021-unbounded-source-and-progress-v1.md`, `0022-keyed-state-and-checkpoints-v1.md`, `0023-adaptive-checkpoint-placement-v1.md`; `docs/plans/phase-15-implementation-plan.md`, `phase-16-implementation-plan.md`; `docs/reports/phase-15-review-remediation-2026-10-29.md`, `phase-16-implementation-report.md`, `phase-16-review-remediation-2026-10-30.md`; `docs/phase-15-state-guide.md`, `docs/phase-16-adaptive-checkpoint-guide.md`.
3. `crates/ade-stream/src/{lib,runtime,kafka}.rs`, `src/state/{mod,checkpoint,store}.rs`, `src/adaptive/{mod,policy,snapshot,remote}.rs`, `src/bin/ade-state-stream.rs`; corresponding Rust tests and `scripts/phase14`, `phase15`, `phase16`; `crates/ade-stream/Cargo.toml` and full workspace gates.
4. Relevant existing batch output commit concepts: `crates/ade-ops/src/{publication,store,runner}.rs`, `crates/ade-distributed/src/{gather,bounded_output}.rs`; **reuse concepts only when semantics fit**. Do not couple streaming to batch/frontends.

As inspected, `ade-stream` has default-off `kafka`, `stateful`, `adaptive-checkpoint`, `remote-checkpoint` features; a single-owner demand loop; manual-assigned fixed 1–4 Kafka partitions; `Config` v1 with 1/2/4 *logical* owners; local full v1 `CURRENT` and adaptive v2 `CURRENT2` (immutable 32-shard full/incremental objects and local/remote/both placement). `runtime::Sink` is a nonblocking byte writer; `State::next_close` / `confirm_close` emit at-least-once `WindowOutput`, and `runtime::run` acknowledges inputs after writing stdout receipts. Both old paths expose output before checkpoint publication. Neither may be rebranded exactly-once.

**Phase 16 blocker:** `docs/reports/phase-16-review-remediation-2026-10-30.md` is `PARTIAL_VERIFICATION` at the inspected head. F16-01 (replay-origin expiry) and F16-02 (incremental-to-full policy admission) have local tests, but the **fresh real Kafka `deleteRecords` + MinIO campaign on the remediation source was not run**. Historical 117-case evidence belongs to the pre-fix source. Reaccept those changed behavior paths in a separate, immutable, source-bound fixture before claiming a clean Phase 17 foundation; preserve historical evidence and failed candidates. A lack of local services is a **gated partial**, not permission to silently waive the run.

**Other existing debt:** Phase 10 physical membership is still partial; Phase 12 production coordinator HA, Phase 13 real multi-host cloud HA, Phase 15 physical state rescale, Phase 16 external-failure-domain durability and automatic GC remain deferred. Do not imply Phase 17 closes them.

## 1. Supported envelope and explicit refusal matrix

| Dimension | Required supported v1 | Must refuse / defer |
|---|---|---|
| Source | Real Kafka, fixed 1–4 explicitly assigned partitions, pinned cluster/topic/starts, retained source ranges; operator attests immutable history; source checks before output | Consumer-group failover, changing topic incarnation, unbounded topic retention assumption, nonreplayable sources, unsupported partition changes |
| Workload | Native `tumbling_sum_count_v1` keyed windows, checked arithmetic, existing frontier/late policy and `payload_fingerprint` | Arbitrary DAG, joins, UDF side effects, SQL streaming, nondeterministic user code |
| Sink | New versioned transactional **manifest-visible immutable local output** with a separately implemented committed-output reader; one atomic commit covers state + source cursor + outputs | stdout/HTTP/Kafka/S3 external exactly-once promises, best-effort callback sinks, arbitrary distributed transactions |
| Visibility | Only committed output is observable through the documented reader; a failed/aborted epoch is never visible | Treating trace, diagnostic, input receipt or precommit bytes as committed business output |
| Placement | Canonical local durable commit domain, single active fenced process, private roots, explicit disk/quota limits; process crash/restart | Disk/media loss, cross-host/multi-writer HA, remote atomic transaction across multiple services, silent failover |
| State/cut | Quiescent cut and complete immutable state+progress+output manifest, with one authoritative pointer; bounded generations/bytes | Independent `CURRENT`/`CURRENT2` publish followed by unrelated sink commit, partial speculative output exposure |
| Savepoint | Named, immutable, self-contained, verified export and explicit import with retained operator-provided exact digest and lineage | Unsigned/unverified path trust, implicit migration between checkpoint v1/v2/v3, arbitrary state graph transplant |
| Pause/resume | Fenced graceful pause at committed cut, process exit and independently verified cold resume | Pausing mid-frame or after partially visible epoch, automatic cross-host orchestration |
| Upgrade | One demonstrable **real** compatible state-codec version transition via explicit migration hook, with fresh-process activation, verification and refusal of incompatible change | General binary rolling upgrade or arbitrary semantic changes, old-version automatic re-adoption of upgraded state |
| Rescale | 1→2→4→1 **logical local owners** with verified keyed redistribution at a committed boundary and fresh process | Physical worker membership change, repartitioning Kafka source, live network state migration |
| Ops | One job, one writer, no background automatic GC; bounded retained output history and savepoint roots, explicit offline cleanup recipe | Multi-tenant quotas, infinite unbounded retention, orphan reclamation without fencing |

Do **not** weaken existing features/defaults or modify historical evidence to satisfy this table. New strict v3 opt-in config/CLI and separate roots are preferable; preserve `PlanV1`, `StreamConfigV1`, state config v1, v1/v2 selectors, all old CLI/error codes/protocols, default feature graph and benchmark/oracle schemas. A reasonable new Cargo feature is `transactional-state = ["stateful"]` with a separate `ade-state-stream-txn` binary. Do not let feature naming mask behavior; inspect actual dependencies and decide in ADR-0024.

## 2. Non-negotiable correctness model

### 2.1 Define exactly-once in observable terms

For a **single logical job lineage**, let `C_e` be the e-th durably visible committed epoch, containing (a) exact confirmed source-offset vector/frontier, (b) canonical keyed state and operator/schema identity, (c) a bounded immutable set of window outputs newly committed in e, (d) the previous committed epoch link or a verified compacted base, and (e) hashes/budgets/capabilities. A committed-output reader lists/streams only records reachable through the **one valid committed pointer** and its immutable authenticated closure. For all valid recovery sequences:

- The visible output history is an append-only prefix of whole committed epochs; no precommit, partial, duplicate or lost window output can be exposed through this reader.
- The restart restores exactly the state and **next source offsets** belonging to that same visible prefix. A crash before the commit must resume from the previous pointer, discard/ignore speculative state and output; a crash after commit must recognize the new pointer even if success telemetry/ACK was lost.
- Within that committed lineage, each logical window output is visible once. A duplicate ID with a different fingerprint is a correctness violation, **never** silently suppressed. Existing Phase15/16 *at-least-once trace-relative* output IDs are not sufficient proof for this v3 contract.
- A subsequent pause/resume/upgrade/rescale preserves the above prefix and exactly the same logical window contents, under compatible semantics and pinned source history.
- Correctness is defined over **actual committed consumer observations** and a separate source-ledger oracle, not `PASS` labels or final aggregate totals alone.

Do not promise a globally unique payload across *different* legal Kafka partition interleavings or unrelated job lineages: Phase15 proved the same window ID can have different fingerprints. For v3, speculative reordered output must remain invisible until the epoch commits; once visible, replay starts from that exact committed cut.

### 2.2 Atomicity and crash states

Proposed cut flow, subject to ADR acceptance:

1. Pause source demand, settle pending input/close, source verification, and prohibit further Kafka data polls while committing. Provisional outputs are accumulated only in bounded private staging, not stdout; input progress is provisional until whole-epoch publication.
2. Serialize the complete logical checkpoint and new sink-output segment(s) as immutable, digest-bound objects under one private local commit domain; verify canonical per-shard data, output IDs/payloads, exact offsets, owner mapping and resource accounting. Keep the prior epoch and its output history reachable.
3. Sync staged contents and directories; publish the new immutable epoch manifest. Its references cover *both* snapshot and new output, and its chain/compaction rules are explicit and capped.
4. Atomically replace **one** synced `CURRENT3` selection marker under an exclusive lock/fence; the marker is the sole external visibility frontier. There is no separately visible checkpoint pointer and no sink commit before/after it. Readback and validate where required.
5. Only then report `committed`; on any ambiguous post-selection failure, poison the running attempt and require fresh-process recovery from `CURRENT3`. Never guess rollback or retry a side effect in-place.

Crash/kill/hook matrix must include every object, fsync, manifest and selector stage; verify output history/state is **entirely old or entirely new**, never a mixture. Treat OS durability assumptions explicitly (single filesystem, atomic rename, same host, fsync semantics). If source/checkpoint/sink cannot share this atomic domain, this **specific v1 mechanism is unsupported**. Avoid calling a two-file or two-service publication protocol atomic without proof.

### 2.3 Replay, failure, and fencing

- Exclusive process lease/fencing identity must reject second active writer, stale process publish, wrong epoch parent, cloned writable roots, mixed config/semantic fingerprint and operator conflicts. Refuse unsafe paths/symlinks/hardlinks/device files; bounded parsing before side effects.
- Full source replay from immutable pinned origins can recover **only** under explicit operator consent and only when it cannot conflict with already published transactional output. Normally recover from the exact committed prefix; never replay a committed output and publish it twice.
- Validate source cluster/inventory, both saved and original low/high eligibility where relevant, immediately before assignment. Live metadata is not topic UUID/incarnation attestation. Fail closed for truncation/regrowth uncertainty or untrusted/changed history.
- Cancellation and late/partial diagnostic failures poison the attempt but cannot retroactively hide committed output. Keep separate structured `ABORTED`, `COMMITTED`, `UNKNOWN_AFTER_COMMIT`, `REFUSED` and `PAUSED` states.
- A cap exhaustion or disk full must refuse **before** a visible partial commit. Reserve for staging+retained state+output+savepoints; account for failed/orphan candidates. Do not silently drop old visible output for space.

### 2.4 Savepoint and migration model

Savepoints are **not** merely copied `CURRENT3` or mutable checkpoint directories. A named export pins a **completed committed epoch** and packs/links a self-contained, versioned closure of all shards, progress, previously committed sink records (or explicit immutable output-history anchors), schema/operator identities, source starts, hash/owner version, retention capabilities, and hashes. Its exact top-level digest is operator-retained; verify every byte before adoption in a fresh root. No savepoint may point into a mutable working tier or depend on a future GC candidate. Export must be atomic; failed export leaves no valid named savepoint. Import with an empty root can change only allowlisted nonsemantic fields (resource limits, destination path, logical owners through a verified explicit step); reject changed input/source identity, event-time/late policy, hash algorithm or job ID unless a separately authorized migration plan formally changes the contract.

Define migration extension points with explicit `(old_schema, new_schema)` compatibility, deterministic transform, checksum/fingerprint verification, bounded work and rollback semantics. Implement **at least one exercised real transition**, e.g. a canonical state-record codec v1→v2 with an added validated field that preserves keyed sum/count behavior; do not claim upgrade merely because unchanged code restarts. A requested incompatible migration fails **before** publishing any new epoch or output. In-place upgrades produce a new immutable committed generation, leaving prior savepoint intact for explicit rollback if still safe for source/output lineage.

Logical rescale redistributes all 32 stable hash shards across 1/2/4 *independent local owner stores* at a stopped committed boundary. The shard hash, state semantics and source offsets do not change. Verify bijection/no loss/no duplicate entries; change owner count only in a new committed epoch. Prove 1→2→4→1 under fresh process restarts with live Kafka, including cold-tier state. Never call this distributed physical rescale.

## 3. Implementation work packages

| Work package | Deliverable | Specific review concern |
|---|---|---|
| WP0 — reconcile & baseline | HEAD/worktree readout, Phase16 post-fix real-service reacceptance, v3 ADR-0024 decision matrix, frozen acceptance criteria | Phase16 unrun fixes, history/evidence provenance, old behavior mutation |
| WP1 — v3 contract | Versioned config / dedicated CLI, canonical IDs, strict parser, capability/refusal table, private roots and operator guide | Accidental v1/v2 auto-migration, source identity loosened, default changes |
| WP2 — transactional local sink | Provisional bounded output writer, immutable segment format, verified committed-output reader, history/retention caps | Side effects/visibility before commit, output ID collisions, malformed/partial frames |
| WP3 — atomic state+sink epoch | Prepare/cut/validate/publish under one pointer/lease, fsync/rename failure handling, exact restore and fence | Old/new mixture, fail-open post-selector retries, one resource domain only in name |
| WP4 — savepoint & resume | Named self-contained verified export/import, graceful pause at cut, cold restart/seek | Open output/unfinished close, corrupt or foreign adoption, lost offset or sink prefix |
| WP5 — upgrade & rescale | Versioned migration hook with exercised v1→v2 codec, semantic checks, 1→2→4→1 logical redistribution | No-op “upgrade,” duplicate/lost state, changed closed/late behavior |
| WP6 — oracle & fault suite | Independent Python producer ledger + committed-reader oracle, real Kafka process kills, corruption/poison/space/race attacks | Self-referential oracle, only final totals, tests use stdout to fake commit |
| WP7 — complete proof | Versioned guide, implementation report, source/executable/service hashes, evidence pack + offline verifier, old/new regression matrix | Source drift after measurement, mutated past archives, claims beyond measured envelope |

Suggested code home: `crates/ade-stream/src/transactional/{mod,sink,epoch,savepoint,migration}.rs`, the existing `state` and `runtime` only for narrow reuse/refactors, a separate binary in `src/bin`, `crates/ade-stream/tests/transactional.rs` and `scripts/phase17/{acceptance,oracle,verify_evidence,package_evidence}.py` with Python tests. Avoid a second unrelated execution kernel, a generic distributed 2PC framework, or importing `ade-ops`, `ade-distributed` or `ade-frontend` into the streaming runtime. Keep implementation idiomatic and small enough to audit.

## 4. Mandatory acceptance gates — independent and source-bound

The runner must record the real service, pinned executable/source IDs, fixture/producer ledger, ordered multi-partition traces, raw stdout/stderr, committed-reader observations, immutable manifests, hash maps and failure injection commands. Predeclare exact refusal reasons and invariants in `scripts/phase17/criteria.json` or equivalent **before** selecting a final candidate. Never manufacture a pass log.

| Gate | Required witness |
|---|---|
| **G0. Authority / baseline** | Exact HEAD, dirty tree inventory, source-of-truth reconciliation; baseline default/stateful/adaptive tests and no scope expansion |
| **G1. Phase16 prerequisite** | Fresh source-bound *real Kafka+MinIO* rerun of F16-01/F16-02 decisive branches on `46a976d` or actual later fixed HEAD, including expired replay/retained checkpoint, both expired, preflight/connect race, effective full admission before remote PUT; independently verified. Failure/unavailable service -> partial blocker |
| **G2. Compatibility/admission** | Strict v3 parsing, old v1/v2/default paths unchanged, feature graph checked; mixed selector/root, unsupported sink/partition/source, invalid schema and wrong fingerprint refuse before effect |
| **G3. Atomic visibility** | Real committed reader sees only whole contiguous epoch prefixes across every injected fsync/object/manifest/selector crash; state+offsets+output match same pointer; old/new pointer plus atomicity proof |
| **G4. Exactly-once crash/retry** | Real Kafka multi-partition fixture; kill before/during/after commit, recover fresh process, verify each committed logical window once with exact fingerprint; no missing/duplicate visible outputs; uncommitted work never observed |
| **G5. Window semantics** | Independent signed sum/count/window/late/frontier oracle with hot+cold tier, negative time, overflow, collisions, changing partition interleavings and repeated-ID/different-fingerprint adversary; not merely output count |
| **G6. Savepoint** | Named self-contained export/import to fresh roots, exact digest and full closure, recover from cold state, tamper/missing/foreign/size/symlink/rollback refusals; failed savepoint remains non-adoptable |
| **G7. Pause/resume** | Real process pause after quiescent committed cut and fresh resume with exact output prefix/source cursor, test idle, busy/blocked sink, signal kill and double-writer fence |
| **G8. Upgrade** | A real changed-version migrator and fresh-process activation, semantic output equivalence, old-version refusal, incompatible/failing migration atomic abort and safe retained old savepoint |
| **G9. Rescale** | Real 1→2→4→1 logical owner cycle at committed boundaries with independently checked shard membership, cold bytes, exact output, no double processing; confirm physical workers remain fixed |
| **G10. Resource/security** | Byte/epoch/output/savepoint/diagnostics limits, cancellation and disk full, bounded parsers, path confinement, crash-orphan accounting, no credential leaks, source/history spoof refusal; graceful refusal at quotas |
| **G11. Regression** | `cargo fmt --all -- --check`, locked workspace check, all-feature Clippy `-D warnings`, serial workspace tests, relevant Python unit tests, smoke, fresh baseline stateless/fixed-state/adaptive regression cases; classify environment failures honestly |
| **G12. Determinism & research** | Repeatable same ordered trace runs plus divergent legal merge orders, planned simple static baseline, measured write-amplification/commit latency/byte accounting; no unproved performance advantage |
| **G13. Evidence integrity** | Immutable failed candidates, frozen source and executable hashes, actual real Kafka and reader version IDs, raw per-case artifacts, independent offline verifier that recomputes output/state/offset hashes, not trust `PASS` markers |
| **G14. Documentation & signoff** | ADR-0024, committed-output/operator/savepoint/migration guide, Phase17 implementation report, live source-bound criteria table, exact unsupported envelope and debt; status matches every gate |

Suggested fault cuts: before output staging; halfway through segment write; before and after sync; before checkpoint stage; after snapshot but before manifest; after manifest before selector; during selector temp write/rename/fsync; after selector before success telemetry; during savepoint copy/export; after migration staging but before commit; during paused shutdown; stale and competing writers. Test not just exceptions but `SIGKILL`, full restart with **new session**, reader concurrency, and durable byte-hash validation. Source and sink oracles must be independent of the runtime's own success markers.

**Evidence discipline:** freeze every measured candidate, preserve failed runs and their source IDs, preserve Phase16/earlier archives unchanged, maintain manifest inventory/checksums, and provide portable replay/verification without live Kafka where practical. Fresh real-service acceptance is still required in addition to offline proof.

## 5. Sequencing, gates and stop conditions

1. **Prerequisite gate:** re-run Phase16 fixed-source F16-01/F16-02 campaigns against real services. If unavailable, record `PHASE16_POSTFIX_UNVERIFIED`, continue only isolated Phase17 design/unit work and do **not** claim Phase17 accepted.
2. **Decision gate:** write ADR-0024 mapping PRD's seven outcomes to bounded features/refusals. Explicitly decide commit filesystem assumptions, transactional reader contract, retention/GC, retry ambiguity, operator-trusted digest, upgrade semver and owner rescale. If a correct local atomic commit is not feasible, stop and propose revised scope—do not substitute stdout/dedup.
3. **Correctness spine:** implement WP1–WP3; prove output visibility/commit invariants and real crash recovery before adding lifecycle APIs. Do not build savepoint/upgrade/rescale on an unproven split commit.
4. **Lifecycle:** WP4–WP5, then cross-feature tests (pause + upgrade + rescale + savepoint + restart under one job lineage).
5. **Independent acceptance:** WP6–WP7; build a clean pinned executable, predeclare gates, use disposable Kafka and a separate MinIO service for Phase16 closure, test real failures, freeze source/executable/inputs before final claims, verify archived artifacts offline. Do not rewrite earlier measurement evidence.

**Immediate STOP / report PARTIAL:** inability to preserve state+sink atomicity; output visible before commit; duplicate/missing *committed* output after crash; stale writer can publish; unsafe source-history assumption; silent checkpoint/savepoint corruption fallback; resource quota violation or nonbounded retention; unverified migration or logical rescale; absent independent reader/oracle; unavailable real service evidence; or a regression in old defaults/protocols. A narrower truly correct milestone is better than an unsupported exactly-once claim.

## 6. Handoff and next phase boundary

Implementation agent must provide, at minimum: baseline and final SHA/worktree, exact supported/refused matrix, ADR decision deviations, per-gate pass/fail/untested evidence paths, all commands and environment identifiers, real injected crash outcomes, immutable output ledger samples with fingerprints, source/reader/checkpoint consistency proofs, resource and security accounting, source/executable hashes, failed-candidate retention, old-feature regression results, and actionable follow-up debt.

**Defer** generalized external sink 2PC, Kafka transactional producer/consumer-group offset coupling, cross-service atomic commits, physical state migration/elastic membership, distributed savepoint coordination, automated checkpoint GC, production HA and rolling upgrades. Phase 18's unified bounded/unbounded optimizer remains separate. A Phase17 follow-up can specialize a true external transactional sink once the supported v1 transaction protocol and its impossibility boundaries are proven; it must not inherit “exactly once” from manifest semantics by assertion.


## 7. Implementation reconciliation — 2026-10-30

The user explicitly submitted the implementation handoff and authorized this phase
in one session, without subagents and with one final local commit. ADR-0024
reconciles the bounded sink, atomic selector, actual codec migration, savepoint
closure and local logical owner transition with the PRD. The preexisting plan is
adopted without altering historical phase acceptance. Completion awaits every
G0–G14 on a matched executable and fresh real services; earlier Phase16 evidence
is not reused as post-fix proof. The final report records the exact source snapshot,
failed candidates and qualifications.

## 8. Acceptance reconciliation — 2026-10-30

All bounded G0–G14 passed at candidate5's source/executable identities recorded in
`docs/reports/phase-17-implementation-report.md`. The frozen primary evidence has
121 transactional cases,144 independent reader observations and376 matched cuts;
five fresh Kafka/MinIO post-fix prerequisite proofs and an actual full-filesystem
supplement close the remaining source/storage gates. Real lifecycle SIGKILL tests,
codec transformation and logical owner-store redistribution are included. All
four predecessors and earlier phase archives remain preserved. The full PRD's
external-sink/physical-rescale/HA aspirations remain explicitly outside this bounded
product; they are not silently claimed complete. Final verification/documentation
strengthening does not change any of the139 matched runtime/Cargo inputs.
