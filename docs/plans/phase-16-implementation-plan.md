# Adaptive Dataflow Engine — Phase 16 Implementation Plan

## Adaptive checkpoint and state placement: bounded, cost-aware streaming v1

**Prepared:** 2026-10-29 (America/Los_Angeles)
**Repository:** https://github.com/jasonkli21/adaptive-dataflow-engine
**Inspected `main`:** [`a89f1aeb2b7c36a23d77d702e705d6ffe4bdbfea`](https://github.com/jasonkli21/adaptive-dataflow-engine/commit/a89f1aeb2b7c36a23d77d702e705d6ffe4bdbfea), "Address Phase 15 independent review findings". **Recheck HEAD before implementation.**
**Status:** COMPLETE_BOUNDED_ADAPTIVE_STATE_V1 — the initial proposal was explicitly authorized in this session; final G0–G12 evidence and qualifications are recorded in `docs/reports/phase-16-implementation-report.md`.
**Authority:** `docs/product/prd.md` > `docs/architecture/architecture.md` > active phase plan > accepted ADRs > implementation reports/current code; `AGENTS.md` governs agent workflow. Reconcile conflicts explicitly. No silent narrowing of the PRD or reinterpretation of historical proofs.

## 0. Product outcome and deliberate boundary

PRD **Phase 16** (`docs/product/prd.md`, lines approximately 947–957) says to apply ADE's original materialization/recomputation thesis to streaming state: decide **(1) checkpoint cadence, (2) incremental versus full snapshot, (3) local versus remote placement, (4) replication, and (5) replay versus restore**. Architecture §§14–15, 18.2–18.3 and INV-1–10 require a separable, explainable policy, correct state+progress cuts, explicit failure domains, bounded resources, reproducible decisions, and a static baseline. Phase 17—not this phase—adds transactional exactly-once sinks, savepoints, pause/upgrade and generalized rescale.

**Proposed Phase 16 bounded product:** opt-in, single-process/single-checkpoint-writer, real Kafka-fed Phase 15 keyed tumbling windows; a deterministic, constraint-first adaptive policy choosing a safe cadence, shard-copy-on-write full/incremental generation, verified **local / S3-compatible remote / two-copy local+remote** placement, and a guarded restart choice between an exact verified checkpoint and a **complete source replay**. The policy is driven by configured risk/price/SLO inputs and bounded measured observations, and emits an auditable decision plus counterfactuals. Every chosen mechanism must work, not merely appear in a policy log. Exercise the choices on a local real Kafka broker plus a **separate real S3-compatible service** with deliberately injected failures; no paid/public cloud dependency is needed.

This is intentionally **not** a general production distributed state backend, automatic recovery, asynchronous/unaligned checkpoints, live state migration, cross-AZ HA, continuous garbage collection, streaming SQL, savepoint API, or exactly-once output. It cannot complete Phase 15's still-deferred *physical distributed rescale*. If a safe end-to-end remote mechanism cannot be delivered, report **PARTIAL**, do not relabel a local directory as remote or substitute a mocked policy for its action. A good Phase 16 milestone is a narrow honest prototype with source-bound research results, not five nominal checkboxes.

### Inspected starting reality

1. `crates/ade-stream/src/state/mod.rs`: the strict `Config` v1 enables only `stateful`, `checkpoint_every_records/ms` are fixed, `State::due` requires changed forwarded progress, `State::checkpoint` refuses pending input/closure; `State::open` validates a local generation then rehydrates separate owners. Its semantic fingerprint deliberately excludes resource/cadence knobs.
2. `crates/ade-stream/src/state/checkpoint.rs`: `Pointer` v1 selects `CURRENT`; `Manifest` v1 points to **32 complete, sorted, fixed64-byte local shard files**, with BLAKE3 verification, validated progress, fsync and atomic pointer replacement. `occupied` counts retained/staging bytes and generations; no automatic GC; quotas are finite. The `regular` helper refuses symlinks and hardlinks (`nlink == 1`). Do not use hardlink-based incremental snapshots or mutate any accepted v1 generation.
3. `crates/ade-stream/src/state/store.rs`: lossy preallocated hot slots plus bounded sorted cold files; 32 stable BLAKE3-key virtual shards, owner count 1/2/4. Mutable working files are *not* recovery copies. Naive per-shard dirty-bit assumptions must account for eviction, read-through, removal and restore.
4. `crates/ade-stream/src/runtime.rs`: a single demand owner; sink-confirmed receipts, one window-close output at a time, quiescent cut with source `pause`/`verify`, control-only Kafka polling during filesystem I/O. A partial NDJSON frame is failure, not a completed receipt. The native source cannot poll stateful partition data while a cut is in progress.
5. `crates/ade-stream/src/kafka.rs` and ADR-0021: explicit fixed source partitions/absolute offsets; immutable trusted uncompressed <=32KiB Kafka history; no group commits, topic-incarnation proof, or cross-partition total order. A different legal replay merge may yield the same logical window ID **with a different payload fingerprint** (F15-01); ID-only deduplication is unsafe.
6. ADR-0022, `docs/phase-15-state-guide.md`, both Phase 15 reports, and `docs/reports/phase-15-support.json`: `COMPLETE_BOUNDED_STATE_V1`, manual restore, local fsync envelope, at-least-once trace-relative output, `checkpoint_bytes` 66,048..256MiB, max 64 generations. Empty/idle cuts must not consume generations (F15-02). Retention/stale locks need operator action; no disk-loss/HA claim. Phase15 review remediation is included in inspected HEAD.
7. Phase 13 `ade-distributed/remote-s3`, ADR-0020 and operator guide demonstrate a **separate S3-compatible object service**, immutable conditional PUT, bounded HEAD/GET, observed HTTP budgets, no redirect/proxy, explicit credentials and no ambiguous-write retry. These are reusable *security/capability patterns*, **not** an existing streaming checkpoint backend. Avoid introducing `ade-stream -> ade-distributed` just to reuse the batch-only remote executor, or silently changing Phase 13 contracts.
8. Phase10 physical elasticity stays PARTIAL; Phase12 production HA and Phase13 cross-host/cloud durability remain deferred; Phase14 stateless and the fixed Phase15 path are accepted baselines. Historical timing/evidence must never be repurposed.

## 1. Support/refusal contract and API isolation

| Dimension | Bounded Phase16 required | Must refuse/defer |
|---|---|---|
| Entry/compatibility | New opt-in adaptive checkpoint mode and strictly versioned policy descriptor, or equivalent separate versioned stateful config path; default legacy stateful v1 still fixed/full/local, unchanged output schema and v1 pointer/manifest interpretation | Implicit migration of v1 roots, modified `StreamConfigV1`, physical `PlanV1`, batch/ops/remote v1, auto-enable of Kafka/S3 |
| Cadence | Quiescent, forwarded-progress-triggered adaptive record/time bounds, cooldown and policy decision at safe cut; fixed controls retained | Empty/unchanged cuts, barrier before output ACK, hard real-time timers during blocking I/O |
| Snapshot format | New versioned **full and incremental-by-immutable-virtual-shard reference** formats; exact verified logical complete state; chain depth bounded and periodically full | Hardlinks, unbounded delta logs/ancestry, calling a full rewrite "incremental", in-place v1 mutation |
| Placement | Local, verified S3-compatible remote-only, and **local+remote**; restore from independently verified required placement | A second local path called remote, unverified PUT success, implicit S3 LIST-based selection, automatic unhealthy fallback |
| Replication | Exactly 1 or 2 explicit copies (local+remote), published only after all required copies verified; operator-specified durability floor | Peer replicas, cross-AZ claims from a same-host fixture, detached retries or background repair, production HA |
| Recovery | Explicit verified decision: restore a selected generation OR fresh deterministic start from configured *original* Kafka offsets after full-history validation, subject to sink replay/conflict consent | Auto-loss of a newer valid checkpoint, hidden offset reset, false exactly-once or merge-independent outputs |
| Economics | Pure deterministic policy+cost model; configured failure risk, prices and SLO; separately measured bytes/throughputs; fixed/static control and decision trace | Uncalibrated speedup, model-estimated dollars called billed dollars, random/learned policy, correctness tradeoff for cost |
| Boundedness | Finite bytes/objects/requests/chain depth, one outstanding cut, conservative reservation, cancellation, exact immutable recovery identity and offline cleanup | Background unbounded upload/read queue, invisible disk/page-cache/RSS cap claims, recursive GC/list-delete |

### Config and identity decisions to settle in ADR-0023

- Preserve the old `Config` JSON v1 parser and `ade-state-stream CONFIG --restore` behavior. Prefer `--adaptive-policy POLICY.json` behind a default-off `adaptive-checkpoint` feature, with an optional `remote-checkpoint` subfeature for S3; alternatively use a **new** strict descriptor version with explicit migration tests. The old fixed fields are the fallback/static control, not silently adaptive. Pick exactly one clean interface; document its canonical schema, bounds and precedence, and do not claim the example field names below are existing APIs.
- `PolicyV1` should distinguish: `mode` (`static_v1` / `adaptive_cost_v1`), min/max records and milliseconds, max full interval/depth, hysteresis/cooldown, confidence/sample minimums, measured-estimate fallback, durability floor, allowed placement set, remote endpoint/bucket/region/private prefix (credentials environment-only), per-request/total-byte/object/time caps, max copy count (2), immutable source replay origin, replay permission and external duplicate/conflict acceptance, risk and configured nano-USD prices, and an SLO bound. Reject unknown/duplicate fields, invalid combinations, overflow, credential-bearing serialization, unsafe URI/symlink paths and incompatible live changes before effects. **Hard constraints first; economic choice second.**
- Never include mutable policy knobs, S3 credentials/endpoint, or owner count in the Phase15 logical job fingerprint; bind new checkpoint format and policy/capability identity separately in a versioned recovery manifest. Preserve the exact logical job/schema/window/hash/source identity and distinguish `attempt/session`, generation, per-placement immutable object keys and observation epoch. A new policy on restore cannot weaken the selected generation's required copies or semantic compatibility.
- Preserve the output `output_id` and `payload_fingerprint` semantics, the current at-least-once caveats, the source-forwarded confirmed offsets/time state, and the 32-shard/stable hash format unless a separately justified new codec is explicitly versioned and fully migratable.

## 2. Non-negotiable correctness properties

### 2.1 Policy/mechanism and cadence

1. Create a **side-effect-free**, integer/fixed-point bounded decision function: observations + configured objectives + admissible actions -> chosen action, rejected candidates and reasons, input identity/version. Cost terms distinguish `checkpoint CPU/scan+write`, `network upload/GET`, `storage capacity/request`, `expected replay/restore time`, modeled failure probability and SLO penalty. Missing/stale/insufficient measurements select an explicitly declared safe static action, never guessed free remote placement. Check all math for overflow/NaN/negative/suspicious observations, reserve before I/O, cap telemetry volume and redact secrets.
2. Adapt cadence **only** after new acknowledged `forwarded_records`, at safe quiescent turns, within configured min/max progress/time/space limits; progress from filtered/closed-window-late receipts counts. Keep a maximum age/record exposure requirement only where the source actually advances, avoiding unbounded idle generations. Time in monotonic session units never becomes a checkpoint-restored processing deadline. A chosen skip cannot violate retention budget, required recovery-point exposure or capacity. Oscillation/cooldown must be testable with a fake clock.
3. Audit every material policy choice with static vs adaptive estimates, assumptions versus observations, hard-bound predicates and chosen action. Never mark an estimate as observed latency or billed cloud cost. Emit a single trace span for a cut, containing candidate, decision, publication and failure/no-success relationship.

### 2.2 Immutable full/incremental logical snapshots

1. Keep the Phase15 **quiescent matched cut** unchanged: source pause, no partition data polling, no pending receipt or close frame, all overdue windows settled, `source.verify` before snapshot and before successful pointer publication. A snapshot must atomically bind 32 logical shard contents, confirmed next offsets, per-partition time/watermarks, global frontier, late counters, job/policy identity, and one generation. Cancellation or failed sinks/diagnostics/storage poison the attempt.
2. An incremental generation contains a **full 32-entry logical shard index**; each entry resolves either to an immutable shard object written in that generation or to a **specific, hash/length-pinned prior immutable object**. Changed shard content must be newly written; unchanged shards may be referenced, never edited. Compute actual canonical shard fingerprints from the store (dirty hints alone are not authority). A physical incremental with no unchanged shard must be reported as full or fail the savings threshold; be explicit that scanning/rehashing all live shards may still cost O(state). No symbolic "incremental" if all bytes are rewritten.
3. Bound prior-reference resolution (e.g. at most four prior generations), total logical bytes, manifest size, object count, recursion, cycles and cumulative bytes *before* reads. Periodically force a self-contained full generation; previous immutable generations remain valid as long as referenced. No hardlinks (`regular` refuses `nlink>1`). Include exact byte/record hash and owner invariants. A corrupted/missing referenced ancestor rejects the selected generation rather than guessing a predecessor.
4. Version the new format (prefer a v2 manifest/pointer reader behind the new mode, with an explicit read-only v1 compatibility route). Never reinterpret a v1 manifest as v2, silently rewrite v1 `CURRENT`, or claim a mixed v1/v2 restore without tests. A fresh v2 root or deliberate explicit migration/copy is safest. Verify every referenced shard and complete logical state **before Kafka connection or output**; rehydrate independent 1/2/4 owner stores. Prove no stale/closed windows, duplicate keys, foreign shards, or offset/state mismatch.

### 2.3 Real remote placement and two-copy commit

1. Use an **actual independently running S3-compatible service** in acceptance, with conditional immutable create, bounded signed requests, exact HEAD/GET/readback hashing, read-after-PUT and reject-on-different-second-create capability tests. Explicit endpoint/bucket/region/prefix, TLS by default; dev HTTP only for literal loopback IPs. Credential source is environment, never config/trace. No redirects, ambient proxy, anonymous/instance-metadata fallback, unbounded SDK retries, multipart/background uploads, broad listing/deletion, or hidden cloud resources. Inspect ADR-0020 patterns but isolate the streaming checkpoint backend.
2. Derive every key from validated job namespace, generation UUID, shard ID/content hash and format; never trust remote LIST or attacker-chosen path components. Remote objects are immutable, exact-size/digest verified and failure/cancellation budgeted. Treat ambiguous PUT, lost response, timeout, object rewrite/delete, client uncertainty and service outage as **failure/poison**, not success or an automatic retry of a potentially completed write.
3. Publication is an explicit eligibility transaction: stage and verify **all** objects and ancestry that the chosen placement requires; write/verify immutable manifest; perform source identity/range revalidation; sync any selected local materials; **only then** atomically publish a trusted selection (`CURRENT` or an equivalent versioned selector). A multi-object remote store has no general atomic rename; remote eligibility must be bound to a precise manifest hash/generation certificate, never inferred from upload existence. A failed/ambiguous final selector is not a successful cut; any stranded objects are orphan debt, not adoptable latest state. Keep old verified selection valid at every injected crash point.
4. For **remote-only**, define the minimal local selection receipt (if any) and an operator-retained, tamper-evident exact remote generation certificate that permits *explicit* restore after local-disk loss. Prove remote-only restore from a clean local scratch using the selected certificate, with no LIST or locally retained shard data. For **local+remote**, prove both independently contain all reachable data needed to reconstruct the same logical checkpoint, including ancestor shards. A request for two copies must never fall back to one on remote error. A same-host S3 fixture is a separate process/API failure domain only, **not** evidence of independent physical rack/AZ durability.
5. No automatic destructive GC. Bound retained+staging local bytes and remote uploaded bytes/objects/request budgets, including ancestors/failed ambiguous writes and uncertain orphans; fail closed at capacity. Define **offline only**, operator-reviewed exact-key prune after proving no running writer, retention pins and one verified prior generation. Repeated cut/expiry behavior must be demonstrated; a long-lived unbounded service is deferred. Do not promote lossy Phase15 working-tier files into checkpoint credentials.

### 2.4 Replay versus restore

1. Recovery requires a **pre-output** decision from independently validated candidates: (a) a selected verified checkpoint with source offsets still retained, or (b) an explicit operator-authorized **complete logical replay from original declared source offsets** with immutable source history, no persisted state, and a new session. Partial state plus reset offsets is forbidden. The planner may recommend replay only when required original offsets and topic history are demonstrably retained and the operator has explicitly accepted external sink duplicates/**same-ID different-fingerprint conflicts**. Otherwise the safe path is checked restore or refusal. Do not silently discard a newer selected generation.
2. Compare costs (bytes, estimated state build/reread, remote GET, checkpoint read) subject to correctness, source availability, hard deadline and risk floor. A decision should change the executed recovery path, not only log a suggestion. Wrong cluster/topic/inventory, offset truncation, changed job semantics, corrupt selector, remote unavailable, and ambiguous source incarnation reject; any permitted operator override must be explicit/audited, never `--force-ignore-checksum`.
3. Restore/replay are still **at least once**. Kafka only orders within partitions; remerge after crash can alter closed-window admission and produce conflicting window payloads under the same logical ID. Test and disclose this, including `payload_fingerprint`. No exactly-once, output retractions or universal equality of replayed traces is asserted.

## 3. Implementation work packages

| WP | Deliverables | Key proof |
|---|---|---|
| WP0 — Pin + baseline | `git status`/HEAD/source inventory; full read-first map; focused Phase15/14 and default feature dependency baseline; record unrelated dirty changes | All later conclusions tied to exact source; legacy v1 remains interpretable |
| WP1 — ADR + surface | `docs/adr/0023-adaptive-checkpoint-placement-v1.md`; strict opt-in policy config, typed bounds/identity and refusal matrix; versioned recovery types | Unknown/duplicate/unsafe policy and v1 incompatibilities refuse before side effects |
| WP2 — Policy and measurement | Pure deterministic cost/constraint engine, fixed baseline, bounded sampled observation ledger, fake-clock/hysteresis tests and decision traces | Chosen costs recomputable externally; missing data fallbacks; no cost-driven unsafe selection |
| WP3 — Full/incremental format | Shard-level immutable copy-on-write v2 index + fully verified restore, ancestry pins, depth/full limits, local staging/publication, quota accounting | Same canonical state/progress as full; actual fewer *physical* bytes written when most shards unchanged; corrupted ancestor refusal |
| WP4 — Remote/two-copy transport | Feature-gated signed S3 transport, capability probe, remote key/manifest scheme, local/remote/both commit matrix and certificate-based remote-only restore | Real separate service and remote-only cold restore; no chosen incomplete copy, no ambiguous create acceptance |
| WP5 — Recovery chooser | Pre-output restore/full-replay planning, immutable source range checks, complete replay-origin semantics and conflict consent; owner redistribution retained | Actual selected path; no reset/partial merge; under-retention or corrupt selection refuses |
| WP6 — Runtime wiring | Safe policy cadence at demand-owner quiescence, no new partition polling during I/O, backpressure/cancellation control pump, bounded metrics | No checkpoint ahead of ACK; observable resource plateau; fixed Phase15 still unchanged |
| WP7 — Independent acceptance | Rust/unit/property/process tests, real Kafka+real S3 fixture and independent ledger/oracle, precise crash/poison matrix; portable source-bound evidence and negative-evidence verifier | All gates G0–G12 below pass on frozen source; semantic tamper with recomputed outer hashes still refuses |
| WP8 — Docs/closure | `docs/phase-16-adaptive-checkpoint-guide.md`, support JSON, implementation report, README/AGENTS and ADR index updates after validation; preserve historical archives | Honest `COMPLETE_BOUNDED_ADAPTIVE_STATE_V1` only on full pass, otherwise `PARTIAL` with deviations |

Suggested code touch points: `crates/ade-stream/src/state/{mod,checkpoint,store}.rs`, `runtime.rs`, `kafka.rs`, `bin/ade-state-stream.rs`, `Cargo.toml`, a **new** policy/placement module behind a default-off feature and focused tests. Factor reusable remote security helpers only if doing so demonstrably leaves Phase13 wire/behavior untouched. Tests: extend `crates/ade-stream/tests/stateful.rs` and add `adaptive_checkpoint.rs`; create `scripts/phase16/{acceptance,oracle,verify_evidence,package_evidence}.py` with Python unit tests; avoid editing accepted old evidence.

## 4. Mandatory release gates — source-bound and adversarial

| Gate | Required witness and pass criterion |
|---|---|
| **G0 Authority/baseline** | Exact HEAD and tree; PRD/architecture/ADR and phase-history reconciliation; default feature tree; unchanged fixed Phase15 and stateless Phase14 contract, supported batch/ops/remote regressions. No borrowed historical proof. |
| **G1 Config/identity** | Strict policy/CLI parsing, bounds and credential redaction; old v1 readers/readmes still work; unrelated policies cannot change job fingerprint or weaken durability; unsupported v2/v1 selection refuses. |
| **G2 Policy correctness** | Independent reference cost calculator verifies every candidate, weights, cooldown/hysteresis, sampling/fallback, overflow, risk/SLO floor; policy selects **different** actions under predeclared conditions and static control stays available. |
| **G3 Cadence/matched cuts** | Fake clock, confirmed vs seen offsets, changed filtered/late receipts, no unchanged idle cuts, pending input/window frame, diagnostic failure, no source data poll during fsync/remote I/O; exact serialized offset/state/closure consistency. |
| **G4 Full/incremental identity** | Same source trace under v2 full/incremental yields byte-identical *logical* state/progress and oracle outputs at matched checkpoints; >=1 fixture demonstrates real reduced physical writes; forced full at depth bound; cold read-through/close/restore and owner1/2/4. |
| **G5 Ancestry/format attacks** | Missing/corrupted/foreign/duplicate/cycle/depth/oversized referenced objects, forged hashes/owner counts/state, path traversal/symlinks/hardlinks, mixed v1/v2 and checksum-consistent metadata attacks all fail before output, even when outer archive hashes are regenerated. |
| **G6 Placement/copy integrity** | Real S3 API service plus actual wire/request logs; capability probe; independent local-only, remote-only and local+remote successful cuts; exact published copy closure; clean-scratch restore with remote certificate; two-copy request never degrades. |
| **G7 Remote failure matrix** | Fail/kill/timeout/connection cut/reordered responses at shard PUT, HEAD/GET, manifest, final selection and post-selection; ambiguous write, divergent second PUT, object loss/tamper, credential/capability refusal and cancellation never yield false success/adoption. Old eligible cut remains loadable. |
| **G8 Replay/restore** | Decision actually chooses both paths on explicit fixtures; complete source origin/retention and idempotence/conflict consent checked; new epoch, preserved offsets for restore, exact fresh replay provenance, lost retention and wrong-topic refusal. Independent oracle models witnessed ordered traces, not fictional global-order determinism. |
| **G9 Resource envelope** | Exhaust hot/cold/checkpoint/remote budgets, retained ancestors, orphan objects, HTTP response/request/bytes, metadata/record/frame limits and cancellation; charged peaks and native/OS/RSS exclusions explained; no unbounded control queues. |
| **G10 Crash/atomicity** | Inject before/after each immutable object publication, local fsync, manifest and pointer step; any selected state is an exact whole old or new generation; failures after selector but before success do not claim success and are operator-visible. Refuse corrupt `CURRENT`, never silently fall back. |
| **G11 Science/observability** | Matched static-full/local vs adaptive fixtures with identical source/semantic settings, complete samples and raw distributions, measured vs estimated costs/latencies/bytes, non-speedup outcomes retained, no invented pricing or performance improvement. |
| **G12 Toolchain/evidence** | `cargo fmt --all -- --check`; `cargo check --workspace --all-targets --locked`; `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`; `cargo test --workspace --all-features --locked`; Python unit tests; focused Kafka/S3 acceptance; portable archive verifier replays raw observations/oracle and source inventory. Log unavailable environment gates explicitly, never claim them passed. |

Predeclare empirical branch criteria and case counts **before** final measurement; do not change thresholds after inspecting results. CI can run unit/fake-client correctness tests, but real external-service, crash and noisy performance campaigns are local/reproducible acceptance. Freeze final source/feature graph/executable and campaign *before* signoff. Keep failed/partial candidates with exact identities; verify archival integrity and **semantic** tamper refusals on copies. Acceptance output must include action coverage for all five axes and the exact support/refusal matrix. A label of `PASS` inside an authored JSON is not independent proof.

## 5. Stop conditions and status

- **Stop as `BLOCKED` or `PARTIAL`** if only local mock S3 evidence exists; remote-only cannot restore without local shards; incremental merely rewrites full snapshots; no credible replicated selection; source replay bypasses immutable-history/sink-consent checks; any quiescent-cut violation; unsupported source/format is silently adopted; evidence cannot be verified. Never lower a safety gate to finish a phase.
- **Stop for explicit operator approval** before paid/cloud resources, production topics/buckets, credential extraction, broad delete, repository push/PR, or unrelated high-impact operations. Local disposable Kafka/S3 fixture processes and scoped temporary roots are within the intended test scope when the environment permits them; if restricted, report that requirement as unrun.
- Prefer modest implementation over extra feature breadth. A repository clean-up that is unrelated to this phase is out of scope. Never reset/overwrite unrelated user changes or modify accepted phase evidence. Tests may expose preexisting failures; document and distinguish them.
- `COMPLETE_BOUNDED_ADAPTIVE_STATE_V1` requires all G0–G12, including independent real-service proofs. This does **not** mean the full long-term PRD's placement/replication/HA roadmap is done. Otherwise disclose `PARTIAL` and exact missing gates. Preserve Phase10/12/13/15 broader debt.
- This plan is a **proposed phase design**, not a runtime authorization. The implementation handoff separately requests implementation in a fresh Codex session. No subagents are needed; run focused self-review before the independent review prompt is used by a separate session.

## 6. Read-first map and evidence provenance

Mandatory in implementation and independent review:

1. `AGENTS.md`; `docs/product/prd.md` §§7–8, 11, 16 Phase16, 17; `docs/architecture/architecture.md` §§3, 8–10, 14–15, 17–18, 22–23, 28–31.
2. `docs/plans/phase-15-implementation-plan.md`, `docs/reports/phase-15-implementation-report.md`, `docs/reports/phase-15-review-remediation-2026-10-29.md`, `docs/reports/phase-15-support.json`, `docs/phase-15-state-guide.md`, ADR-0021/0022.
3. `crates/ade-stream/src/{lib,kafka,runtime}.rs`, `src/state/{mod,store,checkpoint}.rs`, `src/bin/ade-state-stream.rs`, Cargo features, `tests/stateful.rs`, `scripts/phase15/{acceptance,oracle,verify_evidence}.py`, `scripts/tests/test_phase15_evidence.py`.
4. ADR-0014 (expected-cost evidence), ADR-0020 and Phase13 remote code/operations guide for S3 capability and remote failure semantics; Phase10 readiness/re-review, Phase12 operations and Phase14 reports/guide for boundaries; `README.md`, `.github/workflows/ci.yml`.
5. At preparation time the relevant GitHub head was `a89f1ae` (Phase15 remediation) after implementation `7f838db`. Read HEAD/diff anew. Do **not** assume raw `/private/tmp` campaign paths in the Phase15 report are accessible; the tracked evidence archives and portable verifiers are the reviewable authority. Any new proof must have its **own** source/archive identities.

Useful source URLs: [PRD](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/a89f1aeb2b7c36a23d77d702e705d6ffe4bdbfea/docs/product/prd.md), [architecture](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/a89f1aeb2b7c36a23d77d702e705d6ffe4bdbfea/docs/architecture/architecture.md), [Phase15 ADR](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/a89f1aeb2b7c36a23d77d702e705d6ffe4bdbfea/docs/adr/0022-keyed-state-and-checkpoints-v1.md), [Phase15 remediation](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/a89f1aeb2b7c36a23d77d702e705d6ffe4bdbfea/docs/reports/phase-15-review-remediation-2026-10-29.md).

## 7. Implementation reconciliation — 2026-10-29

The current user request explicitly authorizes Phase16, no subagents and one local
commit. ADR-0023 selects --adaptive-policy, separately gated S3, fresh v2 roots and
CURRENT2. Policy counters are bounded; calibration is session-local, with explicit
estimated static fallback. All32 shards are canonically scanned; incremental writes
only changed payloads, with bounded exact prior manifests. Both required closures
verify before an immutable remote certificate and atomic local selection. Remote
requests have durable conservative pre-reservations, including ambiguous orphans;
ledger loss/rollback requires a new writer namespace. Read-only restore is separately
bounded and explicitly selects a copy. It may restore a local required copy while
S3 is unavailable; new required remote cuts still fail closed. Recovery costs use
verified logical bytes/records plus configured read/request/compute/replay prices
and latency estimates, with live Kafka retention veto before assignment/output.
Source history/incarnation remains an explicit trusted operator assumption.

No v1 migration or automatic destructive GC is implemented. The independent reviewer
is a later fresh session; this session performs self-review and independent Python
producer-ledger/state/cost checks without claiming a separate reviewing agent.
Final gate status and immutable source/candidate identities belong to the report.

## 8. Independent review follow-up — 2026-10-30

The review of the accepted implementation identified that live replay retention
was checked only after cost selection and that placement changes could rewrite a
scored incremental choice to full after admission. The remediation performs
selected-checkpoint verification, then read-only eligibility checks for both
replay-origin and saved-checkpoint offsets before ranking; the final connection
rechecks its selected vector before assignment. A failed source preflight does not
assign or emit output. Snapshot planning now filters incremental actions by exact
verified ancestry/placement/floor compatibility before scoring, so full capacity
and SLO constraints are checked before staging or remote probes. A replayed
session's first published cut remains full regardless of its record count.

These changes preserve v1/default behavior and historical source-bound evidence.
The additive remediation report is authoritative for post-review status. Until a
new isolated Kafka/MinIO campaign is run on the remedied source, the historical
G0–G12 archive remains baseline evidence and independent reacceptance is partial.
