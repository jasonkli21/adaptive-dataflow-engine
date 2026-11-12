# Adaptive Dataflow Engine — Phase 22 Implementation Plan

## Multi-Language and UDF Platform — bounded, native-integrated v1

**Document status:** Proposed implementation plan; not an implementation or acceptance claim.

**Prepared:** 2026-11-11

**Repository:** https://github.com/jasonkli21/adaptive-dataflow-engine

**Inspected `main` commit:** [`31b627fa0c1f1a0da7822acb9f3dfaa839729cf3`](https://github.com/jasonkli21/adaptive-dataflow-engine/commit/31b627fa0c1f1a0da7822acb9f3dfaa839729cf3) (`Address Phase 21 independent review`), tree `e6d721c1150899e51d733e0fdd9365e9bca5d211`.

**Release label, if proven:** `COMPLETE_BOUNDED_MULTILANGUAGE_UDF_PLATFORM_V1`. This is a proposed, explicitly *bounded* label, not a claim of production-wide or full PRD feature coverage.

> The implementer must re-read `main` and record the actual working-tree base before editing; the pinned commit above describes this plan's inspected source, not a license to overwrite intervening work.

## 0. Authority and baseline reconciliation

Read these sources, in this order, before design or coding:

1. [`AGENTS.md`](../../AGENTS.md), particularly source precedence, phase discipline, verification, and the last Phase 21 remediation entry.
2. [`docs/product/prd.md`](../product/prd.md), especially §§4–9, 14–17, **Phase 22**, platform success metrics, development rules, and scope boundaries. Phase 22 specifies **Python client, Arrow batch UDF protocol, SQL gateway/Flight SQL, JVM client, optional WASM isolation, and language-neutral plans**.
3. [`docs/architecture/architecture.md`](../architecture/architecture.md), especially §§3, 5, 7–8, 17, 20–23, 26–29 and INV-1–INV-10. Frontends may express semantics but must not own ADE execution; worker/runtime stays frontend-independent; unbounded memory and replay assumptions are prohibited.
4. [`docs/adr/0018-physical-ir-and-frontend-v1.md`](../adr/0018-physical-ir-and-frontend-v1.md), [`0025`](../adr/0025-unified-bounded-unbounded-planning-v1.md), [`0028`](../adr/0028-connector-catalog-v1.md), and relevant execution, reliability, memory, and connector ADRs.
5. Current [`docs/phase-21-connectors-guide.md`](../phase-21-connectors-guide.md), [`docs/reports/phase-21-support.json`](../reports/phase-21-support.json), [`docs/reports/phase-21-independent-reacceptance-2026-11-11.md`](../reports/phase-21-independent-reacceptance-2026-11-11.md), [`docs/reports/phase-20-review-remediation-2026-11-11.md`](../reports/phase-20-review-remediation-2026-11-11.md), and [`docs/reports/phase-18-g14-lowering-2026-11-10.md`](../reports/phase-18-g14-lowering-2026-11-10.md).
6. Source and tests, not just reports. Inspect `crates/ade-physical`, `ade-frontend`, `ade-exec`, `ade-distributed`, `ade-logical`, `ade-connectors`, CI, and the current benchmark/evidence conventions.

**Observed source reality at the pinned commit:**

- `ade-physical::PlanV1` is a versioned, `serde`-serializable, frontend-independent, **bounded** physical plan with strict `from_json`, type/schema inference, structural/target admission, canonical digest, a 64-node cap, and restricted source inventories. `Target::Local` supports one `input` relation and unary operations. There is **no existing batch UDF operator**. `MAX_PLAN_BYTES` is 256 KiB; sources and outputs have separate existing caps.
- `ade-frontend` has a schema-only catalog, DataFusion-assisted SQL translation into native `PlanV1`, typed Rust `DataFrame`, and `Catalog::execute[_until]`; SQL is a restricted subset, not a general SQL engine. `ade-query` is an executable, **not** a Flight SQL server.
- `ade-exec` owns native local operators, memory accounting, source validation and output. Distributed, unified-stream, incremental, and cyclic execution have distinct, narrower contracts; do not silently extend them.
- `ade-connectors` has default-off bounded connectors, an inbound read-only Arrow Flight `DoGet` **client**, staging and a local Arrow IPC sink. **Inbound Flight SQL serving is not present.** Flight `DoGet` is not interchangeable with the Flight SQL RPC semantics.
- Phase 21's original bounded implementation is recorded as complete, but the **latest remediation status** is `PARTIAL_REMEDIATION_PENDING_REACCEPTANCE` (`phase-21-support.json`): current-source service/candidate-7 evidence and green Linux CI have not been established. Phase 20 remains `PENDING_INDEPENDENT_REACCEPTANCE`; Phase 18 remains `PARTIAL_PHASE18_UNIFIED_ADMISSION_V1`. These historical or predecessor claims must not be silently promoted. The `main` CI run for this commit was in progress when inspected; no green result is inferred.
- Workspace uses Rust edition 2024, declared MSRV 1.94, Rust Arrow 59.3.1, and the existing Cargo lockfile. New Python/JVM dependencies, protocol versions, CI services and native-build requirements need independent compatibility/lock verification.

### 0.1 Phase boundary and resolution of inherited partials

Phase 22 may build on **independently verified, existing bounded native local/SQL paths** without claiming all Phase 18/20/21 acceptance gates have passed. Any demonstration depending on a Phase 21 connector must either (a) have matched current-source service/connector proof, clearly segregated from its inherited Phase 21 release status, or (b) remain explicitly qualified. Do not make a new UDF result a substitute for a missing real-broker streaming, cyclic, distributed, transaction, or connector reacceptance campaign. Do not edit immutable predecessor archives or retrofit their source identities. A newly green Phase 22 gate cannot reclassify an unrelated predecessor gate.

### 0.2 Meaningful Phase 22 minimum

Deliver an **end-to-end bounded query** whose request originates in each supported client language, whose SQL or language-neutral plan binds only authorized ADE sources, whose execution uses ADE's native local pipeline, and whose Arrow-IPC batch UDF is genuinely evaluated *within the ADE-controlled execution lifecycle* (rather than silently postprocessing an already returned result). Demonstrate the same semantic query without a UDF via SQL and language-neutral plan APIs. Show cross-language Arrow result parity and exact failure semantics. Both a Python and a JVM client must run against a **real, standards-conformant Flight SQL service** or an explicitly documented native ADE service API where the Flight SQL portion is independently exercised; client scaffolding alone does not satisfy the phase.

## 1. Supported v1 scope, explicit exclusions

| Surface | Mandatory bounded-v1 implementation | Explicitly out of scope / cannot be implied |
|---|---|---|
| Python | Installable/versioned package or minimal SDK with typed, declarative query/plan submission; SQL execution; Arrow results; registered UDF reference; deadlines/cancellation, diagnostic receipt and typed failures; meaningful Python examples and tests | Full PySpark/Pandas emulation; client-side query execution presented as ADE; unconstrained Python code submission |
| Arrow batch UDF | Versioned language-neutral input/output and function-binding contract; one actual **trusted, registered Python** Arrow batch transform invoked by ADE-controlled execution; schema/row/ordering limits; subprocess lifecycle; admission, cancellation, bounded IPC and output verification | Python execution in a privileged scheduler; untrusted-code sandbox security; arbitrary scalar/aggregate/stateful UDFs; implicit distributed or exactly-once UDF replay |
| SQL gateway / Flight SQL | Default-off, read-only **Arrow Flight SQL** service over loopback; real `GetFlightInfo`/`DoGet` statement flow and required metadata/negotiation used by verified clients; ADE frontend compilation and native execution; bounded results and opaque tickets | General ANSI SQL, DDL/DML, anonymous external network access, Flight `DoPut` writes, generic remote catalog exploration, multi-tenant scheduling |
| JVM | Reproducible Java 21 (or evidence-backed compatible LTS) client/library/example using standard Arrow Flight SQL Java and/or versioned ADE plan protocol; complete compile-and-run interop test | Separate JVM execution engine; JNI dependency on ADE Rust internals; Spark/Flink Java API compatibility |
| Language-neutral plans | Reuse **existing `PlanV1` JSON** wherever its semantics suffice, plus a versioned, strictly admitted UDF/function reference if needed; canonical identity, schema, source binding, feature negotiation, strict unknown-version rejection, independent Python/Java invocation | A parallel language-specific planner, arbitrary client-supplied physical storage policy, indiscriminate new IR version, or client-controlled source identity |
| WASM | **Optional** design/evaluation only; implement solely if mature runtime, isolation guarantees and resource costs are measured and explicitly accepted | No WASM claim needed for bounded Phase 22 exit; never label plain subprocess execution a sandbox |
| Execution targets | Local bounded native plan, with existing supported unary operators before/after UDF when admitted; explicit refusals for unsupported plans | Distributed UDF, streaming state, arbitrary cyclic UDF, savepoint/exactly-once external effects, preemption/fair scheduling or Phase 23 quotas |

A genuine **post-UDF native operator** is part of the mandatory proof: for example `scan → batch UDF → native filter/project/aggregate` on a supported physical type/schema, with an independent oracle. If a direct physical UDF node turns out unsuitable, the alternative must still demonstrate semantically equivalent **ADE-owned** execution, bounded replay/materialization and downstream native operations, documented in a superseding ADR/plan reconciliation; a client `map_batches()` on returned results is insufficient.

## 2. Architectural contracts to settle before broad implementation

### 2.1 Plan and semantic identity

1. **Do not replace `PlanV1` casually.** Audit existing serialized field ordering, `PlanV1::digest`, inferred schemas/properties, `Source` pin checks, `validate_target`, and physical operator match exhaustiveness. Reuse the existing IR for plain queries. If UDF operators require an IR extension, make the compatibility decision explicit: either an additive versioned node with robust rejection by older executors or a separately versioned enclosing *query/extension contract*. Avoid ambiguity in old `version == 1` decoders and never weaken existing v1 admissions.
2. A function reference binds **name, ABI version, exact trusted artifact digest, input/output schemas, cardinality/order contract, null handling, and replay/side-effect classification**. Operational executable path, environment, endpoints, credentials and temp roots do **not** become user-trusted semantic pins. Resolve a function only from an operator-owned registry; never execute a client-supplied shell command, Python source string or arbitrary file path.
3. Server-side catalog authorization binds client table names to an **operator-authorized, exact** `Catalog` source snapshot. Reject client-supplied forged `Source` inventories, changed fingerprints, altered schemas, unsafe paths and unsupported tables *before* effects. The client can express logical query/plan intent; the server revalidates and constructs or validates target-specific native plans.
4. Cross-language plan exchange must specify field names/types, JSON encoding, numeric precision (especially u64/i64 > 2^53), UTF-8, nullability, timestamps, ordering/bag semantics, version/feature negotiation, canonical digest ownership, maximum payload size, and unknown/duplicate field behavior. Do not require Python/Java to recompute Rust's BLAKE3 digest unless a canonical byte encoding is formally frozen; have the server return the authoritative digest.
5. Preserve inherited deterministic/replayable property rules and source/attempt distinction. A trusted Python UDF is **not automatically replay-safe** merely because its name or artifact digest is pinned. Default external UDF effects to **nonreplayable** unless a more restrictive, evidence-backed deterministic contract is adopted. Refuse invalid recovery/transaction targets before side effects.

### 2.2 Arrow IPC UDF ABI and lifecycle

- Define and document a versioned **framed Arrow IPC** exchange contract: request/response headers, batch IDs, row counts, schemas, dictionaries/compression policy, maximum serialized/decoded bytes, bounded buffer count, end-of-stream/error status, deadline and cancellation propagation, and correlation to plan/function identities. Prefer Arrow's existing IPC representation and authoritative readers/writers; avoid ad hoc serialized row encodings.
- One function invocation takes one Arrow `RecordBatch` of v1-supported, explicitly documented non-null primitive/string/timestamp columns and returns an independently validated batch with the declared schema. For the first accepted profile, require **row count preservation and stable row order** (use stable `row_id` fixture proof); reject unexpected drops/duplication/reordering. If allowing some controlled expansion later, reconcile limits and proof first.
- The operator, not the Python client, starts an **operator-installed/allowlisted** worker executable/package, pins its digest and owns child process group, IPC streams, temp roots, lifecycle, timeout and cancellation. A worker crash, hang, malformed frame, early EOF, partial output or output of incorrect type must fail closed. Avoid `shell=True`, dynamic code from request payloads, ambient unbounded module search paths and inherited secrets. Clearly document where OS-level isolation is unavailable; process separation alone is not secure sandboxing.
- Account separately for native buffer reservations, Arrow IPC encoded/decoded buffers, worker process RSS, worker concurrency, output materialization/spool size, service ticket retention and filesystem footprint. Adopt explicit finite caps and measured peak evidence; use a contained platform-specific enforcement mechanism where available. If a cap is only estimated or best-effort, **label it as such** instead of claiming an absolute bound.
- Native downstream operators must consume **verified** UDF output, not a client-visible postprocessing substitute. Decide whether to use a direct physical operator with a pluggable UDF host trait, or a verified ADE-owned bounded stage/rebind boundary. Preserve original source and function identities, cancellation, row alignment and plan diagnostics. Reject unsupported distributed/unbounded combinations before launch.
- A failed UDF cannot leak a supposedly successful result or commit sink. If the transport streams partial results during execution, explicitly define whether partial rows may be observed and enforce the corresponding contract; for the proposed **all-or-nothing bounded v1**, verify and materialize the full result within caps **before** publishing a Flight SQL ticket's data.

### 2.3 Flight SQL gateway correctness and security

- Implement **actual Flight SQL** methods and protobuf message flow, not a custom service merely named Flight SQL. At minimum, support advertised read-only statement queries (`CommandStatementQuery`/`GetFlightInfo` and ticket `DoGet`), schemas, bounded `GetSqlInfo`/client metadata necessary for a tested Python and JVM Flight SQL client, cancellation/expiry behavior and well-typed `UNIMPLEMENTED` for explicitly unsupported methods. Record and test exact client versions; only advertise capabilities implemented.
- Bind by default only to an explicitly configured **numeric loopback** interface. Reject an unsafe non-loopback bind unless a separately reviewed TLS/authentication deployment path is truly implemented and tested. Do not expose a local trust model as internet-safe. Pin or verify TLS/endpoint identity if an opt-in remote mode is added. Separate request identity from bearer/auth/session secret.
- SQL compilation uses `ade-frontend` and the same schema-only catalog/native plan APIs; no DataFusion runtime execution. Limit SQL length, parse/planning time, concurrent requests, per-query memory/output size, resulting Arrow stream bytes, server spool size, ticket count/TTL, and total held tickets. Fair queueing, tenant RBAC and preemption remain Phase 23.
- Use an opaque, high-entropy, server-scoped ticket bound to a validated result/plan, session and lifetime, **not** a path, SQL string or executable command. A repeated read must return identical materialized bytes or a typed expired/not-replayable error; it must never silently execute a one-shot connector/UDF again. Prevent cross-session ticket reuse, response/schema drift and replay after expiry. No temp directory traversal/symlink race or directory replacement via user inputs.
- Define disconnect, cancellation and server-crash cleanup semantics. Uncommitted work, retained ticket files and child processes have finite ownership and cleanup; partial/corrupt results do not become discoverable. Require denial/cleanup behavior under process crash and ENOSPC where feasible.

### 2.4 Client design and interoperability

- Keep **one server authority** for plan admission and execution. Python and JVM construct requests from the same language-neutral contract, or exercise a standards-compliant Flight SQL path, rather than baking different semantics into each SDK. Prefer standard Flight SQL libraries; introduce only a thin ADE-specific plan/UDF submission API where the standard protocol lacks it.
- Python: typed errors, explicit per-request deadlines, Arrow result tables/batches, support for *registered function references* (not uploaded Python code), documented byte/numeric fidelity, and no silent in-process execution. A small typed DataFrame builder is encouraged if it actually uses the shared plan contract; it need not clone the full Rust API.
- JVM: compile a real client, run real SQL requests and decode Arrow results; demonstrate plan compatibility or a documented versioned plan endpoint, not merely a copied JSON string. Verify allocator/vector lifetimes, resource close, temporal/numeric semantics and diagnostics.
- Required parity corpus includes SQL without UDF in Rust/Python/JVM, one registered UDF invoked by Python, an equivalent JVM or language-neutral plan request, a post-UDF native operation, empty result, nontrivial UTF-8, u64/i64 boundaries, floating-point edge cases if supported, exact schemas, and negative admission parity. Independently compute source/result expectations from fixtures; don't use ADE output as the sole oracle.

## 3. Work packages in dependency order

| WP | Deliverables and key design checks | Evidence / exit |
|---|---|---|
| **WP0 — inventory and preflight** | Freeze branch/base SHA/tree, worktree, package/toolchain versions, `Cargo.lock`, CI status, schema/IR/connector ownership and predecessor qualification table. Identify test services and truly available Python, Arrow, JDK, Maven/Gradle versions. | Checked-in baseline matrix, readiness log, no assertion that unfinished predecessor gates passed. |
| **WP1 — protocol/ADR and independent fixture lock** | New ADR(s) covering versioned plan/UDF ABI, execution ownership, Flight SQL ticket semantics, trust boundary, rejected alternatives; proposed support matrix and exact positive/negative case inventory. Independently authored fixture datasets, UDFs and oracle files. | Protocol document + self-consistent examples; frozen criteria and fixture identities **before** measuring. |
| **WP2 — typed UDF integration** | Minimal registered `MapBatches`-class semantic/physical integration or a demonstrably equivalent ADE-owned stage; bounded worker launcher/IPC framing, registration, type/schema checks, cancellation, no double execution, output verification, truthful replay flags. Update exhaustive operator/target validation. | Native pre/post-UDF end-to-end tests, verified independent oracle, crash/hang/corrupt/over-limit refusals, native metrics. |
| **WP3 — bounded gateway** | Feature-gated service crate or narrow executable/module with correct Flight SQL RPCs; server-authorized catalog, native plan execution, result/ticket lifecycle, typed diagnostics and strict read-only surface. | Standard-client Flight SQL call path; no use of DataFusion execution; negative transport/security tests. |
| **WP4 — Python SDK** | Installable thin package, version negotiation, SQL/plan/UDF reference APIs, typed exceptions and Arrow results; examples. Pin Python/PyArrow versions in reproducible test setup. | Real Python-to-service interop and failure tests; no Python-side fake UDF/SQL evaluation. |
| **WP5 — JVM client** | Small Java 21 library/example using real Flight SQL client; optional shared plan submission wrapper; pinned dependency/build files, automated lifecycle and resource cleanup. | Real JDK compile-and-run against server; Python/JVM result and error parity. |
| **WP6 — hardening** | Admission limits, function provenance, race-safe staging/ticket ownership, cancellation, bounded concurrency/resources, diagnostic redaction, incompatible-version/target refusals. | Fault/mutation campaign covering UDF subprocess, clients, server crash/restart and input sources. |
| **WP7 — verification and experiment** | `scripts/phase22/` reproducible acceptance, immutable candidate bundle, independent verifier, semantic mutation tests and repeatable resource/latency comparison vs existing native path. | Source- and binary-matched gate report, independent oracle and tamper refusal; distinguish unavailable services from passes. |
| **WP8 — release reconciliation** | Operator/user guides, Python/JVM examples, support/limitation JSON, ADR links, implementation report, CI jobs, revised README/AGENTS with accurate statuses. | Documentation, code and evidence agree. No predecessor claim laundering. |

**Module guidance:** Likely additions are a small `ade-gateway` and/or `ade-udf` crate only if each owns production behavior, plus Python/JVM client directories. Prefer targeted additions to `ade-physical`/`ade-exec`/`ade-frontend` through extension points; do not introduce placeholder crates, a second engine, global coordinator dependency or a custom Arrow wire format without a demonstrated need. Core runtime must compile without optional Python, JVM and Flight SQL dependencies.

## 4. Hard correctness and security boundaries

1. **Physical runtime independence:** no Python/JVM classes or gateway RPC objects embedded in the physical plan/executor interface. The UDF host is a narrow language-neutral boundary; policy stays separate from mechanism.
2. **Pre-effects admission:** reject unsupported physical targets, schema/function mismatch, bad manifests, unregistered functions, untrusted artifacts, malformed plans and invalid query versions before launching UDFs, opening sink writes, or publishing tickets.
3. **Identity and replay:** plan and function identity survive client round trips; retries must not duplicate nonreplayable or side-effectful UDF invocations. No implicit exactly-once claim for external effects.
4. **Privacy/credentials:** neither plan dumps nor Flight SQL error strings/telemetry contain credentials, full environment, secrets, arbitrary source paths or UDF stderr unless safely redacted. Diagnostic IDs map to retained server-side evidence.
5. **Bounded resources:** hard limits or explicitly qualified measurements for plan/request, Arrow frames, decoded batch, memory, subprocesses, concurrent clients, disk, ticket count, spool and time. No hidden infinite queues.
6. **Protocol hygiene:** strict schema/order/metadata/endianness handling, Arrow IPC validation, typed error codes, deadlines, feature negotiation and deterministic serialization; reject unsupported compression/dictionaries instead of guessing.
7. **Security trust statement:** Phase 22 registered Python UDFs are trusted operator-installed code. Process isolation is a crash/fault containment mechanism, **not** protection from hostile arbitrary code absent a proven sandbox. Network clients cannot register or upload executable code in bounded v1.
8. **No feature borrowing:** streaming, distributed worker execution, connectors, durable state, transactional output and generalized cyclic work remain at their own qualified support levels. An unsupported route must refuse before a client-visible result.

## 5. Release gates and adversarial proof obligations

The labels below are **proposed Phase 22 gates**, not current results. All gates G0–G15 are mandatory for the bounded-v1 completion claim; optional O1 is not.

| Gate | Required independent proof |
|---|---|
| **G0 — provenance** | Freeze baseline/final commit and tree, dirty state, source/fixture/criteria/executable hashes, toolchain and dependency locks, OS/CPU/RAM/FS, services/versions, independently retained receipts, exact commands/stdout/stderr/exit and truthful predecessor matrix. |
| **G1 — plan contract** | Cross-language language-neutral plan schema/version negotiation, canonical authoritative digest, strict malformed/duplicate/unknown-field/version rejection, source binding and capability/target refusal before side effects. |
| **G2 — native integration** | Real ADE-native execution, including `scan → UDF → native operator` (or defensibly equivalent sanctioned stage) plus independent data oracle. No DataFusion/SDK postexecution fallback. |
| **G3 — UDF ABI** | Exact Arrow IPC framing, schema/null/row/order semantics, across multiple batches and 0/1/max-size cases; detect encoded/decoded size, dictionary/compression and wrong-type issues. |
| **G4 — function identity** | Registered trusted artifact and ABI digests, schema/ref spoof, stale registry, artifact replacement, changed function binary and unregistered shell path rejection; no arbitrary code upload. |
| **G5 — Python** | Reproducible package import and real Python client SQL/plan/UDF request, Arrow schema/data parity, cancellation and typed error handling. |
| **G6 — JVM** | Reproducible Java compile/run and true client query/plan/result parity; correct resource cleanup, numeric/timestamp fidelity. |
| **G7 — Flight SQL interop** | Real gRPC/Flight SQL protocol exercised by **both** standard Python/JVM clients for statements, metadata, GetFlightInfo/DoGet and explicitly unsupported methods; no mislabeled custom API. |
| **G8 — ticket and result** | Server-authorized binding, full-result prepublication, opaque ticket/session/TTL, exact repeat read without reexecution, cross-client theft/stale/forged ticket rejection and crash cleanup. |
| **G9 — cancellation/faults** | Kill/hang/EOF/partial-write/malformed Arrow/crash/disconnect/timeout/ENOSPC worker, gateway and client scenarios; no unverified result or escaped child. |
| **G10 — resource bounds** | Admission and observed peaks for native + UDF process + IPC + gateway + spool + tickets + concurrency; every unmeasured/best-effort dimension explicitly qualified, no silent success. |
| **G11 — security and read-only** | Loopback-only default; unsafe remote bind, unapproved paths, symlinks, forged sources, SQL unsupported statements, ticket/session confusion, diagnostic secret leakage and malicious payload refusals. |
| **G12 — backward compatibility** | Existing `PlanV1` JSON/digest semantics and local/distributed/streaming target admission preserved or explicitly versioned with migration tests; existing no-default/all-feature workspace tests and phase-specific immutability. |
| **G13 — independent correctness** | Externally computed oracle for supported SQL/UDF operations, cross-language result/bag/ordering parity and negative-result parity; verified independent fixture lock, not self-generated expectations. |
| **G14 — evidence verifier** | Strict detached offline verifier for every expected case, binding source and binary in **current-source** mode, independently pinned fixtures/criteria/receipts and negative semantic witnesses; reject fully rehashed tampered cases, omitted cases and forged statuses. Historical replay mode cannot claim current source. |
| **G15 — CI/release** | Rust format/check/Clippy/tests/default-off and feature-on, Python tests, Java build and real interop on supported Linux runner, existing smoke/benchmark regressions, accurate support report and release doc. A local permission failure or unavailable service is **BLOCKED/NOT_RUN**, not PASS. |
| **O1 — optional WASM spike** | If attempted: measured runtime/host isolation against Python subprocess, supported ABI and memory/time, explicit security review; otherwise `DEFERRED_OPTIONAL`. |

### 5.1 Required negative/fault inventory (minimum, expand during audit)

- Protocol: malformed/oversized JSON, unknown plan version, invalid `PlanV1` node, forged inferred properties, source/table escape, mismatched source bytes/schema, noncanonical and duplicate field encodings, numeric overflow, invalid UTF-8.
- UDF: wrong function digest/ABI, unregistered function, executable replaced between bind/invocation, schema drift, null injection, wrong row count/order, stale/duplicate batch sequence, malformed Arrow header/body, oversized encoded/decoded data, bad dictionary or compression, worker crash/kill/hang, stderr flood, attempted client-supplied code/path, nonreplayable retry.
- Gateway: unsupported SQL DDL/DML, incomplete Flight SQL protocol metadata, wrong ticket owner, expired/forged ticket, query canceled during planning or UDF, multiple DoGet readers, disconnected client, replay after process restart, result emitted before postflight validation, failed atomic publication, disk full, too many concurrent requests/tickets.
- Interop: Python/JVM versions that violate declared support, large signed/unsigned integers, NaN/-0.0 if supported, UTF-8, zero batches, schema field names/nullability, timestamps, row/column ordering, error-code parity.
- Security/operability: accidental non-loopback bind, path traversal and symlink/root replacement, response and diagnostics secret leakage, failure to reap process descendants, cross-request data or memory retention, unbounded spools/RSS, preexisting native query behavior regressions.

## 6. Evidence, measurement, and release policy

- **Freeze before running:** check in `scripts/phase22/criteria.json`, a complete case/negative-contract manifest, fixture hashes, independent expected outputs and declared budgets. Record all later criteria revisions with new candidate identifiers; never rewrite failed attempts or adjust thresholds after seeing outcomes and call that a pass.
- **Candidate lifecycle:** each experiment uses clean, isolated work roots and exact final-source binary/package identities. Keep command-line, exit status, stderr/stdout, Flight requests, worker health, content digests and resulting artifacts; bundle the evidence deterministically. Distinguish local smoke from live service-backed proof.
- **Independent verifier:** prefer standard-library checks with separately pinned dependencies where needed. Verify executable/source attribution, package identity, exact case set and expected semantics; do not let the candidate supply its own expected verdicts. Create **fully rehashed** adversarial archives that try to relabel failures, omit cases, mutate outputs/requests/schema/timing/identity or claim unsupported paths. The verifier must reject each for the intended reason.
- **Measurement:** run a predeclared repeated local-native baseline, native-with-UDF, gateway-SQL and Python/JVM paths on the *same data/host*; record wall/CPU, p50/p95, byte counts, Arrow copy/stage bytes, native and worker RSS, heap/allocator and disk peak, ticket retained bytes and retry count. Include cold/warm distinction and functional correctness guard. Explain overhead; **do not claim performance wins without matched controlled baselines**.
- **Existing regressions:** `cargo fmt --all -- --check`; `cargo check --workspace --all-targets --locked`; `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`; `cargo test --workspace --all-features --locked`; no-default-feature checks for affected crates; `bash scripts/smoke.sh`; `python3 -m unittest discover -s scripts/tests -p 'test_*.py'`; MSRV check; new Python/JVM integration and negative tests. Do not silently skip native Kafka build prerequisites. A changed `Cargo.lock` is expected only if actually required and reproducibly resolved.
- **Status discipline:** publish an append-only `docs/reports/phase-22-implementation-report.md`, `docs/reports/phase-22-support.json`, `docs/phase-22-multilanguage-udf-guide.md` and `docs/evidence/phase22/` receipts; update README and AGENTS without falsifying Phase 18/20/21 state. If a mandatory gate is partial/not run, report a qualified/partial Phase 22 status rather than the completion label.

## 7. Acceptance deliverables

1. Stable, documented versioned plan/UDF ABI and accepted ADR(s), with documented alternative evaluation and exact support/exclusion matrix.
2. Rust ADE-native UDF execution path with operator-owned, bounded Arrow IPC bridge; one full end-to-end native downstream query and independent oracle.
3. A real bounded read-only Flight SQL server, strict ticket/source semantics, and reproducible server executable/deployment instructions.
4. Working Python and Java clients, build/install instructions and meaningful requests against the **same live gateway**; Python registered-UDF request plus cross-language plan compatibility.
5. Automatic positive, negative, mutation/fault, resource and regression tests; frozen predeclared criteria; source+binary matched acceptance archive and independently strict verifier.
6. CI and reproducible lockfiles, gate/support report, operator guide and accurate release status. Immutable prior evidence remains untouched.

**Exit:** The release label `COMPLETE_BOUNDED_MULTILANGUAGE_UDF_PLATFORM_V1` is permitted only for the precise surface above and only when all mandatory G0–G15 proofs pass on the final source. Otherwise, produce a useful implemented partial with an explicit blocking ledger and no unearned acceptance.

## 8. Design questions that must remain challengeable

The plan is a proposal, **not** a substitute for independent engineering judgment. Before coding, compare: (A) direct pluggable physical UDF operator, (B) validated ADE-owned staged transform with native rebind, and (C) a separately versioned extension plan. Evaluate safety, replay semantics, memory, compatibility, operator placement and testability. Evaluate eager bounded ticket materialization versus streaming under strong partial-result semantics. Evaluate an external Flight SQL service versus reuse of an existing process endpoint, and use real library interoperability as the deciding test. Reconcile any different design **before** changing scope or public contracts; document why it better meets the PRD and architecture. The independent reviewer must be able to find gaps in **this very plan**, including missing or misleading feature coverage.

## 9. Execution note

Creating this plan **does not authorize Phase 22 implementation**. The separate implementation prompt is the actionable authorization when submitted to a coding session. It requires the implementer to assess actual branch/source state and finish the implementation with honest, source-matched evidence rather than merely generating another plan.

## 10. Reconciled implementation boundary

[ADR-0029](../adr/0029-bounded-flight-sql-and-arrow-udf-v1.md) records the
adopted Phase 22 interface. The language-neutral request accepts typed
filter and project operations over the server's registered `input`.
It lowers to existing PlanV1. A separate runtime-only, schema-preserving
adapter installs the registered transform immediately after the local scan.
Raw client source inventories and execution policies are outside the request
schema. The outer query digest covers the native plan plus function identity.
Only the explicit request envelope selects the function. Its replay class is
nonreplayable.

The server builds a complete set of Arrow Flight frames in bounded memory,
then publishes one expiring session-bound ticket. Consequently there is no
result spool or filesystem commit operation. The filesystem-full case uses
a disposable filesystem as TMPDIR, reaches actual ENOSPC, and verifies that
no spill, spool or partial result dependency was introduced. Post-admission
worker failures and native overflow leave no published result. Native
spill-disabled execution defers temporary directory creation until actual
spill, which remains disabled for the gateway.

Python uses Apache PyArrow Flight and standard Flight SQL protobuf commands.
Java uses Apache Arrow FlightSqlClient on OpenJDK 17. Both use the same custom
Flight command type for the bounded plan envelope. Native runtime code
does not depend on either SDK or Flight. WASM remains optional and deferred.
The original G0–G15 criteria continue to govern the release label.
