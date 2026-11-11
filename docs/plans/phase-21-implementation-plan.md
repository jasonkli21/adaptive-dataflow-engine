# Adaptive Dataflow Engine — Phase 21 Implementation Plan

## Connector and Catalog Ecosystem — bounded, native-integrated v1

**Status:** Authorized and implemented in the bounded lane. Current acceptance is recorded in the [implementation report](../reports/phase-21-implementation-report.md).

**Prepared:** 2026-11-11.

**Repository:** https://github.com/jasonkli21/adaptive-dataflow-engine

**Observed `main` baseline:** [`f49d42d8a4009cce2b22558dfb65cabcd1929c27`](https://github.com/jasonkli21/adaptive-dataflow-engine/commit/f49d42d8a4009cce2b22558dfb65cabcd1929c27), `fix: harden Phase 20 evidence verification`. Resolve actual HEAD, tree, worktree and prior changes again at implementation time; this SHA is a planning snapshot, not a license to overwrite newer work.

## 0. Authority, current truth and release boundary

**Read before implementation, in this order:** `AGENTS.md`; `docs/product/prd.md` (especially §§1, 5–9, 14–17 and Phase 21); `docs/architecture/architecture.md` (especially §§3, 5, 8–11, 17–23, 26, 29–31); this plan; applicable ADRs (0018–0027, especially 0021–0026); current implementation reports, support matrices and operating guides; then code and tests. PRD owns outcomes, architecture owns invariants, this plan constrains scope, ADRs record reconciled technical decisions, implementation reports describe evidence rather than conferring authorization. Do not silently amend PRD or old ADRs.

**PRD Phase 21 priority set:** object stores/Parquet, Iceberg, Delta, Kafka, JDBC/Postgres, Arrow/Flight and a generic source/sink SDK. **Architecture §21:** connector source capabilities include schema, split discovery, boundedness, snapshot identity, pushdown, locality, offset/checkpoint semantics; sink capabilities include schema, partitioning, idempotence, prepare/commit/abort, exactly-once integration. Connectors must remain extensions, **not scheduler special cases**. Architecture INV-1–10 remain mandatory.

### 0.1 Verified baseline and unresolved dependencies

- `ade-frontend::Catalog` (`crates/ade-frontend/src/catalog.rs`) is a **local manifest-only**, schema-admitting front end. It verifies listed Parquet files, binds only existing `input`/`ade_left`/`ade_right`-style `PlanV1` sources, and currently limits simultaneous registered relations and source schema. `ade-physical::PlanV1` is a strict, version-1, acyclic physical contract; `Source` and `SourceFile` use immutable hashes and hard row/byte/file caps. **Do not repurpose these fields or quietly widen these contracts.**
- `ade-exec` owns the native local Parquet operator runtime and memory/spill contracts. SQL/DataFrame bind into ADE-owned physical plans; DataFusion is a binder, never an alternate execution backend.
- `ade-stream::kafka::KafkaSource` already owns manual Kafka assignment, identity, offset/progress and bounded intake. `ade-stream::unified::Admission` lowers admitted Kafka semantics to a private `NativeStreamSpecV1`. Phase 21 must **reuse** rather than fork this engine, checkpoint or offset implementation.
- `ade-stream::adaptive::remote` already uses `object_store` for a **specialized S3 checkpoint transport**. It is **not** a general query object-store connector or evidence of general S3 read/write support. Preserve its existing credential, security, reservation, and recovery contracts.
- `ade-ops` contains bounded batch recipe, frozen-file, durable-output and source-identity logic; `ade-incremental` and `ade-iterate` have separate opt-in local lineages. Do not merge their selector formats with connector catalogs.
- **Phase 18:** `PARTIAL_PHASE18_UNIFIED_ADMISSION_V1`. The 2026-11-10 native stream lowering remediation fixes source ownership in code but explicitly leaves matched real-broker G14, worker-backed G15, diagnostic quota G16 and broader current-source reacceptance unresolved (`docs/reports/phase-18-g14-lowering-2026-11-10.md`). Do not claim those gates passed because a Phase 21 Kafka adapter works.
- **Phase 19:** bounded maintained dataflows and independent review remediation exist, with source-bound limits and formats.
- **Phase 20:** `PENDING_INDEPENDENT_REACCEPTANCE`, per `docs/reports/phase-20-review-remediation-2026-11-11.md` and `AGENTS.md`. Its native algorithmic route is a **closed finite-local BFS/components template**, not arbitrary cyclic execution. Fresh Phase 20 G0–G12/G14–G15 campaigns are not established by the latest remediation report. Phase 21 cannot convert that into completed general cyclic execution.
- The existing Phase 20 docs say **no Phase 21 implementation was authorized by Phase 20**. Preparing this plan is not that authorization. Submission of a future implementation prompt authorizes work in its stated scope; it does not automatically waive pending predecessor acceptance. Record a separately explicit decision if relying on outstanding Phase 18/20 capabilities. The safe default is to build Phase 21's **isolated bounded/local adapter lane**, mark dependent integration gates `BLOCKED`/`NOT_RUN`, and declare `PARTIAL` until prerequisites or an explicit, narrowly scoped waiver are established.

### 0.2 What constitutes meaningful Phase 21 v1

Ship an **actually executable** opt-in connector/catalog route, not merely enums, SDK skeletons, mocked service clients, or metadata inspection. A connector must bind a versioned source identity/capability contract and supply data to **existing ADE execution** or an already accepted ADE streaming runtime. The v1 mechanism may materialize bounded, authenticated external inputs into **private, verified local Parquet staging**, then use unchanged `Catalog`/`PlanV1`/`ade-exec`. This is a deliberate baseline mechanism with explicit extra I/O and storage cost, **not** direct remote pushdown, zero-copy access, durable cache, automatic adaptation or a new runtime.

The ecosystem breadth must be visible **without dishonestly claiming full format coverage**. Implement a coherent bounded subset for each of the six PRD priority families and state exact exclusions. If a live end-to-end path or necessary semantic verifier for any mandatory family cannot be delivered, **do not label Phase 21 complete**. A partial finish with solid core adapters is preferable to superficial full coverage.

## 1. Executable support contract and scope matrix

The following is a proposed minimum **release target**. A narrowly justified alternative is possible after a design spike and ADR reconciliation; it must meet the same PRD Phase 21 outcome and stronger-or-equivalent tests. Every claim requires a real capability probe and end-to-end proof, not just parse acceptance.

| Family | Bounded v1 positive route | Required honest refusal / deferral |
|---|---|---|
| Local + S3-compatible Parquet/object store | Explicit, pinned object/file inventory; bounded Get/read and local staging; source content + schema + row identity verified; source executes through ADE's native bounded local path. Reuse supported S3 client patterns without changing checkpoint transport. | Arbitrary bucket discovery; unpinned mutable objects; unsupported URI schemes; TLS bypass except explicitly scoped loopback fixtures; unsafe redirects; unsupported compression/types; direct-distributed remote scan; automatic object-store writes except sink contract below. |
| Iceberg | Read-only **one pinned table snapshot**, resolve bounded manifest list/data-file descriptors and validate format/version, schema, partition spec, content and snapshot ancestry; only a precisely supported no-delete, no-evolution, primitive Parquet subset may enter staging and execute natively. | Unknown format versions, branches/tags not resolved to a stable snapshot, equality/position deletes, row-level updates, unsupported transforms/partitions, schema changes, encryption, unknown metadata fields that affect semantics. No write/compaction/merge. |
| Delta Lake | Read-only **one pinned table version**, verify bounded transaction-log continuity, active-file/tombstone semantics and table protocol/features; admit a simple no-deletion-vector, non-evolved primitive Parquet subset; stage active data files and execute natively. | Log gaps, inconsistent removes/adds, unsupported checkpoints, deletion vectors, column mapping, change feed, advanced protocol/features, time travel without a pinned immutable version. No writer/merges. |
| Kafka | One explicitly admitted source using `ade-stream`'s existing native Kafka identity/offset/progress path; catalog/SDK exposes **accurate** unboundedness and replay/checkpoint capabilities, and can bind existing supported finite/history/live modes where independently accepted. | Fake EOF from poll idle; silently changing consumer groups/starts/cuts; unapproved streaming joins; exactly-once-to-external-store; a competing Kafka reader or alternative cursor/checkpoint implementation. Outstanding Phase 18 service parity remains separately gated. |
| PostgreSQL (covers PRD's JDBC/Postgres priority at the Postgres-protocol side) | Parameter-safe, allowlisted **read-only** one-table/specified-column snapshot using a real PostgreSQL client and `REPEATABLE READ READ ONLY` transaction kept alive through bounded extraction; verified Arrow/Parquet staging and native ADE batch query. | Generic JDBC/Java driver API, arbitrary SQL or credential-bearing connection strings, CDC/logical replication, cross-table atomicity, unbounded tables, unstable no-transaction pagination, row types/nullability outside allowlist, writes and external transactions. |
| Arrow Flight | Real **DoGet** read-only adapter to a pinned ticket or explicit non-replayable one-shot source; bounded batches, strict schema, cancellation/deadline, TLS identity; stage verified rows and run native ADE batch query. | Flight SQL (Phase 22), arbitrary DoPut/DoExchange, server-side user code, unpinned ticket assumed replayable, unsupported bearer auth, overlarge streams, schema drift. |
| Generic source/sink SDK and catalog | Versioned stable metadata and capability types; typed source discovery/bind/open/read/checkpoint/replay semantics; typed sink prepare/write/commit/abort/idempotence capabilities; one real local transactional publication sink **and preferably S3 conditional publication** with honest guarantees. Catalog resolves names to pinned snapshots and immutable run bindings. | Promise of universally available exactly-once; cross-system atomic commit; dynamic plugin loading, multi-tenant ACL/quotas (Phase 23), arbitrary UDFs (Phase 22), background discovery, implicit network access at SQL binding. |

**Stage constraints:** Keep old source ceilings unless a *separate versioned* plan is justified. Examples of existing `PlanV1` constraints: `Type` is `U64`, `I64`, `F64`, `U32`, `Utf8` or `TimestampMs`, with non-null source fields; `Source` limits file inventory and bytes/rows; `Catalog::register_manifest` has fixed relation naming and joins. Do not coerce unsupported types or nulls; adapt external names to allowed native binding names **without changing logical external identity**, or explicitly introduce a new versioned binding contract. SDK snapshot tokens are separate from physical lineage/checkpoint selectors.

## 2. Design invariants to freeze before coding

### 2.1 Explicit semantic identity and capabilities

Provide an opt-in `ConnectorSpecV1`/`CatalogEntryV1`/`ResolvedSnapshotV1` family (names illustrative). It must have strict versioning/unknown-field denial, bounded serialized size, canonical semantic digest, and source/sink **capability claims verified by actual adapters**. Define at least: connector kind/version; stable logical dataset name; schema and deterministic field mapping; boundedness; content/snapshot identity; split list and locality; replay eligibility and expiration; pushdown support and what was *actually* applied; progress/offset semantics; error classes; authentication *reference* (not secret contents); lifecycle; sink delivery/idempotence/commit protocol. Unavailable capabilities are `unsupported`, not a permissive default.

Distinguish **semantic identity** (remote table/snapshot, immutable files and schema, chosen columns, provenance) from **operational identity** (endpoint, deployment, credentials env reference, local stage directory, retry attempt). Connection locations are not a substitute for a snapshot. Store complete content digests for staged bytes. An S3 ETag may be useful for conditional validation but is **not a universal content hash**. Bind the plan to an exact resolved source; changes between resolve, stage and query **must refuse** or create an explicitly new source identity, never silently rebind. Don't treat mutable metadata service responses as durable truth without an explicit signed/versioned/verified snapshot contract.

### 2.2 Isolation, resource and correctness boundaries

- No new execution kernel just because a connector has custom semantics. The connector reads and canonicalizes data; ADE runs the query. Any genuine streaming connector goes through the existing `ade-stream` progress/checkpoint path or refuses.
- All remote reads: allowlist schemes/hosts/ports as warranted, TLS verification by default, redirect/SSRF defenses, env-only secret handling, request/time/byte budgets, cancellation, bounded in-flight work, bounded backoff/retries and clear failure on budget exhaustion. Do not log credentials or headers. A loopback-only insecure test option must be explicit and guarded.
- Local staging: private owned directory, exclusive writer fencing, no traversal/symlink escape, caps for compressed and decoded sizes, no unchecked allocation, limit files/rows/bytes, force immutable layout and BLAKE3 digests, and complete source verification before an external result becomes visible. Keep output path and attempt separate from logical source identity. Clean up only owned, uncommitted objects; no user data deletion.
- Snapshot consistency: Iceberg/Delta format protocol requirements must be enforced, not inferred from happy-path fixture names. Only advertise safe read semantics for understood table features; if deletes/evolution or transaction sequence matters, either implement fully and prove it, or reject **before any misleading result**.
- PostgreSQL: snapshot held for entire extraction; bound names/columns (quote identifiers safely; do not concatenate arbitrary predicates); TLS verify by default; avoid large `OFFSET` scans that can duplicate/skip under mutation; handle timeout, transaction loss, cancellation, row conversion overflow and NULL without silent loss.
- Flight: pinned ticket provenance or `replayable=false`; validate metadata/schema for **every** batch, body/batch caps and stream termination; reject unstable server identity and unsupported transport/auth. A successful DoGet alone does not imply recoverability.
- Sinks: express `prepare`, idempotency key, staging and `commit/abort` *capabilities*. Local publish must be atomic under its declared FS model; object-store publish only claims idempotent immutable-object/manifest visibility where conditional operations are *really* enforced. Crash after external commit but before receipt is a core test. No spurious exactly-once or global two-phase commit claim.
- Preserve INV-1–10: policy never relaxes connector correctness; memory and I/O are bounded and observed; recovery does not assume local staging durable; optimizer-disabled/static baseline always available.

### 2.3 Catalog ownership and adapter architecture

Suggested ownership (confirm via dependency-graph spike and ADR):

```text
opt-in catalog/connector crate or tightly scoped modules
  ├─ contract: ConnectorSpecV1, capabilities, stable identities, errors
  ├─ registry: explicit named entries + snapshot resolution + pinned run binding
  ├─ sources: local/S3 Parquet, Iceberg, Delta, Postgres, Flight
  ├─ kafka: facade to existing ade-stream Kafka native admission
  └─ sinks: local publication; optional S3 conditional publication
              |
              v
   bounded, verified Parquet staging + manifest
              |
              v
  existing ade-frontend::Catalog -> ade-physical::PlanV1
              |
              v
         existing ade-exec
```

Do **not** create a second frontend catalog that independently parses SQL, or let the source/sink SDK depend on `ade-distributed` coordinator internals. `ade-logical` and `ade-physical` remain transport/credential agnostic. If the existing manifest binder is too restrictive, prefer a public explicit adapter that emits an **existing valid v1 manifest**, or a **new separately versioned external binding descriptor** that lowers into that manifest, rather than a backwards-incompatible modification to `PlanV1`. Avoid making `ade-stream` a required default dependency for batch or vice versa; use optional integration features.

**Required design spike:** demonstrate one real external source → verified stage → existing ADE SQL/DataFrame native execution and one sink prepare/commit/reopen lifecycle; record actual module/dependency edges and why this is not a synthetic demo. If using a different mechanism (e.g. direct Arrow batches) prove all existing native verification, boundedness, replay and version constraints are maintained. Add a new accepted `docs/adr/0028-connector-catalog-v1.md` (if number still free).

### 2.4 Source/sink capability semantics

A concrete capability matrix should include at least:

| Property | Meaning / negative proof |
|---|---|
| Bounded / unbounded | Known termination vs no implied end-of-stream; do not infer Kafka EOF from temporary idleness. |
| Replay / snapshot | Exact input can be read again under the same identity and retention guarantee; forbid `replayable=true` for a mutable Postgres read or arbitrary Flight ticket after transaction closes unless materialized stage itself is pinned and verified. |
| Split discovery | Frozen complete bounded list, no implicit recursive listing; distinguish physical split identity from task attempt. |
| Schema evolution | Exact compatibility policy and null semantics; unknown primitive/metadata type rejected. |
| Projection/predicate | Advertise only actually supported pushdowns; no optimizer choosing an unsupported pushdown; compare pushed and non-pushed results. |
| Sink commit | Local atomic or conditionally committed object visibility with demonstrated post-crash behavior; state impossible guarantees as unsupported. |
| Checkpoint/offset | Connector-owned **descriptors**, native `ade-stream` owns actual Kafka progress; no fake cross-source exactly-once frontier. |

## 3. Work packages, in dependency order

| WP | Concrete deliverable | Exit proof |
|---|---|---|
| **WP0 — source/dependency audit and predeclared evidence** | Freeze main SHA/tree, worktree status, environment, Cargo.lock and source inventory; enumerate current compatibility and open Phase18/20 gates; record scope/authorization and candidate baseline. Freeze criteria, test inventory, fixture identities and oracle independence **before measurements**. | G0; no unrecorded predecessor waiver. |
| **WP1 — ADR, SDK contract, registry** | Capability grammar, versioned source/sink metadata, canonical hashing, identity vs operational knobs, checked limits, structured refusals, named catalog registration and immutable resolve/bind lifecycle. Add a minimal actual third-party/mock-free adapter outside the executor for SDK extensibility. | Contract roundtrips, unknown-field/version refusal, deterministic digests, capability falsification and no-core-dependency cycles. |
| **WP2 — verified Parquet/object-store transport** | Immutable local and S3-compatible object references, bounded range/whole reads, content verification, private stage and canonical manifest, native local query and bounded output. | Actual MinIO/S3-compatible fixture + changed-object/ETag/timeout/redirect/credential negatives, independent Parquet oracle. |
| **WP3 — Iceberg snapshot** | Parse/validate precise supported table metadata, Avro manifest inventory and data-file status; pin snapshot and stage each admitted data file; apply or refuse deletes/evolution. | Real independently generated Iceberg table + snapshot switching, delete/partition/schema/protocol negatives, independent row oracle. |
| **WP4 — Delta version** | Parse/validate transaction-log actions to one fixed version, active file membership, protocol and metadata; pin and stage; unsupported checkpoints/features explicitly refused. | Real independently generated Delta table + add/remove/version gap/deletion vector/protocol negatives and row oracle. |
| **WP5 — Kafka adapter** | Catalog capability facade and strict bind to existing `ade-stream` native lowering; no cursor fork; opt-in only. | Real broker offset/progress/restart/refusal cases; compatibility with prior Kafka/Phase17/18 behavior; record which historical service debt is **not** resolved. |
| **WP6 — PostgreSQL snapshot** | Live read-only Repeatable Read Postgres export, bounded fetch/decode, strict type/schema mapping, verified stage and native query. | Real Postgres mutation-during-extract, transaction failure, NULL/type/TLS/admission/resource negatives; independent SQL count/bag oracle. |
| **WP7 — Arrow Flight** | Live Flight DoGet source, pinned or one-shot capability, bounds/schema validation, verified staging and native query. | Real Flight service, mutated ticket/schema/oversize/stall/cancel/TLS negative, independent Arrow rows oracle. |
| **WP8 — sinks and integration** | Native-result → staged local output + durable manifest selection; optional S3 conditional publish with truthful idempotency. SQL/Rust catalog entrypoint and small CLI or example to demonstrate each real adapter. | Crash-after-prepare, after-commit-before-ack, concurrent writer, retry, partial output and reopen verification. |
| **WP9 — testing, benchmarks and evidence** | Real fixture services and static local baseline, independent oracle calculations, frozen acceptance, attack/tamper/offline verification, measured cost of stage-vs-local. | G1–G16, exact final source/executable/fixtures; no fake no-service PASS. |
| **WP10 — docs and closure** | ADR, operator guide, per-connector support matrix, implementation report, reconciled README/AGENTS, tested examples and reproducible archive. | User can distinguish complete/partial/unavailable capability and recreate acceptance. |

Suggested dependency features are **default off** and scoped per connector (`connector-s3`, `connector-iceberg`, `connector-delta`, `connector-postgres`, `connector-flight`, `connector-kafka`, etc.); names are illustrative and should follow actual crate conventions. No network service is started during compile, parsing, `EXPLAIN`, or logical binding unless explicitly requested at the connector-resolve boundary.

## 4. Release gates and adversarial inventory

Freeze machine-readable criteria and expected case outcomes before implementation measurements. These are **mandatory minimum** gates; independent review can discover additional necessary gates. Report `PASS`/`FAIL`/`PARTIAL`/`NOT_RUN`/`BLOCKED` per gate with direct artifact links and exact source identity. **No all-PASS from an offline archive alone.**

| Gate | Proof required |
|---|---|
| **G0 provenance & prerequisite disposition** | Exact baseline/final SHA/tree, build identity, clean/dirty state, toolchains, OS/FS/CPU/RAM, services/versions, source/fixture/oracle digests, prior Phase18/20 status and explicit Phase21-only dependency disposition. |
| **G1 SDK semantics & ABI** | Public extension compiles without core internals, strict version/schema/errors/capabilities, source/sink lifecycle, no undocumented feature flags, unknown-field/overflow tests. |
| **G2 catalog & snapshot authority** | Frozen `resolve → bind → execute` identity; swapping metadata, credentials, endpoints, tables, snapshots or files cannot select wrong content; deterministic digests, pre/postflight and no hidden access. |
| **G3 local/S3 Parquet** | Actual native ADE query against pinned local and object-store data, independently checked rows, tampering/truncation/TOCTOU/partial download and protocol/security refusal. |
| **G4 Iceberg** | Real read of a pinned compatible snapshot, exact active-file/row membership, independent reference oracle; documented deletion/schema/partition/protocol negatives. |
| **G5 Delta** | Real read of pinned compatible version, log action ordering/removes/active-files, independent oracle; deleted, checkpointed, protocol-upgraded and corrupted-log negatives. |
| **G6 Kafka reuse** | Live Kafka connector binds existing native progress/identity path, valid history/assigned/live outcomes and fixed native source correctness; no second offset authority. Unrun Phase18-dependent proof stays blocked. |
| **G7 PostgreSQL** | Live consistent read-only transaction under concurrent writer and failure; no SQL injection, credentials leak, unbounded fetch or type/null misinterpretation; native query parity. |
| **G8 Arrow Flight** | Live DoGet, independent output parity, stable-pinned vs one-shot replay truth, TLS/cancellation/oversized/late batch/schema drift fault handling. |
| **G9 source bounds & replay** | Hard request, byte, row, file, decode, memory and time caps, retry storm/fault/refusal, resource accounting and deterministic stable-snapshot replay, or declared non-replayability. |
| **G10 sink atomicity and idempotence** | Local prepare/commit/abort, crash points, duplicate/conflicting commit, re-open, injected ENOSPC, and S3 conditional-object semantics if claimed; no external exactly-once claim without protocol. |
| **G11 integration & optimizer fidelity** | SQL/DataFrame/CLI executes **ADE-owned** physical runtime, does not substitute DataFusion/native connector compute, pushdown disabled vs enabled parity, source/sink provenance in explain/diagnostics. |
| **G12 security, supply chain & protocol evolution** | SSRF/redirect/TLS/secret redaction, malicious paths and metadata, adversarial authentication, unsafe object keys, malicious server behavior, version/feature refusal; vetted dependencies and feature gates. |
| **G13 compatibility & regression** | Old `PlanV1`, native catalog, Kafka configs/offsets, Phase17/18 state/sinks, Phase19/20 formats, old CLI defaults and historical acceptance archives untouched; default-off build and supported feature combinations. |
| **G14 independent oracle & scientific measurement** | Independent Iceberg/Delta/Postgres/Flight/Kafka reference data; stage throughput/bytes/memory/latency measured against honest local-static baseline across repeated process-isolated samples; record costs/regressions, no unsupported speedup. |
| **G15 provenance/evidence & tamper** | Source/executable/fixture/service pin, ordered all-case inventory, signed/hash-bound results and logs, pristine offline replay, semantic rehashed archive attacks that must be rejected, exact failure classifications and no missing cases; cross-environment verifier portability if claimed. |
| **G16 release/toolchain/docs** | Final-source fmt, check, Clippy, tests and smoke; real services for claimed gates; reproducible commands/fixtures; additive reports/support JSON; claims match evidence. |

### 4.1 Required negative/fault set (not exhaustive)

1. Mutate Iceberg snapshot pointer or manifest after discovery; manifest list truncation; unknown delete file; partition spec mismatch; same-name file replaced with different bytes; schema-ID reuse; unsupported v2 row-level feature; bogus object URI.
2. Delta log gap/out-of-order commit, checkpoint without support, duplicate/corrupt JSON action, remove then readd, malicious path, deletion vector, unsupported table feature/reader version, pinned version disappearing.
3. S3 `200` body with incorrect digest, stale/mismatched ETag or version ID, partial/range mismatch, redirect to forbidden host, slowloris/timeout, retry after partial response, access-denied/expired credential, no conditional writes.
4. Postgres concurrent update/delete/insert during snapshot; connection loss mid-export; cancellation and timeout; malicious table/column identifier; type overflow, NaN, NULL, timezone, unsupported array; misreported count.
5. Flight ticket replay with different bytes, schema changes midstream, oversized metadata/batch, stalled/cancelled stream, retry after partial output, invalid status, unverified TLS identity.
6. Kafka idle != EOF, gap/partition expansion, retained-history loss, offset reset, topic identity mismatch, source semantic mutation, retry/restart and checkpoint cut drift.
7. Sink kill at preflight, staged-file flush, manifest flush, visibility selection, post-commit/pre-ack; concurrent writers, repeated idempotency key with changed payload, abandoned stages and ENOSPC; verify exactly one visible terminal artifact **within claimed local/S3 assumptions**.
8. Stage traversal/symlink races, object name `..`/absolute path, malformed archive, corrupt Parquet footer, compressed bomb, too many files/rows, row widths, partial-batch panic, provenance log secret injection, malformed/extra JSON keys, unsafe default credential sources.

## 5. Evidence and scientific protocol

1. **Freeze before measuring:** `scripts/phase21/criteria.json` and a complete positive/negative case manifest; fixture locks for real externally created Iceberg/Delta/Parquet/Postgres/Flight/Kafka inputs; independent oracles and baseline definitions. State in advance any timing comparison predicates rather than tuning them after a run.
2. **Compile once for final run:** record HEAD, tree, full source digest inventory (including lockfile and feature flags), executable hashes, Rust/Python/library and service images/versions, OS, FS, CPU/RAM, hardware/network, credentials *presence* only, not values. Capture worktree dirtiness and changes after measurement.
3. **Real services:** use owned disposable MinIO/S3-compatible, PostgreSQL, Kafka and Flight fixtures with deterministic data; declare the service isolation model and disable production endpoints. Metadata fixture generators may use independently maintained reference clients, not ADE's reader code. All ephemeral roots and ports belong to the test harness; no destructive external operations.
4. **End-to-end correctness:** compare canonical bags/ordered results and source identities with independently computed oracles, and verify both returned data and persistent selected sinks/checkpoints as applicable. Check failure results produce no false complete output; injected failures are reached, not just configured.
5. **Negative campaign:** process-isolated cases, injected read/fetch fault points, restart and crash cuts; enforce every frozen case's request digest, exit disposition, diagnostic class, stdout/stderr policy, filesystem and remote effects. Store raw service/protocol traces with secrets removed.
6. **Offline verifier must be genuinely adversarial:** reconstruct semantic input from raw evidence where possible, enforce exact case inventory and signed hashes/source identity, check independent expected rows and source-to-result binding, and reject fully rehashed but semantically false/relabelled archives. Do not simply trust runner-supplied booleans. Include pristine and adversarial test corpus; replay with a separate environment if portable claim made.
7. **Performance:** paired repeated process-isolated native staged-vs-existing local manifest workloads; report medians, dispersion, stage bytes/read amplification, memory peaks, round-trips and end-to-end latency separately from query compute. Do not require or claim a connector stage to outperform local files. Mark any hard quantitative criterion and sample size in advance.
8. **Finish on final source:** if any code, fixture, acceptance parser, criteria or documentation material to a claim changes, rerun the affected gate and record a new source-matched archive; preserve previous failures/attempts. Never rewrite Phase 17–20 archives or fabricate current-source acceptance from old logs.

## 6. Exit deliverables and release rule

New files/paths (final names may be adjusted in ADR):

- Implementation in existing crates or a **real**, non-empty `ade-connectors`/`ade-catalog` ownership crate; connector-specific features; at least one external extension example and runnable native query/sink example.
- `docs/adr/0028-connector-catalog-v1.md` (if ADR index allows); `docs/phase-21-connectors-guide.md`; `docs/plans/phase-21-implementation-plan.md` reconciled to implementation; `docs/reports/phase-21-implementation-report.md` and `docs/reports/phase-21-support.json` with **per-adapter capability/status**.
- `scripts/phase21/` acceptance, independently specified fixture/oracle, frozen criteria/case contracts, packaging, strict offline verifier and tamper tests; append-only `docs/evidence/phase21/` or documented owned archive; README/AGENTS updates accurately describing **current** source.
- Exact reproducer with versions, commands, hashes, live service prerequisites and test results. Old contracts and evidence untouched.

**Complete** only as `COMPLETE_BOUNDED_CONNECTOR_CATALOG_V1` when all mandatory G0–G16 gates and all six family v1 positives are current-source, service-backed where relevant, and independently oracular; no silent dependency waiver or false beyond-v1 guarantee. Use `PARTIAL_PHASE21_CONNECTOR_CATALOG_V1`, `BLOCKED`, `NOT_RUN` or `FAIL` for any missing mandatory proof. State explicitly that *generic JDBC, full Iceberg/Delta semantics, arbitrary Flight/Flight SQL, cross-source transactions, CDC, remote distributed scans, automatic connector cost optimization, production credentials/ACL, and multi-tenant or platform-wide exactly-once* are outside this bounded release unless separately implemented and proven.

## 7. Implementation choices to challenge before committing

The v1 staging approach is **not required by fiat**. Before implementation, evaluate a direct `RecordBatch` adapter, a versioned physical connector source node, and bounded private Parquet staging against: proof of source identity, compatibility with strict `PlanV1`, replay semantics, failure atomicity, resource caps, ability to share the existing native execution kernel, and end-to-end cost. If another approach is strictly better and defensible, explain it in ADR and reconcile this plan **without shrinking Phase 21's PRD outcome**. Similarly challenge whether the proposed Iceberg/Delta subsets are safe, whether Postgres/Flight can meaningfully claim replay, and whether the sink API reflects actual rather than hypothetical atomicity. Do not let the initial design fence off an architectural flaw.

## 8. Implementation reconciliation

The 2026-11-11 request authorizes Phase 21 in this session. No Phase 18 or
Phase 20 gate is waived. The batch route has no dependency on either phase.
Kafka uses only the existing stateless ADE engine and the sealed native
semantic spec. Phase 21's independent stateless Kafka proof does not transfer
to Phase 18's stateful, distributed worker or diagnostic-quota campaigns.

ADR-0028 records the mechanism choice. A separate default-off integration
binds a verified source through the existing schema-only SQL frontend and
local PlanV1. Iceberg requires field-ID verification and canonical Parquet
normalization because Iceberg's Arrow annotations differ from the exact
legacy native schema. Delta consumes contiguous version-zero JSON history.
PostgreSQL and Flight supply captured stages with explicit one-shot identity.
The local sink selects a single immutable IPC artifact with a key- and
plan-bound envelope using a synced atomic link. SeaweedFS supplies the
disposable S3-compatible service because the selected MinIO binary download
returned HTTP 410.

The acceptance and support documents carry case and gate dispositions.
The original criteria in this plan remain the release bar.
