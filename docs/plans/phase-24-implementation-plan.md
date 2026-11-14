# Adaptive Dataflow Engine — Phase 24 Implementation Plan
## Specialized Libraries / Extension Ecosystem — bounded, independently consumable Rust v1

**Prepared:** 2026-11-14 (America/Los_Angeles)

**Repository:** https://github.com/jasonkli21/adaptive-dataflow-engine

**Inspected `main` HEAD:** [`95e1153b2b75fbe8d9e2445e96b5f9081d293cfa`](https://github.com/jasonkli21/adaptive-dataflow-engine/commit/95e1153b2b75fbe8d9e2445e96b5f9081d293cfa), after Phase 23 independent-remediation commit

**Document status:** Implemented bounded local candidate. See §10 and the [current support matrix](../reports/phase-24-support.json) for the release qualification. Predecessor support matrices retain their existing qualifications.

**Proposed bounded exit name:** `COMPLETE_BOUNDED_EXTENSION_SDK_AND_SPECIALIZED_LIBRARIES_V1` **only if all mandatory final-source gates pass**; otherwise an accurately qualified `PARTIAL_PHASE24_...`.

## 0. Authority and intent

1. Read `AGENTS.md` and the [PRD](../product/prd.md), especially product principles (§§5–9), platform breadth (§14), benchmark strategy (§15), **Phase 24 in §16**, dependency spine (§17), core-versus-ecosystem boundary (§22), and development rules (§24). Phase 24 names **possible external libraries**: graph, ML data prep / iterative algorithms, batch inference, CEP, geospatial, vector/embedding transforms. These are a *possible menu*, not a mandate to fake six families in one phase.
2. Read [architecture](../architecture/architecture.md) §§3, 5–9, 17–23, 25–29 and **INV-1–10** (§31), especially frontend/runtime separation, explicit replayability, bounded memory, provenance, disable-able/observable policy, and the instruction not to scaffold future crates without real ownership. ADRs refine illustrative architecture, not the PRD's intent.
3. Read current [ADR index](../adr/README.md), [ADR-0018](../adr/0018-physical-ir-and-frontend-v1.md), [ADR-0025](../adr/0025-unified-bounded-unbounded-planning-v1.md), [ADR-0027](../adr/0027-cyclic-dataflow-v1.md), [ADR-0028](../adr/0028-connector-catalog-v1.md), [ADR-0029](../adr/0029-bounded-flight-sql-and-arrow-udf-v1.md), [ADR-0030](../adr/0030-bounded-local-multitenant-resource-manager-v1.md), and associated guides, plans, reports, actual code and current support JSON. Later, code-specific ADRs may narrow/adjust *mechanisms*, but must not quietly erase the goal of **externally owned, practical, reusable specialization**.
4. The original thesis remains adaptive, recoverable execution. A specialized library must not become a parallel engine that never meaningfully invokes ADE, and the core must not grow domain-specific graph/ML/CEP algorithms merely to satisfy the demo.

### 0.1 Verified starting state and important caveats

| Surface (at inspected HEAD) | Observed contract / limitation | Consequence for Phase 24 |
| --- | --- | --- |
| `crates/ade-exec/src/lib.rs`, `crates/ade-exec/src/physical.rs` | Public **schema-preserving**, synchronous `ade_exec::BatchMap` and `LocalPlan::MapBatches`; `ade_exec::physical::execute_with_map` inserts the map after verified PlanV1 scan and requires a native downstream local operator. `BatchSource` exists; `LocalPlan` is explicitly **not a stable general physical IR**. | Reuse a **small, versioned adapter**; enforce stronger extension-level schema, row, bounds, cancellation and artifact checks. Do not promise arbitrary schema-changing UDFs, dynamic plugins or distributed extension execution. |
| `crates/ade-physical/src/lib.rs` and `ade-frontend` | Acyclic versioned `PlanV1` with finite type/operator set and source binding; local execution admits Arrow scalars. | Existing PlanV1 wire/serialized semantics stay byte-compatible. Adapter compiles into supported native plans; unsupported ops are refused, not relabeled. |
| `crates/ade-iterate/src/lib.rs`, `engine.rs`, `docs/phase-20-iterative-guide.md` | Opt-in public `ade_iterate::operate`, `Request`, `CyclicPlanV1`, BFS and weak components on **finite, immutable** local Parquet graphs. Native cyclic executor is a **closed template**, not a general composable cyclic operator API. | Graph library may **compose** with exactly those supported algorithms. It must add a meaningful new user-facing graph computation through verified native ADE execution; no generic graph/ML recursion claims. |
| `crates/ade-gateway` / Phase 22 | Operator-registered trusted Python Arrow function, local loopback Flight SQL, one query. UDF identity constraints and unresolved security/reacceptance caveats. | Do **not** make the new SDK depend on Python, Java, Flight transport or untrusted arbitrary code upload. Phase 22 reuse is optional and separately qualified. |
| `crates/ade-connectors`, Phase 21 | Pinned local/S3 Parquet, bounded captures and catalog SDK, with one-shot/nonreplayable distinctions. | Start with immutable **local** Parquet. Expansion to connectors requires independent source-binding and capture claims; no new guarantee inherited from Phase 21. |
| `crates/ade-resource` / Phase 23 | Same-host, operator-controlled tenant scheduler over separate worker cohorts; current support status is `PARTIAL_PHASE23_INDEPENDENT_REVIEW_REMEDIATION_PENDING_REACCEPTANCE`. | Phase 24 libraries are **not** automatically tenant-isolated, resource-manager jobs, or cross-host deployable. Do not couple SDK to this service. |
| Support reports | Phase 18 `PARTIAL_PHASE18_UNIFIED_ADMISSION_V1`; Phase 20 `PENDING_INDEPENDENT_REACCEPTANCE`; Phase 21 and Phase 22 `PARTIAL_REMEDIATION_PENDING_REACCEPTANCE`; Phase 23 reacceptance pending. | A narrow, separately waived local proof may proceed if recorded explicitly; **no predecessor gate turns green by inheritance**. Claims depending on unaccepted behavior remain separately qualified. |
| `Cargo.toml`, CI | Rust workspace edition 2024, `rust-version = 1.94`, Arrow/Parquet `59.3.1`, opt-in feature patterns and source-bound evidence discipline. | Keep MSRV, feature matrix, clean defaults and CI intact. Freeze exact source and dependency identities for experiments. |

**Prerequisite decision:** Phase 24 implementation may be attempted as a **separately bounded local research slice** despite predecessor qualification **only after** its implementation session records the explicit waiver, identifies which exact Phase 20 / Phase 22 properties it relies on, and proves those properties on the **Phase 24 final source**. A waiver is not independent reacceptance. If such a carve-out cannot be justified, stop and remediate the blocking predecessor instead of claiming completion.

## 1. Recommended scope, challengeable by design

### 1.1 Primary outcome

Establish a small, genuinely reusable, **compile-time Rust extension contract** and prove it with **two independently maintained specialized libraries and a third consumer outside the ADE workspace**:

1. **Graph analytics (`ade-ext-graph`)**: a *new composed graph workload*, recommended `component_sizes_v1`: invoke the real Phase 20 weak-components path on a pinned graph; consume only a **verified complete terminal result**; convert its `(vertex, component_min_vertex)` to a bounded, private staged `key: UInt64, value_i64: Int64=1` Parquet stream; aggregate with **ADE's real local `HashAggregate`**; output canonical `(component_min_vertex, size)` with one row per component, including isolated vertices. Add a useful `reachable_within_hops_v1` adapter as a second example **only if** its correctness and source/attempt identities are clean; it cannot substitute for the composed aggregate proof. Every output row must derive from ADE-executed operators, not from a standalone Python graph program. Preserve the graph runtime's restrictive template and checkpoint semantics.
2. **Vector/feature preparation and bounded batch scoring (`ade-ext-vector`)**: implement a fixed-dimensional, schema-preserving Arrow `BatchMap` through `ade_exec::physical::execute_with_map`; at least one **real feature preparation** operator (`l2_normalize_v1` or an explicitly justified bounded clipping/scaling transform) and one **real model-scoring** operator (`affine_score_v1`, fixed ordered weights + bias, writing a predeclared `score: Float64` field) with an ADE-native projection/filter downstream. The operator artifact (parameters / finite model) is checked and content-pinned; every float, result and batch must satisfy explicit limits. This demonstrates vector/embedding transforms, ML prep and *narrow* batch inference without claiming neural networks, generic ONNX, distributed GPU, general model serving or training.
3. **External consumer test**: build and run a **separate, standalone Cargo package** (not a workspace member and not depending on private internal modules) that implements a third trusted `BatchMap` using the publicly documented extension SDK, declares its own identity/capabilities, and runs a nontrivial native query. Prove it survives a clean build using only the documented dependencies and feature flags; this is the ecosystem test, not another in-repo reexport.

Choose the third-party example to be behaviorally independent of both shipped families (e.g. bounded signed-value calibration using existing Int64 columns) so that its success isn't a duplicate of the vector library's implementation.

**Phase 24 is not satisfied by:** only installing Python packages; a hand-authored JSON registry that never executes; a hardcoded graph CLI alias; computing vector results externally and pasting them into a native output; or adding a bespoke new core operator without an external extension API. An implementation with a narrower or different proof may be superior, but must explain and demonstrate equal-or-better real extensibility in a new ADR and re-frozen acceptance criteria.

### 1.2 Scope boundary for the other possible library families

| PRD possible family | v1 decision | Why / follow-up threshold |
| --- | --- | --- |
| Graph | **Required:** meaningful compositional graph library on native Phase 20 plus native local operators. | Both graph semantics and ADE ownership are independently verifiable. |
| ML data preparation / iterative | **Required:** genuine finite feature transform; **defer general iterative training**. | Phase 20 only supports BFS and component labels, not arbitrary differentiable loop bodies. |
| Batch inference | **Required, narrow:** deterministic fixed-dimensional affine scoring with a pinned model. | No arbitrary function upload, model formats, network calls or accelerator runtime. |
| Vector/embedding | **Required:** finite dimension, schema-preserving per-row kernel on Arrow batches. | No vector ANN index, similarity search database, new physical types, GPU or null semantics. |
| CEP | **Explicitly deferred** as a release requirement; design an SDK capability refusal for stateful/event-time patterns. | Real CEP needs explicit event time, watermark/late event, state/checkpoint and output semantics; one toy stream matcher must not imply stream processing. A finite CEP follow-up requires its own contract and evidence. |
| Geospatial | **Explicitly deferred** as a release requirement; document typed coordinate/CRS and numeric concerns. | No validated spatial types/CRS library or topological predicates exist in current native IR; a trivial bounding-box predicate is not sufficient evidence of a geospatial platform. |

The review must reconsider whether **two independent domain libraries plus an external consumer** adequately demonstrate the PRD's extension-ecosystem milestone. If no, elevate a *real* third family and its gates before implementation; do not quietly rebrand a sample as a library.

### 1.3 Exact support envelope

- **Trusted, statically linked Rust** extensions under one operator-controlled local Unix process; external means *separately owned crate*, **not untrusted runtime sandboxing**. No plugin discovery over a network, WASM sandbox, dynamic `.so` loading, cross-tenant running code or delegated secrets. A crashing/panicking, malicious or blocking extension can compromise the host; document this honestly. A safe wrapper may catch failures, but cannot guarantee isolation of arbitrary native code.
- **Finite immutable local Parquet** sources with pinned full-file byte hashes, schema, non-null fields, row/file caps, source preflight and postflight, no mutable source fallback. Graph uses Phase 20 source format and limits; vector uses a fixed, documented ordered field schema supported by PlanV1 (proposed `row_id: UInt64`, four `f* : Float64`, `score: Float64`, all non-null; adjust only after verifying actual validator behavior). Graph native stage is private, bounded and not published as a result.
- **Static per-job configuration**: one model and one transform sequence declared before first source read; no mid-batch code hot-swap. Up to **4** vector dimensions proposed for v1, max **32,768 rows**, **16 MiB verified source**, **4 MiB retained result**, **256-row batches**, **16 MiB native managed limit** and **10s deadline** for vector demonstration; these are *proposed* conservative limits, not claims about current native defaults. Graph is further limited by **existing Phase 20 maxima**, notably 4,096 vertices, 65,536 edges, 64 MiB source, 64 MiB native graph state, 256 MiB retained disk and 120s operation deadline. Do not increase these by composing calls.
- **Failure semantics**: validate-before-effects where feasible; no successful terminal footer or atomic publication after admission, transform, cancellation, source, model or output failure; all temporary graph/vector stage artifacts have a retention/cleanup plan and are never implicitly reused as authority. Use native `CancellationToken` and one absolute deadline covering verification, execution, stage, output and postflight. Native ledger caps are not total-process RSS guarantees.
- **Numerical contract**: finite `Float64` only; reject NaN/Infinity in inputs, weights, intermediates and outputs; define zero-norm result policy (recommended **refuse** rather than divide by zero), checked norm accumulation/overflow, fixed ordered summation, stable row identities, and documented tolerance versus bit-exact comparison. Preserve `-0.0` behavior explicitly and prohibit opportunistic floating reorder in the baseline comparison. FMA/hardware portability claims require tests, not assumption.
- **Security**: extension model and source are operator-authored and pinned, path/canonicalization/permissions verified, never accepted from untrusted remote tenants. Do not put secrets in plan hashes, logs, evidence or model artifacts. Separate Phase 23 tenant authorization does not apply unless independently integrated and audited.

## 2. Proposed versioned extension contract

Create an opt-in crate such as `crates/ade-ext-sdk` **only if** needed to make a real ownership boundary. The SDK is a **thin safe adapter** over public ADE entrypoints, *not* a second physical planner or untrusted-code host. A representative contract (final names subject to ADR review):

```text
ExtensionDescriptorV1 {
  abi: 1,
  id: bounded stable string, version: semver-like/versioned integer,
  artifact_digest: BLAKE3 of exact installed code/parameter descriptor,
  kind: stateless_batch_map | bounded_graph_composition,
  input_schema / output_schema: exact ordered types + nullability,
  cardinality: preserve_rows | explicitly_declared_graph_reduction,
  deterministic: true | false (v1 executable must be true),
  replay: pinned_immutable_input | none (v1 requires pinned),
  execution: local_native,
  max_rows, max_batch_bytes, max_result_bytes, deadline_ms,
  visibility/trust: operator_trusted_only
}
```

- **Derived authority:** a user-facing descriptor is input to verification, never proof of executable behavior. Bind ID, version, exact compiled extension or installed parameter bytes, source snapshot, normalized plan and operator inputs to an **attempt-independent semantic identity**. Keep runtime/attempt ID separate. An arbitrary string supplied by an attacker must not self-certify a code hash. For a statically linked Rust crate, pin exact package source/build identity and normalized parameters; state precisely which fingerprint is a *source* pin versus a *measured executable* pin.
- **2026-11-14 review reconciliation:** the current SDK fingerprints the measured executable together with `type_name::<E>()` and includes that value in logical identity. This distinguishes statically selected Rust extension types in one binary, but it is provenance only; it does not authenticate callback behavior. The bounded v1 remains trusted-code-only, and downstream consumers must not treat logical identity as a code-authenticity proof or an unrestricted cache key. See ADR-0031 and the additive independent-remediation report.
- **Validation and negotiation:** support an explicit `explain/capabilities` API that returns `SUPPORTED` with effective caps or typed `UNSUPPORTED_*`/`INCOMPATIBLE_*` before native effects. No arbitrary fallback to a different algorithm, non-ADE engine, streaming backend or type coercion. One registered implementation per canonical identifier. Reject unknown field/version, duplicate identifier and ambiguous schema/order.
- **Batch contract:** `BatchMap` must have identical schema, row count and stable row mapping for every input batch; validate this after each call. Zero row batch and empty complete source legal. Explicit checked transient bytes cap includes input+output batch ownership plus derived workspace; distinguish ADE-managed versus extension-allocated bytes. No hidden retaining of all batches; no background threads/remote I/O for standard v1 transforms. Probe cancellation on entry and within bounded work. `execute_with_map` must invoke the **native downstream** `Operator::Project` (or other actually supported op), not just the map.
- **Graph contract:** compile an `ade_iterate::Request` / `CyclicPlanV1` using **only** supported Phase 20 algorithms; do not modify its persisted selectors, source pins or replay model. Consume stdout/observer receipts only after complete exit and a valid terminal footer/selected pin. Verify independent pin before stage; use checked staging to feed an **actual** local ADE aggregation. Keep graph and stage identities distinct but traceably linked. Explain possible partial output from failed/blocked readers.
- **Publication:** for graph composition, use private staging and no-clobber atomic result publication or a clearly **non-durable caller-owned result**. If implementing durable publication, provide exact-key locks/fences, crash-safe order (write/fsync/hash/commit), pin validation and orphan handling; never treat a temporary file as committed. For vector output, returning bounded `ExecutionOutput` without durable persistence is acceptable if **documented**; do not smuggle in exactly-once or job-history guarantees.
- **Compatibility:** no in-place edits to `PlanV1`, `CyclicPlanV1`, Arrow Flight SQL envelopes, Phase 20 graph roots, connector state, Phase 12 native recipes, Phase 23 manager journals or old evidence. Any additive API/guard change in `ade-exec` must be separately justified by a minimal ADR diff and regression matrix. No `ade-exec -> ade-ext-*` dependency.
- **Registry:** compile-time/static registration by operator; no unrestricted file-system discovery. A future ABI or runtime loader is explicit future work. A source-local Rust trait is not magically a stable binary ABI.

### 2.1 Dependency direction (enforce in Cargo)

```text
external application / independent third-party crate
      ├──> ade-ext-sdk  ──> ade-exec (+ public ade-physical)
      ├──> ade-ext-vector ──> ade-ext-sdk (+ Arrow)
      └──> ade-ext-graph  ──> ade-ext-sdk + ade-iterate + native ade-exec

ade-exec / ade-physical / ade-iterate  -X->  specialized extension crates
```

Minimize duplicate framework layers. In particular, if `ade-ext-sdk` would only re-export `BatchMap` with no additional enforceable contract, either implement admission/guard/diagnostics there or remove the crate and show genuine external consumability through a lighter API. This is a **design decision to defend, not predetermined code structure**.

## 3. Detailed algorithms and data contracts

### 3.1 Graph `component_sizes_v1` — composite, not a CLI wrapper

1. Receive a verified `GraphInventoryV1` and existing `weak_components_min_label_v1` plan; require a graph root/pin appropriate for `Run`/`Read` and independently retain intermediate pins. A new result is associated with a fresh graph lineage unless the caller explicitly selects a verified existing terminal. Do not infer a valid terminal from a directory name or an operator-supplied success flag.
2. Use `ade_iterate::operate` for the full graph algorithm. On terminal success, verify that each vertex occurs exactly once, its assigned component is present and no intermediate/nonterminal or partial stdout is accepted. Source graph is unchanged before/after. State precisely which Phase 20 public APIs are used and what follow-up conversion is necessary; no duplicative local graph traversal.
3. Convert the complete bounded component output to private Parquet `key: UInt64 = component_min_vertex`, `value_i64: Int64 = 1`; freeze this stage's exact row count, schema and digest before native read. Count bytes and rows **before** writes that can exceed caps, then postflight stage identity. Stage ID must bind graph semantic/output identity and adapter version; graph retry must not multiply counts.
4. Run `ade_exec::LocalPlan::ParquetScan` → `HashAggregate(key,value_i64)` under checked native config. It emits `key, sum, count`; verify `sum == count` and `sum` fits the declared integer bounds. Canonically sort by component ID as a **presentation** step with separately accounted output bytes; do not claim the native aggregate promises ordering. If substituting verified `PlanV1` lowering, prove it truly supports the stage schema and invokes ADE native HashAggregate.
5. Independent fixture oracle: disjoint-set/union-find over **literal graph edges**, with per-vertex minimum component ID and cardinality, not reading ADE's component receipt as expected output. Include empty, singleton, isolated, self-loop, reciprocal/directed bridges, many components, unsorted file order, duplicate/foreign vertex/edge, corruption, interrupted stage, and bad pin. A graph with all isolated vertices must return size one for each.
6. Contrast static control: an explicitly tagged pure reference graph-plus-host-side-count **comparison only**; publish the ADE-native route as the product path. Do not claim performance improvement without equivalent work and measured evidence.

### 3.2 Vector ML feature / affine batch score

Proposed fixed source schema: unique `row_id: UInt64`, non-null `f0`–`f3: Float64`, non-null `score: Float64` output slot. All four feature columns present in schema; use an explicit dimension `d` in 1..4 with the first `d` features active and inactive slots byte-preserved. Require the schema actually passes `PlanV1` validation; otherwise adjust to one known supported ordered schema with a published compatibility amendment. Single pinned local input source, exact model/config artifact and one versioned plan.

- **`l2_normalize_v1`**: replace active feature values by `f_i / sqrt(sum_j(f_j*f_j))` in deterministic index order; reject zero norm, nonfinite operations and out-of-envelope magnitudes, or use a proven overflow-resistant norm algorithm with an independently specified canonical reference. Preserve inactive fields and the `score` slot. Clearly state tolerance; no cross-platform bitwise promise without proof.
- **`affine_score_v1`**: fixed weights `w_i` and bias `b`, produce `score := b + sum_i(w_i*f_i)` in specified order; reject nonfinite intermediate and final results; preserve `row_id` and feature columns. The model is immutable, validated once pre-run, and its exact bytes are hashed and included in the semantic identity. This is **linear batch scoring**, not a general ML serving engine.
- **Composition:** either a safe ordered chain `normalize -> score` inside one audited `BatchMap` or two explicitly bounded maps (if native API supports them), with an ADE-native downstream projection to `row_id, score` and optionally a native supported filter on an independent UInt64 field. Compare against no-map/identity and scalar-reference controls. The user-facing op ordering is frozen and validated; changing order changes semantic identity.
- **Numerical cases:** negative and zero values, extremes near overflow, 0-norm, ±0, exact ties, duplicate row ID, stale model, swapped features, row reordering, bad schema/type/null, NaN/Infinity payloads and all allowed batch splits. An independent Python `decimal` or f64 reference using different code and literal expected cases must check numeric semantics and row identity; acceptance tolerance cannot be chosen retrospectively to hide errors.
- **Resource cases:** maximum columns, batch payload, source/result bytes, 0/1/max rows, cancellation during mapping, output writer stall, ENOSPC for any optional staging, panicking/misbehaving **test-only trusted** mapper. Document that `BatchMap` is in-process and no hostile plugin sandbox exists. If a panic leaves a poisoned process, fail closed without partial success; restart in a fresh process as needed.

### 3.3 Independent third-party consumer

Create `examples/phase24-external-consumer/` **outside the workspace member list**, build once with `--manifest-path` from a clean checkout and again from a copy with a path-only dependency pointing at the documented SDK. It must implement an original `BatchMap` (not just call `ade-ext-vector`) and demonstrate descriptor negotiation, pinned source/model, actual native mapped scan with a native downstream operator, successful output and an intentionally rejected malformed request. It may live in the repo as a fixture but must be buildable as a separate package; ensure no private module/imports or `pub(crate)` assumptions.

## 4. Work packages (ordered; checkpoints and scope controls)

**WP0 — Fresh-state reconciliation and independent experiment spec.** Reinspect HEAD, `git status`, recent Phase 20–23 remediation, APIs and CI; record predecessor dependency/waiver; inventory old contract/fixture formats and benchmark settings. Freeze a **separate acceptance case catalog**, sources, independent oracles, mutations and measurement thresholds **before** the candidate implementation changes. Any later acceptance amendment needs additive provenance and a reason.

**WP1 — Architecture decision and minimal SDK.** Create [ADR-0031](../adr/0031-specialized-extension-sdk-v1.md) (number provisional—check index). Compare (A) thin SDK + `execute_with_map`, (B) direct public ADE APIs with contract guards, (C) a new general extension IR. Choose lowest-complexity path that demonstrates more than a façade. Specify versioning, capability/refusal, trust, code/model/source pins, resource ledger, dependency direction, and whether schema-preserving limitation blocks key outcomes. Do not add generic plugin loading infrastructure.

**WP2 — Real batch extension end-to-end.** Implement guarded `BatchMap` admission with strict schema/cardinality/output bounds and actionable errors. Add finite feature preparation and affine scoring; bind to an actual ADE plan and native downstream operation. Write unit, property and process integration tests; deliberately test misbehaving mappers and source/model drift.

**WP3 — Real graph composition.** Expose graph library API, native Phase 20 binding, exact selected/terminal result read, independent verified conversion and native ADE aggregation. Add partial-publication/cancellation/restart tests, canonical result and oracle. Phase 20 specialization and reacceptance limits remain explicit.

**WP4 — Third-party consumption.** Build clean external consumer from public API, with a minimal `Cargo.toml`, start-to-finish example, capability refusal and version mismatch case. If consumer cannot build without a private implementation detail, fix API ownership or mark G6/G7 incomplete.

**WP5 — Evidence and measurements.** Matched native/identity/reference configurations on immutable fixtures and same build/host; correctness and overhead first, performance second. Collect raw subprocess output, errors, exit status, source/model/plan hashes, budget peaks, stage ownership, exact argv, platform/Rust/Arrow versions. Mutate and **fully rehash** archives to test verifier. Capture crash cut witnesses and optional actual ENOSPC on disposable volume; report NOT_RUN where unavailable.

**WP6 — Release gates, integration and documentation.** Run mandatory workspace and feature matrix, MSRV and hosted Linux as applicable; pin exact final SHA and tree, executables, source inventory, configs and evidence. Publish operator guide, ADR, implementation report, support JSON, reproducible scripts and honest README/AGENTS links; ensure prior docs/evidence are unchanged.

## 5. Mandatory acceptance gates (provisionally G0–G17)

Freeze exact case IDs, guard thresholds and fixture lock as machine-readable `scripts/phase24/criteria.json` / `case-contract.json` **before** running the release candidate. Each gate is PASS, FAIL, PARTIAL, NOT_RUN or DEFERRED (with explicit reason); no aggregate green if a required gate is partial.

| Gate | Release requirement — independently falsifiable |
| --- | --- |
| **G0 — source/baseline** | Clean pre-change tree, current HEAD, exact final source/tree, dependency and executable identities; any predecessor waiver disclosed, all requirement/case/fixture versioning pinned; no candidate-source mismatch. |
| **G1 — boundary/defaults** | New crates are actual owners; compile-time opt-in; dependency graph contains no core→domain edge; unchanged default CLI/wire/storage/PlanV1 behavior and old tests. |
| **G2 — extension identity/capability** | Versioned exact schema, nullability, replay, cardinality, source/model/extension identities; unsupported kinds/versions/policies deny before effects. Identity rejects model/swap/plan-alias changes. |
| **G3 — native integration** | Trace/metrics and source inspection prove registered map runs **inside ADE native execution** with at least one real downstream native op; graph runs actual Phase 20 native cyclic and Phase 2 local aggregate, not host-only computation. |
| **G4 — graph correctness** | Real graph component sizes match literal independent union-find oracle for nontrivial/disconnected/isolated/empty/mixed-edge sets, output schema/multiplicity/digests; at least one multi-round graph, verified terminal/pin chain, no double counting on replay. |
| **G5 — graph lifecycle/faults** | Incomplete graph, fabricated terminal, bad pin, corrupted graph, failed/partial stdout, stage interruption and changed-stage bytes cannot produce success; no stale/foreign state reused; stage cleanup or quarantined orphan inventory. |
| **G6 — vector prep** | L2 or justified alternative transform nontrivially changes features on real Arrow batches; independent numeric oracle, finite/zero/extreme cases, row/schema invariants, varying batch split. |
| **G7 — batch inference** | Pinned affine model scores are correct per independent reference; all feature/model swaps and nonfinite/overflow cases refuse; output uses real native query and stable canonical identity. |
| **G8 — external consumer** | New outside-workspace crate compiles and executes from public SDK, is behaviorally distinct, has a native downstream operator, rejects bad API/ABI/descriptor and needs no private imports or in-repo hacks. |
| **G9 — bounded resources** | Source, decode, mapped input/output, temporaries, stage/disk, result bytes, model/metadata and stdout all have enforced hard bounds and calibrated accounting; deterministic off-by-one tests; no hidden unbounded vectors/channels; RSS caveat explicit. |
| **G10 — cancellation and failure** | Timeouts, cancellation, panic/error (trusted test-only), short read/write, pre/post source drift, output backpressure, worker/process crash, stage ENOSPC/permissions produce nonzero explicit failure without valid success marker. Replay constraints and cleanup demonstrated. |
| **G11 — security/trust** | No path traversal/symlink escape or unpinned model execution; no external tenant code or secrets accepted; logs redact sensitive paths/credentials as appropriate; hostile plugin sandbox is explicitly not claimed. |
| **G12 — determinism/attempt identity** | Same logical source/model/algorithm independent of batch size/restart yields equivalent canonical results; attempt IDs distinct; replay neither multiplies rows nor changes semantic identity; numerical tolerance predeclared and appropriate. |
| **G13 — observability** | Explain/refusal traces include extension/version/model/source/plan, recognized native route, effective caps, attempt, measured vs unknown counters, decision reason and static baseline; no custom fabricated native metrics. |
| **G14 — quantitative comparison** | Predeclared matched **identity-map/static native**, **feature-transform**, and **reference** runs, plus graph native vs reference; report setup, transfer/materialization overhead, throughput, p50/p95 elapsed, peak managed/observed RSS where supported, output accuracy, and variance. A slower result can pass if no promised speedup threshold exists. |
| **G15 — adversarial detached evidence** | Exact required case inventory and expected exit/rows derived **outside** candidate receipts; verifier rejects omitted case, success→failure, changed model, swapped fixtures, truncated/forged output, duplicate row, forged terminal, changed native graph source, incorrect measurement, after a complete rehash. Include at least 8 meaningful negative mutations with independently attributed refusal. |
| **G16 — source-matched build/CI** | `fmt`, workspace default/all-features test, strict Clippy, MSRV 1.94, clean external consumer, scoped native process suite, hosted Linux matching **final SHA**, no post-test source edits without rerun. Show actual CI run/receipt; never invent it. |
| **G17 — release/support truth** | ADR, user-facing guide, support matrix/report, exact commands, retained evidence, limitations and deferred CEP/geospatial/general ML. Predecessor support remains qualified and unaffected. Claim name matches proof. |

### 5.1 Evidence contract (prevent a self-certifying implementation)

- Store immutable observed subprocess-level **request bytes**, model and dataset manifests, command/exit, stderr/stdout raw bytes, temporary-root/marker inventories, evidence timestamps, native metrics, stage sizes, and before/after witness hashes. Derived summaries may be included but must not be the verifier's only source.
- Independent oracle takes **fixture input bytes** and predeclared algorithm/model parameters, not the candidate's computed output, trace or accepted flags. Require complete case inventory and expected success/error stage; malformed requests are hashed as raw bytes.
- Independent verifier checks artifact/member checksums, source identities, code/build/criteria/oracle versions, fixture locks, method and output semantic correctness, numerical tolerances, lifecycle transitions, source-staging linkages and measurements. Two modes: `historical-archive` (integrity/semantics for preserved artifacts) and **`current-source`** (exact checked-out source/executable/hosted CI). Archive integrity alone never means current-source acceptance.
- Independent **out-of-archive SHA-256 pin** for release bundle and CI receipt, coupled to exact final SHA/tree. Do not package a pin and then trust it as an independent anchor. Keep failed attempts, and never silently rewrite an earlier archive to appear successful.
- If source/fixtures/criteria/verifier change after the candidate, re-freeze and rerun all affected gates; record timing deltas and separate historical pass from current pass. Rehashed tamper probes must defeat any validator that merely confirms candidate-authored status flags.
- Actual OS/process fault cases and disposable-volume ENOSPC (when claimed) must be witnessed, not simulated only by an exception. If a service or host is unavailable, report NOT_RUN/partial and adjust claim, never substitute a mock and call the gate passed.

## 6. Measurements and compatibility

Suggested run matrix: tiny nontrivial, empty, max accepted, cap+1 refused; graph 1/2/4+ components with directed bridges; vector d=1 and d=4, 1/256/257/32,768 rows, near-magnitude bounds. Run warmups and repeated isolated executions from **fresh roots** with matched data, profiler flags and environment. Publish exact per-run raw timing and measured variance without selecting only fastest samples. Reject correctness failures before comparing speed. Compute all derived statistics independently from raw samples.

At least these non-regressions must remain explicit: existing Phase 11 local query defaults, Phase 12 native output and worker recipes, Phase 14–18 streaming/state support, Phase 19 retained arrangements, Phase 20 graph checkpoint selectors and limits, Phase 21 snapshots/sink, Phase 22 gateway Python function and Flight wire, Phase 23 manager/ledger/tenant auth; no phase evidence archive rewrites. Selectively run existing acceptance/verifiers only under their documented source-bound assumptions. A local smoke is not reacceptance of a historical full campaign.

## 7. Proposed repo deliverables

- `crates/ade-ext-sdk/` (if defended by ADR), `crates/ade-ext-graph/`, `crates/ade-ext-vector/`, plus `examples/phase24-external-consumer/` as a **non-workspace** Cargo fixture. Names may be adjusted for natural ownership, not to dodge gates.
- Minimal **additive** changes in `ade-exec`/other core crates **only** where a necessary public bounded extension guard cannot live in the adapter; own tests for each changed boundary.
- `docs/adr/0031-specialized-extension-sdk-v1.md` (check next available number); `docs/phase-24-extensions-guide.md`; `docs/reports/phase-24-implementation-report.md`; `docs/reports/phase-24-support.json`; `docs/evidence/phase24/` with retained attempts and hashes; `scripts/phase24/{criteria.json,campaign.py,oracle.py,verify.py,tamper.py,...}` or technically equivalent independently documented tooling.
- README/AGENTS short feature links, exact commands, compiler/OS support, and **limitations**. No fabricated acceptance status or closure of Phases 18/20/21/22/23.
- One completed final-source report: what changed, native route traces, exact source SHA/tree, versions, checks run, case counts, observed correctness, resource bounds, measurements, honest gate table, residual issues and specific follow-on work.

## 8. Stop rules and implementation handoff

Immediately stop **publication and any claim of completion** if: verified source/model/extension identity is ambiguous, a purportedly complete graph lacks an independently selected terminal, a mapper can modify row correspondence without detection, numeric validity is uncheckable, a required resource cap is unenforced, a native operator is not really used, the external consumer cannot run independently, evidence depends exclusively on self-reported receipts, or the final commit differs from the tested/CI source. Fix or mark partial. Continue unrelated safe experiments only when their resources and provenance remain isolated.

**Implementer must challenge this plan first.** Specifically evaluate: whether `execute_with_map` is safe enough without new core guards; whether source/extension pinning proves the right things; whether the graph conversion genuinely adds capability; whether exactly these two libraries justify the ecosystem claim; whether floating determinism is practical; whether the inherited Phase 20 status blocks honest completion; and whether a smaller, more idiomatic public API achieves the same testable outcomes. Document disagreements in ADR/plan amendments **before** implementation, re-freeze criteria, then proceed. The reviewer is **not constrained by this plan** and must independently derive omissions and unsafe assumptions from the PRD, architecture, current source and observed behavior.

## 9. Source links / operator read order

- [Main HEAD at inspection](https://github.com/jasonkli21/adaptive-dataflow-engine/commit/95e1153b2b75fbe8d9e2445e96b5f9081d293cfa)
- [PRD Phase 24 and core/ecosystem boundary](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/95e1153b2b75fbe8d9e2445e96b5f9081d293cfa/docs/product/prd.md)
- [Architecture invariants and dependency rules](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/95e1153b2b75fbe8d9e2445e96b5f9081d293cfa/docs/architecture/architecture.md)
- [Phase 20 guide](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/95e1153b2b75fbe8d9e2445e96b5f9081d293cfa/docs/phase-20-iterative-guide.md), [Phase 20 support](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/95e1153b2b75fbe8d9e2445e96b5f9081d293cfa/docs/reports/phase-20-support.json)
- [Phase 22 guide](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/95e1153b2b75fbe8d9e2445e96b5f9081d293cfa/docs/phase-22-multilanguage-udf-guide.md), [Phase 23 independent remediation](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/95e1153b2b75fbe8d9e2445e96b5f9081d293cfa/docs/reports/phase-23-independent-remediation-2026-11-14.md)
- [`ade-exec` public extension and native-map code](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/95e1153b2b75fbe8d9e2445e96b5f9081d293cfa/crates/ade-exec/src/physical.rs), [`ade-iterate` public API](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/95e1153b2b75fbe8d9e2445e96b5f9081d293cfa/crates/ade-iterate/src/lib.rs)

## 10. Implementation reconciliation

ADR-0031 records the scoped Phase 20 prerequisite carve-out and the choice of
a row callback inside a guarded native BatchMap. The SDK constructs both the
map output batch and the versioned projection, so extension callbacks cannot
return an independently ordered batch. Graph accepts an independently retained
pin and offers an ephemeral in-memory result. Current-source acceptance still
requires G0–G17, including the exact-commit hosted CI gate. Initial numeric,
limit, case and measurement criteria are in `scripts/phase24/`.

The local campaign expanded the original case inventory before final
measurement to exercise the one-dimensional model, native map and result cap
refusals, trusted-kernel panic, stalled-reader partial output, changed stage
bytes, source drift after staging and an orphan-safe retry. These additions
retain the original thresholds and mandatory gates. The fixture lock adds the
one-dimensional model and removes a duplicate copy of the four-weight model.
