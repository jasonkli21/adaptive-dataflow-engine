# Adaptive Dataflow Engine — Phase 11 Implementation Plan

## SQL/Rust DataFrame frontend and ADE-owned physical IR, bounded v1

**Prepared:** 2026-10-25 (America/Los_Angeles)
**Repository:** https://github.com/jasonkli21/adaptive-dataflow-engine
**Inspected `main`:** [`4eef1b802ad9034fbd1e08fa1cc074898d6f13b8`](https://github.com/jasonkli21/adaptive-dataflow-engine/commit/4eef1b802ad9034fbd1e08fa1cc074898d6f13b8) — Phase 10 scheduler liveness/verifier follow-up
**Status:** Proposed phase plan, **not** authorization to implement or a claim of Phase 10 closure.

## 0. Authority, prerequisite, and honest scope

Read, in order, `AGENTS.md`, `docs/product/prd.md` (especially §§4, 7–9, 16 Phase 11, 17, 22–24), `docs/architecture/architecture.md` (especially §§3, 5, 7–9, 11, 13, 17, 20, 25–31 and INV-1–10), this plan, ADRs 0005/0006/0010/0013–0017, the Phase 9/10 reports and recent research notes, then implementation code and tests. Follow the repo's precedence: PRD outcomes > architecture invariants > active phase plan > applicable accepted ADRs > reports/source reality. An ADR can refine architectural recommendations only with explicit rationale and affected sections.

**Unresolved Phase 10 gate:** At inspected HEAD, `AGENTS.md` explicitly says Phase 10 is active and “Do not begin Phase11.” `README.md`, `docs/reports/phase-10-implementation-report.md`, and ADR-0017 all mark Phase 10 **PARTIAL**: correctness and review-fix workspace checks pass, but its six predeclared release timing guards failed on the frozen source; no new source-bound Phase 10 timing pass has been measured. Physical process join/removal, final migration, genuine spot replacement, row-group splitting and leased failover are also absent. Do **not** reclassify these gaps as complete or quietly relax the gates.

The user may later explicitly authorize a **Phase 11 bounded-v1 exception** despite that debt. Before Phase 11 code work, either (A) verify and cite an actual Phase 10 closure with contemporaneous evidence, or (B) record the user's explicit Phase 11 authorization, reconcile the stale Phase 10-only scope in `AGENTS.md`, and prominently carry Phase 10 as an open, separate debt/risk in the Phase 11 report. **Preparing these three documents alone does not invoke option B.** No Phase 10 scheduler tuning, timing-gate changes, or historical-evidence rewriting is part of Phase 11.

**Phase 11 PRD goal:** Make ADE useful beyond hand-authored graphs **without surrendering physical-runtime ownership**. A bounded success here is a useful, explicit SQL and Rust DataFrame subset lowered to a versioned ADE-owned execution contract and run by ADE operators, not a claim to general SQL, Spark parity, Python bindings, or full Phase 11 completion.

## 1. Inspected baseline and design pressure

- Workspace presently has `ade-bench`, `ade-exec`, and `ade-distributed` (Rust 2024; toolchain Rust 1.94). `ade-bench` already pins DataFusion `=55.2.0` with `sql` and `parquet` features as an **independent benchmark baseline**, but `ade-exec` and `ade-distributed` do not depend on it.
- `ade-exec/src/lib.rs` has a provisional `LocalPlan` tree, explicitly **not** stable IR. Real bounded native operators include Parquet/IPC scans, `Filter` (UInt64 column `<` literal), `Project`, `HashAggregate` (key UInt64, value Int64, checked exact arithmetic), external sort (key/row-id), `Limit`, and live partial-aggregate paths; memory/spill and cancellation are already owned here.
- `ade-distributed/src/coordinator.rs::StaticAggregateJob` operates on a file list/config/snapshot, while `ade-distributed/src/join.rs::Spec` is a **separate** two-source inner-equi-join path. The Phase 9 join supports observed broadcast/repartition and hash-to-grace within its own caps. The Phase 10 leased scheduler is an opt-in fixed-hash aggregate-only path with additional admission restrictions.
- `ade-bench/src/engine.rs` maps *hand-authored* `Query` variants to DataFusion SQL for oracle/baseline use; `ade-bench/src/ade_local.rs`, `ade_distributed.rs`, and `ade_join.rs` independently construct ADE plans/jobs. There is no user SQL-to-ADE planner, general physical graph, reusable catalog binding, or DataFrame query API.
- Current protocol `ADETCP12`, exchange wire v2, hash v1, benchmark result schema v1, independent oracle and frozen Phase 6–10 empirical records must not silently change. `README.md` and `AGENTS.md` still identify Phase 10 as active.

**Architectural resolution:** Create `crates/ade-physical` only when it genuinely owns the public, validated IR; create a separate frontend crate only when it genuinely owns parsing/binding/translation and a small query entrypoint. No empty future crates. Maintain the dependency direction:

```text
DataFusion SQL planner ─┐
                       ├─> ADE physical IR ─> ADE local/distributed lowering ─> ADE operators
Rust DataFrame builder ┘

ade-bench ─> native engine APIs and independent DataFusion oracle
```

`ade-physical` must have **no DataFusion, coordinator/RPC, or storage-tier dependency**. `ade-exec`/`ade-distributed` must not depend on the SQL frontend. Reuse current operators and admission logic rather than replacing the working engine.

## 2. Supported end-to-end user contract

### 2.1 Minimal useful API

Implement a documented, callable Rust API and a small `ade-query` CLI (binary may live in a real frontend crate, e.g. `ade-frontend`) exposing:

- `register_manifest(table_name, dataset_root, verified_manifest)` for **allowlisted, manifest-listed** local Parquet inputs, with schema and fingerprint/source identity checked before native launch. For this phase, `input` on `synthetic-events-v1`; `ade_left` and `ade_right` on the existing bounded join fixture. Do not scan arbitrary extra files in a directory.
- `plan_sql(sql)` using supported **public DataFusion SQL/logical-plan APIs for parse/name resolution/type validation only**; return a validated ADE physical plan or an actionable unsupported error. DataFusion must never be the hidden execution fallback.
- A typed, idiomatic **Rust DataFrame builder** for the same core operations, constructing the same validated IR without reparsing generated SQL. One canonical compilation/validation/lowering path is the goal.
- `explain` showing ADE physical operators, source/table and schema identities, distribution properties, native target (`local`, `distributed_aggregate`, `distributed_join`), declared limitations, and a stable plan digest. `execute` returns real ADE batches and existing metrics; a constrained CLI can accept `--manifest`, `--dataset`, `--sql`/`--sql-file`, `--target`, `--explain` and bounded execution options. The exact syntax is flexible; document/test it.

### 2.2 Deliberately narrow SQL subset (fail closed)

The v1 acceptance set consists of **real parsed and resolved SQL variants**, not string/regex templates or a canned benchmark-case switch:

| Shape | Required semantic support | Native target |
|---|---|---|
| Single table scan/projection | Selected non-null columns of the synthetic schema, with defensible column order/aliases | Local |
| Filter/projection | `WHERE secondary < <typed nonnegative UInt64 literal>` and projection; reject incompatible predicates/casts | Local |
| Bounded aggregate | `GROUP BY key`, `SUM(value_i64)` and `COUNT(*)` with precise checked numeric semantics; compatible aliases | Local; distributed only for the already-supported unfiltered single-source aggregate shape |
| Bounded sort + limit | `ORDER BY key ASC, row_id ASC` on supported schema, optional deterministic `LIMIT` within a configured output cap | Local |
| Two-table join | Bounded `ade_left`/`ade_right` inner equijoin on UInt64 `key`, native output `key,left_value,right_value`; no extra post-join operators | Distributed join via the existing Phase 9 path |

Exact SQL examples for tests (normalization and aliases may differ by input):

```sql
SELECT key, value_i64 FROM input WHERE secondary < 25;
SELECT key, SUM(value_i64) AS sum, COUNT(*) AS count FROM input GROUP BY key;
SELECT key, row_id, value_i64 FROM input ORDER BY key ASC, row_id ASC LIMIT 100;
SELECT l.key, l.value_i64 AS left_value, r.value_i64 AS right_value
FROM ade_left AS l INNER JOIN ade_right AS r ON l.key = r.key;
```

This table is **a minimum feature matrix**, not permission to rewrite SQL semantics into approximate existing operators. Resolve DataFusion `Projection`/`Filter`/`Aggregate`/`Sort`/`Limit`/`Join` nodes deliberately, including aliases/type coercion. Where existing ADE operators cannot represent the **exact** logical meaning, either add a bounded ADE-native adapter with correct semantics and tests or explicitly reject it. Do not silently discard a predicate/projection, change output schemas or assume SQL nullable/overflow behavior matches native behavior. An unordered `LIMIT`, non-equijoin, nullable or unsupported type, arbitrary expression/cast/UDF, outer join, multiple aggregates not in the contract, DDL/DML, multi-statement SQL, remote paths, streaming and recursive/CTE/window queries are unsupported in this slice. No ad hoc SQL string pattern matching for semantic correctness.

`distributed_aggregate` and `distributed_join` must explicitly refuse plan nodes/modes they cannot execute; no generic DAG distribution is claimed. The planner may choose the user-declared target, but it must not reinterpret distribution semantics as a fixed materialization tier or bypass existing scheduler/recovery admission.

### 2.3 Physical IR v1 minimum stable contract

Define one versioned ADE-owned public `PhysicalGraph`/`PlanV1` contract with:

- Stable `NodeId`/`SourceId`, typed schema (Arrow types or unambiguous portable representation), validated acyclic edges, one declared output, deterministic/canonical `EXPLAIN`/digest and explicit supported-version behavior. Avoid embedding machine-local paths in logical identity; bind a validated source catalog at launch.
- Concrete operator variants only for **actually executable** Phase 11 forms: source/scan, filter, projection/alias, hash aggregate, sort, limit, and the bounded inner join. Track input/output schemas, column indexes/names, non-null guarantees, orderedness and precise supported expressions. Do not pretend unimplemented UDF/window/state operators exist.
- First-class distribution requirements (`Single`, `Hash(keys)`, `Broadcast`, `Gather` as relevant), boundedness, determinism/replayability and source-fingerprint/lineage references. Distinguish *semantic* distribution from `direct`/`spill`/object retention policy. Carry resource hints only if validated and actually consumed; otherwise omit until real.
- Strict plan validation: unsupported version/operator/type, missing/cyclic ID, dangling input, invalid column/index, schema mismatch, nondeterministic or malformed source, invalid ordering/partition key, excessive graph depth/node count/output rows are rejected **before** execution or partial output. Stable serialization is optional only if a documented in-process Rust API plus deterministic textual representation provides the promised compatibility boundary; prefer a version-tagged `serde` form with explicit compatibility/unknown-field rules if modest.
- A native runtime entrypoint accepting a validated ADE graph (or a thin, documented adapter in the ADE runtime crate), internally lowering to today’s `LocalPlan`, `StaticAggregateJob`, or join `Spec`. The IR must be **used by the execution path**, not merely printed while a separate hand-authored query switch selects execution. Preserve existing standalone `LocalPlan`/job APIs as compatibility adapters until safely migrated; avoid a repository-wide operator rewrite.

### 2.4 Safety, lineage, and observable behavior

- End-to-end validation must use the existing manifest source list, schema checks, content fingerprints (or clearly documented equivalent verified snapshot), and immutable-source assumption. The SQL frontend must not widen trust to arbitrary paths or let SQL create external tables.
- Preserve non-null/type/overflow guarantees and bag-vs-ordered result contracts. The sort test uses unique `row_id` as tie-breaker; join/aggregate outputs are bag-checked independent of iteration order. Make `LIMIT` semantics deterministic only where explicitly guaranteed.
- Preserve existing per-task managed-memory and exchange caps, deadlines, cancellation, failure cleanup, no-partial-commit, and `ADETCP12` boundaries. This is not a total RSS guarantee. Protect against oversized SQL/plan/canonical dumps and output collection.
- Emit plan/IR version and digest, table/source identity, target, rejected reason, effective config, executed native stages, existing resource/exchange metrics, and declared storage/recovery policy; avoid false claims that a plan was executed by DataFusion or that an inactive adaptive mode ran.
- Static/no-adaptation baseline remains selectable. No scheduler lease semantics, wire protocol, benchmark result schema or source snapshots change merely because of frontend introduction.

## 3. Work packages, in efficient dependency order

### WP0 — Gate, contract table, accepted design decision

1. Reinspect actual `main`, working tree, latest Phase 10 report/ADR and existing executable APIs before implementation. Do not overwrite user changes.
2. Resolve Phase 10/11 phase authorization **explicitly** as in §0; update `AGENTS.md`/README only to reflect that authorization while retaining a visible Phase 10 PARTIAL note. Do not counterfeit prior evidence.
3. Write ADR-0018 (suggested `docs/adr/0018-physical-ir-and-frontend-v1.md`) recording crate ownership/dependencies, IR versioning/validation, supported SQL and DataFrame shapes, lowering targets, admission/rejection policy, semantic limits, and unaffected invariants/protocols. Record any intentional architectural refinements.
4. Make the SQL × native backend × source schema matrix authoritative in code/tests/docs. Record every non-equivalent DataFusion coercion or nullable/overflow case as an explicit refusal rather than guessing.

### WP1 — ADE-owned physical IR and validation

Implement `ade-physical` with fully constructed/validated public types, canonical explain/digest, schema-aware operator and distribution validation, source bindings and strict refusal paths. Add focused unit/round-trip/golden tests, including malformed graphs. Wire a native `ade-exec` entrypoint consuming the IR and lowering only supported local forms. Keep `LocalPlan` available for all Phase 1–10 callers.

### WP2 — Genuine frontend adapters

Implement SQL parsing/binding/translation in an outer `ade-frontend` crate using the already pinned DataFusion major/minor API; avoid adding DataFusion to either native runtime crate. Use manifest-restricted registered tables and validate **resolved logical expressions**, not raw SQL text. Add a small Rust DataFrame builder targeting the identical IR path. Provide structured unsupported diagnostics and bounded plan depth/input/output limits. Add the `ade-query` explain/execute entrypoint.

### WP3 — Native execution wiring, local then narrow distributed

1. Make local scan/projection/filter/aggregate/sort/limit execute from the validated graph using existing `ade-exec` operators; add narrow schema/alias adapter only where necessary for exact output semantics.
2. Wire the graph’s **unfiltered** GroupBySum form to the actual existing static distributed aggregate runner; retain configurable accepted exchange/recovery modes and refuse incompatible selection before side effects.
3. Wire the graph’s explicitly bounded two-source inner join shape to the existing Phase 9 distributed join runner; retain its manifest/file limits, output cap, algorithm/adaptation choices and no-unsupported-recovery guarantees.
4. Ensure physical plans execute the bound source identities and never re-plan execution in DataFusion or route to an unrelated benchmark adapter.

### WP4 — Correctness, performance accounting, docs/handoff

1. For supported SQL and DataFrame forms, compare ADE results with **separately executed DataFusion results** (or existing independent canonical oracle); use exact bag checks and schema/type checks, deterministic sort checks, signed/overflow and boundary fixtures, tiny/empty and skewed inputs. Add metamorphic equivalent SQL/DataFrame cases that compile to the same semantics.
2. Test negative SQL/type/alias/overflow/plan/source-drift cases; parser limits; malformed graph; missing/mismatched manifest; target incompatibility; cancellation/resource teardown. Run one real multi-process aggregate and one real multi-process join. Confirm no fallback and no partial results on refusal/failure.
3. Prove a frontend-independent native IR invocation (without DataFusion linked into core), and assert fixed-protocol/result-schema continuity and static baseline. Run Phase 6–9 focused legacy regressions and Phase 10 scheduler correctness/smoke; do **not** reinterpret failed historical Phase 10 timing gates as passing or rerun entire expensive suites during every edit.
4. Add an `ade-query` example and user docs, ADR-0018, a new `docs/reports/phase-11-implementation-report.md`, and reconciled `README.md`/`AGENTS.md`. The report must distinguish bounded-v1 success from full Phase 11/Phase 10 outcomes, list tested source identities and commands, honest remaining gaps and follow-on recommendations.

## 4. Verification and acceptance matrix

| Gate | Must demonstrate | Efficient evidence |
|---|---|---|
| Authority | Phase 10 either closed with evidence or explicitly bypassed for this phase with user authorization; Phase 10 remains visibly PARTIAL otherwise | AGENTS/ADR/report reconciliation |
| True ownership | `ade-physical` independent of DataFusion; ADE exec APIs genuinely consume IR; SQL/DF construct same contract | Cargo dependency inspection + native API integration test |
| Semantic translation | Genuine resolved SQL plans (aliases/types); precise support/refusal with zero silent fallback | Translation unit tests + negative matrix |
| Local behavior | Scan/filter/project, checked aggregate, sorted limit from SQL and Rust DF | Independent output/schema oracle on manifest fixtures |
| Distributed behavior | Only admitted group sum and two-source join; preserve network/retention/adaptation/recovery contracts | Focused 2-worker real-process tests; representative 4-worker regression if feasible |
| Stability | Validated versioned plan; deterministic explain and digest; input/schema/fingerprint fence | Golden serialization/explain + malicious/invalid plan tests |
| Failure/bounds | No partial output/hidden unbounded work; cancellation and cleanup; output/source/plan caps | Focused fault/injection and resource tests |
| Legacy integrity | Existing benchmark result schema, fixed Phase 6–10 behavior and independent oracle are unchanged | `cargo fmt --all -- --check`; `cargo check --workspace --all-targets`; `cargo clippy --workspace --all-targets --all-features -- -D warnings`; `cargo test --workspace --all-features`; `bash scripts/smoke.sh`, selected legacy cases |

Run fast targeted tests during each WP and the full workspace gate **once** near completion (repeat only for consequential fixes). CI remains correctness-focused. Phase 11 has no invented absolute speed threshold; optional baseline comparisons must declare source/plan/dataset/environment and show distributions (not a single best sample). Do not burn time repeating all Phase 6–10 release timing campaigns unless changed contracts justify it. If tests or build cannot run, clearly say so and avoid a passing claim.

## 5. Explicit out-of-scope and deferred items

- Full ANSI SQL, arbitrary DataFusion physical-plan execution, generalized query optimizer, cost-based reordering, arbitrary DAG scheduling, column types/NULL semantics beyond the admitted contract, prepared-statement service, table catalog/remote connectors, writes/transactions, UDFs, vectorized code generation.
- Python/PyO3 DataFrame bindings, Substrait import/export and interoperability claims, Flight SQL/JVM or distributed SQL service. Design room for them, but do not introduce speculative modules.
- New full dynamic worker lifecycle, Phase 10 performance optimization/re-gating, leased join/SQL, replay scope expansion, streaming/checkpoint state, coordinator HA or Phase 12 reliability.

If a supported SQL shape cannot be made semantically correct within the bounded runtime without disproportionate refactoring, **narrow the claimed phase status honestly**, document what refused and why, and keep the stable IR/execution path valid. Never quietly broaden the backend, loosen correctness, or present a parser-only demo as full Phase 11.

## 6. Handoff and completion decision

The implementation handoff must include baseline/final commit and dirty-worktree status, exact scope decisions and rejected forms, changes by crate, ADR/architecture reconciliation, actual native execution proof, oracle and resource evidence, commands and any not-run checks, measured performance only if source-bound, legacy-regression outcome, unresolved Phase 10 gates, and an explicit **COMPLETE bounded v1 / PARTIAL / BLOCKED** recommendation. The user can independently test before requesting review/fixes. Do not publish a release or alter GitHub history without authorization.

**Independent review requirement:** The Phase 11 review must reconstruct requirements from PRD/architecture/current source and sample unsupported cases **before** consulting this plan's acceptance matrix; it must search for architectural, semantic, security/resource and regression flaws that neither this plan nor implementation anticipated. The companion `phase-11-review-prompt.md` enforces that broader review.


## Implementation-session reconciliation (2026-10-25)

The user's explicit request to implement Phase 11 according to the companion
prompt invokes §0 option B. ADR-0018 records the exception; Phase 10 remains
PARTIAL with every failed timing guard and physical-membership gap intact.
The implemented bounded contract and verification are recorded in the Phase 11
report. Native IR supports only the admitted bounded tree, rather than arbitrary
shared DAG execution. Sort admission additionally requires strictly increasing
source row_id (constant metadata) and SELECT of key,row_id,value_i64, allowing
column reordering; SQL aliases remain supported on the other admitted shapes.
Preflight caps a whole decoded Parquet row group at 16MiB and source footer at
1MiB; these are explicit new frontend admission restrictions, not legacy runtime
changes. The SQL binder uses public unoptimized resolved logical plans; its
translator explicitly validates types/expressions instead of running a general
optimizer or DataFusion physical planner. No Phase 12 work is authorized.
