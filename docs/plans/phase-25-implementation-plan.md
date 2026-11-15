# Adaptive Dataflow Engine — Phase 25 Implementation Plan

## Measured heterogeneous CPU execution and bounded code specialization v1

**Prepared:** 2026-11-14 (America/Los_Angeles)\
**Repository:** <https://github.com/jasonkli21/adaptive-dataflow-engine>\
**Inspected `main` HEAD:** [`4e0f298b4b0dae7f6ab7398e3f4f590c1a33b4d8`](https://github.com/jasonkli21/adaptive-dataflow-engine/commit/4e0f298b4b0dae7f6ab7398e3f4f590c1a33b4d8) (Phase 24 independent-remediation changes)\
**Status:** proposed plan, **not** authorization to declare predecessor phases complete\
**Proposed bounded exit:** `COMPLETE_BOUNDED_PHASE25_MEASURED_CPU_SPECIALIZATION_V1`, only with source-bound correctness, demonstrable native usage, reproducible measurement, and release evidence. Otherwise `PARTIAL_PHASE25_...` with explicit blockers.

> **Scope proposition, not foregone conclusion.** The PRD's Phase 25 is “Heterogeneous Hardware and Advanced Code Generation,” with SIMD specialization, NUMA-aware scheduling, ARM/x86 economics, optional GPU operators, and JIT/fusion **only when measured**. A credible first slice is an opt-in, actual native CPU specialization plus measured dispatch/economics, a rigorous scalar fallback, and documented decisions about NUMA/GPU/JIT. Do not pretend that one accelerated filter constitutes a GPU, JIT, NUMA scheduler, a cross-ISA distributed fleet, or completion of every long-term Phase 25 capability. If initial measurement finds no material benefit, record that negative result and keep the implementation disabled rather than engineering an unjustified speedup claim.

---

## 0. Authority, read order, and verified starting point

Read before editing, in descending authority:

1. [`AGENTS.md`](../../AGENTS.md); [`docs/product/prd.md`](../product/prd.md) §§5–9, 11–12, 15–17, 20–22, 24; specifically **Phase 25** at PRD §16; [`docs/architecture/architecture.md`](../architecture/architecture.md) §§3, 5–9, 13–15, 22–23, 25–29, **INV-1–10** at §31.
2. Current architectural refinements, especially [`ADR-0005`](../adr/0005-local-pull-runtime.md), [`ADR-0006`](../adr/0006-managed-local-memory-and-spill.md), [`ADR-0016`](../adr/0016-adaptive-operators-v1.md), [`ADR-0017`](../adr/0017-morsel-scheduler-v1.md), [`ADR-0018`](../adr/0018-physical-ir-and-frontend-v1.md), [`ADR-0025`](../adr/0025-unified-bounded-unbounded-planning-v1.md), [`ADR-0030`](../adr/0030-bounded-local-multitenant-resource-manager-v1.md), [`ADR-0031`](../adr/0031-specialized-extension-sdk-v1.md), and the ADR index. Add a focused **ADR-0032** only if this number is still free at implementation time.
3. [`docs/research/benchmarking-methodology.md`](../research/benchmarking-methodology.md), [`benchmarks/specs/scan-filter-project.toml`](../../benchmarks/specs/scan-filter-project.toml), [`crates/ade-bench/src/ade_local.rs`](../../crates/ade-bench/src/ade_local.rs), [`crates/ade-bench/src/engine.rs`](../../crates/ade-bench/src/engine.rs), [`crates/ade-bench/src/environment.rs`](../../crates/ade-bench/src/environment.rs), the other existing benchmark adapters, [`scripts/smoke.sh`](../../scripts/smoke.sh), and CI.
4. Actual native path: [`crates/ade-exec/src/stateless.rs`](../../crates/ade-exec/src/stateless.rs), [`crates/ade-exec/src/lib.rs`](../../crates/ade-exec/src/lib.rs), [`crates/ade-exec/src/physical.rs`](../../crates/ade-exec/src/physical.rs), [`crates/ade-exec/src/memory.rs`](../../crates/ade-exec/src/memory.rs), [`crates/ade-physical/src/lib.rs`](../../crates/ade-physical/src/lib.rs), relevant tests and Cargo manifests. Review the `ade-resource` and distributed runtime contracts **before** alleging NUMA/heterogeneous task placement can be integrated safely.
5. Latest implementation guides, support matrices, remediation reports, and current source; reconcile discrepancies explicitly instead of silently relying on newer code or older prose.

### 0.1 Observed current reality at inspected HEAD

| Surface | Confirmed starting fact | Phase 25 consequence |
| --- | --- | --- |
| PRD §16 | Phase 25 names SIMD, NUMA, ARM/x86 economics, optional GPU and JIT/fusion; features are conditional on measurement. | Prioritize a research-valid end-to-end demonstration, not a checklist of unsupported optimizations. |
| `ade-exec::stateless::filter_u64_lt` | Builds a null-aware Boolean mask by iterating `UInt64Array` values, then uses Arrow `filter_record_batch`; used by `LocalPlan::Filter`. | A narrow native operator with exact semantics to use for scalar/ISA differential tests and real end-to-end benchmarks. |
| `ade-exec::ExecutionConfig` and runtime | Public finite memory/spill/prefetch/output knobs, synchronous local pull nodes, per-operator elapsed-time metrics. | New hardware policy must not quietly change existing defaults, public struct construction, or the resource-accounting claim. |
| `ade-bench` | Semantic `ScanFilterProject` case, ADE-local adapter, independent synthetic oracle, child-process benchmark runner and provenance; workspace Rust MSRV 1.94, Arrow/Parquet 59.3.1. | Preserve workload semantics, oracle, runner comparability and existing result schema; add adapter-owned kernel configuration and supplementary signed/pinned evidence as needed. |
| Phase 24 HEAD and `docs/reports/phase-24-support.json` | Latest status is **`REMEDIATION_REQUIRED`**. Exact earlier hosted CI failed in an `ade-ops` native recovery test and skipped Phase 24-specific CI; independent Phase 24 reacceptance still open. | Do **not** use Phase 24 extension success, external plugin provenance, or Phase 24 acceptance as a Phase 25 prerequisite unless individually validated and expressly scoped. Preserve its honest status. |
| Predecessor support matrices | Phase 18 partial; Phase 20 pending independent reacceptance; Phases 21–23 have remediation/reacceptance qualifications. | Keep inherited status labels, no opportunistic completion, no unrelated recovery/control-plane rewrites. |
| `docs/research/benchmarking-methodology.md` | Noise warning for small deltas, warm-cache fresh child processes, release-build provenance, independent correct-before-timed analysis. | Predeclare thresholds and evidence; `cargo test` or a debug-run microbenchmark is not proof of hardware economics. |

**Source freeze:** Implementer must record `git rev-parse HEAD`, tree SHA, status, toolchain/compiler flags and full change list at work start. The SHA above records **planning inspection**, not automatically the eventual implementation baseline. Re-inspect if `main` moved; no blind rebasing against this document. The current repository has 17 workspace crates including `ade-exec`, `ade-bench`, `ade-resource`, `ade-ext-sdk`, `ade-ext-vector`, `ade-ext-graph`; do not scaffold a generic GPU/LLVM/NUMA crate just to mirror the roadmap.

### 0.2 Phase boundaries and prerequisite policy

- **Independent core path:** Phase 25 correctness and measurement must run on a native `ade-exec` scan/filter/project using the Phase 0/1/2 substrate and `ade-bench` independently of the unsettled Phase 18/20/21/22/23/24 acceptance status.
- **No inherited green:** A prior `PARTIAL`, `REMEDIATION_REQUIRED`, pending CI, or waived prerequisite stays so. If a later integration truly needs a predecessor contract, require precise reference to that contract, independently scoped revalidation, and a documented carve-out—not status laundering.
- **No untrusted code assumption:** Phase 24's trusted statically linked Rust SDK is not a sandbox/ABI and should not be reused to smuggle arbitrary binary codegen or execution claims.
- **Exact semantics:** Correctness dominates speed or placement; logical source and result identity must not depend on incidental worker architecture or retry attempt. Physical ISA/profile are execution metadata, not a new source identity.

---

## 1. Target outcome and decision tree

**Primary proof:** ADE can safely select between two native implementations of one *real* operator based on CPU capabilities and predeclared measurements, with the same logical result, a static baseline, recorded decision provenance, and no regression in resource limits, recovery boundaries, or existing adapters.

**Proposed demonstrator:** optimize the `UInt64 < threshold` filter mask construction in `ade-exec::stateless` (preserve Arrow record filtering), and execute it through the **existing native** `LocalPlan::Filter` and ADE-local benchmark. Do not create a benchmark-only alternate engine or a Phase 24-only callback that bypasses the ordinary runtime.

### 1.1 Mandatory bounded v1

1. A **scalar reference path** preserving current SQL null behavior (null does not pass), column type/schema/row order, zero-row, offset/sliced arrays, and strict `UInt64` unsigned comparisons; no hidden semantic change. Existing public `filter_u64_lt` and legacy execution entrypoints must remain compatible.
2. At least one **genuinely specialized CPU implementation** using demonstrated SIMD/vector machine instructions on an available real supported ISA (preferred x86_64 AVX2, optional aarch64 NEON). Keep architecture dispatch dynamic and feature-gated; reject forced unsupported ISA cleanly. If SIMD is not cost-effective against both ADE's existing scalar mask and Arrow's best supported equivalent, stop and report the negative result.
3. A **versioned, explicit, opt-in kernel selection contract** (`scalar`, `force-supported-isa`, `measured-auto` or more precise names) separated from the operator mechanism. Existing default behavior remains scalar/unchanged; no unsupported auto-activation. Compatible configuration in ADE-local benchmark must select the same native path.
4. **Hardware capability and performance profile contract** with bounded and sanitized fields; a profile does not constitute hardware attestation. `measured-auto` must use a validated *same-environment, same-semantics* profile and explicit confidence/minimum-benefit threshold. Without a valid profile or meaningful predicted gain it selects scalar and explains why. No constant hardcoded AVX2-wins assumption.
5. **End-to-end correctness and cost observations:** path actually used, rows/batches processed, selectivity, scratch/reservation peak, fallback/refusal reason, phase-specific per-kernel time and end-to-end query/process time; metrics distinguish measured costs from estimates. No change to existing benchmark result-schema semantics without an explicitly versioned additive schema.
6. Controlled, independent **micro** and **process-isolated macro** comparisons across relevant selectivity, null and batch-size regimes, against native scalar and appropriate Arrow/native baseline. Independently validate results before accepting timing.
7. Clear **research dispositions** for NUMA, ARM/x86 placement economics, GPU offload, and JIT/fusion; only implement experimental/production-facing extensions where the feasibility/measurement gate below is satisfied. Publish negative experiments too.

### 1.2 Explicitly out of scope unless an approved, evidence-backed amendment changes it

- General LLVM, Cranelift, expression JIT, dynamic native plugins, executable-code loading, runtime executable writes, or arbitrary fusion DAGs.
- GPU kernels, PCIe transfer machinery, CUDA/Metal/ROCm dependencies, or vendor-specific binaries **without** an independently validated end-to-end crossover demonstrating benefit after transfer/copy costs and memory effects.
- Cross-worker/remote heterogeneous scheduling, distributed NUMA migration, cgroup/tenant quota changes, replacing Phase 10 lease protocol, changing Phase 23 manager, or recovery/lineage formats merely to expose ISA knobs.
- ARM versus x86 **performance rankings** based on compiled code, processor marketing, or results on unrelated datasets. Require actual runs on each physical target; separate throughput, cost, power, and deadline.
- Broad rewrites of Arrow, physical IR, native PlanV1, pre-existing query/output schema, or closed Phase 20 cyclic operators.

### 1.3 Stop / adjust triggers

Before choosing a SIMD approach, measure the existing scalar mask versus Arrow's applicable vectorized predicate or comparison API under identical circumstances. If Arrow already dominates or is indistinguishable, prefer a small dispatch/measurement proof or document `PARTIAL`/`NEGATIVE_RESULT`; **do not** add unsafe intrinsics only to satisfy the word SIMD. Do not optimize a synthetic kernel if it is not invoked by `LocalPlan::Filter`. Do not claim a profile demonstrates general benefits without whole-query evidence.

---

## 2. Proposed implementation architecture

### 2.1 Operator mechanism and ISA dispatch

Keep the public correctness contract: for batch `B`, column `c` must be `UInt64`, retain rows exactly where `valid(B[c], i) && value(B[c], i) < threshold`; preserve all other column values, row order, dtypes and null bitmaps. The output is Arrow `RecordBatch` with the existing schema. Reject invalid column and Arrow failure as before.

Introduce a small private kernel layer, for example `crates/ade-exec/src/stateless/cpu.rs` and `.../simd.rs` **only if justified by real ownership**. Expose a deliberately small selection API, illustrative rather than frozen:

```rust
pub enum FilterKernelRequest {
    Scalar,
    ForceSupportedSimd,
    MeasuredAuto { profile: ValidatedProfile },
}

pub struct FilterKernelDecision {
    pub requested: FilterKernelRequestKind,
    pub selected: EffectiveKernel, // scalar | avx2 | neon | other validated variant
    pub reason: SelectionReason,
    pub profile_digest: Option<[u8; 32]>,
}
```

- Keep stable scalar correctness reference (no clever loops shared with SIMD path that make both wrong identically).
- Use runtime feature detection on x86_64; compile target-ISA functions behind `cfg(target_arch=...)` + audited `#[target_feature]`. On aarch64, use the actual supported NEON capability and test on hardware. A target feature available to the compiler is **not** proof a particular host supports it.
- Vectorize **the comparison and mask construction**, not only an unrelated bit operation, and inspect optimized assembly/disassembly or performance counters where available. No unsafe `target_feature` invocation without prior runtime guard. Carefully handle unsigned compare (especially AVX2), valid-bit masks, unaligned/tail lanes, slices/offsets, and threshold = 0 / `u64::MAX`.
- The Arrow `filter_record_batch` step remains the same unless a separate measured, equivalent strategy is justified. Never use `unsafe` to read outside a slice; write a security comment for each unsafe block explaining preconditions and a dedicated differential test.
- The specialized path must have a finite input cap and explicit scratch-byte bound, e.g. `ceil(n / 8)` for a packed selection bitmap plus fixed overhead; pre-reserve/constrain before large allocations. Do not claim that this bounds Arrow's allocator/RSS or that existing managed memory measures every Arrow internal buffer. Check cancellation at bounded intervals, including long inputs; never turn cancellation/failure into a partial successful output.
- Support `force-supported-simd` only on hardware that passes admission. Force on unsupported hardware returns a typed **refusal**, not a silent fallback; `measured-auto` may fall back and must emit a reason. Detect feature on the executing host at execution time, not only at profile creation.

### 2.2 User-facing/native integration (do not break legacy APIs)

Integrate kernel choice into the ordinary `Kind::Filter` call site in `crates/ade-exec/src/lib.rs`, and into ADE-local benchmark config in `crates/ade-bench/src/ade_local.rs`. Prefer additive entrypoints or a compatibility-preserving `ExecutionOptions` rather than unconditionally adding a required field to public `ExecutionConfig` (which would break downstream struct literals). Any change in public Rust source compatibility must be intentional and covered with an external consumer compile smoke. Keep `execute`, `execute_with_config`, `execute_to_sink`, and PlanV1 default behavior unchanged unless there is compelling evidence and a documented migration. Do not make a CPU-specific field part of the semantic `BenchmarkCase` or logical PlanV1 identity.

A possible command shape—final naming must match existing parser conventions—is:

```sh
cargo run --release --locked -p ade-bench -- run \
  --dataset <PINNED_DATASET> --spec benchmarks/specs/scan-filter-project.toml \
  --engine ade-local --engine-option kernel_policy=scalar
# same frozen dataset/spec with kernel_policy=force-supported-simd or measured-auto
```

Confirm actual `ade-bench` CLI option names from `--help` before implementing/copying this sketch. Invalid/duplicate options must fail with structured messages. A standalone runtime execution should also support an explicit policy without relying on process-wide mutable environment variables.

### 2.3 Calibrated cost-policy contract

Separate **mechanism** (supported scalar/ISA kernels) from **policy** (which valid kernel to use). A first, auditable profile may include schema version, mechanism version, workload family, `rows`/batch/selected fraction/null density ranges, ISA/architecture, relevant CPU capability class, compiler/Rust/Arrow versions, build profile/flags, dataset/semantic identities, calibration sample IDs, and measured setup and marginal costs (including mask+filter total); hash the canonical profile and record its origin. Avoid hostnames, machine IDs, usernames, arbitrary environment variables, or unsafe remote hardware-probe data.

The policy may use a simple predicted `T_scalar = S_scalar + n*C_scalar` vs `T_isa = S_isa + n*C_isa` with shape bands, and should only select ISA if all are true: host supports it, requested mode permits it, dimensions are within measured coverage, enough calibration samples exist, observed advantage exceeds a *predeclared* uncertainty/noise margin, and the predicted end-to-end result is favorable. Outside those constraints choose scalar. Record all alternatives, prediction, confidence and **observed** actual path/timing; no online retraining until frozen criteria and rollback semantics are defined. A local trusted profile is *not* a signing authority and cannot be reused across machines/ISA based on a caller's string alone.

Test profile staleness and mismatch: different source/toolchain/compiler flags, Arrow major, profile/ISA/version, wrong workload, unknown shape, malformed or overflow coefficients, changed digest, duplicate sample keys, and unavailable hardware. Fail closed or use typed scalar fallback according to predeclared policy; never silently mark an unsupported forced path successful.

### 2.4 NUMA, heterogeneous fleet, GPU, and JIT decision gates

| Lane | Feasibility/evidence required *before* implementation | Bounded disposition |
| --- | --- | --- |
| **NUMA-aware placement** | Demonstrate a supported Linux host with >=2 NUMA nodes, explicit affinity/topology observation, and a real ADE schedulable operator/morsel whose placement can change without bypassing existing leases, memory accounting, tenant isolation or recovery. Measure remote/local access penalty and binding overhead using identical data. | If those prerequisites are met, add a **separately gated** local-only advisory placement experiment with fixed baseline and fallback. Otherwise produce a documented design+negative/not-run decision, **not** a fake topology scheduler. |
| **ARM/x86 economics** | Real source-bound runs on physical x86_64 and aarch64 (not mere cross compilation) with matching semantics, hardware/price/power measurement boundary and clearly different compiler targets. | Report per-host scalar/ISA results. Only report dollar/job, energy/job, or optimal placement if real price/power inputs are cited and comparable; else `INSUFFICIENT_EVIDENCE`. Never claim host-independent cross-ISA speedup. |
| **GPU** | Representative transfer sizes, H2D/D2H costs, pinned data, native Arrow interoperability, bounded device/host memory, failure and fallback path, actual GPU on test host; predeclared crossover for complete native operator. | Research spike only until evidence supports an opt-in implementation and its own correctness/failure gates. No fake GPU execution or unconditional GPU dependency. |
| **JIT/fusion** | Compare interpreted/Arrow/native scalar/SIMD kernel against compile+cache+run over realistic query reuse counts; include compile latency, code cache memory, invalidation, sandbox/security/ABI and recovery identity. | Defer production JIT/fusion unless total amortized cost is measurably better and safe. Record data supporting deferral; avoid speculative compiler framework adoption. |

Even if conditional lanes are deferred, **each must have a clearly reported decision**. Defer is not the same as passing an implemented capability gate.

---

## 3. Work packages and sequencing

| WP | Work | Exit condition / artifact |
| --- | --- | --- |
| **W0 — Baseline forensic inventory** | Pin `main`, tree, all predecessor support statuses, current CLI, real operator path and tests. Find comparable Arrow compute kernel and profile shape. Record current behavior and failure envelope. | Baseline matrix in the eventual Phase 25 report; untouched legacy smoke works. |
| **W1 — Preimplementation measurement** | Establish scalar and Arrow comparison with representative `UInt64` distributions, null ratios, threshold selectivities, batches/slices and observed query mix. Do release-mode controlled warmups/repeats. | A raw exploratory report justifying proposed ISA work or an honest stop/negative result. |
| **W2 — Contract ADR** | Write ADR-0032 (if available) covering namespace/version, safe dispatch/ISA, memory, profile validation, metrics, recovery identity, mutable code/cache questions and alternatives. Amend this plan where analysis reveals a better slice. | Contracts and limitations trace to PRD/architecture; no unsupported scope claims. |
| **W3 — Mechanism** | Implement scalar reference, selected ISA specialization, capability detection, explicit refusal/fallback, bounded scratch, cancellation, isolated/justified unsafe. | Cross-ISA differential tests, coverage and compiled instruction inspection. |
| **W4 — Native integration and provenance** | Wire through `LocalPlan::Filter` and additive runtime options; expose benchmark engine-option, metrics and decision audit; remain default-off. | Same native scan/filter/project semantics; no alternate benchmark-only implementation. |
| **W5 — Calibration and measured policy** | Implement deterministic, strictly admitted profile and conservative selection. Do not use untrusted/mismatched performance samples. | Static vs forced vs auto decisions with exact reason and same-oracle correctness; no unsupported ISA execution. |
| **W6 — Evidence campaign** | Native child-process cases, independent oracle, resource/cancellation/kill/semantic negative tests, sealed source/result/provenance inventory, verifier and tamper suite. | Detached verification from frozen archive; substantive mutations rejected for **relevant** reasons, not only hash mismatch. |
| **W7 — Economics and optional hardware studies** | Process-isolated scalar/ISA/auto comparisons on available CPU; document cross-host/NUMA/GPU/JIT feasibility. | Raw runs, fair hardware/config normalization, uncertainty and negative results. No timing claims from CI. |
| **W8 — Release and documentation** | Strict Rust/MSRV/CI/default/no-feature regressions; guide, ADR, report, support JSON and known-debt list. | Exact final-source artifact + CI receipt, or explicitly partial status. No modifications to historical predecessor evidence. |

**Ordering requirement:** no policy model until correctness baseline, real mechanism and independent oracle are proven; no economic claims until process-isolated end-to-end results exist. New platform-dependent test jobs are additive and isolated; normal x86 GitHub CI must run correctly whether special ISA is detected or not.

---

## 4. Mandatory acceptance gates (proposed G0–G17)

A reviewer may add stronger gates for omissions in this plan. `PASS` means independently verifiable final-source evidence, not a comment, unchecked JSON field or self-described result. `NOT_RUN` stays `NOT_RUN`.

| Gate | Requirement and minimally acceptable witness |
| --- | --- |
| **G0 Source/accountability** | Pin baseline, final commit+tree, dirty state, compiler/flags, lockfile, executable digest, exact fixture/spec and all source/evidence changes; verify no result from another tree is treated as final. |
| **G1 PRD/architecture/boundary** | ADR and code prove ADE native ownership, mechanism/policy separation, opt-in static default, explicit replayability/identity, predecessor qualifications and no hidden dependency escalation. |
| **G2 Legacy behavior** | Existing default scalar `filter_u64_lt`, `LocalPlan`, PlanV1, CLI, output schema and smoke remain byte/semantically equivalent; feature-off cross-platform build and tests. |
| **G3 Independent semantic oracle** | Independently compute expected ordered rows and all field values; test `UInt64` min/max and thresholds, nullable semantics, slices/offsets, duplicates, empty, all/none selectivity, varying batch shapes and invalid types. **No oracle that calls the same mask implementation.** |
| **G4 Hardware detection/safe invocation** | Actual feature probe on executing CPU, forced mismatch refusal, no illegal-instruction risk or target-feature misuse, cross-compilation correctness where feasible; documented unsafe invariant and tested tails/unaligned data. |
| **G5 Full SIMD differential** | Forced admitted ISA vs scalar+independent oracle across deterministic randomized/adversarial cases; exact selection/output bytes and order, including masked nulls/tail bits. Show real SIMD instructions rather than same scalar loop relabeled. |
| **G6 Actual ADE native route** | Execute the optimized operator via genuine `ade-exec` `LocalPlan::Filter` and `ade-bench` ADE-local adapter; unforgeable-in-process observations distinguish effective path and fallbacks. No benchmark-only shim. |
| **G7 Resource/cancel/fault** | Input/scratch/output bounds, reservation failures before growth, no misleading RSS claim; cancellation mid-work, exact output on success, no partial output on failure; clean lifetime, no hidden persistent allocations. |
| **G8 Policy/capability safety** | Versioned strict profile admission, mismatch/stale/spoofed/missing shape and unsupported ISA tests; conservative static fallback; forced unsupported is hard refusal; same result regardless of chosen valid path. |
| **G9 Decision accounting** | Explain requested/effective policy, capability, benchmark sample provenance, alternative estimates/confidence, selection reason, measured runtime and fallback; metrics units/scope correctly defined. |
| **G10 Adversarial process campaign** | Fresh child-process executions and retries, boundary cases and malformed flags/profiles; tamper program catches changed kernel ID, swapped source/dataset/binary, forged pass, invalid profile, altered output/measurements. |
| **G11 Measured specialization economics** | Predeclared cases, equal compiler/output/semantic settings, release build, warmed isolated processes, repeat samples and robust statistics; validated before comparing; an actual material end-to-end win **or documented no-win status without auto-enable**. |
| **G12 Hardware scope truthfulness** | Physical host ISA/capability/probe/OS/toolchain/clock/power and availability metadata without private identifiers; x86/ARM claims only when measured on both; all unavailable lanes `NOT_RUN`/`DEFERRED`. |
| **G13 Evidence/verifier independence** | Immutable source-bound candidate, pinned external seal, verifier checks semantics and cross-field constraints independent of producer, mutation re-seals and still detects semantic fraud; exact-source provenance. |
| **G14 Regression matrix** | Workspace default/all-features, Rust 1.94 check, format/strict clippy, Python/CI verifier tests, original `scripts/smoke.sh`, known Phase 24 consumer where applicable; no predecessor code/status rewriting. |
| **G15 Hosted CI / release provenance** | Successful GitHub Actions for exact final commit on hosted Linux; no test skips masked by workflow order. If inherited `ade-ops` failure persists, show it explicitly and mark release blocked despite local passes. |
| **G16 Support and operator clarity** | ADR, operator guide, reproducible commands, report with failures/deviations, status JSON per gate and hardware lane, precise feature/defaults/cost limits; report separate code-complete vs independently accepted. |
| **G17 No misleading advanced-codegen claims** | NUMA, GPU, JIT/fusion, hetero fleet, ARM/x86 economics each has measured evidence or explicit deferral/unknown; no claims beyond the bounded observed CPU specialization. |

**Conditional extension gates (not silently waived):** N1 multi-node NUMA topology and affinity correctness; N2 affinity/no-affinity end-to-end benchmark and rollback; H1 real x86+ARM same-semantic host measurements plus a clearly sourced pricing/energy comparison if claimed; P1 GPU device/transfer/failure accounting and correctness; J1 compile+cache+run amortized comparison, code lifetime/security and byte-identical/tolerance-justified results. If not implemented, the support matrix must say `DEFERRED` and why. These are not required to label the specifically bounded CPU-v1 exit, but they **are** required to assert those broader subfeatures.

### 4.1 Correctness test matrix

- Data: length `0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 31, 32, 33, 255, 256, 257, 8191, 8192` and one larger bounded case; with Arrow slices at nonzero offsets and null bits across byte/lane boundaries.
- Values: `0`, `u64::MAX`, `2^63-1`, `2^63`, adjacent threshold values, duplicates, alternating low/high, descending and random; thresholds `0, 1, 2^63, u64::MAX`; density 0%, low, 50%, high and 100%.
- Nulls: none, all, alternating, random, boundary bit positions and sliced arrays; ensure filtering's three-valued SQL semantics (null predicate excludes row).
- Output: each retained row's every column and null validity, same order/duplicates, exact `UInt64` math and no out-of-bounds loads on tail.
- Dispatch: no feature present, ISA supported, request force/auto/static, profile stale/replayed/edited/incompatible, high/low confidence, insufficient samples, calibration threshold crossing.
- Recovery/lifecycle: pre-cancel, cancellation during large batch, allocation refusal, panic/error cleanup where applicable, later execution unaffected; no mixed result/decision claiming ISA work that did not occur.

### 4.2 Measurement design and go/no-go rule

Use **two** independent layers: (a) microbenchmark mask construction and full Arrow filtering separately, to know what improved; (b) process-isolated **existing** `ScanFilterProject` end-to-end query, whose canonical result is checked against a generator-based independent oracle. Include at least selectivity near 0%, 1%, 50%, 99% and 100%, null density 0/10/50%, small/medium/large batches, and representative uniform/skew distributions. Freeze input bytes and semantic workload identity. Compare scalar / Arrow-equivalent baseline / forced ISA / measured-auto with identical Rust profile, LTO, target CPU, dataset, caches and output; document per variant branch counters and entire child RSS separately. Run >=1 warmup and >=7 measured **fresh-child** trials per arm in randomized/interleaved order, multiple trials on suspicious/noisy results; report all samples, p50/p95, spread, 95% confidence interval or robust uncertainty estimate, and worst-case regressions. Do **not** use a single best timing. Treat deltas below 5% or within measured noise as unproven; the **auto** policy may enable the optimized path only for independently validated shape bands with both a conservative gain and no material memory/latency regression. These numeric defaults may be revised *before* executing the campaign with written rationale, never after looking at favorable results. CI only gates correctness; hardware speedup remains a separate release decision.

**No misleading benchmark claims:** If only debug measurements are available, mark `EXPLORATORY`. If only one architecture exists, do not publish cross-arch economics. If Arrow's intrinsic kernel is already superior, prefer reuse. If hardware counters/RSS are absent, record `null/unavailable`, not `0`. When comparing cost across architectures, specify compute rental rate, clock/power and measured lifecycle overhead, or explicitly refrain from cost conclusions.

---

## 5. Evidence and provenance contract

Do not build a self-certifying measurement system. Proposed artifacts (adapt exact conventions to implementation):

- `scripts/phase25/criteria.json`: frozen case IDs, expected outcomes, baseline/measured lane, expected bounds, selected policy, predeclared timing rule and test/no-test reasons. This file belongs to a source-bound experiment; semantic changes require an explicit version/baseline update.
- `scripts/phase25/campaign.py`: launches **real** ADE executables, captures stdout/stderr/exit, native metrics, environment/host capability class, source/spec/dataset binary hashes, and full individual timing results; no fake proof for missing device/node.
- `scripts/phase25/oracle.py` or an independently compiled equivalent: computes expected filter output from original immutable inputs and compares canonical rows/bytes without calling specialized ADE routines.
- `scripts/phase25/verify.py`: detached read-only verifier, rejects missing/duplicate/extra/unexpected cases, wrong mode/ISA, inconsistent identity, incorrect outputs, impossible observations, and invalid confidence/cost claims. Verify actual binary/commit association where external receipts exist; explain trust boundary otherwise.
- `scripts/phase25/tamper.py`: mutations that update/re-hash archive records to test semantic validation, not just checksum protection. Include forged kernel success, suppressed fallback/refusal, swapped optimizer policy, altered sample, edited source/dataset, changed compiled ISA and inflated gains. Verifier must flag the specific fraud.
- `docs/evidence/phase25/`: immutable final release candidate with small metadata and source/binary hash pointers; avoid committing multi-gigabyte raw run data. Explicitly immutable historical failed attempts and re-run ledger. A detached SHA-256 seal **outside** the archive and an exact GitHub CI receipt are required to establish the release claim; a seal stored inside the same self-authored artifact is only integrity bookkeeping, not independent authority.
- `docs/reports/phase-25-support.json` and `docs/reports/phase-25-implementation-report.md`: gate statuses, hardware lanes, raw results and honest partial qualifications; link to prior supports unchanged. `docs/phase-25-hardware-guide.md`, ADR, research note and README/AGENTS small updates as appropriate.

The verifier must be reproducible from a **fresh checkout** with only pinned dependencies/fixtures. If a mutable local profile is used, it must be explicitly trusted as operator input and included in reported identity; do not treat a caller-provided SHA as proof the input came from a particular machine. Do not make results reproducible by manually editing expected data after the run.

---

## 6. Proposed code and documentation touch points

**Likely** (reconfirm exact interfaces at implementation time):

- `crates/ade-exec/src/stateless.rs` and an earned small ISA module; `crates/ade-exec/src/lib.rs` at existing `Kind::Filter`, execution option/metrics; narrowly scoped unit/functional tests.
- `crates/ade-bench/src/ade_local.rs` for adapter-specific selector; possibly `crates/ade-bench/src/environment.rs` and result/sidecar logic for sanitized capabilities/provenance, without breaking `BenchmarkCase` identity; `benchmarks/specs/scan-filter-project.toml` remains semantically unchanged.
- `Cargo.toml` / `crates/ade-exec/Cargo.toml`, `.github/workflows/ci.yml` only for minimal optional feature/correctness-target additions, not arbitrary C++/CUDA/LLVM toolchains.
- `docs/adr/0032-measured-cpu-specialization-v1.md` if free; `docs/phase-25-hardware-guide.md`, `docs/research/phase-25-experiment.md`, `docs/reports/phase-25-implementation-report.md`, `docs/reports/phase-25-support.json`, `scripts/phase25/*` and small integration notes in README/AGENTS.

**Avoid:** modifying ADR-0031 or prior evidence to make Phase 25's source look accepted; introducing new transport protocols, generic bytecode formats, GPU crates or an arbitrary distributed CPU-place scheduler without a demonstrated interface and feasibility gate.

---

## 7. Final verification and handoff

Minimum commands (adjust only to the repo's actual features and environment; never claim an unrun command passed):

```bash
git status --short && git rev-parse HEAD && git rev-parse HEAD^{tree}
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo check --workspace --all-targets --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
cargo test --workspace --all-features --locked
cargo +1.94.0 check --workspace --all-targets --locked
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p 'test_*.py'
bash scripts/smoke.sh
# Phase 25 source-bound campaign and detached verification per actual script CLI
# Existing Phase 24 external consumer and native CI stages remain independently relevant
```

The final report must state: exact baseline/final tree and GitHub SHA; effective kernel and how it was observed inside ADE; mechanism/ISA/version and cancellation/memory envelope; what `unsafe` exists and why; every full and partial gate; exact environment and timing samples; confidence/noise and real end-to-end gain or negative result; what NUMA/GPU/JIT/ARM-x86 claims are **not** supported; unchanged predecessor release statuses; independent release CI pin and any unresolved blocker.

**Completion rule:** Passing a local run is not release acceptance. If exact-commit hosted CI is red/absent, the status is partial. If specialization is semantically correct but not shown better, describe the result as a valid research outcome and leave auto disabled, not `COMPLETE_BOUNDED_PHASE25_MEASURED_CPU_SPECIALIZATION_V1` on the strength of code alone. A future reviewer should evaluate this plan critically and may identify a better, more representative operator or require additional system-level proof.

## Implementation reconciliation before native measurement

The baseline Arrow and exploratory ARM64 measurements keep the original filter as
the candidate. The Apple M3 pilot found that Arrow already emits vector unsigned
comparisons. An explicit NEON comparison and byte-packing prototype improved
median mask time against Arrow on every pilot shape and improved full-filter
medians on the three smaller shapes. The pilot ran with background build load,
so it qualifies the mechanism for further measurement only. The same Arrow row
gather remains common to all arms.

Use compile-time opt-in feature `filter-kernels-v1`, explicit
`scalar`, `arrow`, `force-neon`, and `measured-auto`. Recognize `force-avx2`
as a typed refusal; no AVX2 implementation has been qualified. Ordinary builds
and callers without an explicit option keep the existing scalar code. The
calibration model uses an exact per-batch shape and seven paired observations
instead of fitting slope and intercept from sparse data. Require each paired
filter and query observation to beat both controls by at least 5%, with no
material process-wall or RSS regression. For identical measured lengths, the
median per-batch observation is the prediction. Profiles bind the executable,
CPU class, workload and verified dataset bytes. Missing identity and unmeasured
shapes fall back. These controls preserve the original acceptance gates.
