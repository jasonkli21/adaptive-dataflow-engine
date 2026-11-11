# Adaptive Dataflow Engine — Phase 20 Independent Review and Remediation Handoff

**Review date:** 2026-11-11 (US Pacific)
**Verdict:** **`REMEDIATION_REQUIRED`**
**Reviewed commit:** [`74eb752a8ef464600d4183b612eb9e3bc7bf64d8`](https://github.com/jasonkli21/adaptive-dataflow-engine/commit/74eb752a8ef464600d4183b612eb9e3bc7bf64d8)
**Mission:** Perform the repairs and final independent acceptance required by this report. This is a self-contained implementation handoff; no further chat context or decisions are needed. Preserve all prior source-bound evidence and do not reclassify unexecuted tests as passing.

## 1. Executive verdict

The implementation contains a real, finite, synchronous native feedback queue, a checked round-advancing delay, directed BFS and weak-components kernels, a source-attested local generation chain and extensive archived process/fault evidence. A fresh independent replay of the archived Parquet inputs and Python BFS/union-find/round oracle *did* check 380 recorded commands, 33,978 rounds, 1,099,948 feedback rows, 146 selected cuts and the two existing rehashed tamper probes, **once a diagnostic-only floating-point comparison was corrected in memory**. No incorrect BFS distance, weak-component label or checkpoint mathematical state was detected in that replay.

However, the acceptance verifier is unsound for required success/failure cases. **A fully rehashed archive in which a required successful BFS run is replaced with a failed run still passes its semantic validator.** A second rehashed archive in which a pre-admission duplicate-vertex refusal emits a fabricated result row and an unrelated error also passes. The unmodified verifier additionally fails to replay the *original unmodified* archive on this Linux/Python 3.12 host due to strict floating-point standard-deviation equality. These are independently reproduced defects, not suspicions.

**`COMPLETE_BOUNDED_CYCLIC_DATAFLOW_V1` is not yet independently substantiated.** The main immediate blocker is **G14 evidence integrity**, not a demonstrated algorithmic wrong answer. G13 and other native fault/live-process gates could not be freshly executed in this environment (Rust/Cargo not installed); the archived toolchain results are useful evidence but not this reviewer's fresh PASS. Moreover, the physical graph is a fixed three-node template with a specialized executor, so a claim of *general* composable cyclic execution would be too broad; the bounded two-primitive route is materially more defensible. Maintain the narrow local-finite claim or explicitly implement and independently prove a more reusable physical operator boundary; do not silently expand the architecture.

**Release rule:** Until the verifier repairs, new negative tamper tests and final-source native acceptance complete, change any unqualified current-source G14/overall PASS statements to `REMEDIATION_REQUIRED` / `PENDING_INDEPENDENT_REACCEPTANCE` in an **additive** report/support amendment. Keep the original Phase 20 report, archive, original criteria, earlier attempts and earlier-phase archives immutable as historical evidence.

## 2. Source/review baseline and provenance

| Item | Independently observed or recorded identity |
| --- | --- |
| Repository, branch | `https://github.com/jasonkli21/adaptive-dataflow-engine`, `main` |
| Target SHA / tree | `74eb752a8ef464600d4183b612eb9e3bc7bf64d8` / `731c1da1c211d9940f33874307c026bc0a500189` |
| Parent / implementation base | `c0e1c1faec96219fb79c864de1d6194bf5d8571e` / tree `ae94710731c9c8656003d4faddc3f968220d58c0` |
| Review checkout | Clean newly cloned `main`; `git status --porcelain=v1` empty; `git diff --check HEAD^ HEAD` clean |
| Change scope | 70 added/modified paths: new `ade-iterate`, native `ade-exec::iterative`, logical/physical cyclic modules, Phase 20 docs, evidence and scripts; no Phase 17–19 evidence paths changed |
| Current source inventory | Archived 180 Rust/Cargo/campaign input hashes all matched the current checkout in `verify.py --repo .`; inventory SHA-256 `01fa4216ea0af0482975f8829ca990fe3eb9dc7a8bf71d7be017fd2b03dd96b3` |
| Claimed measured binary | SHA-256 `ae2de07d49ca896df7dd58ec559cd532d0a86f0fee49ca3c24e956d94c25fad2`; **not independently hashed or rebuilt** on reviewer host |
| Frozen criteria / fixture lock | SHA-256 `ffe6c0037b18d940d9426f66222708ebd85da7bb8c4c2da78e339f96d80647de` / `cc72307084dc3032555d4e3b96745114a07b326e65471cbf654d5aaba998ce04` |
| Original Phase 20 archive | `docs/evidence/phase20/acceptance.tar.gz` SHA-256 `534703e4c37d6c61a86e93c8028a5b56141d5c8214ca0330ba2d9699b46f8441`; member manifest SHA-256 `6cf00fba9c8ec3d3687bd269180dcf0892526e7140a33bb399370404837a1084` |
| Archived toolchain | `docs/evidence/phase20/toolchain.tar.gz` SHA-256 `de09b7b8eb6f253eda82f8ff5f9241dc4237e8927563a25269c269af06bf9d9e`; its `exits.tsv` reports zero for all 15 archived checks |
| Claimed original measurement environment | macOS 27.0.1 arm64, Apple M3, 24 GiB, local APFS/HFS+ test image, Rust/Cargo 1.99.1, PyArrow 21.0.0, Python BLAKE3 1.0.10, release build with `fault-injection` |
| Independent review environment | Linux x86-64 `6.18.57` on overlay filesystem; Python 3.12.14, PyArrow 21.0.0, BLAKE3 1.0.10. **No `rustc`, `cargo` or Rust binary installed.** No Kafka/MinIO or disposable native test image used. |
| Original Phase 19 archives | Original acceptance archive SHA-256 `54512c4f5c0985b3daea78d28f65f16713af296d157df8ffbe8d42ff97cc5d36`; review-remediation acceptance archive SHA-256 `0aac3c3286396ab6a409167b221da7a1abe81c64d697c59377b95558bd172d4a`. Both unchanged by commit diff. |

Read: repository `AGENTS.md`; `docs/product/prd.md` §§1–9, 15–20, Phase 18–22; `docs/architecture/architecture.md` §§3, 5, 7–8, 18–19, 31/INV-1–10; `docs/plans/phase-20-implementation-plan.md`; `docs/adr/0018`, `0021`–`0027`; `docs/phase-20-iterative-guide.md`; `docs/reports/phase-20-{implementation-report.md,support.json}`; Phase 18 G14 lowering/support and Phase 19 remediation report; all new Rust/source files in the target diff; `scripts/phase20/{criteria,fixtures.lock,fixtures,oracle,campaign,verify,checks,enospc}`; Phase 20 evidence manifests/logs/attempt inventory; prior source/evidence paths. Original and remediation Phase 19 archives were not freshly replayed here (archived historical logs claim they passed). The current reviewer did not inspect every unrelated implementation file line-by-line and makes no blanket repository-wide correctness guarantee.

## 3. Prioritized actionable findings

### F20-01 — P1 CONFIRMED: offline verifier accepts rehashed invalid release evidence

**Location:** [`scripts/phase20/verify.py:164–214`](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/74eb752a8ef464600d4183b612eb9e3bc7bf64d8/scripts/phase20/verify.py#L164-L214), specifically `if case["exit"] != 0` followed by `if not ledger: continue` at lines 169–173; also `Evidence.faults()` lines 284–329 and `validate()` lines 402–432. `cases_and_credits()` takes the archive's reported exit as the classifier instead of deriving the expected disposition independently from the frozen case contract. It does not require each mandatory normal run to succeed, nor every mandatory refusal to have the correct failure reason/no output and no root mutation. `faults()` hardcodes only a subset of negative names. The aggregate observation count is not checked against the frozen original support claim.

**Witness A — suppress required success:** In a *temporary extracted copy*, change `commands/0035-mixed-bfs-0.json` from `exit: 0` and a valid 608-byte successful stdout summary to `exit: 1`, empty stdout and an arbitrary `INCOMPLETE_LIMIT` diagnostic. Repack archive, recompute member and outer SHA-256 manifest, and validate with the diagnostic-only numeric adjustment described in F20-02. **Observed: validator accepts.** It now reports **130** refusals/kills instead of the original **129**, yet still reports 380 commands/33,978 rounds/146 cuts. This is a falsified required successful computation accepted as evidence of release compliance.

**Witness B — corrupt pre-admission refusal:** Change `commands/0096-duplicate_vertex.json` from its genuine nonzero `invalid local plan/schema: duplicate vertex` refusal with empty stdout to the same nonzero status but arbitrary stderr `some unrelated failure` and stdout `{"vertex":999,"distance":999}\n`; repackage/re-hash. **Observed: validator accepts.** The verifier thus cannot establish *why* this required negative refused or that it emitted no unexpected data. An honest engine may still be correct—the flaw is the independent certification logic.

**Impact:** G14 can be satisfied by a semantically contradictory, completely rehashed archive. G0/G4/G6/G15 successes and G1/G5 negative claims that rely on the archived runner's assertions are not independently enforced by the offline verifier. The current all-PASS support JSON and `COMPLETE` statement therefore overstate the independent evidence strength. This directly violates Phase 20 plan G14, §5 evidence protocol and architecture INV-3/5/8.

**Minimum fix:** Make the verifier derive expected command names, operations, fixture/plan/source identities, success/refusal category and required receipts/root behavior from an independent **frozen scenario specification**. It must not take those expectations from a mutable archived `exit`, `stderr`, `stdout`, `PASS` field or performance sample. For every mandatory positive case assert `exit == 0`, exact terminal/output or summary, expected lineage and cases/round counts. For every negative/fault case assert the *specific* expected refusal/fault marker, nonzero or signal exit as appropriate, no false complete footer and the explicitly expected stdout/root/selector effects; pre-admission cases must have empty stdout and no root. Assert an exhaustive expected case inventory (no omissions/duplicates/extra cases) and per-category totals, including successes. Maintain source and executable attestation. Verify continuation/fault inputs and sequence, not just terminal snapshots. Keep no-root and read-only side-effect predicates separate from allowed partial terminal-output-prefix-on-broken-sink behavior.

**Required regression tests:** Add at least the two witnesses above with recomputed outer manifests, plus (i) replace a required negative's request with a valid graph while keeping a failed exit; (ii) remove a required case; (iii) change error stage/reason; (iv) fabricate selected receipt or change process/round sequence; (v) alter a selected snapshot and rehash its outer archive without satisfying the anchored state oracle. Every attack must be **rejected for the intended semantic reason**, rather than coincidentally rejected by unrelated timing/format failures. The pristine original must pass. The verifier should expose named checks and diagnostic context instead of bare `AssertionError`.

### F20-02 — P2 CONFIRMED: byte-exact floating statistics make the portable verifier reject valid evidence

**Location:** [`scripts/phase20/verify.py:353–357`](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/74eb752a8ef464600d4183b612eb9e3bc7bf64d8/scripts/phase20/verify.py#L353-L357), `Evidence.timings()`.

**Trigger and exact result:** On independent Linux/Python 3.12.14, run:

```bash
python3 -m venv /tmp/ade20-review-venv
/tmp/ade20-review-venv/bin/pip install -r scripts/phase20/requirements.txt
/tmp/ade20-review-venv/bin/python -B scripts/phase20/verify.py \
  docs/evidence/phase20/acceptance.tar.gz \
  docs/evidence/phase20/manifest.json --repo . --tamper
```

It exits **1** at line 356 (`AssertionError`), *before* the ENOSPC and tamper checks. The only measured mismatch was sparse/indexed `compute_ns` population stddev from five recorded values `[143802, 142286, 147947, 146538, 142363]`: archive `2278.2889544568307`, independent `statistics.pstdev` `2278.288954456831`, difference `4.547473508864641e-13` ns. Medians and the other statistics matched. Python implementations/platform rounding can differ at this ULP scale; a numeric statistic is not a byte-identity proof.

**Impact:** Pristine, source-matched evidence is not portably reproducible. G14's advertised portable detached verifier fails; a strict reviewer cannot run `--tamper` normally. This is not a performance threshold failure and should not trigger relaxation of the *frozen* runtime/work guards.

**Minimum fix:** Keep integer count, sample and median comparisons exact. For serialized floating stddev, compare with a documented **very tight explicit tolerance** (e.g. `math.isclose(..., rel_tol=1e-12, abs_tol=1e-9)` in nanoseconds) or compute/serialize exact integer sum-of-squared-deviations and derive display stddev secondarily. Test an adjacent-ULP cross-platform fixture and a materially forged variance value that must be rejected. The threshold predicates themselves must remain unchanged, and source/executable/per-run sample checks must stay exact. Do not solve this by skipping all timing assertions.

**Diagnostic result, not a fix:** Re-evaluating the original script in memory with *only* this stddev equality replaced by exact median and `abs(stdev_delta) <= 1e-9` allowed complete `--repo . --tamper` replay: 380 commands; 161 terminal reads; 129 refusals/kills; 77 roots; 146 cuts; 33,978 rounds; 1,099,948 feedback rows; 15 measured pairs; eight checkpoint and two terminal kill boundaries; both built-in rehashed semantic attacks refused. No repository file or original archive was changed in this diagnostic.

### F20-03 — P2 CONFIRMED architectural limitation, acceptance decision required: hardcoded closed operator topology

**Location:** [`crates/ade-physical/src/cycle.rs:78–157`](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/74eb752a8ef464600d4183b612eb9e3bc7bf64d8/crates/ade-physical/src/cycle.rs#L78-L157), [`crates/ade-exec/src/iterative/mod.rs:283–384`](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/74eb752a8ef464600d4183b612eb9e3bc7bf64d8/crates/ade-exec/src/iterative/mod.rs#L283-L384). The plan/ADR explicitly permit one *closed* three-node region, but the new `BoundCycle` reduces the physical plan to `algorithm` and `backedge`; the executor directly calls `expand`, reducer and `drain` with algorithm-specific conditionals. The physical node/forward-arc objects themselves do not dispatch executable operators, and no third operator behavior exercises the delay substrate. `Feedback` carries only `usize` vertex indexes, not typed portable row/delta messages. The `CyclicPlanV1` wire is strict, but its executable generality is unproved.

**Impact:** This is not evidence of a wrong BFS/component result. The native loop **does** carry delayed frontier rows and retire synchronous credits; it is more than a CLI repeatedly invoking PlanV1. But it is not a demonstrated general physical cyclic graph executor or graph/iterative ML platform. PRD Phase 20, architecture §§5.3/7/18–19 and plan §0/§2/mandatory design challenge emphasize a reusable cyclic substrate, not merely two hardcoded programs. `COMPLETE_BOUNDED_CYCLIC_DATAFLOW_V1` must be interpreted strictly as the closed finite local two-primitive slice, or the missing reusable boundary must be implemented and accepted.

**Minimum safe action:** Before any broad runtime refactor, explicitly reconcile Phase 20 plan/ADR/support language with this narrower reality. If the current bounded claim is intended to imply composable physical operators, implement a small shared typed Expand/Reduce/Delay dispatch boundary that actually consumes each validated arc, and add at least one non-BFS/non-components operator or synthetic typed delta flow proving reusable queue/progress semantics. Keep old `PlanV1`, default features and provenance unchanged. If that is too broad for the authorized v1, **qualify the claim instead** and record a named next-phase design dependency. Do not silently claim distributed/nested/async/cross-crate cyclic support. The P2 classification is for overclaim/design incompleteness; no additional runtime bug has been reproduced here.

## 4. Independent plan/architecture critique and positive observations

- **Real mechanism within strict limits:** `CyclicPlanV1::bind` rejects anything other than the exact canonical typed delayed loop; `BoundCycle::feedback_round` checks overflow. `Executor::advance` runs rounds in `ade-exec`, not a CLI loop or DataFusion executor. `Feedback` checks `produced - acknowledged == queued + executing`, open producer ownership and duplicate/wrong-round acknowledgments. Indexed BFS uses current frontier adjacency; components inserts reverse arcs for weak connectivity and monotonically lowers labels. The independent archived oracle reconstruction found no semantic mismatch.
- **Meaningful durability:** The private `CYCLE20`/`LEASE20`/`o20`/`w20`/`s20` root family uses inode-bound anchor, kernel writer/shared-reader fence, source pin, no-clobber selector hardlink, fsync ordering and checkpoint/terminal distinction. `Root::open` requires the exact externally retained pin and checks the whole selected parent chain. The archived fault markers/ENOSPC cases and post-restart object states are consistent; fresh local process/fault proof is **NOT_RUN** here. Trust boundary is one honest operator-owned local filesystem, not arbitrary rollback/hostile storage, disk loss or power-loss HA.
- **Source/effect limits:** `GraphInventoryV1::admit`, `Source::bind` and `BoundFile::attest` require exact role-specific manifests, byte BLAKE3, row/schema and inode/stamp checks. `operate` creates roots after source admission. `Terminal::inspect` accounts the footer/newline exactly and distinguishes provisional cuts. Public `operate` callbacks/writers explicitly own their own backpressure/budgets; that exclusion must remain visible.
- **Remaining generality:** The typed plan uses a fixed template rather than SCC/topology graph algorithms. That is safe for the closed template but cannot establish arbitrary multi-SCC or nested-cycle support. One synchronous owner means real concurrent late-producer races do not occur in the accepted runtime; unit tests for the queue do not establish a future multithreaded progress protocol. The limited 4,096-vertex, 65,536-edge and 4,096-round envelope is a research slice, not a scalable graph platform. No new P0/incorrect-result proof was found.
- **Scientific claims:** Five paired measured runs per sparse/dense/resume regime, a runnable scan-all-arcs policy and observed edge counts are present in the archive. The indexed sparse compute median is much lower, but its process wall median is slightly *slower* than scan-all (28.29 vs 26.67 ms). Do not advertise general end-to-end speedup. The frozen work and compute guards can be evaluated independently after F20-02 is repaired; their criteria are not to be relaxed.
- **Plans vs docs:** The Phase 20-specific waiver of Phase 18 current-source completion is explicit, and Phase 18 remains `PARTIAL_PHASE18_UNIFIED_ADMISSION_V1`. No new evidence justifies Phase 17/18 Kafka/MinIO/worker acceptance. Preserve architectural INV-1–10 and the predecessor evidence. No finding justifies changing old physical or maintained formats.

## 5. G0–G15 independent gate matrix

The categories below describe **this review's independently established state**, not the implementation author's archived matrix. `NOT_RUN` means full fresh native proof was unavailable, even when the source-matched archive contains credible supporting records. `PASS` is used only where the independent offline check itself satisfies the relevant sub-gate. This is intentionally stricter than echoing `docs/reports/phase-20-support.json`.

| Gate | Review status | Direct witness / limitation |
| --- | --- | --- |
| **G0** provenance/dependency | **NOT_RUN** | Target SHA/tree, clean checkout, matching 180 source inputs, criteria/fixture/archive SHA and explicit Phase 18 waiver verified. No local release executable for independent hash/build attestation. |
| **G1** typed IR/admission | **NOT_RUN** | Static proof: canonical three-node `bind` equality and old PlanV1 acyclic admission; archived invalid-plan refusals. No fresh native malformed-JSON execution. |
| **G2** native feedback | **NOT_RUN** | Static `Executor::advance` → `Feedback::send/deliver/ack`, archived 4,096-round chain and credit traces independently decoded. No fresh instrumented native process. |
| **G3** progress/quiescence | **NOT_RUN** | Static checked synchronous conservation and archived per-round oracle equality; no fresh failure/race execution, no asynchronous progress claim. |
| **G4** BFS/weak semantics | **PASS** | On original archive, independently decoded raw Parquet, recomputed directed BFS/union-find results and exact terminal rows/hashes and checkpoint state/rounds after F20-02 diagnostic tolerance. No semantic mismatch. |
| **G5** source/lineage | **NOT_RUN** | Exact archived source inventory and raw Parquet checked; inode-swap/TOCTOU are recorded, not freshly induced here. |
| **G6** convergence/limits | **NOT_RUN** | Source code separates empty frontier from `rounds` limit; archived incomplete cases inspected. No fresh limit/cancellation executable test. |
| **G7** checkpoint/resume | **NOT_RUN** | Offline parent/snapshot/next-frontier oracle checks and archived process-ID restarts support behavior; no fresh independent process restart. |
| **G8** atomic/fencing/ENOSPC | **NOT_RUN** | Original archive records eight reached checkpoint kill cuts, two terminal kills, source-bound ENOSPC OS error 28 and old pin; not repeated on review host. |
| **G9** resource caps | **NOT_RUN** | Source code checked and archived exact/under-cap cases; no fresh near-limit native stress. OS memory/RSS outside managed ledger. |
| **G10** determinism | **NOT_RUN** | Offline canonical outputs agree on archived permutations; process scheduling/checkpoint perturbations not freshly run. |
| **G11** performance/work | **NOT_RUN** | Raw paired timing/work arithmetic checked with diagnostic tolerance; matched local executable absent, unable to rerun threshold campaign. Wall-time benefit is not established. |
| **G12** legacy compatibility | **NOT_RUN** | Diff preserves Phase 17–19 evidence; default-off cargo feature tree inspected; archived historical checks are zero-exit. No current native build/smoke. |
| **G13** toolchain | **NOT_RUN** | Rust/Cargo absent on review host. Archived `exits.tsv` reports all 15 checks zero; Python regressions independently **64/64 PASS**. |
| **G14** offline evidence/tamper | **FAIL** | Unmodified verifier rejects pristine archive on a numeric ULP; after diagnostic-only tolerance, two distinct fully rehashed semantically invalid archives **pass**. Two canned built-in tamper mutations do reject. |
| **G15** multi-process/docs | **NOT_RUN** | Source-matched archived restart/read/verify cases exist; no fresh multi-process Rust run. Docs need an additive independent status correction. |

Additional cross-cutting review results: **X1** strict closed operator template/generality = **scope gap** (F20-03); **X2** negative/positive release-case certification = **FAIL** (F20-01); **X3** portable numeric replay = **FAIL** (F20-02); **X4** historical Phase 17–19 evidence preservation = **PASS** for `git diff HEAD^ HEAD -- docs/evidence/phase17 docs/evidence/phase18 docs/evidence/phase19 docs/evidence/phase19-review-remediation` (empty); **X5** no false generic graph/HA performance claim = **documentation qualification required**.

## 6. Reproduction, test and evidence record

### Commands independently executed

```bash
git clone --filter=blob:none https://github.com/jasonkli21/adaptive-dataflow-engine.git
cd adaptive-dataflow-engine
git rev-parse HEAD HEAD^{tree} HEAD^
git status --porcelain=v1
git diff-tree --no-commit-id --name-status -r HEAD
git diff --check HEAD^ HEAD
git diff --exit-code HEAD^ HEAD -- docs/evidence/phase17 docs/evidence/phase18 \
  docs/evidence/phase19 docs/evidence/phase19-review-remediation
sha256sum scripts/phase20/criteria.json scripts/phase20/fixtures.lock.json \
  docs/evidence/phase20/acceptance.tar.gz docs/evidence/phase20/toolchain.tar.gz
python3 -m venv /tmp/ade20-review-venv
/tmp/ade20-review-venv/bin/pip install -r scripts/phase20/requirements.txt
/tmp/ade20-review-venv/bin/python -B -m unittest discover -s scripts/tests
/tmp/ade20-review-venv/bin/python -B scripts/phase20/verify.py \
  docs/evidence/phase20/acceptance.tar.gz docs/evidence/phase20/manifest.json \
  --repo . --tamper
```

**Results:** clean git checks and historical evidence diff; source fingerprints/criteria/archive hashes matched; 64 Python unit tests passed; pristine unmodified verifier **FAILED** at `verify.py:356` due the 1-ULP stddev variance; subsequent in-memory numeric-only repair allowed full semantic+ENOSPC-record+two built-in tamper replay. Two further independently generated rehashed mutations described in F20-01 were **incorrectly accepted** with this numeric-only repair. No source file, original archive, upstream repo, branch, PR or commit was edited or published by the reviewer. Temporary test archives were created outside the repository and discarded.

**Fresh native commands NOT_RUN** (no Rust/Cargo):

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo check -p ade-incremental --no-default-features --locked
cargo check -p ade-iterate --no-default-features --locked
bash scripts/smoke.sh
bash scripts/phase20/checks.sh /tmp/ade20-review-checks
cargo build -p ade-iterate --release --features fault-injection --locked
```

### Independent mutation reproduction recipe (review-only)

The following reproduces both F20-01 mutants **without modifying the repository**. Its in-memory stddev adjustment is diagnostic only; the implementation must fix F20-02 in committed source instead. Run from repo root with the review Python environment:

```bash
/tmp/ade20-review-venv/bin/python -B - <<'PY'
import json, sys, tempfile
from pathlib import Path
sys.path.insert(0, 'scripts/phase20')
p = Path('scripts/phase20/verify.py')
s = p.read_text()
old = '''                    assert recorded[name]["stats"][f"{mode}_{key}"] == {
                        "median": statistics.median(values), "stdev": statistics.pstdev(values)}'''
new = '''                    claimed = recorded[name]["stats"][f"{mode}_{key}"]
                    assert claimed["median"] == statistics.median(values)
                    assert abs(claimed["stdev"] - statistics.pstdev(values)) <= 1e-9'''
assert s.count(old) == 1
scope = {'__name__': 'diagnostic_only', '__file__': str(p)}
exec(compile(s.replace(old, new), str(p), 'exec'), scope)
original = Path('docs/evidence/phase20/acceptance.tar.gz')
manifest = Path('docs/evidence/phase20/manifest.json')
for file_name, mutation in [
    ('0035-mixed-bfs-0.json', 'success_to_failure'),
    ('0096-duplicate_vertex.json', 'invalid_refusal_to_partial_output'),
]:
    with tempfile.TemporaryDirectory(prefix='ade20-rehash-check-') as d:
        d = Path(d); work = d/'work'; work.mkdir()
        scope['unpack'](original, manifest, work)
        path = work/'commands'/file_name
        case = json.loads(path.read_text())
        if mutation == 'success_to_failure':
            assert case['exit'] == 0
            case['exit'] = 1
            case['stdout'] = ''
            case['stderr'] += '\nINCOMPLETE_LIMIT: arbitrary failure\n'
        else:
            assert case['exit'] != 0
            case['stdout'] = '{"vertex":999,"distance":999}\n'
            case['stderr'] = 'some unrelated failure\n'
        path.write_text(json.dumps(case))
        packed, idx = d/'changed.tar.gz', d/'changed-manifest.json'
        scope['package'](work, packed, idx)  # rehash every member + outer archive
        reopened = d/'verified'; reopened.mkdir()
        scope['unpack'](packed, idx, reopened)
        try:
            counts = scope['validate'](reopened, Path('.'))
        except AssertionError as error:
            print(f'{mutation}: correctly REJECTED ({error})')
        else:
            print(f'{mutation}: INCORRECTLY ACCEPTED ({dict(counts)})')
PY
```

Observed `success_to_failure: INCORRECTLY ACCEPTED`, count `refusals_or_kills=130`; `invalid_refusal_to_partial_output: INCORRECTLY ACCEPTED`, with original 129 aggregate count. After remediation, both must print **correctly REJECTED**, and the pristine unchanged archive must pass with *unmodified* verifier code. Neither output is an instruction to accept a changed original archive.

### Archived evidence distinguished from fresh measurements

- Source-matched original archive and manifest: 380 commands, 161 terminal reads/verifies, 129 refusals/kills, 77 roots, 146 selected cuts, 33,978 rounds, 1,099,948 delayed rows, eight selected checkpoint kill boundaries and two terminal kill boundaries. These counts are validated for the pristine archive once the numeric flaw is isolated; they are not native reexecutions.
- ENOSPC: archived test reports HFS+ image with zero available bytes and actual OS error 28, retains selected round-1 pin, later resumes. No fresh ENOSPC was performed on Linux overlay.
- Five valid pairs per work regime after one warmup. Indexed/scan compute median ratios: sparse `0.086`, dense `0.764`, resumed `0.087`; independently recomputed with unchanged per-run integer samples. Process wall median sparse indexed `28.29 ms` vs scan `26.67 ms`; cannot claim net wall speedup. No fresh matched binaries/performance run.
- Historic Phase 19 original/remediation archives are unchanged and their archived checks report zero exit. Do not substitute a newer verifier for a historical archive with a different source/fixture contract.

## 7. Scoped remediation order and instructions for the implementation session

**Do not request further prompting. Proceed autonomously within this scope.** Keep changes focused on evidence verification, necessary contracts/docs and only a minimal cyclic-runtime correction if a demonstrable new bug is found. Do not rewrite Phase 17–19 storage, Kafka paths or physical-v1 formats. Preserve original evidence and criteria. Use one focused commit once the final-source gates pass; if a required service/environment is absent, accurately stop partial with a ready-to-run command and never fake PASS.

1. **Freeze and audit before edits.** Check clean `main` ancestry, exact source and evidence hashes above. Read this handoff, active plan, ADR-0027, all original Phase 20 evidence/attempts and the authoritative PRD/architecture; inspect functions cited in F20-01–03. Copy the pristine archive/manifest only into owned temporary directories for tamper tests. Do **not** overwrite `docs/evidence/phase20/acceptance.tar.gz`, its manifest, `criteria.json`, `fixtures.lock.json`, its recorded `verification.log`, attempts, or any Phase 17–19 evidence. Confirm commit target remains baseline; if upstream changes, work on a branch and rebase review targets explicitly.
2. **Repair F20-02 first (portable numeric replay)** so a pristine archive can pass the unmodified verifier on at least two supported Python/platform environments. Preserve exact sample/median and frozen work/compute guards. Add close ULP and large forged-stat tests. Any changed verifier source invalidates the old source-inventory identity: retain old archive as historical, generate new source-bound additive verification evidence.
3. **Repair F20-01 (exhaustive independent case expectations)** using frozen case identities, expected operation/result/error, prescribed root effects and case counts independent from mutable archived process fields. Verify that the release acceptance actually runs all mandatory positives and negatives with correct semantics. Recheck correct status/error and absent side effects in the **campaign itself**, not only in the verifier. Add every required rehashed semantic attack. Ensure all tests reject for their intended semantic reason and no generic `AssertionError` masks the actual cause. Do not accept an archive merely because its own manifest self-consistently hashes it.
4. **Resolve F20-03 explicitly.** The pre-authorized closed typed region and two-primitive claim may be a defensible minimal v1, but explicitly document that the node/port descriptor is a strict admitted template and the runtime dispatch is specialized. Do not claim general user-defined physical loops, arbitrary iterative ML, nested scopes or distributed progress. If the existing Phase 20 plan's reusable-mechanism outcome requires an executable typed operator abstraction beyond the current code, implement the smallest safe reusable route, independent synthetic test and design reconciliation first; rerun every affected native and oracle gate. If this would require broad unauthorized scope, downgrade the release qualification and record a separate named follow-up rather than representing it as complete.
5. **Re-run full source-matched native campaign.** On a host with pinned Rust and Python toolchains and owned local test dirs, run the exact commands below. Confirm matched executable SHA against new source; run fault tests with reached markers (not configured-only), actual temp-image ENOSPC where supported, selected old/new pin, two different process IDs, source swap/recovery and no partial complete. If ENOSPC or Kafka/MinIO is unavailable, mark the relevant gate `NOT_RUN`, without claiming success. No tests may fill user/system disks or delete unowned roots. Run additional directed graph adversarial fixtures and reference BFS/union-find if source code changes.
6. **Revalidate default/legacy and prior archives.** Run `cargo fmt --all -- --check`, locked workspace check/all-features Clippy/tests, no-default builds, worker smoke, Python regressions, `scripts/phase20/checks.sh`, Phase 19 original/remediation historical verification with their respective verifier revisions, `git diff` invariance and `ade-physical::PlanV1` cycle rejection. Existing default behavior and selectors must be byte-compatible. Do not claim fresh Kafka/MinIO service parity without current matched services.
7. **Publish additive remediation evidence and honest report.** Put a new immutable archive/manifests/attempts under `docs/evidence/phase20-review-remediation/` or similarly named distinct path; include raw pristine and adversarial cases, manifests, criteria, compiled source inputs/executable hash, full per-test logs/exit, fault reached markers, independent verifier version and exactly reproducible commands. Include **failed attempts**. Update/add report and support matrix showing original Phase 20 archive as historical, current source and G0–G15 outcomes. Freeze new criteria before measurements; do not alter original thresholds. Keep Phase 18 partial and Phase 10/12/17/18 debt visible. Only restore `COMPLETE_BOUNDED_CYCLIC_DATAFLOW_V1` after all applicable gates independently pass.

### Required final native/evidence command set

```bash
git status --porcelain=v1
git rev-parse HEAD HEAD^{tree} HEAD^
python3 -m venv /tmp/ade20-remediation-venv
/tmp/ade20-remediation-venv/bin/pip install -r scripts/phase20/requirements.txt
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo check -p ade-incremental --no-default-features --locked
cargo check -p ade-iterate --no-default-features --locked
bash scripts/smoke.sh
/tmp/ade20-remediation-venv/bin/python -B -m unittest discover -s scripts/tests
cargo build -p ade-iterate --release --features fault-injection --locked
# Run only in new owned directories; do not overwrite original evidence.
/tmp/ade20-remediation-venv/bin/python -B scripts/phase20/campaign.py \
  target/release/ade-iterate /tmp/ade20-remediation-run
# On macOS with hdiutil/HFS+ and a disposable image only:
/tmp/ade20-remediation-venv/bin/python -B scripts/phase20/enospc.py \
  target/release/ade-iterate /tmp/ade20-remediation-enospc
bash scripts/phase20/checks.sh /tmp/ade20-remediation-checks
# Copy frozen criteria/fixture lock and the separate ENOSPC result to the
# campaign directory exactly as described by the original implementation report.
# Package and replay new evidence with --repo . --binary ... --tamper;
# also replay the immutable historical archive with the appropriate verifier.
git diff --check
git diff --exit-code c0e1c1faec96219fb79c864de1d6194bf5d8571e -- \
  docs/evidence/phase17 docs/evidence/phase18 docs/evidence/phase19 \
  docs/evidence/phase19-review-remediation docs/evidence/phase20
```

**Important:** `scripts/phase20/campaign.py` uses `work.mkdir()` and expects a nonexistent target directory. Choose fresh owned paths each run. The original `scripts/phase20/verify.py --pack` and `--tamper` workflow is in the implementation report; use distinct new archive/manifest filenames. If running on Linux, document that the original `enospc.py` targets macOS HFS+ and either implement an equivalent safe throwaway-image fixture with authorization or mark actual ENOSPC `NOT_RUN`. Treat `smoke.sh` result validity separately from comparative benchmark warnings.

## 8. Final verification checklist / stop criteria

- [ ] The **unaltered** original archive passes a **committed, unmodified**, portable verifier on at least Linux/Python 3.12 and the intended measurement host; no timing threshold softened.
- [ ] Rehashed success→failure, wrong negative diagnostic, unexpected negative stdout, omitted case, changed request, missing root/selector, bad process/fault marker and wrong checkpoint/credit tamper all fail for the intended reason; correct original cases still pass. Expected counts and case inventory come from an independent frozen specification.
- [ ] Final source inventory, lockfile, plan/oracle/criteria hashes, exact release executable SHA, compilation features and host are recorded, source-bound and checked **after** final edits.
- [ ] `CyclicPlanV1` admits only intended positive delayed feedback; old `PlanV1` still refuses cycles; true native backedge/credits/three-plus-round traces and independently calculated BFS/weak components are observed in a fresh binary.
- [ ] Native incomplete round/quota/deadline/cancel tests never select terminal success; exact footer/newline limits and source pre-/postflight remain sound.
- [ ] Separate-process checkpoint/kill/resume cases show reached markers, correct independently pinned old/new head, zero outstanding credits, byte-identical replay and reader refusal on provisional cuts. Actual ENOSPC result is source-bound if claimed.
- [ ] Fresh default/feature/legacy smoke/Clippy/workspace tests pass. No Phase 17–19 archive changes; both historical Phase 19 verifier revisions replay their own evidence.
- [ ] Five paired native-vs-scan sample sets per frozen regime with all raw samples, runnable baseline, work counts and honest wall-time observations; no fabricated speedup.
- [ ] The operator guide, ADR, plan, support JSON and additive report make a precise **closed local two-algorithm** or newly independently proven reusable cycle claim and give every G0–G15 status without converting `NOT_RUN` to PASS.

**STOP/leave partial** if any tamper remains accepted; the clean archive still fails portability; source/executable/criteria fingerprints mismatch; actual marked fault stage was not reached; a native correctness or replay test fails; a required gate is unrun/unavailable; or scope semantics cannot be reconciled. Do not push, create a PR, delete evidence or broaden phases merely to suppress a failure. If implementation changes the cyclic runtime, obtain fresh acceptance of all affected gates. The next review should independently rerun the new verifier and invent at least one additional malicious mutation not in the implementer's test list.

## 9. Outstanding debt that Phase 20 does not close

- **Phase 18** remains `PARTIAL_PHASE18_UNIFIED_ADMISSION_V1`: current-source G14 real-Kafka source-semantic parity/mutations, G15 actual worker-backed join and G16 matched 8/16 KiB diagnostic-quota campaign; fresh G0–G13/Phase 17 service reacceptance after the stream-runtime change is still required. A Phase 20-specific finite-local waiver is separate from Phase 19's and does **not** close these gates.
- **Phase 17** historical Kafka/MinIO and transactional-state source acceptance applies to its frozen runtime; current-source service reacceptance remains unrun.
- **Phase 10/12** retain physical worker elasticity and production HA/durability limitations; a finite local cyclic checkpoint does not solve them.
- **Phase 20 v1:** no dynamic graph updates, arbitrary recursive SQL/UDFs, user-composed/multiple/nested loops, distributed/async feedback, arbitrary iterative ML, external transactions, cloud/disk-loss/production HA, hostile concurrent filesystem mutation, strict process-RSS memory cap or end-to-end wall-time speedup. Record any proposed expansion as separate authorized work.

**Handoff result expected:** committed minimal fixes, independent replay with adversarial mutations, source-matched native tests where available, additive immutable evidence, honest G0–G15/report/support updates and a concise final status with exact commit/source/binary hashes. If native execution is blocked, deliver a truthful partial remediation with failing/unrun commands and no claim of final completion.
