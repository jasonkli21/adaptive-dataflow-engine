# Repository-wide consolidated findings — 2026-11-15

## Review and scope

The original handoff and [independent Sol High review](independent-repo-wide-review-sol-high.md)
cover `e3ed9a9f24a954c1ce790512730b0e4fb2d8ebbf`, tree
`e11a9c60ccdb9d3ce57f5f571792d5d43733ba1a`. The original handoff arrived
from the user. It was held back from the isolated reviewer, whose first
dispatch was rejected by the platform. The same Sol High agent completed the
defensive read-only review, and substantive parent work began after its
terminal result. Tracked files were clean at entry. The existing untracked
Phase 0 prompt was outside this change.

The 28 review IDs map to 23 canonical findings below. Five are P1 and none
are P0. The review of native sorting reproduced output divergence under spill.
The local Phase 26 reproduction confirmed a hosted campaign assertion on an
otherwise successful refusal. A second campaign assertion was reached only
after that repair. No product scope, historical verdict or earlier acceptance
was promoted.

## Consolidated register

| Canonical ID and related IDs | Priority and baseline assessment | Disposition and evidence |
| --- | --- | --- |
| ADE-RW-001 | P1 REPRODUCED | **FIXED_AND_VERIFIED.** Cache admission names the replayable-source requirement. The full campaign checks exit 1 and empty stdout for this case, then proceeds through malformed PlanV1, unsupported suffix, decline-after-miss and fault controls. The new verifier checks these receipts. |
| ADE-RW-002 | P2 FALSE_POSITIVE | **DISPROVED.** On the baseline binary the exact LIMIT fixture returned exit 1, empty stdout and `invalid request: PlanV1`. It places LIMIT on unordered aggregate output. The separate Filter-after-aggregate fixture is valid native PlanV1 and refuses caching with `unsupported native cut or suffix`. The uncached execution succeeds. Both dispositions are now verified independently. |
| ADE-RW-003; ADE-RW-004, ADE-RW-012, ADE-IR-011 | P2 DOCUMENTATION_DRIFT | **FIXED_AND_VERIFIED.** [README](../../README.md) is feature-oriented with a runnable local query. [AGENTS](../../AGENTS.md), the [component map](../index.md) and [ADR index](../adr/README.md) route to scoped sources and qualifications. A pinned historical AGENTS link preserves dated authorizations. The three related IDs share this canonical disposition. |
| ADE-RW-005; ADE-IR-012 | P2 CONFIRMED | **BLOCKED_EXTERNAL** for licensing. Local Git history, tracked manifests and filename search did not establish rights to license the project and stored executables or fixtures. [Contribution and provenance guidance](../../CONTRIBUTING.md) is in place. Owner approval and provenance review remain. |
| ADE-RW-006 | P2 DOCUMENTATION_DRIFT | **FIXED_AND_VERIFIED.** The component map routes to current support records, this report attributes the baseline hosted failure to its SHA, and the archive replay uses its frozen source. No support matrix was promoted. |
| ADE-RW-007 | P2 CONFIRMED size, unproven redundancy | **FIXED_AND_VERIFIED** for new-evidence guidance. [Retention policy](../index.md#evidence) points to independent pins and durable storage for future large evidence. Historical migration remains a separate investigation. |
| ADE-RW-008 | P2 REPRODUCED allocation difference | **DEFERRED_ENGINEERING.** The CLI keeps a JSON value tree and encoded buffer outside managed Arrow state. In a 200,000-group probe, 2,690,290 bytes of JSON accompanied 82,821,120 bytes peak child RSS and 6,992,400 bytes peak managed allocation. This observation includes the full process. Characterize serialization separately and select a justified byte limit before changing the public output envelope. |
| ADE-RW-009 | P2 CONFIRMED prior API metadata, detailed policy unknown | **BLOCKED_EXTERNAL.** The handoff recorded `protected: false` and a 403 for details. Repository owner must inspect effective rulesets and choose required checks and review policy. |
| ADE-RW-010 | P2 CONFIRMED qualification debt | **DEFERRED_ENGINEERING.** The feature records independently govern current status. The remaining external campaigns, hardware studies and fresh exact-source proofs need individual source and fixture locks. |
| ADE-RW-011; ADE-IR-013 | P3 CONFIRMED floating refs, unmeasured CI cost | **FIXED_VERIFICATION_PENDING.** [CI references](../../.github/workflows/ci.yml) pin the upstream commits resolved from the existing refs, with the stable and minimum Rust lanes separate. Hosted execution and measured dependency/setup costs remain pending. No tests were removed. |
| ADE-RW-013 | P3 INTENTIONAL_LIMITATION | **ACCEPTED_NON_GOAL.** No invariant-equivalent test deletion was established. Existing native, oracle, process, portable, tamper and archived tests remain distinct. |
| ADE-RW-014 | P3 INTENTIONAL_LIMITATION | **ACCEPTED_NON_GOAL.** Keep the anchored cache path, authenticated resource reads and loopback Flight boundary within each documented trust model. |
| ADE-IR-001 | P1 REPRODUCED | **FIXED_AND_VERIFIED.** [PlanV1 validation](../../crates/ade-physical/src/lib.rs) resolves Sort ordinals to original scan columns through projections and pass-through operators. The forged ordinal refuses on deserialization. Reordered legitimate inputs validate; the native control returns value 0 at 64 KiB with five sort runs and at 256 MiB with none. Prior forged output was 512 versus 0. |
| ADE-IR-002 | P1 CONFIRMED source trace | **FIXED_AND_VERIFIED.** [The benchmark runner](../../crates/ade-bench/src/runner.rs) binds each child to exact TOML bytes and parsed physical manifest. A process test refuses a semantically equal dataset with alternate file layout and a comment-only spec change. Canonical output checks pair fingerprint and row count. The [benchmark guide](../../benchmarks/README.md) requires immutable listed Parquet bytes over the run and defines checksum placement. |
| ADE-IR-003 | P1 CONFIRMED | **FIXED_AND_VERIFIED** locally. [Historical command](../../scripts/phase26/historical.sh) uses the pinned implementation verifier and Git source. It replayed the archive's 308 receipts, 207 oracle results, 239 source inputs and eight semantic tamper refusals. An independent CI step now runs it separately from current-source smoke. |
| ADE-IR-004 | P1 CONFIRMED | **FIXED_VERIFICATION_PENDING.** Current release prerequisites require favorable economics, exact Git source inventory, the archived campaign executable, its matched ENOSPC witness and a separately pinned exact-SHA hosted receipt. Valid negative economics evidence can still be verified in detached mode. Missing source files and absent economics refuse. Final hosted receipt and fresh release/economics campaign are outstanding. |
| ADE-IR-005 | P2 CONFIRMED source race | **FIXED_AND_VERIFIED** locally. [Exchange](../../crates/ade-distributed/src/exchange.rs) linearizes gather abandonment and completion under the registry lock. A deterministic paused-persistence test checks the late receiver; real original-terminal and incomplete-attempt controls exercise both orders. |
| ADE-IR-006 | P2 PLAUSIBLE_UNVERIFIED | **DEFERRED_ENGINEERING.** Instrument complete hash/decode and descriptor identity per authorized chunk. Plan a bounded verified handle or session preserving per-chunk reauthentication and native digest proof. |
| ADE-IR-007 | P2 CONFIRMED documentation and runtime gap | **FIXED_AND_VERIFIED** for the scoped clock contract. Pre-creation observations refuse reuse. Monotone expiry and value rely on a nondecreasing operator clock, as recorded by the [ADR](../adr/0033-cross-job-shared-materialization-v1.md), plan and guide. Durable last-seen time is separate engineering. |
| ADE-IR-008 | P3 CONFIRMED | **FIXED_AND_VERIFIED.** [Cache policy](../../crates/ade-cache/src/policy.rs) ranks equal-count right subsets by sorted key order before combining halves. With equal loss and freed bytes the eight-entry witness chooses keys e and h instead of f and g. Runtime maximum inventory test retained. |
| ADE-IR-009 | P2 PLAUSIBLE_UNVERIFIED | **DEFERRED_ENGINEERING** for reader lifetime changes. [Gateway guide](../phase-22-multilanguage-udf-guide.md) distinguishes admission expiry from active-read lifetime. Verify slot release under benign stalled-consumer backpressure before changing the stream owner. |
| ADE-IR-010 | P2 CONFIRMED missing watchdog | **DEFERRED_ENGINEERING.** Benchmark child group ownership, bounded captures, timeout cleanup and platform RSS accounting need a dedicated design/test pass. |
| ADE-IR-014 | P3 CONFIRMED | **FIXED_AND_VERIFIED.** The current detached oracle streams the best exhaustive subset and bounds pressured fixtures at 16 eligible entries. The runtime still has its separate 32-entry coverage. |

Coverage audit groups ADE-RW-003/004/012 with ADE-IR-011, ADE-RW-005 with
ADE-IR-012, and ADE-RW-011 with ADE-IR-013. All original and independent IDs
are accounted for once in the table. Duplicate IDs share their canonical outcome.

## Verification ledger

Host was macOS 27.0.1 ARM64 (26A434), Rust and Cargo 1.99.1, with Rust 1.94.0
for the separate MSRV check, and offline Python 3.10.20 with
`blake3==1.0.8` and `pyarrow==24.0.0` for Phase 26. The local workspace used
the installed Homebrew OpenSSL libraries. Full diagnostics and disposable
outputs live under `/private/tmp/ade-repo-wide-2026-11-15`.

- Baseline exact-SHA hosted [run 43745505023](https://github.com/jasonkli21/adaptive-dataflow-engine/actions/runs/43745505023)
  had `rust` fail in the Phase 26 process step. `msrv`, `phase22-interop`
  and `phase23-local` succeeded in that run. Local baseline reproduced the
  failed nonreplayable assertion. First repair run reached the empty-root
  policy assertion. A miss with negative write-inclusive value correctly emits
  `miss`, then `skip`, with no prefix work or entry; the campaign and detached
  verifier now require those observations.
- Runtime and release-verifier changes are committed through
  `6cd3d77169f407940cdbc20564efbaf1dd593afe`. The revision inventory covers
  243 inputs. Public documentation and this ledger follow in a separate
  documentation commit. Local results do not provide the hosted verdict of that commit.
- Frozen cache archive SHA-256 remains
  `827a17cfb297aa9dce400700984edc53c3306b60aa52de776587b55593592148`.
  It was replayed at `9134b4745c33029e27421b4d0a4c57c604c50ef2`, with
  its original seal, executable and 239 locked inputs.

| Check | Outcome |
| --- | --- |
| `cargo fmt --all --check`, `cargo check --workspace --all-targets --locked`, `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | PASS on stable. |
| `cargo check --workspace --all-targets --locked` with Rust 1.94.0 | PASS with separate target directory. |
| `cargo build --workspace --all-features --locked` and `cargo build --workspace --locked` | PASS. The macOS linker emitted compact-unwind warnings. |
| `cargo test --workspace --all-features --locked --no-fail-fast` | PASS from Cargo output with loopback permitted, 362 passed and 10 ignored across 72 runners. The sandboxed pass stopped with permission errors on 29 socket tests. The rerun's wrapper status write hit a read-only zsh variable after Cargo completed. All test runner summaries were green. |
| `bash scripts/smoke.sh` after the default build | PASS with loopback enabled. The initial sandboxed run reached the distributed step and stopped. Covers native spills, two-worker aggregation, join and independent output checks. |
| `cargo test --locked -p ade-cache --features shared-v1` and `--features fault-injection` | PASS, seven policy tests on each feature. A separate fixture checked pre-creation refusal and restored-time hit. |
| `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p 'test_*.py'` | PASS, 80 tests. Workflow YAML parsed with all four jobs. |
| `cargo build --locked -p ade-cache --features fault-injection --bin ade-cache` then `python scripts/phase26/campaign.py BINARY NEW_DIRECTORY --no-economics` | PASS, 149 process receipts using the current native binary. Replayability, malformed LIMIT, unsupported suffix, declined write, distinct successful hits and process kill and reopen cases reached. |
| `python scripts/phase26/verify.py NEW_DIRECTORY --seal` then `--pin PIN`, and `python scripts/phase26/tamper.py NEW_DIRECTORY` | PASS, 55 independent result checks and eight semantic tamper refusals. Campaign seal `4b1e70d52561d0fd34a98f1dd2779c271ab26d892b421373fad1761c23490e54` is local, produced in this run. Runtime binary SHA-256 `2075d75c71ce9dac16fb3cbdc6674808ae12c0b59f3b2f147ae9b0e217c0aebb`. |
| `python scripts/phase26/enospc.py BINARY SCRATCH MOUNT` on a new 32 MiB HFS+ image | PASS. Matching binary reported kernel error 28 at zero available bytes with no stdout. Independent oracle checked six receipts and four jobs, including the old hit and fresh fill and hit. Image detached. |
| `bash scripts/phase26/historical.sh` | PASS at frozen SHA and archive pin. Verifier, source comparison and tamper controls completed. |
| `python scripts/phase26/source_inventory.py REPO INVENTORY --compare --revision SHA`, and current `verify.py ... --release` negative | PASS for full locked source, refusal for a rehashed inventory missing one source input, and refusal for a correctly sealed campaign without economics. Positive release remains pending. |
| README local build, fixture, ordered query and independent aggregate validation | PASS, five query rows and validated aggregate. Relative links and explicit anchors checked. `git diff --check` passed. |
| `bash scripts/phase25/portable-smoke.sh target/debug/ade-bench` after building with `filter-kernels-v1` | PASS, scalar and Arrow output match legacy and unsupported AVX2 refuses. |
| `cargo test --manifest-path examples/phase24-external-consumer/Cargo.toml --locked`, `cargo test --manifest-path examples/phase25-external-consumer/Cargo.toml --locked`, `cargo bench --locked -p ade-bench --bench smoke -- --test` | PASS, both standalone consumer tests and Criterion's blake3-4k smoke. |
| Phase 24 no-benchmark process, detached and tamper campaign | PASS as scoped smoke with pinned Python packages. The detached verifier checked 51 observations and 14 attempts. Ten semantic tamper cases refused. |
| Phase 23 six-case hosted-workflow local smoke | PASS with loopback permitted. Auth, preemption, overlap, commit race and two native crash cases completed. |
| Phase 26 release economics, Linux cross-UID and hosted checks on final commit | NOT_RUN. Release needs fresh matched economic samples, a Linux witness and independently retrieved exact-commit hosted receipt. Kafka, MinIO, Phase 21 services and live Python/Java reacceptance are outside these checks. |


## Remaining work, in order

1. **P1 release blockers (ADE-RW-001, ADE-IR-004).** Obtain required hosted
   checks for the exact final commit and an independent CI receipt bearing
   that SHA. Run the predeclared Phase 26 release campaign with matched native
   binaries, Linux cross-UID, macOS ENOSPC, an independent seal and the
   unchanged economic thresholds. Keep a negative outcome if the criteria
   fail. Current Phase 26 status remains partial.
2. **P2 owner actions (ADE-RW-005/009).** Verify copyright and permission
   for code, client and bundled binary or fixture distribution before adding
   a project license. Resolve branch rules in repository settings and verify
   all four required jobs from an unprivileged review context.
3. **P2 live reacceptance (ADE-RW-010).** Start from the first unresolved gate
   in each support matrix. Use the matched Kafka/MinIO, PostgreSQL/Flight,
   isolated resource cohorts or physical ISA platform as appropriate. Gate
   completion on source, executable, fixture and independent proof. Preserve
   existing source-bound acceptances and waivers. Extend the recovery matrix
   with real stale-attempt, worker-kill and native publication fault cases.
4. **P2 resource and lifecycle (ADE-RW-008, ADE-IR-006/009/010, ADE-IR-007
   broader clock scope).** Begin with representative high-cardinality
   JSON/RSS observations, a multichunk decoder/byte counter, a benign paused
   Flight consumer and a sleeping benchmark child. Require unchanged result
   schemas, fail-before-result where promised, descriptor continuity and
   auth revocation, reader slot release, bounded capture/group teardown and
   unchanged timing placement. For strictly monotone expiry across clock
   regressions, first define the persisted time authority and test restart,
   expiry and backward re-entry without reinterpreting current manifests.
5. **P3 maintenance (ADE-RW-007/011/013).** Inventory historical evidence
   references and independent replay before proposing migration. Time
   hosted steps before changing Python environments, compilation tiers or
   gates. Verify new action pins as separate dependency changes and revisit
   the upstream deprecation of Setup Java v4 in a targeted update.
   Use actual code ownership and test-backed invariants before extracting
   large modules. General SQL, physical elasticity, distributed feedback,
   production HA and broader remote or multitenant execution require
   separate scoped authorization and evidence.
