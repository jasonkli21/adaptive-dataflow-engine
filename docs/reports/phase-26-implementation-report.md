# Phase 26 implementation report

## Qualification

`PARTIAL_PHASE26_HOSTED_CI_PENDING`. The bounded local implementation passes
the local gates. G20 requires hosted jobs on the exact commit. This work was
authorized as one local project commit. The support matrix attaches the
qualification to the commit containing the report, with a separately sealed
runtime input inventory that is checked byte-for-byte against that commit.

The start was `b364f63c27dc866a78116dea832419cd46d1ba46`, tree
`ab0e0fc401db93208f94015fb49eadd2e2751d8e`. Tracked inputs started clean.
The supplied Phase 26 plan and a separate Phase 0 prompt were untracked.
The Phase 26 plan is included here. Earlier Phase 18, 20, 21, 22, 23, 24 and
25 support matrices and evidence retain their recorded qualifications.
The isolated local feature uses the established native Phase 0–2 and
Phase 11 contracts.

The exact runtime input inventory, Parquet fixtures, raw jobs, retained
predecessor trials, local release logs and macOS/Linux executable identities
are in the [detached evidence bundle](../evidence/phase26/README.md).
The 239-file runtime input identity is
`266a68081bf7d9784232549ea5caf495cbae970277f4944dade798c79d0ff7ba`
(SHA-256). The detached verifier checked 308 subprocess receipts and
207 exact original-source results, plus the separate disk-full and Linux
UID supplements. The confirmation passed both fixed whole-sequence
guards, with 34.5% lower median time under reuse and 3.1% added median
time without reuse.

The native process was built with Rust 1.99.1 on macOS ARM64 in Cargo's
release profile with `fault-injection`. Fault controls were inactive during
timed jobs. The Linux UID supplement used an ARM64 executable built using
Rust 1.94.0 and `shared-v1`; the binary was stripped before executing the
recorded UID trials.

## Supported behavior and evidence

`ade-cache/shared-v1` accepts one externally pinned replayable local Parquet
catalog and a validated PlanV1. The cut is Scan → UInt64 Filter, optionally
followed by a Project. The native suffix supports GroupSumCount and optional
projections. Filesystem limits, recovery, private roots and commands are
described in the [operator guide](../phase-26-shared-materialization-guide.md).
ADR-0033 maps the architecture invariants and reconciles the narrowed
suffix, forecast horizon, lock model, and use of a separate strict
operator catalog.

The producer scanned 8,194 original rows and filtered them to 4,001 rows
across three files. It ran one native grouping and exited. A different
process reopened the same key and ran a second grouping using 4,001 cached
rows and no native prefix filter. The original source still passed
preflight and postflight verification. The independent PyArrow/Python
oracle checks both complete aggregate bags, aliases and nullability and
the exact ordered prefix bytes decoded from the cached Parquet. Variants
cover changed threshold, projected order/aliases, cut, file order, unknown
format/operator versions, a revoked or replaced source, physical nullable
data, integer boundaries, empty output, overflow and output caps.

The fault campaign killed child processes before and after data, manifest
and parent-directory synchronization boundaries. Restart admitted a
verified selection or rebuilt from source. Two same-key processes
contended for publication. Readers blocked exclusive eviction and expired
entry deletion. A real 32 MiB HFS+ volume returned ENOSPC (28) with zero
allocatable bytes and the committed incumbent remained readable. The
Linux supplement exercised UIDs 501 and 65534 against each other's root,
including direct denial of foreign metadata. Same-UID and filesystem
configuration are trusted. Snapshot and path verification is pre/post
attestation against that trust boundary.

Under the 330,000-byte quota, the candidate reserved 235,000 bytes.
Expected-value eviction physically removed the recently touched entry
(97,758 bytes). It retained the older higher-value entry. The archived
policy oracle independently recomputes offer values, displaced value,
chosen victims and charged bytes. A protected reader, a tied value,
infeasible staging and invalid or missing forecasts have explicit witnesses.

## Gates

| Gate | Status | Evidence |
| --- | --- | --- |
| G0 Source and authority | PASS | Baseline, build inputs, lockfile, compiler, executable hashes and commit-tree verification. |
| G1 Architecture | PASS | ADR-0033 and reconciled plan. |
| G2 Native execution path | PASS | Native scan/filter metrics in the producer and cached native suffix on the hit. |
| G3 Rewrite semantics | PASS | Independent schema, bag and ordered prefix, edge and refusal cases. |
| G4 Source and catalog identity | PASS | Strict catalog pin, native source hashes and lineage changes. |
| G5 Cache integrity | PASS | Reopen validates footer, key, exact schema/count and digest. Rehashed cross-entry substitution refuses. |
| G6 Durable publication | PASS | Sync and no-replace publication exercised across process kills and restart. |
| G7 Reader lifetime | PASS | Shared pins, contending writers, GC refusal, slot saturation and killed holder recovery. |
| G8 Security boundary | PASS | Real Linux cross-UID refusals and macOS path, mode, traversal and hardlink cases. |
| G9 Resource accounting | PASS | Encoded reservation, exact inventory lengths, output caps, cancellation and HFS+ ENOSPC. |
| G10 Source mutation and race | PASS | Same-size mutation, catalog change, inode replacement and revoked source refusal. |
| G11 Defaults and compatibility | PASS | Disabled/never controls, feature matrix, external Rust consumer and legacy workspace tests. |
| G12 Policy | PASS | Independent exact rational oracle, checked coefficient refusal, zero/missing forecasts and a tie. |
| G13 Eviction | PASS | Lower-value victim physically removed with an opposite LRU ordering. |
| G14 Audit | PASS | Versioned decisions and operator/byte observations with independent identity checks. |
| G15 Independent process and oracle | PASS | Distinct PID and downstream plan, process exit, root reopen and exact original-source results. |
| G16 Detached verification | PASS | Sealed executable/source/fixtures and eight separately resealed semantic mutations. |
| G17 Economics | PASS | Both fixed whole-sequence thresholds pass on one Apple M3 host, with a confirmation. |
| G18 Crash and fault | PASS | Reached process kills, real ENOSPC, source drift, quota, cancellation and restart. |
| G19 Local release checks | PASS | Workspace fmt, check, Clippy, all-feature tests, smoke, standalone consumer and Rust 1.94.0. |
| G20 Exact-commit hosted CI | NOT_RUN | Requires hosted `rust`, `msrv`, `phase22-interop` and `phase23-local` receipts for this commit. |
| G21 Reporting | PASS | ADR, guide, experiment, source-bound support matrix and detached evidence. |

## Local releases and earlier attempts

| Command | Exit | Notes |
| --- | ---: | --- |
| `cargo fmt --check` | 0 | Matched runtime inputs. |
| `cargo check --workspace --all-targets --locked` | 0 | Default feature surface. |
| `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | 0 | A targeted existing Phase 25 integration fixture received the line-count annotation required by this toolchain. |
| `cargo test --workspace --all-features --locked` | 0 | 353 passed and 10 existing ignored cases with loopback enabled. Initial sandbox run exited 101 on refused sockets. |
| `bash scripts/smoke.sh` | 0 | Loopback rerun. Initial sandbox run exited 1. |
| `cargo check -p ade-cache --no-default-features --locked` | 0 | Default-off cache crate. |
| `cargo test -p ade-cache --features shared-v1 --locked` | 0 | Feature-on policy test. |
| `cargo test --manifest-path examples/phase25-external-consumer/Cargo.toml --locked` | 0 | Original struct literal. The initial target-directory move interrupted an earlier attempt, which was retried. |
| `cargo +1.94.0 check --workspace --all-targets --all-features --locked` | 0 | Direct installed toolchain on PATH. |

Stable Rust used the Homebrew OpenSSL paths. A first native cache candidate
rejected its own artifact because the added Parquet key/value metadata
changed the Arrow schema. Key attestation moved to the footer's writer
identity. Other preflights exposed a limit-fixture assumption, a blocked
macOS RSS query, and a driver scoping bug before a matched run completed.
Their receipts and logs are retained. The timing campaign preserves all
samples and is described in the [experiment report](../research/phase-26-experiment.md).

The acceptance envelope is finite local native inputs under one trusted UID
and an honest filesystem. General DAG matching, nonlocal durability,
dynamic demand learning and multi-principal shared service operation remain
deferred.
