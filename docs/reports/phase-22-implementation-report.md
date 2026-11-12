# Phase 22 implementation report — 2026-11-11

**Release** `COMPLETE_BOUNDED_MULTILANGUAGE_UDF_PLATFORM_V1`

Phase 22 provides a default-off, finite local Flight SQL gateway, installable
Python client, Java 17 Flight SQL client, versioned shared query request, and
operator-registered Arrow batch functions. Python and Java queried the same
live service and matched an independent oracle. The mapped fixture ran three
worker invocations on 521 native scan rows. The native filter and projection
returned 247 rows with the map and 84 rows without it.

## Source and support envelope

The implementation base was `31b627fa0c1f1a0da7822acb9f3dfaa839729cf3`, tree
`e6d721c1150899e51d733e0fdd9365e9bca5d211`, on `main`. The initial checkout
contained the supplied untracked Phase 22 plan and an unrelated untracked
Phase 0 prompt. Candidate 6 measured the final working-tree runtime files
before the one project commit. Its Git status and exact hashes for 221
runtime inputs are in `environment.json` and `inputs.json`. Linux matched the
same input inventory. This makes the attribution independent of the
self-referential problem of storing a final commit SHA in its own tree.
The baseline CI run later completed successfully. That run is predecessor
build and regression evidence only.
[Baseline receipt](../evidence/phase22/baseline.txt)
[Baseline CI](https://github.com/jasonkli21/adaptive-dataflow-engine/actions/runs/43118673934)

| Identity | SHA-256 |
|---|---|
| Final runtime input inventory | `cd66def47aaa5662132b0b4a19fb0eac40a46bc6a07269f8e88949b59c4a5217` |
| macOS gateway | `725e269030381904b5ed159c84825ebbbb22c3109b44b4d66b608c6bac01684c` |
| macOS native probe | `a2d7cf449e4b379e853e2bdc5d87c04a44c64f1d2059e2218e2e04d3664c24a9` |
| Linux gateway | `961e90361e7350842f5ee4124f7a623d07e95fb577c50300d21e36f1d6471d2d` |
| Linux native probe | `a6c7dbc45a43eff2281e76e99fd4e78774fd4dc494d5df4abcbed018a2295a69` |
| Criteria | `9ae1edb9ccbee59cc7ec9caa92977100ec2b890a06e83d6ece2b83dcd0550fe6` |
| Fixture lock | `92b06a6a1a2c8759ba1e5c8b28e1438ab4d3abd092541816c92a1b11d33f85e2` |
| macOS detached seal | `53e39d2ab7e3082eabb7f1ec76b819969ded982b580816313f5fbeed4e62fd74` |

Both gateways used Rust 1.99.1, Rust Arrow 59.3.1, PyArrow 24.0.0 and Java
Arrow Flight SQL 19.0.0. The macOS arm64 host was a Mac15,13 with eight
logical CPUs and 24 GiB RAM. The macOS Python was CPython 3.10.20 and Java
was OpenJDK 17.0.20.1 with Maven 3.9.11. The local Linux job ran on Debian
Bookworm aarch64 in OrbStack with CPython 3.11.2 and OpenJDK 17. It used
Docker's Rust image digest
`sha256:365468470075493dc4583f47387001854321c5a8583ea9604b297e67f01c5a4f`.
Class files, JVM dependencies, Python package, source files, fixtures and
executable digests are in the platform receipts. Linux shares this physical
host. The release is bounded to the local trusted-operator deployment.
[Candidate 6](../evidence/phase22/candidate-final.tar.gz)
[Linux runner logs](../evidence/phase22/linux-runner.tar.gz)

## Implementation

- WP0–WP1 froze the baseline, criteria, fixture hashes and independent oracle
  before the acceptance candidates. ADR-0029 preserves PlanV1 wire and digest
  semantics. The strict language-neutral request compiles filter and
  projection over an authorized source to native PlanV1 and binds the function
  as a separate, nonreplayable query identity.
- WP2 added a schema-preserving pull adapter in `ade-exec`. The adapter wraps
  the verified local scan, checks its returned batch and hands it to native
  downstream operators. The gateway owns ABI framing, interpreter and script
  pins, process groups, frame and decode validation, cancellation and reaping.
  The runtime adds no Python, Flight or frontend dependency.
- WP3 added a real Apache Arrow Flight SQL endpoint in `ade-gateway` with
  read-only statement and SQL Info handling. It fully encodes verified results
  before issuing random session-bound tickets. Readers reuse retained frames.
  The path has zero result spool bytes. Spill-disabled native collection uses
  lazy temporary storage so it performs no spool initialization.
- WP4–WP5 added the versioned Python package and Maven Java SDK and example.
  Python uses official Flight transport with standard Flight SQL protobuf
  commands. Java uses Arrow FlightSqlClient. The shared ADE command carries
  strict JSON with decimal-string UInt64 thresholds. Both decode Arrow with
  exact column ordering, nullability, signed and unsigned widths, UTF-8 and
  timezone-free millisecond timestamps.
- WP6–WP8 exercised failures against real services and subprocesses, kept
  prior attempts, added detached evidence verification and wired Linux CI
  interoperability. There was no separate implementation or review agent.
  The row oracle and offline verifier are independent of ADE result production.
  [ADR-0029](../adr/0029-bounded-flight-sql-and-arrow-udf-v1.md)
  [Operator guide](../phase-22-multilanguage-udf-guide.md)

The accepted contract is one immutable, operator-controlled EventV1 Parquet
catalog and one trusted schema-preserving Python function that may modify
`secondary`. It covers one local query and worker at a time. The child
environment is scrubbed. The parent kills and reaps failed work, and the
bootstrap supervises loss of the gateway parent. Linux constrains address
space and both Unix platforms apply a CPU cap. macOS worker RSS and native
process RSS are observations. The native memory ledger and retained wire bytes
are independently capped. The deployment trusts the Python installation,
the source directory and the local host. Optional WASM is
`DEFERRED_OPTIONAL`. Remote listeners, arbitrary code upload, distributed or
unbounded UDFs, external transactions and production multi-tenancy are outside
the accepted surface.

## Acceptance

The 54 observed cases, native/Flight receipts and 48 descriptive samples are
in the sealed macOS candidate. Positive row and schema checks include no rows,
one row, exactly 256 rows and three batches. Java and Python agree on full-range
UInt64, large Int64, UTF-8, negative timestamps, signed zero and infinity.
The two plain request routes share a PlanV1 digest. A wrong-row worker
completed the first native batch and was refused during the second. No
ticket was returned. Separate cases exercise encoded and decoded overflow,
malformed IPC, dictionaries, compression, artifact/source drift, symlink
escape, timeout, disconnect, process termination, ticket expiry, replay
across session/restart, capacity and native result overflow.

The HFS+ test reached real ENOSPC on 1 MiB, 4 KiB and 512-byte writes with
zero available allocation blocks. The gateway then returned the exact
oracle result with that filesystem as TMPDIR and zero spool bytes. The
supplementary Linux full-campaign attempt reached the other fault cases but
stopped at its oversized 6.3 GB container shared-memory mount. Linux's
separate interoperability and workspace jobs completed. The canonical ENOSPC
qualification is the macOS disposable filesystem proof.
[Final candidate](../evidence/phase22/candidate-final.tar.gz)
[Linux runner logs](../evidence/phase22/linux-runner.tar.gz)

| Gate | Status | Proof in the archived receipts |
|---|---|---|
| G0 | PASS | Matched runtime inputs, executable hashes, fixtures, environments and baseline |
| G1 | PASS | SQL/plan digest parity, strict request admission, authorized source binding |
| G2 | PASS | Native scan, three maps, downstream filter/project and independent 247-row oracle |
| G3 | PASS | Valid multi/empty/single/boundary results and typed ABI/IPC refusals |
| G4 | PASS | Exact registered function identity, stale artifact, bad ABI/digest and path refusal |
| G5 | PASS | Installed Python SDK, real SQL/plan/UDF, timeout classification and refusal tests |
| G6 | PASS | Maven compile and live Java SQL, plan, mapped and numeric-boundary parity |
| G7 | PASS | Standard Flight SQL statement, schema, SQL Info and ticket DoGet on macOS and Linux |
| G8 | PASS | Repeat read, 15-second expiry, forged/stolen tickets, restart and slot exhaustion |
| G9 | PASS | Wrong rows on batch two, worker exit/hang, client disconnect, gateway kill and actual ENOSPC |
| G10 | PASS | Native/IPC/result bounds, active query contention, ticket counters, observed RSS and zero spool |
| G11 | PASS | Loopback, authentication, read-only surface, path/source isolation and redacted errors |
| G12 | PASS | Existing PlanV1, workspace and legacy correctness regressions on both platforms |
| G13 | PASS | Independent fixture oracle, exact Python/Java/native results and typed refusal parity |
| G14 | PASS | Detached current-source match, historical replay and 12 fully rehashed mutation refusals |
| G15 | PASS | Local Linux job, macOS workspace checks, Rust 1.94 checks, client builds and support artifacts |
| O1 | DEFERRED_OPTIONAL | WASM |

The previous parallel macOS workspace attempt exited 101 after a legacy
join-pressure test expected a source-drift diagnostic. The full serial
macOS run and the standard Linux run each passed 338 tests with 10 inherited
service-dependent tests ignored. Format, all-target check, all-feature Clippy,
no-default checks, legacy smoke, 72 repository Python tests, SDK and verifier
unit tests, Java build and MSRV checks exited 0. The first supplemental
all-feature MSRV run could not find OpenSSL through CMake. It passed after
setting the Homebrew OpenSSL root and search paths. The final Linux
interoperability job and detached verifier ran locally through OrbStack.
The final commit's GitHub-hosted workflow has no run in this evidence.
[macOS command logs](../evidence/phase22/macos-checks.tar.gz)
[Linux command logs](../evidence/phase22/linux-runner.tar.gz)

Candidates 1–3 stopped on streamed refusal or fault-injection harness
errors. Candidate 4 exposed a PyArrow deadline status race, which the Python
SDK now handles. Candidate 5 reached ENOSPC on a large write while the
filesystem still had free blocks, and the strict offline verifier refused
its proof. Candidate 6 reached the original disk-full requirement without
relaxing a criterion. The archive preserves the previous output and
diagnostic logs.
[Predecessors](../evidence/phase22/predecessors.tar.gz)
[Candidate 4](../evidence/phase22/candidate4-failed.tar.gz)
[Candidate 5](../evidence/phase22/candidate5-unqualified.tar.gz)
[Tamper refusals](../evidence/phase22/fully-rehashed-tamper.jsonl)

## Descriptive measurements

All four paths ran 12 samples on the same 521-row fixture. Milliseconds below
use a median and nearest-rank p95. The first sample includes cold path costs,
and the macOS campaign overlapped Linux build work. The native probe times
execution before its separate Arrow witness write. The language client paths
include transport and result decoding. The mapped plan performs different
work and emits more rows than the plain baseline.

| Path | First ms | p50 ms | p95 ms | CPU observation |
|---|---:|---:|---:|---|
| Direct native plan | 33.82 | 18.98 | 33.82 | 781.70 ms child user + system |
| Direct native plan with UDF | 4065.97 | 1291.49 | 4065.97 | 11798.01 ms child user + system |
| Python Flight SQL | 138.60 | 30.55 | 138.60 | 60.64 ms Python process CPU |
| Java Flight SQL | 1149.38 | 45.94 | 1149.38 | 4141.04 ms JVM user + system |

The mapped gateway receipt observed 12,034 managed native bytes at peak,
37,408 input IPC bytes, 35,096 output IPC bytes, 38,354,944 bytes of
worker-reported high-water RSS, and 13,264 encoded Flight bytes. Its ticket
store held 39,184 wire bytes after that publication. Main gateway peak RSS
was 44,548,096 bytes. The process statistics do not isolate every allocation
or copy. No native spill or result spool bytes were written. The timings are
descriptive and support no performance claim.
[Raw sample and resource receipts](../evidence/phase22/candidate-final.tar.gz)

## Predecessors and independent reproduction

Phase 18 remains `PARTIAL_PHASE18_UNIFIED_ADMISSION_V1`. Phase 20 remains
`PENDING_INDEPENDENT_REACCEPTANCE`. Phase 21 remains
`PARTIAL_REMEDIATION_PENDING_REACCEPTANCE`. Phase 22 uses local native
manifest bindings and did not borrow any connector or state acceptance.

Extract the final tarball, use its independently retained seal digest, and
run `verify.py --mode historical-archive` for offline semantic replay.
For `current-source`, supply the checkout and both exact campaign executables.
A rebuilt binary with a different digest requires a new matched campaign.
Run `tamper.py` on the archive to reproduce the verifier refusals. Rebuild,
live campaign and ENOSPC setup are documented in the guide. The verifier
and fixture oracle are under `scripts/phase22/`.
[Reproduction instructions](../phase-22-multilanguage-udf-guide.md#reproduce-the-acceptance-campaign)
