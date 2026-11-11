# Phase 20 implementation report

## Qualification

**`COMPLETE_BOUNDED_CYCLIC_DATAFLOW_V1`** under ADR-0027 and the reconciled
Phase 20 plan. The user authorized the isolated finite-local route and its
Phase 20-specific waiver of the open Phase 18 prerequisite. Phase 18 retains
`PARTIAL_PHASE18_UNIFIED_ADMISSION_V1`. No Kafka semantic parity, native worker
join, quota campaign or current-source Phase 17/18 service reacceptance is
credited to this work.

The run used same-session implementation and review, with separate Python
BFS, union-find, and round/credit arithmetic. The user requested a single
local project commit and no subagents. The supplied plan and the unrelated
untracked `sol-medium-phase-0-bootstrap-implementation-prompt.md` were present
at baseline. The commit includes the reconciled plan.

| Supported | Refused or deferred |
| --- | --- |
| Immutable digest-pinned local Parquet vertex and directed-edge inventories | Missing, altered, oversized, null, malformed, duplicate or endpoint-incomplete inputs |
| Directed single-source BFS with UInt32 shortest hops | Absent seed, weighted paths and arbitrary recursive SQL |
| Weak components with UInt64 minimum vertex labels, including isolated vertices | Dynamic graph updates, arbitrary operator cycles, general iterative ML and PageRank |
| Closed, typed Expand → Reduce → Delay → Expand with one positive round delay | Zero-delay, extra, nested, dangling or schema-incompatible cycles |
| One finite local root and kernel-fenced writer, concurrent terminal readers, opt-in round checkpoints and explicit resume | Unpinned or changed lineage, cross-host recovery, remote durability, production HA, automatic GC and external transactions |
| Canonical converged output with exact NDJSON byte caps | Successful terminal output on limit, cancellation, unresolved credit, or a provisional checkpoint |

## Architecture and failure boundary

The logical module binds ordered file names, schemas, decoded row counts and
BLAKE3 content digests to the physical plan, algorithm, seed and iteration
algebra. The native decoder scans an attested open descriptor. The adapter
checks descriptor and path identity before decode, after scan, before
checkpoint staging, and just before selector publication. Distinct plan and
result IDs let inventory permutations yield the same canonical terminal
bytes while retaining distinct provenance.

The native executor consumes the validated delayed arc. The indexed policy
expands only active adjacency. The executable `scan_all` baseline scans
every arc each round, filters by the same active frontier and uses the same
reducer and delay. Feedback rows are transported in a bounded queue. Each
ack is checked against its round and sequence. A round cannot settle with
an open producer, queued work or a delivery awaiting acknowledgement.
BFS records a first discovery in round order. Components only replaces a
label with a smaller graph vertex ID. The fixed-point test occurs after all
credits settle and checks whether the next frontier is empty.

Checkpoint objects include the complete cut, next frontier, round, plan and
source IDs. A root anchor binds the directory and lease. Each witness
authenticates its object and previous pin. One no-clobber hard link selects
the synced witness. Old selections remain readable for chain validation
and the external pin must match the latest selected cut. An incomplete
selection leaves either the previous complete cut or the candidate complete
cut. A fresh process can resume from the matching independent receipt.
Checkpoint-free replay uses a new root and writes only the terminal cut.

A 32 MiB temporary HFS+ volume ran out of space at checkpoint round 2.
The OS returned error 28 with zero available bytes. The round-1 pin remained
valid and provisional. Freeing space allowed an exact resume. Crash tests
cover process interruption on local APFS, with separate-process kill and
restore for both primitives. This envelope assumes an honest local
filesystem and immutable input under one operator. Filesystem blocks,
allocator overhead, Arrow/Parquet internal memory and OS cache are outside
managed byte accounting. A filesystem syscall can overrun the cooperative
deadline. The CLI handles stalled output pipes with nonblocking writes.

## Source and evidence

| Identity | Value |
| --- | --- |
| Baseline commit | `c0e1c1faec96219fb79c864de1d6194bf5d8571e` |
| Baseline tree | `ae94710731c9c8656003d4faddc3f968220d58c0` |
| Final source inventory SHA-256 (180 Rust, Cargo and campaign inputs) | `01fa4216ea0af0482975f8829ca990fe3eb9dc7a8bf71d7be017fd2b03dd96b3` |
| Final acceptance executable SHA-256 | `ae2de07d49ca896df7dd58ec559cd532d0a86f0fee49ca3c24e956d94c25fad2` |
| Frozen criteria SHA-256 | `ffe6c0037b18d940d9426f66222708ebd85da7bb8c4c2da78e339f96d80647de` |
| Fixture lock SHA-256 | `cc72307084dc3032555d4e3b96745114a07b326e65471cbf654d5aaba998ce04` |

The acceptance archive SHA-256 is
`534703e4c37d6c61a86e93c8028a5b56141d5c8214ca0330ba2d9699b46f8441`. Its member inventory SHA-256 is
`6cf00fba9c8ec3d3687bd269180dcf0892526e7140a33bb399370404837a1084`.
The report and archive live in the final commit. The detached source
inventory identifies the measured executable inputs without a self-referential
commit hash. The final binary uses `--release --features fault-injection
--locked`; the operator build enables `cyclic-v1` alone. Runtime fault
switches require the acceptance build. The host was a Mac15,13, Apple M3
with 8 logical CPUs and 24 GiB RAM, macOS 27.0.1 on arm64, local APFS,
Rust/Cargo 1.99.1, PyArrow 21.0.0 and BLAKE3 Python 1.0.10.

The [raw acceptance archive](../evidence/phase20/acceptance.tar.gz) and
[member inventory](../evidence/phase20/manifest.json) include Parquet
fixtures, graph inventories, command requests, stdout/stderr, exact
process and fault observations, filesystem inode/link layouts, all
selected snapshots, source provenance, samples, and the ENOSPC supplement.
The offline verifier checked 380 process observations, 161 terminal reads
or verifies, 129 refusals or kills, 77 roots, 146 selected cuts, 33,978
rounds, and 1,099,948 delayed rows. The
[parent-binding supplement](../evidence/phase20/supplement/parent.tar.gz)
refused a modified parent digest with both the original pin and the
rehashed witness pin before successfully restoring the original witness. It rejected two semantic mutations after
the archive and member hashes were regenerated. The
[preliminary runs](../evidence/phase20/attempts/README.md) retain all earlier
campaigns and their stage of failure or supersession.

## Acceptance matrix

| Gate | Status | Source-matched witness |
| --- | --- | --- |
| G0 | PASS | Baseline, exact source/executable/fixture/criteria, host provenance and isolated Phase 18 waiver. Prior evidence unchanged. |
| G1 | PASS | Typed physical topology, strict request/plan checks, reference identities, preserved PlanV1 rejection of a self-cycle. |
| G2 | PASS | Native delay receipts on mixed, chain and dense inputs, including a 4,096-round chain. |
| G3 | PASS | Per-round Python conservation checks plus Rust missing, wrong-round and duplicate ack, open producer, pending delivery and overflow tests. |
| G4 | PASS | Independent BFS and union-find equality on empty, isolated, diamond, cyclic, reciprocal, skewed, disconnected and maximum-size graphs. |
| G5 | PASS | Native scan and offline Parquet decode agree. Missing files, schema drift, forged bytes, duplicate rows, unknown endpoints, and inode replacement after decode, during computation and before selection all refuse. |
| G6 | PASS | Fixed point separated from round cap, deadline, cancellation, disk quota and output quota. Provisional reads refused. |
| G7 | PASS | Separate process restores a zero-credit cut and matches checkpoint-free terminal bytes. Exact terminal retry does zero round work. Wrong parent, source, plan and round refuse. |
| G8 | PASS | Eight reached checkpoint kill points, two terminal kill points, reader/writer fences, concurrent readers, tamper, selector rollback, alias, short write and actual ENOSPC. |
| G9 | PASS | Real source and graph ceilings, 4,096 rounds, 1,024 credits, 131,072 arc proposals, observed decode/managed/disk/output/metadata bounds, exact and one-byte-under budgets, request/file caps and stalled stdout. |
| G10 | PASS | Exact terminal bytes under input/file permutations, batch size 1, credit window 1, reverse scheduling, cadence changes and restarts. Unsupported mode refuses. |
| G11 | PASS | Five valid measured pairs per regime after one warmup. Repeated full-arc scan work and frontier work checked independently. All frozen local guards pass. |
| G12 | PASS | Old physical, benchmark and streaming contracts, native-only dependency tree, default-off build, maintained named views, older archives and workspace regressions. |
| G13 | PASS | Locked workspace format, check, Clippy, all-features tests, no-default feature checks, Python regressions and real-worker smoke. |
| G14 | PASS | Detached verifier recomputes Parquet semantics, terminal bytes, round work/credits, selected snapshots, chain digests and timing guards. Rehashed row and credit attacks refuse. |
| G15 | PASS | Both algorithms complete run → checkpoint → SIGKILL → separate resume → read/verify, with matching fresh replay bytes and the operator guide. |

## Work and resources

Five measured pairs and one warmup were retained for each row. Compute time
covers only native round work. Process wall time includes verified admission,
storage, and command overhead. All values are local medians in milliseconds.

| Regime | Arcs examined indexed / scan-all | Indexed / scan-all compute ms | Compute ratio / guard | Indexed / scan-all wall ms |
| --- | ---: | ---: | ---: | ---: |
| Sparse 1,024-vertex chain BFS | 1,023 / 1,047,552 | 0.144 / 1.676 | 0.086 / ≤1.25 | 28.29 / 26.67 |
| Dense 256-vertex cyclic components | 57,088 / 81,920 | 0.221 / 0.289 | 0.764 / ≤1.50 | 29.23 / 29.96 |
| Chain BFS resumed at round 512 | 511 / 523,776 | 0.072 / 0.835 | 0.087 / ≤1.25 | 25.87 / 26.73 |

The scan-to-index work ratios are 1,024.0, 1.435 and 1,025.002. They exceed
the frozen minima of 8, 1 and 8 respectively. The final sparse indexed wall median was 28.29 ms versus 26.67 ms for
scan-all. The first measured candidate had a higher indexed resume wall
median of 24.82 ms versus 24.61 ms for scan-all. Its raw pairs are preserved. All per-pair compute and process
times, population standard deviations, CPU times, file I/O counters, retained
bytes and source bytes are in `performance.json` in the acceptance archive.
No production performance or billing claim follows from these samples.

The campaign reached 67,108,864 verified source bytes, 4,096 vertices,
65,536 directed edges, 4,096 rounds, 131,072 proposals in one round, 4,095
delayed rows in one cut and 1,024 in-flight credits. Maximum managed backing
storage charged was 3,534,856 bytes. The separate decoded Arrow batch peak
was 16,576 bytes. Maximum terminal stream was 166,945 serialized bytes.
The largest successful retained root used 20,182 logical bytes and the
largest observed selected root had 14 entries. Effective disk and output
caps were tested at exact measured size and one byte below. The full
allowed ceilings and accounting boundaries are in the
[operator guide](../phase-20-iterative-guide.md).

## Reproduction

From the repository root, with local loopback available to legacy tests
and Homebrew OpenSSL paths exported for the existing Kafka feature build.

```sh
python3 -m venv /private/tmp/ade20-verify-env
/private/tmp/ade20-verify-env/bin/pip install -r scripts/phase20/requirements.txt
cargo build -p ade-iterate --release --features fault-injection --locked
/private/tmp/ade20-verify-env/bin/python -B scripts/phase20/campaign.py \
  target/release/ade-iterate /private/tmp/ade20-reproduce
/private/tmp/ade20-verify-env/bin/python -B scripts/phase20/enospc.py \
  target/release/ade-iterate /private/tmp/ade20-reproduce-enospc
bash scripts/phase20/checks.sh /private/tmp/ade20-reproduce-checks
cp scripts/phase20/{criteria.json,fixtures.lock.json} /private/tmp/ade20-reproduce/
cp -R /private/tmp/ade20-reproduce-enospc /private/tmp/ade20-reproduce/enospc
/private/tmp/ade20-verify-env/bin/python -B scripts/phase20/verify.py \
  /private/tmp/ade20-reproduced.tar.gz /private/tmp/ade20-reproduced.json \
  --pack /private/tmp/ade20-reproduce
/private/tmp/ade20-verify-env/bin/python -B scripts/phase20/verify.py \
  /private/tmp/ade20-reproduced.tar.gz /private/tmp/ade20-reproduced.json \
  --repo . --binary target/release/ade-iterate --tamper
/private/tmp/ade20-verify-env/bin/python -B scripts/phase20/verify.py \
  docs/evidence/phase20/acceptance.tar.gz docs/evidence/phase20/manifest.json \
  --repo . --binary target/release/ade-iterate --tamper
```

Only the separate ENOSPC script mounts a disposable 32 MiB HFS+ image. The
verifier can omit `--repo` and `--binary` for detached semantic verification.
The previous Phase 10/12/17/18 debt and all historical gates retain their
existing status. Further work requires separate authorization.

The workspace run passed 334 Rust tests and retained 10 existing ignored
tests. All 64 Python regressions and the real-worker smoke passed. Smoke
recorded configuration and dirty-tree warnings during its one-iteration
comparison. Both historical Phase 19 archives replayed using the verifier
from their respective source revisions. The initial attempt against the
original archive with the remediation verifier is preserved separately.
