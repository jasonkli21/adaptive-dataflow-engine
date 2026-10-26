# ADR-0018: ADE physical IR and bounded frontend v1

Status: Accepted; implemented bounded v1. Final verification is in the Phase 11 report.
Date: 2026-10-25. Baseline: 4eef1b802ad9034fbd1e08fa1cc074898d6f13b8.

The user explicitly requested implementation of Phase 11 using the attached
prompt, in this session, without subagents and with one final commit. This is
the plan §0 option B ordering exception. Phase 10 remains PARTIAL: all six
frozen timing guards failed, and physical membership is fixed. Its evidence,
thresholds and scheduler are unchanged. No Phase 12 work is authorized.

Refines architecture §§5,7–9,20,25–26; preserves INV-1–10. ade-physical owns a
strict serde version-1 graph with portable schemas, ordered stable IDs, source
fingerprints independent of absolute paths, semantic distribution, ordering,
boundedness and replayability. Native crates depend on it; it has no runtime,
DataFusion or transport dependency. ade-frontend alone owns SQL binding using
public DataFusion 55.2.0 logical planning APIs. No DataFusion execution is used
for ADE queries. The independent benchmark oracle is untouched.

The native adapter validates the complete graph and all bound files before
launch, lowers to existing operators/jobs, checks fingerprints again before
returning output, and applies exact schema adapters. UInt64 native counts are
checked into SQL Int64; SUM has SQL nullable schema but non-null group values.
Aggregate accumulation remains checked i128 with checked Int64 final output;
this intentional native overflow contract refuses values outside Int64 rather
than adopting any oracle wrapping. NULL input, casts, arbitrary expressions,
DISTINCT, unordered LIMIT, non-inner joins and arbitrary distributed DAGs refuse.

The SQL/Rust builder target the same IR. Local projection/filter/group/sort/limit,
static unfiltered grouped sum/count and the existing bounded two-source join are
the intended matrix. Exact admitted shapes are validated before side effects.
Sort SELECT requires exactly key,row_id,value_i64 (column order may vary),
ordered by key,row_id ascending with strictly increasing source row_id verified
in constant space;
source order alone is not an ordering guarantee. Distribution describes data
semantics, not spill/direct/object retention; execution configuration remains
explicit and native. No protocol or result-schema changes are needed.

Source admission is bounded to registered manifest-listed local Parquet tables:
input or ade_left/ade_right. Catalog paths are canonicalized under the dataset
root; content hashes, row counts and non-null schemas are checked. Local files
must remain immutable throughout execution; this is not transactional isolation.
Plan/SQL/source/output caps and cooperative cancellation are explicit. Managed
memory excludes existing decoder/kernel/allocator transients and is not RSS.
The report distinguishes the completed bounded contract from broader deferred
SQL/runtime work, with explicit refusals and evidence.

Implementation refinements: the versioned IR is a bounded topologically ordered
tree (64 nodes), not a general shared DAG. Each source and every non-root node
must have exactly one consumer. Output aliases are explicit projection nodes;
SQL SUM is nullable Int64 and COUNT is non-null Int64. Source schemas are exact,
all-non-null standard events/join schemas at the frontend. A table function
registry is emptied and URL table discovery is never enabled before SQL binding.
DataFusion's public unoptimized logical planner retains some ON equalities in a
typed join filter; the translator extracts exactly one opposite-side column
equality and refuses additional conditions. No general optimizer is introduced.

Source registration verifies physical files; generator provenance is bound by
the exact manifest-byte hash, with no promise to regenerate logical contents.
Native preflight rechecks schema/rows/nulls and file fingerprints before launch,
then hashes again before returning output. Limits: SQL16KiB; plan256KiB;
64 files/1m rows/64MiB physical bytes per source; 1MiB footer;16MiB declared
uncompressed row group; output<=1m rows/64MiB accounted Arrow buffers plus batch
metadata; local input/output batches<=8192. Existing decode/allocator transients
remain outside managed state. Source verification uses bounded byte buffers and
1024-row batches. Sort checks strictly increasing row_id rather than retaining
an unaccounted uniqueness set. These conservative frontend refusals preserve
architecture INV-4/6 and do not narrow legacy LocalPlan/job entrypoints.

Static aggregate options retain the native five exchange modes and accepted
recovery/copy admission; Phase 10 leased scheduling and Phase 8 sampled topology
are refused through this frontend. Join options retain observed/forced strategies
and hash/grace with existing spill-only/no-recovery restrictions. Wire12/2/hash1
and benchmark result schema1 are unchanged. Core execution never depends on the
frontend. No historical timing or independent oracle code is edited.

## Review remediation amendment (2026-10-26)

Distributed Phase 11 output limits are applied while the native runners stream
committed gather IPC batches. Each batch is first adapted to the declared plan
output schema, then its row count and `get_array_memory_size() + 128` accounted
bytes are checked before it is retained. Aggregate and join use the same bounded
collector; legacy coordinator/join APIs retain their original unbounded result
collector semantics. An over-limit execution tears down the active gather and
returns no batches. The collector still allows one decoded/adapted batch beyond
the remaining budget transiently, and it is not a process RSS cap.

Manifest registration now recomputes the canonical schema-definition hash for
the two allowlisted v1 schema names and rejects a supplied mismatch. The logical
fingerprint remains asserted by, and bound to, the exact manifest-byte hash; the
frontend still does not regenerate dataset rows from generator parameters.

The `ade-query --timeout-ms` budget starts at CLI invocation and is shared by
manifest registration, SQL planning and native execution. Verification checks it
between bounded reads/decode batches, and the planner/execution receive the
remaining duration. Individual OS calls and synchronous planner work cannot be
preempted; the CLI checks immediately after those calls return. The reusable
catalog registration method keeps its independent 120-second default and also
exposes an absolute-deadline variant. `Catalog::execute_until` accepts that same
deadline, so graph validation and catalog binding do not reset the CLI budget.

The real-worker frontend integration test resolves `ade-worker` beside Cargo's
`CARGO_BIN_EXE_ade-query` in the active target directory. Workspace tests build
the worker automatically. Running only that test target requires explicitly
building `ade-worker` into the same target directory first; the test fails with
the exact missing path and build command instead of falling back to another
target directory. That focused worker build must enable `research-hooks`, which
is active in the frontend integration-test dependency graph through `ade-bench`;
the worker and test process must share that wire shape.
