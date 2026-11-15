# ADR-0033 — Bounded cross-job shared materialization v1

Status: Accepted for implementation. Release qualification is tracked in the Phase 26 support matrix.

## Native cut and lineage

The default-off `ade-cache/shared-v1` crate uses only the local Phase 11 PlanV1 and native runtime, the Phase 0 benchmark discipline and the Phase 1–2 local execution substrate. The operator independently pins the raw BLAKE3 of a local v1 catalog. The catalog lists ordered files, exact Parquet byte and row counts, digests, non-null field types and a logical snapshot fingerprint. The native binding must match the resulting Source exactly. The Phase 21 connector or Phase 23 authentication paths have no role in this contract.

The supported cut is Scan → UInt64 less-than Filter → optional positional Project over non-null UInt64/Int64. The suffix is GroupSumCount and optional projections. Native aggregate results have bag semantics. Sorted and limited suffixes are outside this cut because original scan tie-breaker validation and rebatching/early termination would require additional equivalence evidence. Native source and output bounds still apply. Parquet preserves the prefix's typed rows and their order across native batches and original files. Rebinding remaps only the suffix's node references and preserves schemas, properties, output row and byte caps. The native engine validates both derived graphs and still rejects overflow.

The BLAKE3 key domain versions native operator and Parquet behavior. It includes the canonical prefix and a fixed-size UID/device/root-inode namespace. The cache manifest and Parquet footer both bind the key. File bytes, artifact digest, exact schema, rows, decode bounds and key binding are verified before exposing a result. A hit executes original native catalog preflight, cached suffix preflight/execution/postflight, then original catalog postflight. The original Parquet is read and decoded for verification. The hit omits its scan and filter *operators*. The shared native streaming entry point uses the same output cap calculation and source verification as collected execution.

## Local ownership and transactions

Linux and macOS use descriptor-relative storage operations, symlink refusal, a kernel flock, no-replace publication and directory fsync. The root must be effective-UID-owned, mode 0700 without access-granting ACLs, with safe ancestors. Root-owned sticky directories such as canonical /private/tmp are permitted. Files are single-link mode 0600. Eight shared-read slots bound concurrent cache readers. One exclusive root pin serializes staging, collection and eviction. A pin covers the full native suffix and source postflight. Lock waits are finite. The immutable local catalog and its paths are operator-owned or root-owned. Native file access remains path-based, with descriptor-relative checks and pre/post file identity and data verification. The operator must keep the inputs and root stable throughout each job. The deployment trusts the same UID, root and filesystem, including ACL/mount configuration. Hashes are integrity checks under that trust model.

Staging uses one private directory. Encoded writes stop before the reserved limit. The manifest and Parquet file are synced, read back and checked by native execution in staging before atomic no-replace publication and parent directory sync. A reopened hit syncs the parent again. Eviction first moves the selected directory to a private trash name and syncs the root, then removes it. A subsequent exclusive operation cleans the one abandoned stage and trash directory. A failed cache fill may leave earlier quota evictions committed. A killed process releases kernel locks. The cache tolerates bounded lost work and requires a local filesystem with the documented rename/fsync behavior.

## Policy and accounting

The policy evaluates operator forecasts in integer microseconds, reuse milli-uses and survival basis points. Restoring includes original verification, decode and cache read. Admission debits write/sync, rent and invalidation. Retention excludes the sunk write cost. Forecasts are fixed to a publication horizon and may be reassessed by new admission after expiry. Greedy eviction orders eligible entries by remaining value and digest, and requires the candidate's net value to exceed the sum of displaced positive value. Forced uncalibrated entries are eligible only at expiry. An exclusive root pin removes reader races from the decision. Logical charges are actual file lengths plus a single configurable conservative stage reservation including 4 KiB for metadata. Block allocation, directory metadata and process RSS have separate accounting. Quota never authorizes a write past the reserved limit.

## Architecture impact

- INV-1 and INV-8. The pinned replayable catalog and canonical prefix describe logical lineage without process/job identifiers.
- INV-2 and INV-7. The local immutable copy is independent of execution placement and has local filesystem durability.
- INV-3 and INV-9. Pure policy acts over admitted metadata. Disabled and never use plain native execution.
- INV-4 and INV-5. Source/plan/output, reader counts, staging and inventory have explicit caps. Job receipts name real native operators and distinguish estimated costs from observed work.
- INV-6 and INV-10. Runtime stays frontend-independent and reuses native lowering, Arrow, Parquet and filesystem primitives.

General DAG matching, remote or multi-user cache services, arrangements, dynamic demand learning, and distributed durability remain deferred.

An uncalibrated or nonpositive adaptive forecast declines before cache lookup.
That job executes the original plan without opening a root. Forced always and
positive adaptive requests require an initialized private root.
