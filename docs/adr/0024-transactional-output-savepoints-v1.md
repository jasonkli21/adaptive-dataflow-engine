# ADR-0024 — Transactional output and savepoint operations v1

Status: Accepted for implementation, 2026-10-30. Authorized by the user's Phase17
request, one session, no subagents, one final local commit. Baseline 46a976d.
The preexisting untracked Phase17 plan is adopted; the unrelated bootstrap prompt
is excluded. Refines architecture §§17.3/18/21 and INV1–10 without changing them.

| Product outcome / concern | Bounded decision |
|---|---|
| Exactly-once sink | Explicit default-off transactional-state feature, separate v3 CLI/config/root. Only CURRENT3-reachable immutable epoch output is business output. No stdout receipts. |
| Transaction integration | One atomic rename selects an immutable hash-bound epoch containing state, source progress and output, linked to its parent. Sync objects/directories before selection, then sync root. Never a second checkpoint/sink pointer. |
| Writer | Kernel advisory exclusive lock on a permanent private inode; verify inode/path and expected parent on publication. Single trusted OS user/private directory. Poison on every failed mutation; reopen after ambiguous selection. |
| Identity | Job and original starts/history attestation are pinned; session UUID changes. No automatic replay, root clone adoption, v1/v2 import or corruption fallback. |
| Source | Existing native Engine/Arrow/keyed state and single Kafka demand owner. Trusted immutable uncompressed history attestation plus live identity/range checks; public metadata is not a topic-incarnation proof. |
| Bounds | Finite retained epoch count/bytes, per-epoch stage/output/state/metadata and total output/savepoint limits; count crash orphans, never automatic GC. Working hot/cold quotas remain separate, not RSS. |
| Savepoint | Named immutable self-contained closure, exact operator-retained top digest, verified before fresh-root import. No overwrite, traversal, symlink or hardlink. Export and import use the same selector discipline. |
| Pause/resume | Source demand stops, closures settle, a PAUSED epoch commits; process exits. SIGINT requests this operation. SIGKILL is failure, not pause. |
| Upgrade | Explicit canonical record codec1→2 transformation with checked window-end/tag/checksum; exact semantic equality before atomic activation. Old codec admission refuses newer state. |
| Rescale | Explicit quiescent fresh-process 1/2/4 owner redistribution through existing separate hot/cold stores; stage and verify all32 shards, commit new owner assignment. No physical workers or Kafka partition rescale. |
| Failure domain | Single local filesystem atomic rename/fsync; process-crash proof only. No disk-loss, cross-service2PC, Kafka/HTTP/S3 output exactly-once, consumer-group coupling, cloud/multi-host HA, rolling upgrade or automatic recovery/GC. |

A selected epoch is authoritative even when telemetry was lost. A reader never
uses LIST to choose a generation, never falls back, verifies the entire bounded
parent closure and rejects repeated output IDs including conflicting payloads.
Different legal source interleavings in separate lineages may produce different
payloads for the same window ID; no global merge determinism is promised.
Savepoint imports pin the output prefix; they are explicit lineage continuations,
not authorization to run two externally merged branches as one writer.

Phase16 post-fix live reacceptance is mandatory and separately source-bound. Prior
117-case evidence and failed candidates remain immutable. Same-session audit and
independent Python source/output arithmetic replace no independent reviewer.
All G0–G14 are required before completion; PRD external-sink/physical rescale and
Phase10–16 broader debt remain deferred.

## Review remediation amendment — 2026-10-30

The independent review found that a missing `CURRENT3` could be interpreted as a
fresh lineage. `ACTIVATED3` is now written and directory-synced before the first
selector publication or savepoint import. It is a monotonic refusal witness only;
it never selects state or output. With no selector, admission is allowed only for
an initialized root containing exactly `ROOT3` and `WRITER3`, with no activation
marker. Any staged object, import artifact, selector temporary or other entry is
a hard error. `CURRENT3` remains the sole visibility/commit point. Historical
roots without `ACTIVATED3` still fail closed when selector absence is accompanied
by their retained epoch objects.

The public `Transaction` handle treats a committed `PAUSED` operation as terminal.
Only an unchanged repeated PAUSED call is idempotent; data and other commits
require a fresh `open(..., resume=true)`. Public callers cannot request a
`Transition` commit directly; codec/owner changes use the validated transition
entry point.

Savepoint import now requires the operator to provide the retired predecessor's
authoritative head digest, which must equal the selected head embedded in the
savepoint. This detects an honestly supplied newer source head and makes
continuation intent explicit. The digest is an operator attestation, not a
cross-root lease or cryptographic proof that the predecessor is retired. Separate
roots still require one active continuation and must not be merged as one lineage.
Named export publishes with the platform no-replace rename (`RENAME_EXCL` on
macOS, `RENAME_NOREPLACE` on Linux); unsupported atomic semantics fail closed.

The follow-up criteria add G15 for selector loss, empty PAUSED roots, interrupted
imports and stale savepoints. The candidate5 G0–G14 evidence remains historical;
it is not evidence for these remediation changes. Current acceptance requires a
matched rebuild/regression and a fresh source-bound service campaign.
