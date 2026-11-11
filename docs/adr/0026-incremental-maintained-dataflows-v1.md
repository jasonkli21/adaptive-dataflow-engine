# ADR-0026 — Incremental maintained dataflows v1

Accepted for bounded local Phase 19 on 2026-11-11. Refines architecture §19 and applies INV-1, INV-3–6 and INV-8–10. The user waived the Phase 18 acceptance dependency. Phase 18 retains its existing qualification.

## Semantic contract

An independent maintained plan pins the native Parquet file inventory, a namespace and an inclusive signed threshold for the filtered view. It derives a bounded, deterministic, replayable epoch computation with local committed delivery. The native scan accepts exactly two non-null columns, `key: UInt64` and `value: Int64`, and verifies byte fingerprints, schemas, decoded row counts and postflight identity. Delta batches bind the plan, epoch number and parent chain. Each epoch normalizes signed weights by row before applying changes. Retractions precede insertions internally, with widened arithmetic and checked final multiplicities, counts and sums. Both views emit keyed `sum: i64` and `count: u64`. Empty groups disappear. The filtered view includes values greater than or equal to its threshold.

One arrangement holds weighted rows, split across 128 fixed shards by key. A generation references immutable shard objects. Each key record holds its weighted values and both view aggregates. One traversal of its normalized delta updates both aggregates. Incremental execution decodes and rewrites only touched shards and recomputes no unaffected aggregate. The admission step verifies the parent object checksums without decoding its unaffected shards. Full replay uses the native ADE finite scan with a synchronous bounded batch sink. It verifies the same epoch sequence, including intermediate cuts.

## Commit and refusal

An existing private directory is bound to its device, inode, lease inode and plan. It uses a kernel file lock and append-only `g-XXXXXXXX` generation directories. A staged manifest is synced and hard-linked as an independent `w-XXXXXXXX` witness before atomic no-replace rename of the entire directory. The root directory is synced before acknowledgement. Readers take a shared lease and verify the selected generation and its referenced objects. Interrupted witnessed stages can resume only at the exact independently pinned chain after revalidation. Prior generations and witnesses remain available for integrity checking. A stage with no durable witness can be cleaned under the writer lease.

The caller pins the expected chain for each command. This detects a rollback of all local evidence when the requested head is independently retained. It is impossible for a single local filesystem to attest against arbitrary coordinated deletion and replacement of every trusted byte. Use one operator-owned active root, trusted immutable source history, local rename/fsync and retained expected chain. Cloned or foreign roots, missing witnesses/selectors, symlinks and unexpected root contents refuse. Whole disk loss, remote/physical state, generalized differential joins, Kafka CDC, external transactions, automatic GC and cycles are outside this contract.

## Policy and accounting

The pure chooser ranks at most three runnable choices after source, checkpoint and resource admission. It reports checked work estimates separately from native counters. Restore verifies and serves the existing exact target. Incremental updates a compatible selected parent. Full replay builds the target independently and publishes it, or verifies equivalence against the exact existing target without replacing it. A fresh root allows full replay to any bounded target with complete inputs. Forced and disabled policy controls pass through the same legality checks.

Hot entries, encoded shard state, disk occupancy, decoded scan batches, source and metadata caps, output bytes and operation deadline are finite. The hot bound charges keyed groups, distinct values and normalized delta rows. Sparse operation retains one decoded shard at a time. Immutable objects avoid rewriting unchanged shards. Quotas count logical file bytes, with fixed file and generation limits for filesystem metadata. Decoder internals, allocator overhead, OS cache and RSS are outside the ledgers.

## Independent review reconciliation — 2026-11-11

An omitted baseline and an invalid supplied baseline are distinct request states. Only omission permits parent-only continuation. Supplying a baseline requires all descriptor files to pass regular-file, no-symlink and content verification during input admission; a missing constituent file is an error before root initialization, route dispatch or publication.

`output_bytes` bounds the exact serialized stdout stream for CLI `read` and `verify`, including the footer and newline. The writer counts actual bytes and refuses any byte beyond the cap. Partial output is not a successful result; consumers require both successful exit and the footer. The library callback API does not serialize output and leaves callback byte accounting to its caller. Generation admission no longer treats a per-row estimate as an output guarantee.

Policy work records `admission_bytes` paid before route choice, shared by every candidate, and `route_verify_bytes` for additional mandatory reads by that route. The deterministic integer proxy includes source fingerprints, selected checkpoint object verification, route-specific source attestations and touched-shard reads without claiming wall-clock calibration. The independent Python oracle recomputes both fields.

The two named views remain stored in one arrangement. `read_view` and CLI `read-view` select `all_v1` or `filtered_v1` independently while reading the same committed generation. `defined_views` describes the two stored definitions; each `ReadOutcome.views_read` reports one or two actual views traversed. The combined `read` API remains compatible.
