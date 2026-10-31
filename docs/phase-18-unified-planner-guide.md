# Phase18 unified planner v1

The default-off `unified-planner` features in ade-frontend and ade-stream use the
same neutral `ade-logical::LogicalPlanV1` admission, property inference and pure
optimizer. Batch semantics lower to unchanged PlanV1 and native execution.
Kafka semantics select the existing native demand/state/transactional machinery.
No DataFusion physical execution fallback is part of the query route.

## Build and batch

```sh
cargo build -p ade-frontend -p ade-stream -p ade-logical -p ade-distributed \
  --all-features --bins --examples --locked
ade-query execute --dataset /absolute/manifest-dataset \
  --sql 'SELECT key,SUM(value_i64) AS sum,COUNT(*) AS count FROM input GROUP BY key' \
  --unified-planner --worker-bin /absolute/ade-worker
```

With no explicit `--target`, the optimizer can select local or the existing fixed
2–4-worker unfiltered aggregate. An explicit `--target` is a hard constraint.
`--disable-optimizer` executes the declared static target (local by default).
Joins require `--target distributed-join` for the existing bounded join binder;
all Phase11 shape, schema, overflow, source and output limits remain. Controls
never authorize leased scheduling or unsupported distributed operator graphs.

SQL API: `Catalog::plan_sql_unified(sql, binding_target)`; Rust API:
`DataFrame::build_logical()`. `Catalog::execute_unified_until` checks supplied
capabilities, optimizes and regenerates native PlanV1 under the caller's deadline.
The SQL binder and typed builder reuse existing semantic operator construction;
physical schemas/properties/target are discarded before inference and choice.
The optimizer-selected target is the target that actually executes.

The fixed proxy model costs local `4*manifest_rows`, distributed aggregate
`20000+2*manifest_rows`. Ties choose local. These are conservative declared units,
not measured latency or dollars. Source rows come from the manifest and remain
subject to native physical pre/postflight verification. There are at most three
candidates,64 tree nodes/depth,256KiB input and16KiB decision JSON. Every rejected
candidate and hard control is visible in the decision. Streaming has one legal
physical candidate; sink guarantees and checkpoint mechanisms are never ranked
as interchangeable optimizations.

## Kafka descriptor and native execution

`ade-plan PLAN.json [CONTROLS.json]` performs strict shared admission and explains
the inferred properties, candidate ledger, semantic digest and `bound_job_hex`.
It does no Kafka/root I/O. The complete descriptor example is
`scripts/phase18/test-fixture.json` (its `plan` member); acceptance.py constructs
actual source/state descriptors. Types in ade-logical/src/lib.rs are authoritative.

The version1 envelope contains `computation` and `sink`. Kafka computation binds
source cluster/topic/full partition inventory, original starts, an operator-pinned
immutable-history token, assigned or history_then_live mode, logical job seed,
native key filter, allowance, exact projection, optional tumbling window,
receipt_drop_closed_v1, hash1 and bounded diagnostic timers. Schema, termination,
ordering, replay, delivery and time/frontier properties are derived, not supplied.
Duplicate/unknown fields and incompatible operators/sinks refuse. KafkaRange is
reserved/refused; a finite offset interval has no new EOF contract in this v1.

```sh
ade-stream STREAM_CONFIG --logical-plan PLAN --test-stop-records 100
ade-state-stream STATE_CONFIG --logical-plan PLAN --test-stop-records 100
ade-state-stream STATE_CONFIG --logical-plan PLAN --restore
ade-state-stream STATE_CONFIG --logical-plan PLAN --adaptive-policy POLICY
ade-state-stream STATE_CONFIG --logical-plan PLAN --adaptive-policy POLICY --recover
ade-state-stream-txn CONFIG3 run --logical-plan PURE_LIVE_PLAN
ade-state-stream-txn CONFIG3 resume --logical-plan PURE_LIVE_PLAN
ade-state-stream-txn CONFIG3 read --logical-plan PURE_LIVE_PLAN
```

All native lifecycle flags remain. `--disable-optimizer` selects the static native
Kafka choice. Supplying a logical flag without its feature refuses. Source and
transform fields must match the native config exactly before effects. Diagnostic
timers remain native, bounded and nondurable; stateful timers remain refused.

For every opt-in v1/v2 stateful descriptor, set the native config's `job_id` to the
UUID representation of `bound_job_hex` from `ade-plan`. The logical `job` is the
stable seed, not that derived native UUID. This binds original starts, history,
cut and all transform/sink semantics through the unchanged native fingerprint.
Adopting this route for v1/v2 requires a fresh native lineage/root; old jobs
continue through their legacy APIs and no implicit migration is provided. Keep
both descriptors unchanged across restore; pass original starts, never saved
cursors. Native restore chooses the verified saved cursor. Resource/root/endpoint
settings are absent from the semantic digest. Owner changes still follow native
explicit quiescent restore rules.

V3 pure live instead uses the original native job UUID in `job`, the exact
trusted_history token and original starts. Its existing fingerprint, sole CURRENT3
selector, ACTIVATED3 refusal witness, fences, terminal PAUSED, no-replace savepoints,
retired-head attestations, codec migration and logical rescale remain authoritative.
**Transactional history_then_live is refused before root or broker effects.** No
sidecar can authorize it, and no stdout/at-least-once fallback exists.

## Historical prefix and live tail

Supply an explicit exclusive cut for every assigned partition. The fixed assigned
set may be a sorted nonempty subset of the pinned full topic inventory; unassigned
partitions do not enter the frontier. An admitted record
with actual offset below its partition cut is historical; every other admitted
record is live. Gaps (including aborted/control records) are legal. The same native
source, cursor, state, window/frontier and sink session process both. A cut does
not seek, reset, close windows, advance time or force EOF. Idle remains Pending.
`unified_confirmed` traces classify actual offsets only after sink ACK; a partially
written frame is not confirmed. Filtered and closed-late receipts still advance
native progress. Late/window behavior is deterministic for the observed merge
trace, not every possible legal merge.

Before assignment and during verification, the complete original history must
remain retained, the full topic inventory must match and cuts must not exceed
observed high watermarks. This deliberately refuses even a valid saved state if
its original history expired. Restart binds the same cut through job identity and
restores native state/progress. No per-partition vector is a globally atomic
snapshot. Kafka metadata cannot detect every truncate/regrow or prove incarnation;
trusted immutable uncompressed <=32KiB history remains an operator assumption.

Working hot/cold state, Kafka queues/responses, checkpoints/output and plan/search
limits are separate domains. No historical buffer is introduced: one record and
one sink frame remain in flight. Decoder, allocator, native client, page cache,
OS and RSS are outside those ledgers. Cancellation/failure poisons pending work;
there is no automatic recovery, destructive GC, physical rescale or HA.

## Evidence and limits

Run `scripts/phase18/acceptance.py --help` for owned disposable real Kafka/MinIO
campaign inputs; all topics/buckets must be fixture-owned. Final raw portable
proofs and exact commands are in the implementation report. The independent
verifier derives properties/costs from raw plans and windows/state from actual
producer-offset ledgers; it does not trust PASS labels. Tamper checks recompute
outer hashes and state checksums to test semantic refusal.

General streaming SQL, stream joins, unbounded sort/LIMIT/collect, multi-source
CDC continuity, UDFs, physical worker/Kafka rescale, external transactions,
production HA and Phase19 maintained deltas remain out of scope.
