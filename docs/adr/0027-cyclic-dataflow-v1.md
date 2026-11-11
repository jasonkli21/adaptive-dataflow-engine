# ADR-0027 — Bounded native cyclic dataflow v1

Accepted for the Phase 20 implementation on 2026-11-11. This extends
Architecture §§3, 5.3, 7–8, 17–19 and INV-1, INV-4–9 through an
independent cyclic physical family. The handoff submitted with the explicit
implementation request authorizes a Phase 20-specific waiver of the open
Phase 18 acceptance dependency for this isolated finite-local route.
Phase 18 remains partial. Phase 19 and its remediation stay source-bound.

The closed physical body uses an Expand kernel, Reduce kernel and one
typed Delay operator. Its sole delayed arc returns the reduced frontier
to Expand at round r+1. The remaining arcs form an acyclic body. Exact
node and port templates bind the algorithm, forbid nested or disconnected
regions and simplify validation. The executor binds the admitted backedge
and transports actual next-frontier rows through it. A static scan-all-arcs
execution policy shares kernels and the delay protocol with the indexed route.
Neither route enters the frontend, worker or streaming runtimes.

The reducer owns a production right until all synchronous expansion and
reduction is finished. Delayed rows receive checked ordered identities.
Transfers move credits from produced to queued, executing and acknowledged.
A barrier requires the producer to be closed and all credits settled, then
examines the resulting next frontier. Empty queues can coexist with an open
producer or a delivered but unacknowledged row. No such cut can complete.
BFS discovers each vertex at most once in round order. Minimum-label
propagation strictly lowers labels drawn from a finite vertex set. The
first empty next frontier at a settled cut proves a fixed point. A bound
or failure before that point leaves an incomplete run.

The opt-in adapter binds exact ordered, role-specific Parquet inventories.
It holds a regular file descriptor during native decoding and checks size,
schema, decoded counts, BLAKE3, device/inode and change/modify stamps.
Full postflight precedes staging and repeats immediately before selection. The plan
ID includes the ordered manifest, semantic version and seed, and excludes
paths and operational controls. The result digest includes the algorithm's
output shape and canonical rows, so equivalent inventories can share a
result digest even when their plan IDs differ.

The private Phase 20 root has its own kernel lease, anchor, staged object,
witness and append-only selectors. Witness and object are synced before
the selector's no-replace link. A pin independently retained by the caller
binds the exact selected sequence, parent digest, plan and root identity.
A selected descendant requires its exact pin, including after interruption.
The writer emits a prepared receipt before selection and a selected receipt
after directory sync. Recovery with the old pin is legal only while the
old cut is still selected. An unselected stage can be discarded under the
exact selected pin. A terminal retry uses the already selected snapshot.
Roots use an honest, operator-controlled local filesystem and a private
directory with one active lineage. These assumptions exclude external
transactions, arbitrary power loss, disk loss, remote HA and hostile
filesystem manipulation while an operation is running.

Checkpoint and terminal objects bind the state, next frontier and zero
in-flight credits. Only the latter permits terminal row output. The CLI
accounts actual newline-delimited bytes including the terminal footer.
Budgeting covers retained serialized bytes and runtime-owned graph and
queue allocations. Filesystem block allocation, Arrow/Parquet internals,
allocator overhead, cache, RSS and time blocked in filesystem syscalls
are outside those measures. The frozen bounds and comparison criteria
are in `scripts/phase20/criteria.json`.

## Independent review reconciliation — 2026-11-11

The accepted physical input is a **closed admitted template** for exactly the
two named algorithms above. `BoundCycle` preserves the validated algorithm and
delay selection; the executor has specialized expansion/reduction branches.
The node and arc descriptors are not a user-composable operator dispatch API,
and the evidence does not establish arbitrary cyclic regions, nested loops,
multiple SCCs, distributed progress, or general iterative ML. The shared
feedback ledger and delayed-frontier execution are reusable implementation
mechanisms inside this closed route, but their composability is unproved.

This limitation narrows the Phase 20 qualification and operator-facing claim;
it does not widen Phase 20 into a general cyclic runtime. Any general typed
operator-dispatch boundary requires a separately authorized design and new
source-bound proof. No Phase 21 work is authorized by this clarification.
