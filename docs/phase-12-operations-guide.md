# Phase 12 local batch operations

`ade-batch` runs validated physical IR v1 static distributed aggregates and bounded
inner joins using a fixed registry of2–4 externally supervised same-host workers.
The durable root survives coordinator-process loss. This is bounded v1, not
cross-host HA. Existing `ade-query` and benchmark defaults remain unchanged.

## Build and register workers

```bash
cargo build --workspace --locked --offline
# Parent must exist; choose a NEW canonical nonsymlink local directory.
target/debug/ade-batch --state /absolute/private/batch-state init
target/debug/ade-batch --state /absolute/private/batch-state capabilities
```

Keep the returned store UUID. Create separate canonical mode0700 scratch directories
for workers0..N-1. Start each worker under an external supervisor that survives a
coordinator crash (launchd/systemd or a test-parent supervisor):

```bash
target/debug/ade-worker --managed 0 /absolute/private/worker0 STORE_UUID 127.0.0.1:9100 127.0.0.1:9200
```

stdout starts with8-byte ADEOPS01, a4-byte little-endian JSON length, then the Ready
JSON (`endpoint` and `boot`). Record `boot.instance`, `endpoint.id/control/exchange`
and the canonical scratch path in the recipe. Do not start workers as children
owned by a temporary LocalCluster. Do not restart/replace a worker and assume its
old boot identity remains valid. Bound addresses must be distinct loopback ports.
Workers0..N-1 are contiguous and use the same store, source build and feature set.

Recipes have `version:1`, `plan` (PlanV1), `inputs`, `workers`, `timeout_ms`1..120000,
`max_attempts`1..3 (default choice1), `staging_bytes`1..67108864, and optional
`runtime_bytes`1..67108864 (default67108864 per worker/coordinator/attempt).
Each input is `{table, root, manifest}` with canonical absolute paths and plan
source order. Each worker is `{id, control, exchange, instance, scratch_root}`.
Unknown fields refuse. SQL is first compiled by the existing frontend:

```bash
target/debug/ade-query plan-sql --dataset /absolute/data --target distributed-aggregate --sql 'SELECT key, SUM(value_i64) AS sum, COUNT(*) AS count FROM input GROUP BY key' --max-output-rows 10000
```

Use the JSON PlanV1 portion of this output (omit the trailing plan_hash/execution_policy lines) with exact registered sources; do not hand-edit source
hashes. The executable full examples/fixtures are in
`crates/ade-ops/tests/support/mod.rs`, for aggregate and duplicate-key inner join.
Registration verifies manifests/schema/rows/hashes and pins file inode/device/
mtime/size plus native-runtime and coordinator/frontend compatibility identities.
The job expiry starts before source hashing. Registration releases `owner.lock`
while hashing, then reloads state and rechecks idempotency and capacity before it
persists. `verify-sources` also releases and reloads around file hashing. A recipe
is immutable. Changed sources, runtime/configuration, coordinator compatibility,
worker boot or registry require a separately registered job; existing claims
cannot be retargeted. Copying/restoring old metadata to reset counters is unsupported.

## Commands and publication

```bash
target/debug/ade-batch --state ROOT submit --key request-1 --recipe /absolute/recipe.json
target/debug/ade-batch --state ROOT run JOB_UUID
target/debug/ade-batch --state ROOT status JOB_UUID
target/debug/ade-batch --state ROOT list --limit 16
target/debug/ade-batch --state ROOT history JOB_UUID
target/debug/ade-batch --state ROOT cancel JOB_UUID
target/debug/ade-batch --state ROOT recover
target/debug/ade-batch --state ROOT verify-output JOB_UUID
target/debug/ade-batch --state ROOT gc JOB_UUID          # dry run
target/debug/ade-batch --state ROOT gc JOB_UUID --apply
target/debug/ade-batch --state ROOT cleanup              # abandoned metadata temp only
```

Use absolute canonical ROOT paths. JSON responses have schema_version1. Errors
exit1 with stable code and <=1024-byte message on stderr; Clap usage errors exit2.
`run` exits successfully only for SUCCEEDED. `status` verifies committed output
before reporting success and labels it `output_verification: VERIFIED`. List rows
retain the persisted `state` and expose the same value as `recorded_state`; every
list row labels `output_verification: NOT_CHECKED`, including recorded successes.
Status includes owner, engine job, namespace, attempts,
expiry, cancel intent and resource reservations. Unknown failure counters are null,
not invented zero. Listing uses ascending UUIDs, exclusive `--after`, limit1..32.
History retains newest32 structured events; sequence gaps mean bounded eviction.
A repeated key returns the original job/expiry; a different recipe with that key
refuses. All32 historical job records are retained; no automatic eviction/reset.
The legacy metadata-handle `Store::run` refuses; Rust callers use `runner::run`
so a metadata lock cannot span engine execution.

Only `jobs/JOB_UUID/COMMITTED.json` exposes results. It references immutable IPC
and binds source/plan/config/worker/attempt identities, native terminal coverage,
exact schema/rows/bytes/hash. Use `verify-output`, then read only its referenced
file. Private stages are never successful output. No-clobber hard-link publication
is atomic; data/manifest/directory sync surrounds publication. A second marker
cannot replace the first. Cancel before the serialized commit prevents link;
a cancel after link preserves success. Lost final metadata update is repaired by
verifying the marker and repeating directory sync. Corrupt/conflicting markers
freeze the job and remain evidence; they are never overwritten or erased by GC.

## Restart, refusal and operator runbook

`runner.lock` excludes a second active run/recover/GC. `owner.lock` serializes
bounded metadata transactions; monitors retry `OWNER_BUSY` on their next poll and
do not treat it as cancellation. Never unlink or replace either.
A transaction syncs state.next, replaces state.json and syncs root. After an
ambiguous transaction, close the handle and reopen; validate only state.json,
discard only a safe abandoned temp. Checksum/unknown schema/missing state refuses.
Version1 metadata migrates to2; a legacy ambiguous claim without native mapping
becomes NEEDS_OPERATOR. Older v2 jobs without coordinator compatibility pins
remain readable, but uncommitted jobs are not replayed; a valid committed marker
can still be verified and reconciled. No backup restore/rollback or history
reclamation exists.

After a coordinator crash, retain workers, source files and durable root. Run
`recover` to reconcile without launching new worker tasks, or `run JOB_UUID` to
reconcile and continue. Before any fresh instance the old cohort's drain gate
must prove all tasks/receivers/retention owners gone. Safe interrupted work gets
fresh numeric ID and UUID, bounded200/400ms backoff and original expiry. Staged
validated output can resume publication without re-execution. Only explicit
transient native I/O/refusals retry; integrity/quota/shape failures are permanent.
No in-place producer/join/leased recovery is enabled.

NEEDS_OPERATOR means boot mismatch, unreachable/poisoned worker, unproved drain,
legacy unmapped claim or ambiguous output. Repeated run does not replay it.
Inspect attempt errors, the pinned registry, supervisor logs and marker. Keep all
evidence and quarantine the job/root. There is deliberately no force-reset/adopt
command: operator replacement/rebinding is outside bounded v1. A valid published
marker can still reconcile success. Do not delete failed markers to induce retry.
GC requires a terminal, proved-quiesced job, is dry-run by default, and deletes only
recorded uncommitted artifacts and bounded private coordinator namespaces.
Committed data, canonical marker and unknown output files are protected.

## Resource and trust envelope

Aggregate uses fixed hash, static topology, Buffered exchange,32MiB task budget,
operator spill disabled. Join uses HashOnly with operator spill disabled,
byte-budgeted spill input exchange, and local-materialized coordinator gather.
Memory pressure may fail a valid larger query. Native Arrow-accounted output
bytes and actual serialized IPC limits are both enforced; they differ.

Hard covered domains:32 jobs,8 pending,1 active native instance,32events/job,
3attempts,128-byte key,512KiB recipe,256KiB IR/64nodes,64files/1m rows/64MiB per
source table,1MiB manifests,8MiB snapshot (up to two files during replacement),
1MiB control frames,1m output rows/64MiB IPC,64KiB publication manifest, and the
configured IPC+manifest staging cap. Runtime budgets cumulatively reserve every
admitted exchange/gather write per worker/coordinator per attempt, including
failed writes. Across attempts the bound is max_attempts*(N+1)*runtime_bytes;
crash remnants are included in that finite bound until GC. Worker journals and
filesystem metadata are separately small/bounded. Attempt counters can be unknown
after failure. Neither these budgets nor the local ledger cap RSS, Arrow/Parquet
transients, kernel buffers, physical disk capacity or SSD write amplification.

Assume trusted Unix user/local filesystem honoring fsync/atomic links, stable wall
clock and immutable inputs. Backward wall clocks refuse; blocking OS calls are
not preemptible, so cancellation/expiry can be delayed by a blocked syscall.
SIGKILL testing does not establish power-cut or cross-host reliability. No
replication, automatic worker replacement, operator spill/grace, remote sources,
arbitrary SQL, cloud/external transactional sink, streaming or multi-tenant HA.
Phase10 physical membership remains fixed and its historic timing evidence stays
source-bound.

## Verification

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked --offline
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
cargo build --workspace --all-features --locked --offline
cargo test -p ade-ops --all-features --locked --offline -- --nocapture
cargo test --workspace --all-features --locked --offline
bash scripts/smoke.sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p 'test_*.py'
```

Build feature-matched worker binaries **before** process tests. Fault hooks are
feature gated and off by default. Reached-stage witnesses distinguish real native
fault tests from metadata/publication primitive fixtures. See the implementation
report and ignored local `benchmarks/results/phase12-completion/` evidence bundle.
