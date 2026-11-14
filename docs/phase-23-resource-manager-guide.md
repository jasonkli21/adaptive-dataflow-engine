# Phase 23 local resource manager

`ade-resource` schedules finite local Parquet jobs across two separately supervised,
fixed managed-worker cohorts. A tenant can submit a pinned static aggregate or
bounded HashOnly inner join, inspect and cancel its own jobs, and download verified
Arrow IPC. The service listens only on a local Unix socket. The trust boundary is
one operator-controlled Unix account. Use a separate operating-system deployment
to isolate mutually untrusted local users.

## Provision

Build the executables with matching worker and manager features:

```sh
cargo build --locked -p ade-resource -p ade-ops -p ade-distributed --bins
ade-batch --state "$ops_a" init
ade-batch --state "$ops_b" init
```

Create private scratch directories for each worker. Supervise each process
independently. Use distinct loopback control/exchange ports and capture each
process's ready frame. Supply both complete cohort registries in the manager
configuration, using canonical paths without symlinks.

```sh
ade-worker --managed 0 "$scratch" "$store_uuid" 127.0.0.1:7410 127.0.0.1:7411
umask 077
ade-resource init --root "$manager_root" --config manager.json > operator.token
ade-resource serve --root "$manager_root"
```

The initialization command requires two empty native stores and verifies worker
boot UUID, store UUID, endpoint and runtime compatibility. The service leases
both stores. Do not use the standalone `ade-batch` mutating commands on a
manager-owned store. The manager root must have a canonical parent and be short
enough for the platform's Unix socket limit.

The configuration is JSON with `version: 1`, `mode` (`adaptive` or `fifo`),
`aging_ms`, `standby_idle_ms`, `preempt_after_ms`, `limits`, optional `price`,
and `cohorts`. Each cohort has `root`, `store`, and `workers`. Each worker has
`id` (zero-based within the cohort), `control`, `exchange`, `instance` (boot UUID)
and `scratch_root`. Limits apply independently at operator and tenant scope.

| Limit | Unit | Example pool |
|---|---|---:|
| `queued` | Active queue obligations, including one saved position for a replay-eligible running job | 16 |
| `running` | Exclusive cohort reservations | 2 |
| `slots` | Sum of workers in admitted cohorts | 4 |
| `managed_bytes` | Admission allocation for native task, join and exchange accounting domains | 805306368 |
| `staging_bytes` | Reserved combined IPC and manifest bytes for active jobs | 1048576 |
| `retained_bytes` | Output reservations plus actual committed bytes | 8388608 |
| `cumulative_write_bytes` | Lifetime per-attempt native write allowances plus staging allowance | 536870912 |
| `cost_microunits` | Lifetime prices and active ceilings, nullable | 2000000 |

The retained limits are eight tenants, 64 logical submissions, 32 registrations
per cohort and at most two launches per logical job. Absolute ceilings and
request validation are implemented in the versioned contract. Retained history
cannot be recycled. A full store defers work until its stable deadline or
accepts eligible work on the other available cohort.

The price model has `version: 1`, a three-letter uppercase `currency`, and
positive integer `microunits_per_slot_ms`. Model identity is pinned for the
lineage. Slots count from reservation to manager settlement. Outage and drain
occupancy is charged through the original deadline. Later time is reported as
unpriced. Failed and preempted native attempts keep their write allowance and
earned charges. Missing runtime counters are null. Disk and RSS process
measurements are separate from native admission accounting.

## Control API

The endpoint is `$manager_root/control.sock` in a private directory. Send one
UTF-8 JSON envelope preceded by its four-byte unsigned big-endian length. The
response uses the same framing. Frames cap at 256 KiB. A peer has two seconds to
send its frame and two seconds to receive the reply once generated. There are
eight concurrent handlers. The CLI accepts the envelope from stdin, keeping the
token out of argv and shell history when supplied in a private input file.

```sh
ade-resource request --socket "$manager_root/control.sock" < request.json
```

The envelope is `{"version":1,"token":"...","request":{"op":"list"}}`. Success
returns `{"ok":true,"data":...}`. Refusals return a bounded `{"ok":false,"code":...}`.
Operator and tenant scopes are distinct.

| Caller | Request | Data fields |
|---|---|---|
| Operator | `provision` | `config` with canonical `source_root`, `weight` (1–16) and `limits`. Returns tenant UUID and a one-time token. |
| Operator | `rotate`, `revoke` | `tenant` UUID. Rotation invalidates the old token. Revocation is terminal and latches cancellation. |
| Operator | `limits` | `tenant` UUID or null for the pool, plus `limits`. Tightening below use is refused. |
| Operator | `price` | `price` before any job and with an unpriced lineage. |
| Operator | `inspect`, `inspect_job` | Pool summary or individual `job` UUID, with grants, charges, states and reasons. |
| Tenant | `submit` | `key` and `spec`. |
| Tenant | `list`, `status`, `cancel` | No fields for list. `job` UUID for status or cancel. |
| Tenant | `result` | `job` UUID and byte `offset`. Returns a hex chunk, size, row count, schema and BLAKE3. Advance offsets by decoded byte count. |

A submission spec includes `version: 1`, an admitted distributed `plan` in
PlanV1, matching `inputs` (`table`, `root`, `manifest`), `deadline_ms`
(1–120000 from receipt), `priority` (0–2), `work` (1–64), `preemptible`,
`runtime_bytes` per worker and coordinator, and combined `staging_bytes`. All
sources must belong to the tenant's exclusive operator-managed tree. Use the
Phase 12 manifest and schema contracts. The manager checks every file the native
catalog can read and pins filesystem identity and source content. A duplicate
key returns the original job. A changed recipe on the same key is refused.
`QUEUE_FULL` and `REQUEST_EXCEEDS_CAPACITY` are submission refusals. An admitted
queue entry may wait for capacity without starting workers.

Priority uses a finite age promotion. Adaptive dispatch chooses the least
cumulative declared work per tenant weight among the highest-ranked admissible
jobs. FIFO supplies a one-cohort priority-independent baseline. Preemption can
interrupt a lower-ranked running job after the minimum running interval, only
with explicit replay consent and budget for a new attempt. The old cohort and
reservation remain held until native drain or commit is verified. A native
marker that won cancellation produces exactly one logical result. Standby
activation changes eligible execution capacity while both cohorts stay
running under their supervisors.

The configured aging interval is capped by each request's deadline minus one
millisecond, so an unexpired request reaches the top rank before its original
deadline. This is bounded progress only when the request remains eligible and a
cohort completes work; impossible deadlines, quotas, source drift and unavailable
cohorts do not receive a service guarantee. If all permitted native stores have
reached their immutable 32-registration history limit, new requests receive
`NATIVE_HISTORY_FULL`; previously queued requests become terminal with the same
reason and are never mislabeled as deadline expiry.

Operator inspection includes a bounded durable `decisions` receipt list for
dispatch, expiry, permanent history refusal, preemption and standby changes.
Each record carries a policy version, config digest, reason, time, queue depth,
rank and bounded work/share inputs. The current receipts do not yet cover every
admission denial, deferred quota comparison, settlement or quarantine transition;
the release matrix keeps diagnostics acceptance partial until those records and
the independent reconstruction campaign are complete. The list refuses further
policy changes at its fixed cap rather than deleting history.

## Recover and inspect

Restart `serve` against the same manager root with the same worker processes,
source pins and native runtime features. A durable intent without a mapping is
reconciled by its exact native key. An interrupted coordinator is recovered
under the original native lock and fence. Preemption may launch one fresh
whole job after proved cancellation and drain. A failed crash-drained attempt
with no preempt intent remains failed. A published native marker can reconcile
success. A quarantined cohort retains its reservation. Save the manager state,
native state, marker and supervisor logs for an operator. There is no force
reset, adoption, automated reclamation, or replacement operation. Never roll
back a store, copy it into a second manager, or replace any worker in a still
owned cohort.

The deployment envelope is a trusted local filesystem honoring sync and atomic
rename/link. Physical provisioning, hard process or kernel quotas, remote
tenancy, arbitrary code, streaming, connector or UDF execution, automatic
failover and generic distributed cluster scheduling are deferred.
