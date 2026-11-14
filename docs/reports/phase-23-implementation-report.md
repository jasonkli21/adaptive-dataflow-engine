# Phase 23 implementation report

**Local acceptance:** G0–G14 and G16–G17 pass on candidate 11. G15 local checks
pass. Its hosted part is recorded separately against the final commit SHA. The
checked-in [support matrix](phase-23-support.json) is the state at recording.
A green exact-final-SHA GitHub Actions receipt completes G15 and permits
`COMPLETE_BOUNDED_LOCAL_MULTITENANT_MANAGER_V1`.

Phase 23 adds an opt-in authenticated Unix scheduler for finite local Parquet
static group-sum/count and HashOnly inner join. Four real managed-worker
processes in two separately fenced stores execute independently. Existing
operations and native defaults retain their contracts. The only changes to
`ade-ops` export source preflight and an idempotent absolute-deadline variant of
registration. Both lead through the existing admission and publication path.

## Implementation and envelope

| PRD feature | Bounded implementation |
|---|---|
| Queues | Monotonic private ledger, tenant/idempotency scope, absolute expiry and retained logical history |
| Priorities | Three tiers, finite age promotion, deterministic sequence tie-break |
| Quotas | Atomic tenant and pool limits on queued obligations, cohort reservations, worker slots, managed allocation, staging, retained results, cumulative write allowance and optional tariff |
| Fairness | Compare cumulative granted work divided by tenant weight within each eligible rank |
| Admission | Reject incompatible demand and full queues before native effects. Defer blocked accepted work. Register only on dispatch. |
| Preemption | Persist intent against a running replay-consenting victim, native cancel and drain, then at most one fresh native launch under the same logical ID/deadline |
| Cost | Per-attempt slot occupancy and versioned optional pricing, retained spent attempts, explicit unknown native counters |
| Autoscaling | Activate and deactivate one already-provisioned standby cohort under eligible demand and idle cooldown |

ADR-0030 and the [operator guide](../phase-23-resource-manager-guide.md) define
trusted same-UID Unix access, canonical operator-controlled sources, one native
owner per cohort and one local filesystem lineage. A native immutable marker
wins the cancellation race. An uncertain drain or output retains its capacity
and requires an operator. A crash-drained native failure remains terminal
unless durable preempt intent and a cancelled terminal outcome permit relaunch.
Dynamic physical worker provisioning, OS process quotas, hostile local tenants,
remote jobs, general dataflow, streaming, UDFs, external sinks, automated GC
and production HA remain deferred.

The handoff's plan was already present as an untracked repository file. The
implementation selected in-process native coordinator threads so a manager
SIGKILL also stops its coordinators while external workers retain their fences.
The baseline is `a9ccf823f320c683d68c05d7d27ad25c3deb6f14`. Before code changes
the baseline native aggregate/join suite passed seven tests with feature-matched
worker binaries after the default sandbox denied loopback. Baseline hosted CI
succeeded on [run 43628106281](https://github.com/jasonkli21/adaptive-dataflow-engine/actions/runs/43628106281).
The distinct Phase 18, 20, 21 and 22 qualifications remain as recorded in
their own reports and support matrices. No predecessor was reaccepted here.

## Acceptance and measurements

Candidate 11 records 37 cases, including 17 reached SIGKILL/restart scenarios,
112 verified IPC reads and two real HFS+ volume-exhaustion scenarios. It exercised
both native targets through crashes around claim, gather and publication. Worker
death and boot change preserve conservative reservations. The two fairness cases
used live native admission chronology, equal and 2:1 weights, older low-priority
work and an independent rational-arithmetic policy oracle. The socket tests
also verified concurrent key deduplication, foreign and unknown ID equivalence,
token revocation, bounded malformed inputs and a multi-batch result.

The local fixture campaign used the same executable set, ten jobs per sample,
the same price table and both provisioned worker cohorts. Queue durations are
milliseconds from request receipt to reservation. RSS is the peak sampled sum
for the manager and all four workers. CPU times and logical disk file lengths
are in the raw samples. These descriptive runs include same-host debug
execution and all polling overhead.

| Mode | Wall ms, each trial | Queue p50 ms | Queue p95 ms | Charged slot-ms |
|---|---|---|---|---|
| FIFO | 3512.5, 3452.7, 3523.5 | 1034.5, 945.5, 981.0 | 2651, 2588, 2652 | 6272, 6162, 6326 |
| Adaptive | 1635.0, 1674.2, 1653.3 | 393.0, 427.0, 409.0 | 874, 915, 878 | 5244, 5554, 5406 |

Peak combined RSS across the samples was 89,728 KiB for FIFO and 109,520 KiB
for adaptive. The price was one USD microunit per slot-ms. These observations
apply to the pinned local campaign, with no cross-host performance or billing
claim.

Candidate 11 source-inventory SHA256 is
`01c7e8be229449a731459b999ed37946299450523f82b1db4224084439205053`.
The independently stored archive seal is
`7165717dd546291cfe352b2ef9894eb734b86f360f901a80ebe445d9c8b7fc5d`.
The sealed inventory includes SHA256s for manager `628dffb18316`,
batch `5697855283cf`, worker `74a81965f126` and decoder `07b892174490`. The
host is Apple Silicon macOS 27, Rust 1.99.1, workspace all-features. Every
candidate is retained in the [evidence directory](../evidence/phase23/).
The final archive preserves raw IPC, marker bytes, native UUID/boot/port/PID
identities, clock/kill witnesses, quotas, prices and raw observations. The
detached verifier passed in both modes and rejected eight fully rehashed
semantic mutations. Current-source verification also reopens the sealed IPC
with the pinned executable and compares decoded values and schema to the
literal oracle.

## Commands

All local release checks exited zero. The workspace suite recorded 341 passed,
0 failed and 10 ignored. The Python suite recorded 72 passed. Three focused
policy tests passed in each feature mode. The MSRV check used the local
1.94.0 toolchain. OpenSSL used Homebrew pkg-config paths. See
[command outcomes](../evidence/phase23/local-gates-exits.txt) and
[log hashes](../evidence/phase23/local-gates.json).

```sh
cargo fmt --all -- --check
cargo check --workspace --all-targets --offline --locked
cargo clippy --workspace --all-targets --all-features --offline --locked -- -D warnings
cargo build --workspace --all-features --offline --locked
cargo test --workspace --all-features --offline --locked
cargo test -p ade-resource --all-features --offline --locked
cargo test -p ade-resource --no-default-features --offline --locked
bash scripts/smoke.sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p 'test_*.py'
cargo +1.94.0 check --workspace --all-targets --offline --locked
```

Build the fixture decoder with the same flags as the other executables.
Use immutable copies of all four for the live and supplemental campaigns.
The final candidate passed `scripts/phase23/campaign.py`, `enospc.py`,
`verify.py --mode current-source`, `verify.py --mode historical-archive`,
and `tamper.py`. The final-SHA hosted CI job adds a Linux native manager
smoke and keeps existing workspace, MSRV and Phase 22 jobs.

```sh
evidence=docs/evidence/phase23
mkdir -p /tmp/ade23-evidence
tar -xzf "$evidence/candidate-final.tar.gz" -C /tmp/ade23-evidence
pin=$(cat "$evidence/candidate-final.seal-sha256")
PYTHONDONTWRITEBYTECODE=1 python3 scripts/phase23/verify.py /tmp/ade23-evidence \
  --mode historical-archive --receipt-pin "$pin"
```

The candidate launcher, independent verifier and final self-review were
performed in the same session. No separate reviewer or additional project
commit is claimed.
