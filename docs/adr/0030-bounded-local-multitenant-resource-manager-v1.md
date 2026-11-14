# ADR-0030: Bounded local multi-tenant resource manager v1

**Status:** Accepted
**Date:** 2026-11-14

## Decision

Phase 23 implements the eight PRD §16 capabilities in an opt-in Unix service
above Phase 12. The service owns the logical ledger. It runs native coordinators
in-process on separate threads with one private operations store per independent
externally supervised cohort. The native kernel runner lock, boot/store fence,
drain, and marker remain authoritative. Workers remain separate processes.
This preserves architecture §§8, 13, 15, 17, 22–24, 26–27 and INV-1–10.

Extending the operations store would mix tenant quotas and scheduling with the
one-unresolved-instance predicate and complicate schema migration, compatibility
and recovery. A new coordinator could support additional admission points, but
would duplicate worker fencing, source validation and atomic publication. The
wrapper costs two small metadata transactions per launch boundary and keeps
the Phase 12 surface intact. Operator provisioning can later supply additional
cohorts under a new contract. There is no physical provisioning in v1.

## Durable protocol

| Durable state | Allowed next state and required proof |
|---|---|
| Queued | Reserved on atomic admission; Cancelled by owner; Expired at original absolute deadline |
| Reserved (intent, native key, no ID) | Starting after exact native registration keyed by logical UUID and ordinal; terminal without a native job only on a proved pre-effect refusal; Quarantined on unclassifiable error |
| Starting (mapped native ID) | Running on observed native Running; cancellation intent may be latched; terminal native result may arrive directly |
| Starting/Running with intent | Native cancel then recover/read terminal; verified committed result wins; native Cancelled with drain permits bounded preemption requeue |
| Starting/Running | Succeeded on verified native marker, Failed/Cancelled on native terminal proof; Quarantined on missing identity or ambiguous drain/state |
| Quarantined | Reservations remain held; no automatic readmission/reset |
| Succeeded/Failed/Cancelled/Expired | Terminal, retained history |

Submit persists the complete logical submission before worker effects. Repeating
a key for the same tenant and specification returns the original ID before
source reads. Native reservation and the internal registration key precede
native registration. Restart reconciles the exact key and recipe before run.
Native registrations use one attempt and the original manager expiry. A
manager crash during a native execution kills the coordinator thread along with
the service. The external worker processes still require native recovery and
proved drain. No fresh native attempt follows a crash-drained failed attempt
unless the manager had latched preemption and native cancellation is proved.
Native success needs marker verification even on restart. A service-level
exclusive kernel lock and checksummed fsync-and-rename snapshots preserve
monotonic generations. An ambiguous metadata transaction stops the service.
This contract assumes an honest filesystem and one operator-owned lineage.
An older backup must never be installed as the current root.

## Limits and scheduling

The fixed bounds are eight principals, 64 logical jobs, 32 queued obligations,
two cohorts of two to four workers, one unresolved native job per cohort,
32 historical registrations per native store and at most two launches per
logical job. Request keys are at most 128 restricted ASCII characters. Requests
and replies use finite length-prefixed frames with a peer limit and absolute
read/write deadline. The privileged local operator pins the store UUID and full
worker registry. Source paths must be in the tenant's pinned private/owner-managed
tree, with no symlink, writable foreign ancestor below that root or hard-linked
input. Native validation pins manifests and data content at admission and
rechecks before publication. Tenants authenticate with opaque random credentials,
kept as constant-time checked fingerprints on disk. Revocation blocks subsequent
tenant access and cancels outstanding work. The trusted operator controls local
files and supervisors. Kernel-level separation between processes of the same
UID and direct native worker socket access are outside this API boundary.

FIFO uses one cohort and no preemption. Adaptive policy selects the highest
effective priority among admissible jobs, where every eligible waiter promotes
to the top rank after the fixed age interval. Within a rank, the least cumulative
declared work divided by tenant weight wins; submission order breaks ties.
Age is measured from original submission and active work counts as granted.
There is no guarantee for jobs whose quotas, sources, deadlines or cohort
availability preclude service. Preemption requires an observed running,
non-aged lower-priority job, explicit whole-job replay consent, budget for a
fresh launch and one preemption per job. A preemption demand must fit the
capacity actually released by the victim. Native cancellation is cooperative.
Standby activates when eligible demand cannot fit the primary and downshifts
after an idle cooldown with no unresolved standby work.

The 2026-11-14 review remediation derives the effective promotion interval per
job as `min(configured_aging_ms, deadline_ms - 1)`. This makes promotion
reachable before expiry while preserving the strict original deadline. It does
not promise service when the job cannot remain eligible until a cohort is
available. Submission refuses with `NATIVE_HISTORY_FULL` when every cohort
permitted by the current mode is permanently at its native registration cap;
queued work that becomes impossible receives a durable terminal failure with
that reason. Native history is not recycled.

Scheduler transition receipts are persisted in a bounded versioned list,
operator-only in the inspection response. They include policy and config
identity plus a limited scheduling snapshot, and contain no credentials or
filesystem paths. Full admission/defer/settlement/quarantine event coverage and
independent reconstruction are still release requirements; the support matrix
does not mark G13 accepted on these initial receipts alone.

Admission reserves counts, actual worker slots and native managed-budget
allocations, staging and output before effects. A replay-eligible active job
retains one queue obligation. Attempt disk write allowance is charged
cumulatively, including failed work and staging. The staging cap jointly
bounds IPC and publication metadata and reserves retained output until native
terminal reconciliation. The optional fixed-rate model reserves currency
microunits over the remaining deadline in worker-slot-ms. Settlement uses
observed reservation occupancy capped at its reserved duration; excess time
and missing native bytes are explicit. Charges, retained bytes and slot release
are committed together once. These are native accounting domains. CPU, RSS,
filesystem amplification and physical disk occupancy are separate observations.

Each managed cohort also holds a private manager lease on its operations root
for the service lifetime. This supplements the native runner lock and closes
simultaneous use by a sibling manager. An authenticated operator provisions
disjoint source roots. Every file the manifest could cause the native frontend
to open is checked against the PlanV1 inventory before source reads. File device,
inode, size and modification/change times are pinned across queue and dispatch,
alongside the content identities in the plan.
