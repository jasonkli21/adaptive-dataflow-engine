# ADR-0025 — Unified bounded/unbounded planning v1

Accepted and implemented bounded v1; authorized 2026-10-30 Pacific, accepted 2026-10-31. User authorization: Phase18 in
this session, no subagents, one local commit, no push. Baseline fbe0e2b. Refines
architecture §§3.5,5,14–18,20,26–31, preserving INV1–10 and PRD Phase18.

## Same-session plan challenge

A boolean bounded flag cannot prove time, replay or sink legality. A batch group
and an event-time window must remain different operators. Kafka cuts are offset
vectors, not watermarks or atomic snapshots. Gaps are legal and an empty prefix
must not close windows. Metadata cannot detect truncate/regrow between observations;
immutable incarnation/history needs operator attestation. Filters must retain
receipt progress and may never move across state/time/checkpoint operations.

A neutral `ade-logical` crate owns semantic admission, derived properties, digest
and bounded deterministic optimization. It reuses portable ade-physical types and
its exhaustive finite schema inference; it has no DataFusion/Kafka/runtime edge.
Batch composition extracts semantic operators from existing SQL/Rust binding,
discards physical properties/target and rebuilds PlanV1 from the optimizer choice.
The shared optimizer genuinely selects the native aggregate target. This avoids a
second SQL parser/type system and leaves old entrypoints unchanged.

| Concern | Accepted bounded decision / refusal |
|---|---|
| Semantic tree | Strict version1 descriptor, flat canonical topological tree, <=64 nodes/depth, <=256KiB. No caller-derived properties. Finite schema inference reuses unchanged PlanV1. |
| Kafka | Exact EventV1 native filter/project, optional tumbling_sum_count_v1 and receipt_drop_closed_v1. Assigned/live and history_then_live; range descriptor is reserved and refuses until native finite progress has an EOF contract. |
| Hybrid | Fixed same topic/partition inventory, explicit original starts and exclusive cut. One demand owner and native state. No boundary event changes cursor/frontier/state. Actual input offset classification is observable. |
| Identity | Canonical semantic fields include job/source/history/original starts/cut/window/late/hash/sink; exclude connections, secrets, roots and physical choices. Unified v1/v2 state uses digest-derived job UUID checked before opening roots, binding cut via the unchanged native fingerprint. |
| v3 | Pure live uses native original job/history/offset fingerprint and unchanged CURRENT3. Hybrid v3 refuses before root/network; no unauthoritative sidecar. No cheaper delivery downgrade. |
| Optimizer | Enumerate <=3 fixed targets, legality before checked integer proxy cost. Local cost rows*4; fixed distributed cost 20000+rows*2. Conservative declared estimates, not measured prices. Distributed only 2–4 workers with explicit executable capability. Forced target hard; disabled selects explicit static baseline. |
| Bounds | <=16KiB decision, <=3 candidates, O(64) inference per candidate. Native source/output/state/checkpoint limits remain separately enforced, no RSS claim. |
| Compatibility | New default-off unified-planner features and explicit CLI flags. No old serialized contracts/defaults/selector changes. No new kernel, scheduler, checkpoint/delivery policy. |

Runtime decisions are derived from the actual parsed plan and native capabilities;
no user-supplied trace is executable authority. Forced local on a join refuses.
Source stats are manifest assertions verified by native preflight/postflight before
results; they are never trusted as proof to skip source verification.

Stateful hybrid restore verifies original history remains retained as well as the
saved next vector. Changed cut/history changes the derived job ID and refuses the
selected native checkpoint. Attestation remains necessary; no arbitrary replay,
source swap, global Kafka order, automatic recovery or HA claim.

No Phase19/physical rescale/general streaming SQL/joins/UDFs/connector expansion.
Same-session self-review and independent Python oracles do not claim a separate
reviewer. Completion requires every G0–G13 and fresh Phase17 G15 service acceptance.

## Same-session hardening before final freeze

All opt-in v1/v2 stateful plans bind the complete semantic digest through a derived
128-bit native job UUID, including pure-live original starts. V3 instead retains
its full existing source/job/history fingerprint and refuses hybrid. Physical
choice, endpoints, roots and owner/resource knobs are excluded; finite output
ceilings are normalized to the v1 maximum for semantic hashing while their actual
values remain hard admission/execution constraints. No cap is weakened.

The Phase17 v4 criteria's old starting-head field names bd5d1b9 rather than the
actual remediation baseline fbe0e2b. The additive phase18/phase17-criteria.json
changes only that provenance field for the fresh campaign. All case inventories,
refusals, quantitative predicates and G0–G15 remain identical. Acceptance gains an
optional --criteria path; existing historical criteria and archives are unchanged.
The fixture spells --cluster-id=VALUE so a randomly generated leading dash cannot
be mistaken for a CLI option. The ENOSPC supplement supplies/checks the actual
retired predecessor head now required by native import. These are harness fixes,
not relaxed correctness or performance gates.

Final local G0–G13 and source identities are in the Phase18 report. Phase17 current
G0–G15 is freshly reaccepted on the same runtime. The final source retains the
original static defaults, protocols and earlier evidence. A fixed assigned subset
of the full pinned topic inventory is valid; only assigned partitions enter the
frontier/cut vector. The decision view is immutable, and plan/metadata bounds are
checked before derived copies. All failed candidates and slower measurements are
preserved. No separate reviewer or future-phase authorization is claimed.
