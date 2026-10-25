# Phase 10 experiment v1 — premeasurement declaration (2026-10-24)

No Phase10 release timings have been observed when writing this declaration.
Freeze this file, runner and verifier in a clean temporary source commit before
building. Preserve the user's single implementation commit on the main checkout.
Record exact source SHA, crates tree, source tar SHA256, Git bundle, binary SHA256,
locked Rust toolchain/build settings and `ade-bench env`. The temporary measured
source must have the same runtime files as the final handoff. Documentation may
later record outcomes without pretending those edits were measured.

Host: current macOS arm64 localhost machine; registered2-process primary and4
confirmation, shared filesystem, no physical remote-locality/fault-domain claim.
Before ANY release measurements take3 top/vm_stat/uptime/process readings10s apart.
Require each CPU idle>=70%, load1<=8, swapout increase<=1000 pages and no other
cargo/rustc/ade-bench/ade-worker. Failure preserves a zero-measurement attempt;
postpone or report unverified. No overlap with compilation or other benchmarks.
After timing preserve telemetry. Never remove an observed slow sample.

All data: synthetic-events-v1, seed42,4000000 rows,32files,1024keys,uniform keys,
8192row groups,snappy,payload16. `file_row_weights` is optional physical layout
only: absent produces unchanged historical manifests, weighted layout preserves
identical ordered logical rows/fingerprint. Imbalance weights=(15,1,1,1)*8;
uniform has equal file ranges. Paired arms use the EXACT SAME manifest/files.
Full verification, original independent DataFusion oracle, checked sum/counts.

All controls: workers2/4, fixed_hash, static routing, spill exchange1048576bytes,
memory33554432, batch8192,prefetch0,recovery_enabled=false. Only scheduling knobs
differ. Static source assignments remain modulo; no-steal and leased_v1 perform
per-file partial aggregation into persistent producer sessions. This extra
partial-row/verification/control overhead is part of the comparison, not hidden.
Default lease10000ms, heartbeat250ms, all producers active, equal capacity1 and
no locality labels. Bounds use admitted defaults and appear in effective config.

For EACH worker count run, in order: imbalanced static/leased_no_steal/leased_v1,
then uniform static/leased_v1. One warmup + FIVE measured children per primary2
arm; one + THREE per4 confirmation.62 total successful children include the
additional controls below;49 measured. Every child is a new process and must pass
oracle/source/lease/fingerprint/terminal checks before its timing is valid.

Gates (unchanged from plan): imbalanced leased_v1 median query_time_ns <=0.90x
static; median maximum producer coordinator_finished_ns <=0.90x static. The
latter measures the last producer completion relative to the job start including
prepare/verification/control, not CPU. Uniform median query <=1.15x static.
Apply all3 separately for2/4; confirmation never substitutes for primary. Show
no-steal ratios without a separate numeric gate. Require actual off-preference
leases in imbalanced leased_v1; no-steal must retain every preferred owner.
Record every sample, median, range, partial rows/bytes, source/operator work,
lease/control/queue observations, task/queue bounds and child RSS separately.

Additional4-process controls, same uniform source, one warmup +3measured each:
capacity unweighted1,1,1,1 versus declared1,4,4,4; BOTH have deterministic
scheduler_research_delay_worker=0/delay_ms=100. Report actual file/byte shares,
capacity decisions and observed elapsed; no required speed win and no invented
physical speed. Live participation: initial_active2, delayworker0=1000ms,
after1completion activate2, after2 spot-notice0(grace5000ms), after3 drain0.
Worker3 remains parked. One warmup +3measured, real leases after activation,
notice while0 owns a lease, no new grants after cutoff, successful terminal
drain and all4 finals required. This is logical participation in a fixed registry.

Expected negatives are standalone validate calls, NOT successful timing samples:
worker0 exit AFTER first exchange frame; delayedworker0 with no heartbeats and
lease80/heartbeat20ms; notice1ms with delayedworker0. Each returns no valid result.
Changed source/fences/ACKs, dead service, poison/cancellation and final-owner loss
also require focused correctness tests. Actual process deaths/cleanup are tested.

Verifier must require exact arm inventory, unique run/slot/PID, release clean
build/source identity, exact effective config and manifest-derived ordered source,
full coverage/unique nonce/owner, no inflight reassignment, allN*N terminals,
row/byte/frame conservation, empty producer terminals, activation/drain facts and
all configured bounds before computing guards. Sidecar3 is bound to result v1.
Use Rust offline schedule-digest/policy reconstruction and copied-artifact
negative tests. Bundle raw failures/samples/manifests/commands/sidecars/quality
logs and exact Git source with SHA256 inventory. No timing guards in CI.

A guard failure remains FAILED, never retuned after observation. Readiness
failure is UNVERIFIED. Broader physical process add/remove remains a PRD gap
irrespective of numeric outcomes; the final report must say partial accordingly.
Historical Phase6–9 timings retain their ORIGINAL identities. New legacy release
regressions, if run, get their own raw results and source; otherwise say clearly
that only correctness was rerun. Do not attribute historical timings to this code.
