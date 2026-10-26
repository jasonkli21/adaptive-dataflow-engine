# Phase 10 — bounded morsel scheduler implementation

Status: **PARTIAL — bounded-v1 correctness and all six unchanged local release
guards now PASS at ee9f8da; physical process membership remains an explicit PRD gap.**

Current stabilization (2026-10-26): continued remediation passes all 62 successful
children/49 measured samples, the independent oracle, source/lease/terminal checks,
and three reached negative stages. Uniform4 passes narrowly at 1.1436 against
1.15. Static remains the supported default. Phase 11 bounded v1 is COMPLETE;
Phase 12 remains out of scope. See the [dated readiness report](repo-wide-readiness-2026-10-26.md).

The original source below and the first remediation fcc325e each failed all six
guards despite passing correctness. Their samples, source identities and original
“No Phase11” declarations remain historical provenance; no failed campaign is
reclassified as passing.


## Baseline and authorization

User explicitly requested Phase10 implementation, all in this session, no
subagents, one final commit. No subagents were used. Baseline HEAD was exactly
3e89edfd0230674f0e06437cccb58fa99e369422; existing untracked Phase10 plan was
incorporated, unrelated bootstrap prompt preserved and excluded. Phase9 bounded
closure remains valid. Its frozen timing source is9b0330a/runtimefab08a0; Phase8
closed separately atab7404b/runtime432cc7dd. Neither historical evidence nor
failed experiments are relabelled as measurements of this implementation.

PRD Phase10 authorizes scheduling; stale AGENTS Phase9-only text was reconciled
before code. ADR0017 refines architecture §§6.2,9,13,17,22,27–31 and preserves
INV1–10. The plan explicitly records spill-only leased v1, fixed physical registry,
completion-count controls and exact partial contribution draining. Final frozen runtime source is f32da29ce6435eeb27cf89fda0750343eec6acf2,
crates tree96b0a4e913518821d5781110d9724de9b0cf8154, committed in a temporary
clean repository before release measurement. No intermediate commit was added to
the user branch. The earlier090d782 snapshot was unmeasured and superseded before
timing to bound source-hash cancellation. Final report/ADR wording may differ;
runtime-equivalence.json checks every crates blob against the frozen source.

## Supported and refused contract

| Execution | Supported | Refused |
|---|---|---|
| static (default) | Existing five retention modes, Phase6 final-only recovery, Phase7 cost copies, Phase8 sampled48 routing, Phase9 non-fixed partials/join through existing adapters | Existing failure/boundedness restrictions remain |
| leased GroupBySum |2–4 loopback processes, manifest whole-file morsels, fixed hash build, spill exchange, static final owners, exact checked sum/counts, verified source identities, no-steal/steal controls | Recovery must explicitly be disabled; snapshot must be non-replayable; sampled/prefix routing, non-fixed partials, cost copies, other retention modes and joins reject at admission |
| participation | Already-running producers activate/drain; spot notice uses the same drain with grace deadline; all original finals remain alive | Physical process add/remove/replacement, state migration and broader recovery |
| failure | Cancel survivors/quarantine provisional gather; parent owns child cleanup | Any ambiguous/expired lease, stale fence/ACK, source drift, producer/final death or service failure returns no successful output; never replay leased output |

One immutable MorselId/file descriptor is separate from nonce/worker/task/attempt.
Coordinator serializes Pending→Leased→Completed with one outstanding lease per
producer; identical ACKs are idempotent and conflicting ACKs fail. Fences bind
job, execution UUID, epoch1 and digest of ordered source, registry/config/hash1
final topology. All workers prepare/ACK and all producer clients become ready
before the first grant. Preferred modulo queues remain explicit; only pending
work can be stolen. Every decision reconstructs through the pure policy before
successful output. Declared locality is a hint; shared localhost disk is not
remote placement. Capacities are bounded integer declared weights, not CPU speed.

Each producer appends sequential file contributions to ONE persistent sender set
under original TaskId(worker),attempt0. ExactlyN terminals close, including empty
parked/drained roles. Context-bound aggregate schema4 on unchanged wire2 verifies
terminal fingerprints; static schema1 is unchanged. Every lease, source/producer
counter andN×N row/byte/frame conservation check precedes attempt-safe gather
commit of the entire original final set. Gather uses the same schedule context.

Hash partials retain i128 sums internally and use the existing bounded contribution
drain to split otherwise unrepresentable Int64 partial sums while conserving
count. This avoids a false overflow at the new file boundary. Final checked Int64
overflow still fails. This is complete-file fixed hashing, without raw tail or
an adaptive operator switch; spill fragments may contain repeated keys.

## Bounds and limitations

Default max256files, explicit ceiling1024, source catalog<=128KiB of the1MiB
control/evidence frame; source<=1GiB (config ceiling1TiB),100m input rows,
1GiB encoded ingress/1mframes PER final/gather store. One outstanding lease/worker;
audit ring default256/ceiling512 with dropped count; at most16 participation events;
100k controls default/1mceiling. Control socket500ms server/1000ms client; finite
stage<=120s, heartbeat20–1000ms, lease>=4heartbeats and<=120s. Early renewals do
not extend a lease. One service thread and one independent renewal thread/producer
keep scans and synchronous sends from starving heartbeats. No opportunistic expiry
reassignment. Every idle leased receiver polls cancellation, including partial
length prefixes; a healthy interval between file outputs may exceed legacy5s.

Task memory/prefetch/spill and bounded exchange queues remain separate. Producer
and final budgets overlap; spill volume in the local aggregate remains unquotaed.
Arrow/Parquet/control/IPC transients, OS buffers, source hashing and harness/RSS
are separately scoped; the ledger is not a process RSS cap. Files verify before
preparation and before/after each read; filesystem immutability during a read is
still assumed, not transactional snapshot isolation. OS reads remain non-preemptible.
Physical process membership and destination topology remain fixed throughout.

Private controlADETCP12 rejectsADETCP11; exchange wire2/hash1/result schema1 stay.
Leased sidecar3 binds result run/slot/PID, effective config, exact manifest-derived
source, frozen task/registry/digest, bounded lease/audit/event facts and terminal
metrics. Standalone validate has no result run/slot. New offline verifier recomputes
source/config identity, every pure decision and source/terminal conservation.
Legacy acceptance helpers can explicitly select protocol12 while historical
verifier11 remains available; no old timing result is rewritten.

## Verification and empirical evidence

Final commands PASS on the runtime byte-identical to the frozen source:

- cargo fmt --all -- --check
- cargo check --workspace --all-targets --offline
- cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
- cargo test --workspace --all-features --offline:154 tests plus doc tests
- cargo build --workspace --all-features --offline
- bash scripts/smoke.sh
- Python syntax checks and6 existing verifier unit tests

Logs are benchmarks/results/phase10-quality/. The first153-test gate passed;
source-hash deadline/cancellation review justified the final repeat with154.
Local TCP tests required loopback access outside the filesystem-only sandbox.
Debug linking emitted the existing macOS large-unwind-table warning, without
failing the build/tests. Two unmeasured release builds were explicitly stopped;
only the final clean runtime source is eligible for timing. No timings from
those binaries count.

Focused new cases include real2/3/4-process coverage; singleton and empty files;
a persistent leased stream idle past5s; actual pending steals; activation plus
spot/drain while owning a lease; independent renewals; nonce/job/epoch/source/ACK
conflicts and duplicate ACK idempotence; malformed/dead service; source fingerprint,
byte/frame/row-bound refusals; poisoned store; real producer exit after bytes and
final exit after sending its first gather frame; expired/no-heartbeat and insufficient-notice cancellation;
exact i128 file contributions with signed cancellation and final overflow refusal.
Legacy tests include five retention modes, real final-only recovery and complete
copy restore, cost decisions/cleanup, sampled split/coalesce and pinned recovery,
join bag/context/failure and adaptive partial cases. CI correctness has no timing
threshold. Historical frozen Phase6–9 timing remains separately attributed.

## Source-bound empirical outcome — FAILED, preserved

The complete unmodified v1 protocol ran on clean source
f32da29ce6435eeb27cf89fda0750343eec6acf2/runtime96b0a4e913518821d5781110d9724de9b0cf8154.
Source tar SHA256: ee5b98ebc22378b1f02899514785c7498152c6153f5a94624db403ec193b24da.
`source.bundle` contains the exact Git objects, and the verifier checks the tar
against that commit/tree. The release build passed in6m32s (after explicitly
stopping the superseded unmeasured builds). No Rust runtime edits followed measurement.
Later changes affect reports, an ADR explanation and the negative-test helper only.

The first readiness attempt took ZERO measurements: idle75.40/80.71/77.41%,
load11.65/9.44/8.37 exceeded the frozen<=8 guard; swapouts stayed6202888 pages;
no other cargo/rustc/ade-bench/worker process. The unchanged retry passed:
idle76.70/81.60/79.12%, load3.80/3.29/3.15, unchanged swapouts. Both attempts remain.

All 62 successful children (49 measured), all 13 declared arms,1+5 primary/1+3
confirmation repetitions and3 expected negatives are retained. Inputs are4m
rows/32files/key1024, exact seed42 and physical weights from the declaration.
Imbalanced/uniform logical fingerprint is the SAME
`a568675dab5518ae407fb3601b3f8057fb14156d79ca3ad4703d29813821e139`;
physical source bytes83539241/83626120 respectively. Every paired arm uses its
exact same manifest. All original queries use independent DataFusion validation.

All medians/ranges below are milliseconds; every sample is in verification.json.

| Source/workers | Policy | Median query | Query min–max | Median last producer completion |
|---|---|---:|---:|---:|
| imbalanced/2 | static | 180.935 | 180.112–182.439 | 165.033 |
| imbalanced/2 | leased_no_steal | 504.254 | 499.657–516.424 | 460.272 |
| imbalanced/2 | leased_v1 | 375.357 | 373.013–377.721 | 331.460 |
| uniform/2 | static | 112.496 | 111.123–116.381 | 96.576 |
| uniform/2 | leased_v1 | 382.577 | 379.581–382.663 | 337.773 |
| imbalanced/4 | static | 184.189 | 181.228–190.787 | 157.216 |
| imbalanced/4 | leased_no_steal | 479.786 | 472.677–489.115 | 421.753 |
| imbalanced/4 | leased_v1 | 286.602 | 272.649–286.680 | 222.531 |
| uniform/4 | static | 80.090 | 79.089–85.595 | 54.889 |
| uniform/4 | leased_v1 | 279.810 | 276.002–289.140 | 223.582 |

| Frozen guard |2 workers ratio |4 workers ratio | Limit | Outcome |
|---|---:|---:|---:|---|
| Imbalanced job | 2.074546 | 1.556017 | 0.90 | FAIL both |
| Imbalanced last producer | 2.008443 | 1.415445 | 0.90 | FAIL both |
| Uniform job | 3.400814 | 3.493694 | 1.15 | FAIL both |

No threshold, source size, file layout, arm, repetition or sample was changed after
measurement. Warmups are separate and every measured child enters its median;
no best-only selection. These are failures of the predeclared local release
criteria, not accepted Phase10 speed results. Whole-job leased steal is slower
than static despite being faster than leased-no-steal. Example primary measured
child actually steals6 pending morsels, file ownership10/22; four-process child
steals12 with8files/worker. Actual off-preference leases and policy reconstruction
prove the mechanism; they do not establish an end-to-end win.

The diagnostic evidence explains much of the cost: primary uniform sample
operator work97.815/97.560ms versus static90.975/91.103ms; leased pre/post source
verification86.030/86.121ms, acquire/wait26.865/26.868ms and ACK23.221/23.219ms,
plus coordinator admission verification and adapter setup. Leased output32768
partials/64frames versus static2048/4frames. These measured facts motivate future
work on verified scan/source access and control overhead; no unmeasured fix or
hidden baseline slowdown is claimed. Hash work itself improved under imbalance,
but last-producer completion includes required verification/control and failed.

Live4-process participation succeeds: a measured child activates2 at22ms after
one completion, notices0 at37ms while it owns morsel20, requests repeated drain
at40ms and closes0 at1037ms within the5000ms grace. Worker2 really executes15
files, worker1 executes16, worker0 one; parked3 executes0 and still sends4 empty
terminals. All4 original finals and16 producer terminals commit;3 independent
renewals keep0's lease alive. No post-cutoff grants to0 and no source duplication.
The participation median1197.284ms is a hook/control measurement, not autoscaling.

Capacity release controls yield median396.159ms unweighted/402.104ms declared
1,4,4,4 with the SAME100ms slow-worker hook. File counts are2/10/10/10 in every
measured child of BOTH arms; exact byte/path assignment differs slightly, but
there is no material count redistribution or speed advantage from the weights
on this fixture. Pure tests show weights change valid decisions. Do not infer
physical heterogeneity, throughput calibration or a capacity-policy performance win.

All 3 standalone negatives (real exit after exchange output, no-heartbeat expiry,
and1ms inadequate notice) return failure with no validated output. Process/final
loss and cancellation/cleanup are additionally covered in the real multiprocess
correctness tests. The verifier reports correctness_passed=true,passed=false;
its nonzero exit is expected from the6 failed timing gates, not a query error.

### New legacy regressions on the SAME frozen runtime

- Phase6 acceptance passes: five static modes, normal and single-final replay,
  adaptive2/4-worker cases and explicit missing-snapshot unrecoverable negative.
- Phase7 predeclared empirical acceptance passes: expected cost proxy
  4969988640.6525 nanoUSD below always6583048397.6814995,
  never9388274890.8135 and threshold9378656257.104; original normal-overhead
  and decision/restore/cleanup criteria pass. This is the declared proxy, not
  provider billing or measured cloud economics.
- Phase8 all 39 children and original gates pass: primary skew work0.639453,
  whole-job0.974560, uniform1.007542;4-worker skew work0.097608/job0.766757;
  split/coalesce, raw-hot negative, static_packed attribution and pinned
  replay/restore remain. The first verifier invocation lacked ADE_WORKER_BIN
  in the temporary checkout; its FileNotFoundError is preserved, and rechecking
  the SAME raw samples with the absolute binary succeeds. No samples were rerun
  or removed to obtain this verification.
- Phase9 join/partial correctness and smoke ran in the154-test gate. The full
  Phase9 timed118-child matrix was NOT rerun. Its historical frozen timings
  retain9b0330a/runtimefab08a0; these new Phase6–8 results do not rewrite its release.

### Integrity, negative evidence and reproducing

Raw failed Phase10 release: benchmarks/results/phase10-v1-ready2/. Zero-measurement
readiness failure: phase10-v1/. New legacy results: phase10-legacy/.
Seven copied attacks reject: run ID, slot, manifest-derived source inventory,
effective config, nonce, terminal rows and best-only samples. The final best-only
attack recalculates a consistent one-sample result/summary and matching sidecar;
`check-result` accepts it, but the fixed release inventory rejects it. The initial
weaker attack and both helper versions are preserved. This supplemental negative
test changes no runtime, positive sample or timing guard.

The ADR's precise10% predictive-steal hysteresis clarification was saved BEFORE
readiness/timing in phase10-premeasurement-policy.json with SHA256; it expands
prose shorthand to match the already-frozen pure code. This is a documentation
clarification outside the runtime commit, not a changed experiment or hidden
mechanism. Its exact text/hash is included in the closure.

Local closure: `benchmarks/results/phase10-closure-2026-10-24-bundle.tar.gz`
SHA256: `b63035c80442fc4eba2b7fe075e899ef441dbaaeee5b05bee9091a27373a1c3b`.
It contains 492 checksummed files: exact source tar/Git bundle, matching captured
release binaries for offline verification, every successful/failed measured
sample, manifests/oracles/commands/sidecars, both readiness attempts, legacy
regressions, quality/development logs, negative evidence and supplemental helpers.
Offline re-verification using the bundled binaries reproduces correctness=true
and all 6 failed gates; every file digest and outer archive hash matches. Follow
REPRODUCE.md. These artifacts are local/ignored and were not pushed or published.

## Independent review follow-up — acceptance still partial

The review handoff is treated as findings and evidence, not as authority to
change the phase boundary or timing gates. The follow-up fixes F-02 through F-05:

- A producer with a scheduled future activation waits while that event is still
  pending, including when all remaining morsels are leased. A parked producer
  with no future activation can close immediately.
- `min_steal_bytes` remains a preference. If it excludes every off-preference
  candidate, an active requester has no local pending work, and all pending
  preferred owners are parked/draining/drained, leased_v1 takes the best
  remaining morsel to preserve progress. If any preferred owner remains active,
  the requester waits. `leased_no_steal` is unchanged.
- Admission validates event thresholds against the actual source catalog and
  rejects an infeasible last-active drain before worker preparation. An event at
  the catalog's final completion remains valid and is applied before closure.
- Acceptance and verifier checks now use an explicit `VerificationError` /
  `require` path that remains active under optimized Python. Dataset generation
  and oracle commands execute outside checks. The Phase10 verifier performs its
  own source-bundle/commit/tree/archive checks instead of importing Phase9's
  assertion-based helper.

Focused ledger tests pass for the four-worker late activation interleaving, the
under-threshold stranded queue, empty parked closure, active-owner threshold
behavior, actual-catalog event bounds, final-completion activation, and
last-active drain refusal. The full workspace gate also reran the real
multi-process steal/activation/drain integration test successfully with local
loopback access.

Follow-up quality gate on this source: `cargo fmt --all -- --check`,
`cargo check --workspace --all-targets --offline`,
`cargo clippy --workspace --all-targets --all-features --offline -- -D warnings`,
and `cargo test --workspace --all-features --offline` all pass (158
unit/integration tests plus doc tests). `bash scripts/smoke.sh` passes. The smoke
comparison emitted a one-sample regression classification for its configured
static comparison; it is a correctness smoke, not Phase10 timing acceptance.

The optimized verifier was run against the original local release inventory and
captured binaries: correctness remains true for 62 children/49 measurements and
the same six predeclared timing guards remain failed at ratios 2.074546/2.008443,
3.400814 (2 workers) and 1.556017/1.415445, 3.493694 (4 workers). A copied
evidence tree rejected eight attacks under `python3 -O`. The local
closure archive and its 492-file checksum manifest also verify against the
reported outer SHA256. The archive remains ignored and unpublished, so external
reviewers still need a release asset or supplied bundle to reproduce that audit.

F-01 remains open: no timing optimization was implemented or measured in this
follow-up. The original measurements already quantify source hashing, lease/ACK
control and per-file partial amplification, and remain preserved. The current
runtime therefore has no source-bound timing acceptance evidence, and Phase10
stays PARTIAL. Do not attribute the old ratios to this follow-up source or relax
the guards.

## Remaining PRD gaps and handoff

Phase10 remains partial. The current slice fails all empirical guards; even a future passing slice would not mean full Phase10 completion:
physical process joining/removal, final-state migration, true spot replacement,
row-group splitting and leased failover remain future, separately gated work.
Phase11 SQL/DataFrame/general IR is not implemented or authorized as cleanup.

The user's one final commit contains this report and the byte-identical frozen
runtime; obtain that final commit with `git log -1` after handoff. No source-only
snapshot commit was added to the user's branch, and nothing was pushed/published.
The unrelated bootstrap prompt remains untracked and untouched.


## Phase 10 remediation addendum — 2026-10-26

Baseline bcfdb05; measured clean candidate fcc325ec4364fe75a4af7df0b61485ad6bf82b4e,
crates f0f8d45fc068e6a573ce24b6c76fee8db2a1390a. One bounded candidate, no
subagents. Nonblocking control admission isolates one stalled peer with at most
2*N+1 sockets on one serialized-ledger thread. Bounded 256KiB hash reads retain
catalog admission and pre/post lease content checks. New release-evidence v2
independently verifies readiness, postflight, command/result identity and three
reached negative stages; original schemas/protocols and historical bundles stay.

183 Rust tests, 17 Python verifier tests, workspace fmt/check/clippy/build and
smoke pass. Focused legacy correctness includes final-only replay/copy, sampled
pinned recovery, joins/adaptive partials and Phase 11 native IR/SQL/output caps.
No Phase 6–9 timing campaign was rerun. All original timing attributions remain.

The unchanged 13-arm release retains 62 successful children/49 measured samples
and 3 stage-proven failures with empty stdout; correctness_passed=true,
status=FAILED. Readiness passes (idle81.31/80.12/81.30%, load3.05/3.21/2.95,
swapouts unchanged). All ratios below are leased_v1/static:

| Frozen guard | 2 workers | 4 workers | Limit | Outcome |
|---|---:|---:|---:|---|
| Imbalanced job | 1.857053 | 1.350122 | 0.90 | FAIL both |
| Imbalanced last producer | 1.700962 | 1.249904 | 0.90 | FAIL both |
| Uniform job | 2.861890 | 3.022507 | 1.15 | FAIL both |

Eight original and nine new copied-artifact attacks reject, including resealed
best-only and preflight-only negatives. Raw failures/readiness attempts remain.
The source/scan critical path remains too costly for a contribution/control-only
second candidate; the diagnostic quantifies that bound without summing parallel
workers. Further verified-source/contribution redesign is deferred. Phase 10 is
PARTIAL, static is the supported baseline, leased mode is experimental, physical
elasticity is deferred, Phase 11 stays COMPLETE bounded v1, and no Phase 12 began.
The dated report contains full medians/ranges, source/binary/data hashes, failure
facts, closure checksum, reproduction instructions and the conditional readiness
decision. Raw evidence is in benchmarks/results/phase10-remediation-20261026/.

## Continued remediation acceptance — 2026-10-26

Clean source `ee9f8da63b3910bfa99276f71e5ee72a19f3c021`, crates tree
`4b780320d99cfa421be380f15f110d951cd384b5`, passes all six unchanged guards in
`phase10-remediation-20261026/release-2-ready-3`. Ratios (leased/static) are
imbalanced whole/tail2=0.660238/0.642063, whole/tail4=0.431919/0.415751,
uniform2=0.833954 and uniform4=1.143644 (limits0.90/1.15). Uniform4's 0.378ms margin
is narrow; this is local release evidence, not a broad timing guarantee.
All 62 successful isolated children/49 measured samples pass the independent
oracle and complete source/lease/terminal/gather checks. Death after an actual
frame, actual no-heartbeat expiry and actual spot deadline tests fail closed as
intended. Twenty copied evidence attacks reject; archived verifiers reproduce
both the failed fcc325e outcome and this successful outcome without editing them.

The implementation keeps every full-content pass and per-file completion. It
uses scoped bounded hashing, explicit projected decode, persistent fenced control,
exact bounded provisional frame packing, lazy unused spill setup, a raw partial
drain without unused sorting, and compact count-positive table slots. Shared
changes apply to both controls and leased execution. Wire2/hash1/ADETCP12, result1,
manifest/child contracts and the independent oracle remain. Source/drift, lease
ambiguity, poisoning and unsupported loss still cancel; leased recovery was not
added. The final gate passes 192 Rust tests (one ignored local diagnostic), 18
Python tests, fmt/check/strict Clippy, build and smoke, including accepted static,
recovery, retention, topology, join and frontend correctness paths. Old release
campaigns retain their source identities and were not re-timed here.

The [dated readiness report](repo-wide-readiness-2026-10-26.md) contains precise
source/binary/archive checksums, raw evidence paths, diagnostic failures,
resource bounds, negative stages, reproduction instructions and the closure
archive reference. Physical process membership remains fixed at2–4 workers, so
Phase10 remains PARTIAL. Static remains default; Phase11 stays COMPLETE bounded v1
and Phase12 was not started. The old failed criteria above remain failed on their
original sources.
