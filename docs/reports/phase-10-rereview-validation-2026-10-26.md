# Phase 10 remediation re-review validation — 2026-10-26

**GO for bounded research use and a narrow local imbalanced scheduling claim.**
Historical archive replay, all six supplementary matched comparisons and the
fresh original 13-arm confirmation pass. Independent host/day repeatability is
UNVERIFIED. Static remains default; Phase10 remains PARTIAL.

## Scope and identity

Baseline: `5f144eb7ce9ed0a3de0752b7906c3c442170bedc`. This responds to F1–F4
in the supplied independent remediation re-review. The attachment supplies review
findings; the user's request authorizes addressing them and one final commit.
No model/executor selection, subagents, push, PR, merge or Phase12 work occurred.
The unrelated untracked bootstrap prompt remains untouched.

Measured source is the clean detached snapshot
`b4a2a70dca70adf1eaac1a994f1cce7ee8fb1f74`, crates tree
`28a9989e6d3cbb57ea4f101c3211cc25ef83b810`. It is a measurement object retained
in the evidence's Git bundle, not an extra commit on the branch. The single final
branch commit adds this report and status documentation; code, configuration and
scripts match all 117 entries in `quality-source-equivalence.json`. Measurements
belong to the snapshot, never to a relabelled final HEAD.

Environment: Darwin arm64, kernel 27.0.0, eight logical cores; rustc 1.99.1
(58cf8f9c3), cargo 1.99.1. Cargo.lock SHA256
`9e6e1c1c9ff17000f35b15b271e9cfc264ba5d0ea39b5a013e396d6219caba87`.
Release uses opt-level3, thin LTO, codegen-units1, debug-info1 and no debug
assertions. `build-command.json` records the clean checkout, target cache,
command and captured binary SHA256 values. Full workspace release build passed.

## F2: historical evidence replay

The immutable local archive is available at
`benchmarks/results/phase10-remediation-20261026-bundle.tar.gz`, outer SHA256
`850b166a48b062558f32c617ce59f536d9081ab349d478ad6e8317a23b823877`.
Fresh extraction verified all 733 inner SHA256 inventory entries. Each release's
own source archive/verifier and captured sibling binaries replayed its full
verification result exactly: source bundle/archive/commit/crates tree, manifest
and independent oracle, 13 arms, 62 unique children / 49 measured samples,
run/slot/PID/effective configuration, leases, all terminals and row/byte/frame
conservation, recorded readiness, three stage-specific negatives and all six
medians recomputed from every original measured iteration. No original artifact,
including verification.json, was rewritten. All 733 entries still matched after
replay. The reusable `scripts/phase10-replay.py` repeats this read-only procedure.

| Historical guard | fcc325e replay | ee9f8da replay | Frozen limit |
|---|---:|---:|---:|
| Imbalanced job / 2 | 1.857053 FAIL | 0.660238 PASS | 0.90 |
| Imbalanced tail / 2 | 1.700962 FAIL | 0.642063 PASS | 0.90 |
| Uniform job / 2 | 2.861890 FAIL | 0.833954 PASS | 1.15 |
| Imbalanced job / 4 | 1.350122 FAIL | 0.431919 PASS | 0.90 |
| Imbalanced tail / 4 | 1.249904 FAIL | 0.415751 PASS | 0.90 |
| Uniform job / 4 | 3.022507 FAIL | 1.143644 PASS | 1.15 |

F2's local evidence-access/replay gap is closed. SHA integrity and referenced host
readings are not signed attestation. This verifies archived observations on the
same machine; it does not independently reproduce old timing or attest a trusted
host. Full physical datasets remain in ignored local directories; all descriptors
and exact manifests remain archived. No public hosting or independent-host replay
is claimed.

## F1: supplementary comparator

`producer_pipeline=static_projected` is an opt-in diagnostic with static modulo
ownership and descending-byte/MorselId local order. The shared file producer uses
identical projected key/value decoding, first-footer reuse, per-file fixed-hash
contribution drain, lazy spill, source admission/pre/post hashes, bounded scoped
hashing, parallel preparation, row-preserving packing, and final merge. Every
file must advance successfully sent plus owned pending rows by its exact output
count before proceeding; pending rows flush before terminal validation. Full
catalog metadata is capped by the same source admission, with at most the capped
file count in producer reference lists. Task, hash scratch, provisional routing,
receiver and transient decoder memory remain separate; none is a process RSS cap.

This control has no lease service or ACK and uses ordinary static transfer
framing. `leased_no_steal` retains the entire leased control/ACK/typed transfer
path while enforcing the same modulo owners, so its comparison with leased_v1
isolates policy more closely. The verifier requires equal modulo file/byte shares,
contribution rows and packed frame counts between static_projected and no-steal,
plus source/oracle/terminal correctness for every sample. All imbalanced leased
samples must include actual off-preference grants; no-steal must include none.

Legacy static remains unchanged and default. The comparator refuses recovery,
non-fixed partials, sampled topology, other retention, scheduler tuning and fault
hooks. It is refused through the bounded Phase11 frontend. Control ADETCP12,
exchange wire2/hash1, result schema1, manifests and independent oracle are
unchanged. No new dependency or public upstream API assumption is introduced.

The separate premeasurement declaration is
[phase-10-rereview-experiment.md](../research/phase-10-rereview-experiment.md).
It freezes one 16-arm matched campaign (80 children / 64 measured) followed by one
original 13-arm confirmation (62/49), with the original repetitions, six limits,
resources/lease/grace settings, full verification and timing/sidecar boundaries.
Both reuse the original release-2-ready-3 files. Exact manifest-byte SHA256:
imbalanced `76c1087a0cdba5033a6c7a91c64f43f0f6e141bf5960d56b5b07a2863197590b`;
uniform `7a8ff587161c0e785fd588756c5cfa215beb7a6148efa512d34417c8bad53a16`.

## F4: research operating envelope

Trusted localhost peers and a fixed 2–4-process registry must receive enough host
scheduling time to renew leases. Defaults remain lease 10000ms / heartbeat 250ms;
the admitted minimum 80/20ms is a fail-closed boundary, not a CPU-starvation SLA.
The two actual historical minimum-lease aborts during concurrent thin-LTO
compilation remain in the original archive and readiness report. This follow-up
neither widens their limits nor relabels them as successful jobs.

A single stalled peer is isolated within the existing bounded 2*N+1 connection
service. Multiple malicious peers, CPU starvation, sustained host contention and
production HA are outside the availability claim. Expiration/ambiguous renewal
cancels with no output; no in-flight reassignment occurs. Existing real-process
producer death, disabled-renewal expiry, final loss and inadequate spot-notice
regressions still assert error/no result, surviving-worker responsiveness and
owned temporary-root cleanup. Graceful producer participation is not spot survival
or physical process addition/replacement, migration or leased failover.

## Correctness and quality

All final commands passed against the same runtime inputs:

- `cargo fmt --all -- --check`.
- `cargo check --workspace --all-targets --locked --offline`.
- `cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings`.
- `cargo build --workspace --all-features --locked --offline`.
- `cargo test --workspace --all-features --locked --offline`: **194 passed**, one explicitly ignored local diagnostic; no failed unit/integration/doc test.
- `bash scripts/smoke.sh`: **PASS**, correctness only.
- `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p 'test_*.py'`: **20 passed**.
- Clean snapshot: `CARGO_TARGET_DIR=.../target cargo build --workspace --all-features --release --locked --offline`: **PASS**.

New tests cover strict comparator admission/default serialization, real 2/4-worker
exact output/modulo ownership, matching no-steal file/contribution/frame counts,
source refusal and cleanup, comparator-label forgery and removed repetitions.
The workspace gate also runs Phase6 real final replay/partial gather isolation,
Phase8 sampled topology/pinned recovery, Phase9 join/adaptive-partial exactness,
Phase10 source/fence/stale/expiry/death/notice/renewal/control regressions and
Phase11 IR/SQL/native/output-cap cases. Historical expensive Phase6–9 timing was
not rerun or relabelled. CI remains correctness-only; Linux/MSRV CI was not run
locally. The macOS linker emitted its existing compact-unwind size warning in
debug builds; strict Clippy and tests pass.

Development failures remain in raw logs: the first focused socket test was
denied by the sandbox and passed with local loopback access; initial Clippy
rejected a truncating cast in the new test, corrected before the final gate;
explicit py_compile inherited an unwritable home cache, while syntax compilation
and all Python tests subsequently passed with bytecode disabled. No environment
failure or failed check is presented as a successful run.

## Matched campaign results

`matched-1` was UNVERIFIED with zero measurements after the build (load1 up to
19.50, final15.24, limit8). It remains archived. After cooldown, `matched-2` passed
readiness at idle 82.31/80.95/82.61%, load1=4.55/4.32/3.89 and unchanged swapouts
1,823,764 pages. All16 arms / 80 children / 64 measured samples, both independent
oracles, full sources, matching projected/no-steal contributions and packed
frames, lease reconstruction, terminal conservation and three reached negatives
pass. There was no measured retry or best-of selection.

| Regime / workers | Legacy static job / tail ms | Projected static job / tail ms | No-steal job / tail ms | Leased v1 job / tail ms |
|---|---:|---:|---:|---:|
| imbalanced / 2 | 167.448 / 162.263 | 113.503 / 109.844 | 119.221 / 115.654 | 87.538 / 83.664 |
| uniform / 2 | 105.688 / 101.287 | 83.977 / 80.620 | 110.524 / 105.153 | 85.529 / 80.598 |
| imbalanced / 4 | 160.145 / 155.254 | 138.424 / 131.488 | 138.691 / 133.053 | 73.761 / 67.119 |
| uniform / 4 | 59.730 / 53.762 | 63.976 / 58.668 | 67.203 / 61.276 | 66.658 / 61.384 |

Every median uses ALL five measured 2-worker or three 4-worker samples. Full
individual samples/ranges and per-worker stages remain in verification.json.

| Guard | Leased / legacy | Leased / projected | Leased / no-steal | Limit for first two |
|---|---:|---:|---:|---:|
| imbalanced_job-2 | 0.522777 | 0.771238 | 0.734250 | 0.90 |
| imbalanced_tail-2 | 0.515604 | 0.761659 | 0.723395 | 0.90 |
| uniform_job-2 | 0.809256 | 1.018475 | 0.773845 | 1.15 |
| imbalanced_job-4 | 0.460591 | 0.532867 | 0.531839 | 0.90 |
| imbalanced_tail-4 | 0.432316 | 0.510455 | 0.504451 | 0.90 |
| uniform_job-4 | 1.115985 | 1.041917 | 0.991892 | 1.15 |

All six projected-static comparisons and all six legacy comparisons PASS.
Imbalanced measured off-preference grants are [8,7,7,7,8] at 2 workers and
[10,12,10] at 4; the full per-morsel preference/owner/nonces/reasons remain in
sidecars. No-steal has zero off-preference grants. Uniform leased_v1 also has
**zero actual steals** in every sample. Its no-steal timing difference is not
evidence of a stealing benefit; those ordered local observations may reflect
host/control variability. The supported causal claim is the local imbalanced
improvement over matched static ownership; projection/setup also materially
improve the legacy baseline, so the full historical speedup cannot be credited
solely to scheduling. No universal uniform speed advantage is claimed.

For the median-query uniform4 child of each file-pipeline arm, the following
are per-worker wall-time ranges in ms. These overlap between workers and
producer/final stages; they are **not added** into job latency or CPU.

| Uniform4 control | Catalog hash / prepare | Worker source hash | Decode | Partial aggregate | Acquire / ACK | Route / send | Final aggregate / source pull |
|---|---:|---:|---:|---:|---:|---:|---:|
| static_projected | 13.011 / 1.467 | 25.584–26.906 | 7.790–8.078 | 4.769–5.110 | 0.000–0.000 / 0.000–0.000 | 0.096–0.106 / 1.245–1.552 | 0.650–0.807 / 43.009–43.172 |
| leased_no_steal | 13.132 / 1.401 | 24.734–26.053 | 7.982–8.161 | 4.614–4.994 | 1.403–2.071 / 1.055–1.334 | 0.087–0.118 / 0.893–1.158 | 0.497–0.543 / 45.455–47.254 |
| leased_v1 | 13.753 / 1.361 | 24.035–25.341 | 7.776–7.953 | 4.617–5.045 | 1.454–2.017 / 1.130–1.425 | 0.095–0.137 / 0.993–1.176 | 0.489–0.574 / 44.359–46.267 |

## F3: separately declared confirmation

After F2 replay and matched evaluation, `confirmation-1` passed a fresh readiness
session at idle 81.49/81.46/80.56%, load1=2.67/2.65/2.70, unchanged swapouts
1,823,764 pages, and no competing compiler/benchmark/worker. The FIRST complete
confirmation uses the original 13 arms, 62 children / 49 measured, one warmup and
five/two-worker or three/four-worker iterations, independent DataFusion oracles,
full source/lease/terminal checks and all three reached fault stages. All six
unchanged original comparisons PASS; no alternate measured attempt was selected.

| Original guard at b4a2a70 | Confirmation ratio | Limit | Result |
|---|---:|---:|---|
| imbalanced_job-2 | 0.520425 | 0.90 | PASS |
| imbalanced_tail-2 | 0.506829 | 0.90 | PASS |
| uniform_job-2 | 0.862750 | 1.15 | PASS |
| imbalanced_job-4 | 0.436957 | 0.90 | PASS |
| imbalanced_tail-4 | 0.415309 | 0.90 | PASS |
| uniform_job-4 | 1.116132 | 1.15 | PASS |

Uniform4 is **66.421ms leased / 59.510ms static**; the frozen 1.15 limit
is 68.436ms, leaving 2.015ms of local median margin. The historical
ee9f8da 0.378ms margin remains its own result. Both new campaigns ran on the
**same host and same day**, with only three measured4-worker samples per arm.
This is a fresh-session local confirmation at b4a2a70, not an independent-host,
independent-day, or bit-identical ee9f8da timing replication. Those stronger
repeatability claims remain **UNVERIFIED**. No best-of repeats, baseline changes
or lease/grace/threshold tuning occurred.

## Disposition

**GO for the existing bounded research runtime and a narrow, locally measured
imbalanced scheduler-policy gain.** The immutable historical end-to-end PASS,
new matched comparisons and fresh confirmation are distinct evidence. Uniform
stealing benefit is not established because no uniform sample actually stole.
No P0 defect was found. F1 is closed for the declared matched experiment; F2 is
closed for local archive access/integrity/replay; F3 has a passing local
confirmation with independent host/day repeatability UNVERIFIED; F4 is documented
and existing no-output/cleanup assertions pass. Production availability under
CPU starvation/hostile peers is outside scope.

Static remains the supported default. Phase10 stays PARTIAL for fixed physical
membership; Phase11 bounded v1 remains COMPLETE. Physical worker addition/removal,
migration, leased failover, spot survival, broader frontend admission, and
Phase12 remain deferred. The original failed sources/experiments, two real
compiler-contention expirations and the historical six criteria are preserved.
No Phase6–9 timing acceptance was rerun or inferred.


## Immutable follow-up bundle and reproduction

The complete local follow-up archive is
`benchmarks/results/phase10-rereview-20261026-bundle.tar.gz`, SHA256
`8fdfa0adda72f6ad7b1196dc334761e1298ed6584d8fffa67c1a8359966722bb`.
It contains 796 checked entries, including the UNCHANGED original archive
as `historical-bundle.tar.gz`, captured binaries, source bundles/tars,
predeclarations, full raw results/sidecars, host seals, commands/statuses, quality
logs, zero-measurement refusal, offline replays and retained copied forgeries.
`phase10-rereview-20261026-bundle-check.json` records fresh extraction, inventory
verification and exact archived-source/binary replays of both new campaigns,
without modifying any archived artifact. Both replays reproduce PASSED.
The earlier historical replays independently preserve FAILED/PASSED exactly.

Measured source archive SHA256:
`cf33f42c054bf44ab34e371d118d38ccf428592337f916fb877e36600c318bc3`.
Captured ade-bench SHA256: `cda6929050938c47a9f83eb9ebef73df90fa0a7ff79908373676d053fb627f6e`;
ade-worker: `2a5e9f2e44e425e34342b4eda08b89a4102f2651509e5bee35af59a0e41edbf5`.
All 117 code/configuration/script inputs match the frozen source at closure.
Three resealed copied attacks reject: best-only samples, removing the projected
pipeline label, and compensating modulo row-count forgery. Originals remain
unchanged; copied payloads and precise rejection reasons remain in the archive.

To replay the original archive read-only on this macOS arm64 machine:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 scripts/phase10-replay.py \
  benchmarks/results/phase10-remediation-20261026-bundle.tar.gz \
  /private/tmp/ade-historical-replay-NEW \
  --sha256 850b166a48b062558f32c617ce59f536d9081ab349d478ad6e8317a23b823877
```

Use a new output directory. For the follow-up bundle, verify its outer SHA256,
extract to a new folder, verify every sha256.json entry and follow REPRODUCE.md.
Use each archived verifier with its captured sibling binaries. New-campaign
verify() calls use `matched=True` for matched-2 and `False` for confirmation-1;
calling the function leaves verification.json unchanged. Offline checks do not
rerun timings. Exact invocation JSON and deterministic manifest descriptors are
retained for prospective fresh query reruns. The archive is local evidence with
no public hosting, signed-host or independent-host replication claim.
