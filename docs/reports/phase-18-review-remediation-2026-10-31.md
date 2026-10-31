# Phase 18 independent review remediation — 2026-10-31

Status: **`PARTIAL_PHASE18_UNIFIED_ADMISSION_V1`**. This report records the
bounded response to the supplied independent review of `43672fd`. It does not
change candidate5's historical measurements, archive contents, or acceptance
status for its frozen source. No push or service mutation was performed.

## Baseline and working tree

- Reviewed baseline: `43672fd2a42f0fd42eb253c688856f108e95c2c0`
- Baseline tree: `d6a78974293433eb09cc71cf221afdffb554b1a7`
- Initial tracked diff: clean. The pre-existing untracked
  `sol-medium-phase-0-bootstrap-implementation-prompt.md` was left untouched.
- Historical Phase 18 archive SHA-256:
  `fa38f4dbc0922d20b0d0075cd85dc54ee3f82b73c36ae7defb3a5c8beb2f2794`
- Historical Phase 17 reacceptance archive SHA-256:
  `ace36b8fa54ef0a279b4050e04481c18a9d7da39764aecee29eed1db4cf99e05`
- Both archives were replayed after remediation and retained their original
  summaries. No files under `docs/evidence/phase18*` were changed.
- Cargo 1.99.1 and Rust 1.99.1 are available. Java is unavailable, so no fresh
  Kafka/MinIO service campaign, G14 runtime campaign, or G16 real-broker quota
  campaign ran. An attempted worker-backed frontend test also encountered
  sandbox `Operation not permitted` while starting `ade-worker`.

The one local remediation commit is reported in the delivery response. It
contains the source, regression tests, reconciled plan/ADR/guide/support matrix,
and this additive report. The pre-existing untracked prompt remains outside it.

## Findings and changes

| Finding | Result |
|---|---|
| F18-01 stream semantic ownership | **Unresolved; bounded partial status.** The opt-in Kafka path still checks a separately supplied native configuration. The same-topic prefix/tail evidence proves offset classification and continuous native execution, not a logical-derived executable stream lowering. No native state, progress, or CURRENT3 selector contract was changed. A safe `NativeStreamSpecV1` lowering plus fresh G14 evidence remains separate structural work. |
| F18-02 target-first batch binding | **Binding fixed; native execution acceptance partial.** Added target-neutral structural validation for semantic extraction. `Catalog::plan_sql_unified(sql)` and the new `DataFrame::scan_unified(...).build_logical()` bind without a physical target; capability and forced targets are applied by the shared optimizer. The opt-in CLI registers join inputs from the SQL form and refuses before output when worker capability is absent or local is forced. Focused tests confirm the selected `DistributedJoin` native plan, local fallback for a small aggregate, and refusal paths. The worker-backed execution test could not complete in this sandbox. Legacy `plan_sql` and `DataFrame::build` retain their target checks. |
| F18-03 Kafka token parity | **Fixed.** Logical topic and cluster validation now match the native field-specific ASCII grammars. History/job token validation remains separate. Rust parser and `ade-plan` CLI negatives cover colon, slash, Unicode, case, and maximum-length examples; Python's independent oracle has matching positives and negatives. Error output does not echo the rejected token. |
| F18-04 diagnostic quota amplification | **Not measured.** No change suppresses per-record diagnostics. The paired 8/16 KiB real Kafka V2 comparison was not possible without Java and the owned service fixture. The original severity remains conditional; do not treat this as resolved. |

The plan and ADR now define additive G14–G16 and distinguish semantic admission,
target-neutral binding, native stream lowering, and same-topic cut classification.
The candidate5 implementation report is explicitly marked as historical. README,
AGENTS, guide, and machine support matrix show the current partial qualification.

## Current review gate matrix

| Gate | Current result | Evidence / limitation |
|---|---|---|
| G0 provenance / prerequisite | PARTIAL | Baseline and local changes are identified; no fresh matched Kafka/MinIO or Phase 17 service reacceptance for this source. |
| G1 contract/property security | PARTIAL | F18-03 parity negatives pass. No complete source-bound security campaign for changed source. |
| G2 optimizer | HISTORICAL_PASS | Candidate5 archive replay passes; optimizer arithmetic/measurement identity is unchanged. Not a fresh campaign for this source. |
| G3 batch parity | PARTIAL | Target-neutral join binding/selection and pre-output refusals pass. Native worker-backed join execution is NOT_RUN because child process I/O was denied. |
| G4 streaming parity | HISTORICAL_PASS | Original archived evidence replays. No fresh stream runtime execution on changed source. |
| G5 hybrid continuity | HISTORICAL_PASS | Original archived evidence replays; no fresh broker/source campaign. |
| G6 recovery / transactions | HISTORICAL_PASS | Phase 17 archive replays; no fresh Kafka/MinIO campaign on changed source. |
| G7 adversarial refusal | PARTIAL | New topic/cluster parser and CLI negatives pass; remaining changed-source adversarial matrix is not run. |
| G8 resource envelope | NOT_RUN | G16 near-minimum diagnostic quota and fresh resource peaks are not measured. |
| G9 measurement honesty | HISTORICAL_PASS | Candidate5's 12 samples and slower distributed sample remain unchanged; no new measurements were made. |
| G10 compatibility | PARTIAL | Existing wire formats are untouched and focused feature build/tests pass; full feature/API compatibility matrix not run. |
| G11 toolchain/regression | PARTIAL | See commands below. Workspace Clippy, full workspace tests and smoke are not claimed unless listed as successful. |
| G12 independent evidence | HISTORICAL_PASS | Both immutable archives replay successfully. No new live evidence bundle exists for changed source. |
| G13 honest closure | PASS | Plans, ADR, operator guide, README, AGENTS, support matrix and this report use the qualified current status. |
| G14 stream executable semantic ownership | FAIL | No logical-derived immutable native stream specification is consumed by the source/engine/state/sink path. |
| G15 target-neutral binding and native join | PARTIAL | Neutral binding, optimizer target choice, and no-worker/forced-local pre-output refusals pass. Native worker execution and matched source-bound acceptance remain NOT_RUN. |
| G16 diagnostic quota parity | NOT_RUN | No real Kafka comparison at 8 KiB/16 KiB was possible. |

## Commands and results

Successful focused checks:

```text
cargo fmt --all -- --check                                           PASS
cargo check --workspace --all-targets --locked                       PASS
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings PASS
cargo test -p ade-logical --all-targets --locked                         PASS
cargo test -p ade-frontend --features unified-planner --test queries \
  unified_join_binds_without_a_physical_target_and_cli_refuses_pre_output \
  --locked -- --nocapture                                                PASS
cargo test -p ade-frontend --features unified-planner --test queries \
  unified_typed_scan_and_build_do_not_require_a_physical_target \
  --locked -- --nocapture                                                PASS
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests \
  -p 'test_*.py'                                                        PASS (64)
python3 -B scripts/phase18/verify_evidence.py --archive docs/evidence/phase18/acceptance.tar.gz --manifest docs/evidence/phase18/manifest.json PASS
python3 -B scripts/phase17/verify_evidence.py --archive docs/evidence/phase18-phase17-reacceptance/acceptance.tar.gz --manifest docs/evidence/phase18-phase17-reacceptance/manifest.json PASS
```

The focused independent Phase 18 Python contract run also passed (7 tests). The
broader command
`cargo test -p ade-frontend --features unified-planner --test queries real_distributed_aggregate_and_join_oracle --locked -- --nocapture`
returned exit 101 when the sandbox rejected starting `ade-worker` with
`Operation not permitted`; it did not reach the new join case. This is not
reported as a passing worker execution. The full command
`cargo test --workspace --all-features --locked -- --test-threads=1` returned
exit 101 in `ade-distributed`: that crate reported 67 passed and 29 failed when
socket creation returned `PermissionDenied (Operation not permitted)`. Later workspace
test crates were not reached. `bash -x scripts/smoke.sh` returned exit 1 at its
distributed aggregate validation step after local checks passed; that path also
requires local process/socket execution. The focused target-neutral binder test
itself passes and checks that the CLI refuses before stdout when capability is
missing or local is forced. The complete feature matrix was not run.

No new performance samples, Kafka cuts, service identities, reader observations,
or resource peaks were collected. Candidate5 performance evidence remains
descriptive, with its distributed aggregate median ratio 1.4882 and no claimed
performance advantage. Historical resource ledgers continue to exclude decoder,
allocator, client, OS/page-cache and total RSS.

## Remaining work and boundary

G14 requires a separately designed, immutable native stream spec derived from the
logical plan and consumed by existing native Kafka/Engine/state/sink components,
with no duplicate semantic authority. It must preserve trusted source history,
assigned starts/cuts, v1/v2 derived job identity, pure-live v3 fingerprint and
CURRENT3 fencing. If that cannot fit the existing contracts, retain the partial
status and plan a separately authorized architecture/remediation step.

G15 needs worker-backed no-override join execution and fresh matched-source tests.
G16 needs the paired real Kafka diagnostic-budget campaign and an evidenced
bounded resolution if amplification causes an earlier failure. Fresh G0–G13
source-bound acceptance is also required before any qualified completion claim.
No Phase 19, general streaming SQL, cross-source CDC, physical rescale, automatic
recovery/GC, external transaction, or production HA is authorized or inferred.
