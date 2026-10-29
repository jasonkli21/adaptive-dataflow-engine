# Phase16 implementation report — bounded adaptive state v1

Verdict: **COMPLETE_BOUNDED_ADAPTIVE_STATE_V1**, 2026-10-29 Pacific.
All G0–G12 pass for this bounded product. This is not production HA, cross-host
state durability, physical rescale, automatic recovery/GC, exactly-once or savepoints.

## Authority and identities

The user authorized Phase16 implementation in this session, no subagents, one
local commit, no push. No subagent or separate reviewing agent was used. The base
and preparation target both were `a89f1aeb2b7c36a23d77d702e705d6ffe4bdbfea`, branch
`main`, with no tracked changes. The preexisting untracked Phase16 plan was adopted
and reconciled; the unrelated untracked bootstrap prompt is preserved outside the
commit. The final commit is the single commit containing this report; its SHA is
supplied in the handoff rather than embedded self-referentially here.

Accepted measurement is **candidate5**, with117 real process cases,104 distinct
admitted sessions,136 matched cuts and13831 observed signed service requests.

| Identity | SHA256 |
|---|---|
| Frozen295-file source archive | `31f2763bc272c97be37e97e3d0ba05540dc3c949d89de09fb2cc7c0193e2daf5` |
| Frozen source inventory | `7b3b1e1f21d0ac041d9c28e06b9d06c216525aeb23ce0b99d81f2d980afb6535` |
|107 product inputs, compact sorted JSON SHA map | `1250d01ad67549d806204dbb562c40b4dc3f4fd9f2a4a973694785a6311d8a9d` |
| Pinned measured executable | `ff94600bbdd4adb3a1ac5afa7061aa185642489b014b766ab528651fa65fd619` |
| Real MinIO server executable | `5c5445919b1060311618aa4efe5450fadc29580ef86f2ddc34e931859eda02c0` |
| Closure296-file source archive | `f4b90797b49ed3e9a7c7233cfc1fb5be405d75150327c3662e54d93a3e90389e` |
| Portable acceptance archive | `3b47fda4ad766c5577e7c3ab14bf3ec71007f8cae048142136ee6978dc411214` |

All107 product inputs (Cargo manifests/lock, crate build scripts and src files)
match the measured inventory at closure. Later changes are verification tooling,
additional matched-trace test coverage, test-only lint cleanup and documentation;
no product input changed after measurement. The final source/inventory and
validation addendum beside the acceptance archive bind those closure additions.
The debug executable is hash-pinned before the campaign and checked after it;
it is excluded from the portable package, which includes buildable source.

Environment: aarch64-apple-darwin, rustc1.99.1/Cargo1.99.1, JDK17, Kafka3.9.1,
Python3.9.6, Homebrew OpenSSL3, debug builds. MinIO is a separately running real
storage server built from `RELEASE.2025-09-07T16-13-09Z`, module
`v0.0.0-20250907161309-07c3a429bfed`; its Go build metadata is archived. A separate
bounded fault proxy records actual signed PUT/HEAD/GET bodies and outcomes without
credential headers. The proxy is not the object store. All services/topics/buckets
were disposable local fixtures; no production/paid cloud service, push or PR.

## Implemented contract

[ADR-0023](../adr/0023-adaptive-checkpoint-placement-v1.md) reconciles PRD Phase16,
architecture14–15/18 and the phase plan. Default features remain empty. The strict
old Config/StreamConfigV1, fixed/full/local v1 CLI and checkpoint bytes remain;
PlanV1, batch/ops/remote protocols, result/oracle schemas and historical evidence
retain their identities. New `adaptive-checkpoint` enables local v2 through
`--adaptive-policy`; separate `remote-checkpoint` enables S3. Mixed roots and
implicit migration refuse. `ade-stream` still uses native Arrow kernels and the
same single source-demand/receipt/window-close owner; no frontend or distributed
executor dependency was added.

Pure checked integer policy enumerates full/incremental, local/remote/both and
min/max cadence candidates. Copy/remote/risk/SLO/remaining-capacity constraints
precede configured expected-cost ranking. Session-local measurement minimums,
cooldown/margin and an explicit estimated static full fallback are bounded and
auditable. All scores and recovery proxies are independently recomputed in Python.
Missing calibration never makes remote storage free. Candidate resource estimates
are conservative; mechanisms separately reserve/veto each actual operation.

V2's complete32-shard canonical index pins original starts, confirmed offsets,
progress/frontier, state, owner inventory, policy identity and durability floor.
Only changed canonical shard payloads are physically rewritten; references bind
exact immutable object generation/size/hash and prior manifest. Full cuts reset
ancestry, depth<=4, with placement/floor changes forcing full. Every dependency and
state/progress invariant validates before adoption. Restore redistributes real
separate1/2/4 local owner stores. No hardlinks, mutable deltas, working-tier recovery
or automatic destructive GC.

The existing quiescent cut pauses and verifies source demand, settles pending
receipt/closure ACKs, and never polls partition data during I/O. The owned control
poller continues servicing native statistics/errors. Source identity/ranges are
verified again immediately before selection. All required copies and ancestors
verify before an immutable remote certificate, exact readback and a synced atomic
CURRENT2 selector. Ambiguous PUT never retries/adopts; errors poison the attempt.
A post-selector crash may leave a whole valid new generation without success;
corrupt selectors never silently fall back.

Remote-only cold restore uses operator-retained exact certificate bytes/hash with
empty local scratch and no LIST/local shards. Both copies restore independently by
explicit selection, including local restore during a real S3 outage. Continuing
from lost local metadata requires a fresh remote budget namespace and a full cut;
reusing the old namespace refuses. An explicit verified remote copy can rebuild a
missing local required copy on the next full cut. No unhealthy-copy fallback occurs.

Recovery ranks only supported complete paths: verified checkpoint state+offsets,
or empty full replay from pinned original starts with exact selected-certificate,
immutable-history and duplicate/conflict consent. Live Kafka range/cluster/topic
checks veto before assignment/output; no partial state plus reset offsets. A fresh
session fences each attempt. At least once remains trace-relative: the two causal
window fixtures emit the same logical ID with different fingerprints/sum/count
(12,3 versus5,2). Consumers must reconcile conflicts; ID-only deduplication is unsafe.

## Gate witnesses

Paths below are members of [acceptance.tar.gz](../evidence/phase16/acceptance.tar.gz)
with exact file hashes in [manifest.json](../evidence/phase16/manifest.json).
The offline verifier checks semantics/costs/copy closure, not authored PASS labels.

| Gate | Result | Independent/raw witness |
|---|---|---|
| G0 authority/defaults | PASS | `validation/identities.json`, baseline28 tests, default feature tree without Kafka/S3/frontend; full workspace/smoke; separate fresh30-case stateless and70-case fixed-state archives; three real-MinIO Phase13 regressions |
| G1 strict surface/compatibility | PASS | Strict policy/CLI/version/origin/consent tests; missing/wrong credential and capability refusals before output; mixed v1/v2 and checksum-consistent identity attacks; unchanged fixed v1 campaign |
| G2 pure policy | PASS | Every published candidate/score independently evaluated by `oracle.py`; controlled cadence20/80 versus20, dynamic placement, copy/risk floors, static/fallback/cooldown/overflow unit tests; recovery costs independently recomputed |
| G3 matched cuts | PASS |136 source-confirmed quiescent cuts checked against producer ledger/ordered receipts and closure oracle; no idle/direct unchanged/pending/failed cuts in Rust; slow-control stationary progress with increasing native poll/stat callbacks; inherited full-frame deadline/backpressure tests |
| G4 real incremental | PASS | Local/remote/both sequences write32,32,1,1,32,1 objects at depths0,0,1,2,0,1; canonical state/progress verified; deterministic identical-order full/incremental Rust fixture compares every shard/progress and100 window outputs; owner1/2/4 restore tests |
| G5 ancestry/integrity | PASS | Missing/corrupt/foreign/cyclic/depth/oversized/duplicate/owner/codec/progress/link attacks in Rust, including rehashed metadata; full dependency expansion and conservation checks; portable forgery attacks below |
| G6 real placement/copies | PASS | Actual signed service logs/bodies and divergent conditional-create412; independent local/remote/both cuts; explicit restore-local/restore-remote, clean-scratch and clean-scratch-resume; remote-rebuild-local-copy |
| G7 remote failure envelope | PASS | PUT/HEAD/GET corruption/error, manifest/certificate failures, lost-response ambiguity, redirect refusal, oversized response, real5s timeout/6s delay, cancellation and real MinIO outage; selected prior remains eligible; local outage restore is explicit |
| G8 executed recovery | PASS | Actual full-replay and checkpoint-restore traces/outputs; independently ranked costs; wrong consent/topic/origin/untrusted history and real lost-retention pre-output refusals; fresh sessions and conflicting fingerprints |
| G9 finite resources | PASS | Cold/checkpoint-byte/generation/remote-object/request/response/trace exhaustion; durable REMOTE-LEDGER reserves attempted/orphan writes before network effects; disk-loss namespace reuse refusal; bounded32 shards/5 manifests/one frame/one request; native/RSS exclusions explicit |
| G10 atomic selection | PASS |21 real kill/restore pairs across local/remote objects/manifests/certificates/fsync/selectors; all32 local object write/sync positions additionally faulted in Rust; exact whole old/new selection, retained prior, no failed cut success/corrupt-current fallback |
| G11 science/provenance | PASS | Three predeclared static/local and adaptive/local samples, same240-record source/semantics; all samples/raw ordered traces retained; costs configured, throughput measured, recovery estimated; actual physical savings and non-speedup remote results below |
| G12 tools/portable evidence | PASS | Locked fmt/check/Clippy, all-feature workspace,48 Python tests,117 real-service cases, portable source-bound offline replay; record/object/manifest/certificate BLAKE3 and enclosing SHA inventories regenerated in attack copies, still refused |

## Measurements and accounting

These are descriptive local observations, not isolated performance acceptance or
billing. Some validation work overlapped measurement; Kafka merges can differ at
intermediate record-count cuts. Compare intermediate canonical cuts only with a
matched confirmed vector/ordered trace. The deterministic Rust fixture supplies
that exact full/incremental comparison; all live final240-record states agree.

| Arm | All elapsed samples, ms | Physical shard bytes / canonical initial-pass bytes |
|---|---|---|
| static full/local |10451,10420,10440|28160/28160;27776/27776;28160/28160|
| adaptive/local |10152,10106,10119|13184/28160 each|
| adaptive/remote |25356|13184/28160|
| adaptive/both |20478|13184/28160 per logical snapshot; both physical copies really verified|

Adaptive/local sample0 avoided14976 shard payload bytes (53.18% of its exact full
canonical-write counterfactual). A representative incremental writes one192-byte
shard versus5120 canonical bytes; all32 logical shards still validate. Metadata,
repeated changed-shard scans, readback and complete ancestry verification are
additional work. `canonical_scanned_bytes` counts the initial canonical hash pass,
not every subsequent verification/read-through. No causal speedup, cloud cost
saving, absolute recovery SLO or RSS cap is claimed.

Prices/risk/retention, read/compute/replay rates and recovery latency estimates are
CONFIGURED. Last native preparation/whole-cut/scan throughput and physical/wire
bytes are MEASURED. Recovery and expected-cost scores are ESTIMATED proxies. The
failure_ppm term is a configured per-record opportunity hazard in the per-million-
record proxy, not measured fleet reliability. Fixed fallback1000B/s and1ms/record
is explicitly estimated. Request-price proxy excludes complete-chain auditing,
capability, source connection and cleanup; actual counters/reservations include
attempted requests and ambiguous orphans. No provider-billed value is asserted.

Managed domains include fixed hot allocations, bounded cold rewrite overlap,
retained+staging checkpoint quotas, one<=1MiB object buffer, conservative4MiB
metadata charge, one<=2KiB sink frame and finite diagnostics. Native Kafka/TLS/SDK,
thread stacks, allocator, OS/page-cache and external service storage overhead are
separate domains. Read-only recovery has a finite per-session request budget;
write budgets persist across restarts in the local ledger. Trusted operator fencing
and no ledger rollback are assumptions. Ledger loss requires a new namespace;
production external accounting and automatic cleanup remain deferred.

## Commands, regressions and preserved failures

Rust commands used `OPENSSL_DIR=/opt/homebrew/opt/openssl@3` and matching
OPENSSL_ROOT_DIR; cached locked dependencies permitted `--offline`.

- `cargo fmt --all -- --check`, locked workspace all-target check, all-feature
  Clippy `-- -D warnings`, matching workspace build and serial all-feature tests.
- `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p 'test_*.py'`:48 passed.
- Unchanged `bash scripts/smoke.sh`: exit0, including spill/distributed/join tails.
  Its185.5% one-sample cross-engine diagnostic carries incompatible-configuration
  warnings and is not performance evidence or a Phase10 timing result.
- Real campaign command/inputs are in `commands.json`; MinIO module/build identity
  and all request bodies/counters are retained. Current portable verifier passed117
  cases/104 sessions/136 cuts without private original paths or running services.
- Original Phase15 archive replay passed66 cases/51 sessions/81 windows unchanged.
  Original Phase14 requires historical mode and retains its known F14-01 limit;
  review-remediation archive passed strict30-case current contracts unchanged.
- Fresh regression packages: [stateless](../evidence/phase16-stateless-regression/manifest.json)
  (30 cases) and [fixed state](../evidence/phase16-fixed-state-regression/manifest.json)
  (70 cases/55 sessions/81 windows). The Phase15 adapter pins the original verifier
  source and applies its stricter70-case schema to the actual a89f1ae base; no
  evidence base/source bytes or historical verifier were rewritten.

All four earlier adaptive candidates are preserved in
[development/inventory.json](../evidence/phase16-development/inventory.json), with
source/executable/archive identities and failure logs. Candidate1 stopped on a
negative-fixture reason mismatch (and preceded remote-only calibration correction).
Candidate2 reached all kill boundaries then exposed a missing proxy POST route.
Candidate3 stopped during a restore on a real request failure; candidate4's raw
HEAD took1.098s against its configured1s cap. Both failed closed under overlapping
release load. Candidate5 uses the supported5s cap with explicit6s timeout faults;
no historical guard or acceptance threshold was relabeled. Compilation/style,
missing public binary download and premeasurement test failures remain in logs.

Two workspace attempts reproduced the historical Phase12 OWNER_BUSY startup race.
The test now retries only that existing pre-hash refusal for<=2s with the same
idempotency key/recipe, inside its unchanged8s prepared pause. The actual>=5s hash,
concurrency and succeeded/not-cancelled assertions remain; production ops code is
unchanged. A matching full workspace confirmation passed290/0/10; the final
additional matched-trace proof is included in the release run. Large existing
macOS debug links emit a compact-unwind size warning. Required all-feature Clippy
passes; the local-only feature check passes with an unused conditional import
warning. Neither is hidden as production behavior or new HA capability.

Three unchanged Phase13 real-MinIO tests passed: genuine turnover/scoped cleanup,
corruption/missing/source/reader-death refusal, and cleanup capability isolation.
The first attempt's negative helper used its legacy public fixture credential
literals while the service had different disposable credentials; MinIO correctly
rejected it. That failure is preserved; the confirmation used the fixture's
required throwaway environment credentials. No Phase13 product code changed.

Tamper copies with forged receipt semantics, forged checkpoint payloads (including
new record/object/manifest/certificate BLAKE3 pins), and forged policy costs all
fail independent semantic checks after regenerating enclosing SHA inventories.
One-byte archive mutation fails integrity. The accepted archive is unchanged;
see `tamper.log` and `scripts/phase16/tamper_checks.py`.

## Closure and remaining debt

The implementation is intentionally synchronous and finite. Retention/cleanup is
fenced offline exact-key operator work; no LIST/latest discovery or destructive GC.
Same-host MinIO proves service/process separation, not cross-host/AZ/disk-loss HA.
Physical distributed rescale, automatic recovery, remote operations, transactions,
exactly-once/savepoints and topic-incarnation attestation remain deferred. Preserve
Phase10 physical-elasticity and Phase12/13 production-HA debt and every historical
failed/timing source identity. No Phase17 authorization is inferred.

Same-session self-review and independent producer-ledger/Python arithmetic/state
oracles were used; no separate reviewer is claimed. Suggested follow-up: a fresh
read-only Sol High or XHigh session using the separately prepared Phase16 review
prompt, without subagents. See the [operator guide](../phase-16-adaptive-checkpoint-guide.md),
[support matrix](phase-16-support.json), [ADR](../adr/0023-adaptive-checkpoint-placement-v1.md)
and [plan](../plans/phase-16-implementation-plan.md).

## Portable replay commands

```sh
python3 scripts/phase16/verify_evidence.py \
  --archive docs/evidence/phase16/acceptance.tar.gz \
  --manifest docs/evidence/phase16/manifest.json
# Attack generation additionally uses rustc and cached blake3 build artifacts.
python3 scripts/phase16/tamper_checks.py \
  --archive docs/evidence/phase16/acceptance.tar.gz \
  --manifest docs/evidence/phase16/manifest.json
```

The final source archive and [validation addendum](../evidence/phase16/validation-addendum.json)
are separate from the immutable measured archive. The final source archive excludes
this self-referential report, evidence files and the unrelated user prompt. Product
input hashes are identical in both source snapshots. No bulk broker/MinIO datasets, executable copies or private credentials are committed; the intentional acceptance archives are retained for review.
