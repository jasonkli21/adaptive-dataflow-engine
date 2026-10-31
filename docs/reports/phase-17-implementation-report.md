# Phase17 implementation report — 2026-10-30

Status of the original candidate5 run: **COMPLETE_BOUNDED_TRANSACTIONAL_STATE_V1**
on its frozen local G0–G14 evidence. Current repository status after independent
review: **PARTIAL_VERIFICATION** until the additive remediation acceptance is
completed. The candidate5 evidence below remains historical and does not qualify
the remediation source. See
[`phase-17-review-remediation-2026-10-30.md`](phase-17-review-remediation-2026-10-30.md).
No push, PR, production mutation or Phase18 authorization occurred. The attachment
supplied review findings; it did not supersede the PRD/architecture or itself
establish acceptance.

## Identity and supported product

Baseline: `46a976d41d8642d54fe0829bd7944c9a1ddd39d1`. Initial worktree had only the
untracked Phase17 plan and unrelated bootstrap prompt. The plan was adopted and
reconciled; the bootstrap prompt remains unchanged and excluded from the commit.
The final project commit contains this report; its SHA is returned in the task.
Measurement identity is the frozen candidate5 source/executable pair, not a guessed
self-referential commit SHA or any earlier phase's evidence.

- Source inventory SHA256: `9bee1c3cba79ab0a91f2fe4e4f302bb6fad54d1cf23fef9db2b2e2a45ad04149` (320 files).
- Matched runtime inventory SHA256: `57d14486a535b17264ad64a9140beab3449ca064208a6939b7de247735b3ea4b` (139 Rust/Cargo inputs; all still byte-identical in the final tree).
- Transactional executable SHA256: `6c2b3f57c4df0cae105f11e00cb931480553b82de3906eb061e06bd7e195c26f`.
- Prerequisite legacy executable: `47010b8c80456b4c096baee860814f54aa8c2ddd5021aba4d1cc6a6165960e79`.
- Preflight probe: `a3ec46709738538289702653c94afa565ea4df26479b784fda13fe25447d7811`.
- Measured placement probe: `0c1116b95aca6cf17790225e6a421669a4b2f1188d83f0182996806cfccf1ac5`.
- Primary archive SHA256: `0802436c9760a0b1547a22a551c4a056ee3907383fbd404763c53584c24ed175`.

The primary archive is immutable. Final verifier strengthening, the Python test
import-isolation correction and report/status edits are verification/documentation
changes after measurement; no runtime input changed. The final verification source
inventory and logs are retained in the [final verification bundle](../evidence/phase17-verification/manifest.json). Fresh legacy regression bundles record
their own actual executable IDs; workspace feature unification can produce a
different binary from a package-only all-feature build. They are not substituted
for the primary pinned executable.

[ADR-0024](../adr/0024-transactional-output-savepoints-v1.md) reconciles all seven
PRD outcomes with the bounded implementation. The new default-off
`transactional-state` feature and `ade-state-stream-txn` CLI use strict config3,
private roots and CURRENT3. Native Engine, Arrow transform, demand/ack, keyed
windows and independent hot/cold owner stores remain the execution mechanism.
Legacy/default protocols, schemas and selectors remain; old modes explicitly
refuse v3 roots before source/output. There is no frontend/distributed dependency.

A single synced atomic selector publishes an immutable epoch containing complete
state, source progress and new output, with a bounded authenticated parent chain.
Only the separate committed reader exposes business output. The writer holds a
kernel lock on a permanent inode, checks parent/path fences and poisons after any
mutation error. Before-selector failure retains the old epoch; after-selector
failure requires reopening the selected new epoch even when telemetry was lost.
There is no rollback, fallback checkpoint, full replay or ID-only deduplication.

Named savepoints carry a self-contained verified closure and exact operator-held
digest. Import requires a fresh root and matching semantics/codec/owners. Graceful
SIGINT pauses demand, settles closures, commits durable PAUSED and exits; unchanged
already-PAUSED cuts are idempotent. Fresh transition processes perform the real
64→80-byte codec1→2 transformation and local logical1→2→4→1 redistribution, checking
full semantic equivalence before atomic activation. Failed migration leaves the
old cut visible. Physical rescale is explicitly refused.

## Historical candidate5 gate results

The [raw primary manifest](../evidence/phase17/manifest.json) binds21,160 files.
Candidate5 has121 transactional cases,144 separate reader observations,376 matched
cuts and65 admitted sessions. Five decisive Phase16 prerequisite proofs use real
Kafka and a separate real MinIO (848 signed requests,172 successful remote objects).
An additional matched-executable actual-ENOSPC proof retains the complete old cut.
The offline verifier validates records and state, rather than trusting PASS labels.

| Gate | Result | Evidence / exact scope |
|---|---|---|
| G0 | PASS | environment/source/runtime inventories, initial worktree inventory, adopted plan, ADR-0024; no unowned content committed. |
| G1 | PASS | Fresh expired-origin20/20 restore, both-vectors-expired refusal, assignment race, measured forced-full placement and capacity veto before remote PUT. Additive [Phase16 report](phase-16-postfix-reacceptance-2026-10-30.md); historical117-case archive unchanged. |
| G2 | PASS | Strict v3 config/feature/CLI; old-mode root refusal, default/legacy tests and fresh stateless/fixed-state campaigns. PlanV1/StreamConfigV1/current v1/v2 wire contracts unchanged. |
| G3 | PASS | Actual SIGKILL at all18 declared output/state/manifest/selector stages; concurrent separate reader and fresh-process restore always wholly old or new offsets/state/output. Includes partial output/selector writes, fsync boundaries and lost telemetry. |
| G4 | PASS | Real2-partition Kafka kill/restart at every cut; exact80-window output prefix after recovery, no hidden/missing/duplicate committed payloads. |
| G5 | PASS | Independent Python producer-ledger sum/count/window/frontier/late/full-shard oracle at every observation; negative time, cold eviction/read-through, filtered progress, overflow refusal, hash-consistent semantic forgeries and conflicting same-ID/different-payload legal interleavings. |
| G6 | PASS | Three named independently checked self-contained exports; fresh-root complete import/resume; actual export/import kills, failed export, collision, traversal, wrong digest, foreign identity, corrupt/path/size/hardlink refusals; earlier valid export preserved. |
| G7 | PASS | Durable signal PAUSED, idle/cold resume, unchanged pause idempotence, competing writer and stale-inode refusal,3s blocked local-I/O demand pause with SIGINT failure and safe restart. |
| G8 | PASS | Real codec1→2 record transformation, fresh-process activation, exact semantic hash equality, old-codec refusal, failed/killed migration before selection and incompatible/no-op transition refusal. |
| G9 | PASS | Complete80-entry cold/hot state and all32 shards through fresh-process1→2→4→1 owner stores; exact offsets and output history preserved. |
| G10 | PASS | Stage/output/history/epoch/root/savepoint/parser budgets; crash orphans charged; hash-consistent parent/owner corruption; unsafe symlink/hardlink/clone; real kernel partial-file EFBIG plus actual ENOSPC on an owned16MiB HFS+ image with zero available blocks. Selected old state/offset/output stayed exact. Image detached; no real disk formatted. |
| G11 | PASS | Locked fmt/check/strict Clippy;301 Rust tests,0 failed,10 existing ignored;54 Python tests; smoke; fresh30-case stateless and70-case fixed-state real-Kafka regressions and12 adaptive tests. |
| G12 | PASS | Two causally ordered repeated real traces match exactly; differing legal traces retain conflicting payloads in distinct lineages; three same-fixture static-v1/transactional descriptive samples, no speedup gate. |
| G13 | PASS | Frozen source/executable/service/config/producer/reader/epoch inventories, independent portable offline replay and hash-consistent state/output tamper refusal. All four earlier candidates and premeasurement failures preserved. |
| G14 | PASS | ADR, reconciled plan, operator guide/example, this full gate report, additive prerequisite report, accurate status and limitations. |

Actual full-disk evidence is `supplemental/enospc/`: raw OS errno28, statvfs with
zero available blocks, owned-image create/attach/detach commands, before/after
reader observations and selected-state closure. The same transactional executable
hash is checked. The independent oracle revalidates its80-record/80-entry old cut.

## Validation and reproduction

Passed, with raw logs under primary `validation/` and the final verification bundle:

```
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked -- --test-threads=1
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p 'test_*.py'
bash scripts/smoke.sh
```

The initial workspace test run was sandbox-refused at loopback sockets (67 passed,
29 permission failures in ade-distributed). The permission-enabled full rerun
passed301/0/10. No test assertion was relaxed. The existing large macOS debug-link
compact-unwind warning is retained. The initial full Python run exposed module-name
collision with the old oracle; the new test now loads isolated modules and all54
pass. These harness/environment failures remain in the development evidence. Narrow Git whitespace attributes preserve raw native stdout tabs and compiler-log EOF bytes without normalizing the evidence.
Smoke's single-sample cross-engine timing/configuration warnings are not performance
or historical Phase10 release evidence. No previously failed timing gate is relabeled.

Offline verification, with no services, engine build or third-party Python modules:

```
python3 scripts/phase17/verify_evidence.py \
  --archive docs/evidence/phase17/acceptance.tar.gz \
  --manifest docs/evidence/phase17/manifest.json
python3 scripts/phase17/verify_legacy_regression.py \
  --archive docs/evidence/phase17-fixed-state-regression/acceptance.tar.gz \
  --manifest docs/evidence/phase17-fixed-state-regression/manifest.json
python3 scripts/phase14/verify_evidence.py \
  --archive docs/evidence/phase17-stateless-regression/acceptance.tar.gz \
  --manifest docs/evidence/phase17-stateless-regression/manifest.json
```

The fixed-state adapter pins the unchanged historical verifier's exact SHA and
only admits the actual46a976d base to its existing70-case schema; it preserves all
oracle/negative assertions and never edits old archives. The stateless bundle uses
the unchanged strict current verifier. The independent BLAKE3 implementation is
checked against public vectors and actual Rust-produced multi-chunk objects.

To reproduce live acceptance, build bins/examples with the package's all features,
then run `scripts/phase17/acceptance.py --output NEW_ABSOLUTE_DIRECTORY` with
`--kafka-home`, `--java-home`, `--minio` and `--service-python` (the latter needs
boto3/requests). `criteria.json` is declared before the campaign. Service endpoints,
fixture/admin commands and environment are recorded; all topics/buckets are owned,
disposable and loopback. Run `scripts/phase17/enospc.py --campaign CAMPAIGN --output
NEW_DIRECTORY` for the supplemental macOS owned-image full-disk proof. See the
[operator guide](../phase-17-transactional-state-guide.md) for actual job commands.

## Descriptive cost and preserved failures

Three120-record no-close fixture samples: transactional whole-process times
0.991–1.318s (median1.162s); static-v1 times1.158–1.662s (median1.234s).
Transactional epoch publication latency ranged110–219ms across nine cuts; retained
local bytes were23,445 in each sample. These include local debug/startup/I/O effects
and are descriptive only. Different committed-output and stdout delivery mechanisms
are disclosed; no throughput superiority or production latency is claimed.

[Development inventory](../evidence/phase17-development/inventory.json) preserves:

- Candidate1: sandbox socket refusal before services.
- Candidate2: prerequisite placement fixture expected a calibrated choice after restart; runtime correctly used its uncalibrated fallback.
- Candidate3: measured warm-up probe initially omitted prepare-time measurement, so runtime again correctly used fallback.
- Candidate4:108-case preliminary campaign passed; it predates the later lifecycle/full-disk/status refinements and is not final acceptance.
- Compiler/style iterations, the Python import collision, sandbox workspace refusal,
  initial wrong stateless verifier CLI invocation, the final export-selector offline adapter correction, and owned-image CLI/fill experiments.

Candidate5 is the sole final transactional campaign. Existing Phase6–16 archives,
failed candidates, source identities, schemas and timing criteria remain immutable.
Same-session self-review plus independently implemented producer/state/output
oracles were used; no separate human/agent reviewer is claimed.

## Limits and remaining debt

Exactly once means **committed-visible window output within one pinned local job
lineage**, through the versioned reader. Reading a prefix twice is not a new
transactional delivery to another system. Input diagnostics/receipts are not this
contract. Kafka history is operator-attested immutable/uncompressed <=32KiB;
metadata cannot prove topic incarnation or undetected truncate/regrow. Cross-partition
late behavior remains trace-relative; no global interleaving determinism is claimed.

The writer assumes a trusted OS user/private namespace and working local atomic
rename/fsync semantics. Process crashes and real storage errors are proved; disk
loss, hardware/power-loss durability, external Kafka/HTTP/S3 exactly once, consumer
groups, cross-service2PC, multi-host/cloud/AZ or multi-writer HA are not supported.
Explicit imported continuations require one active branch under operator control;
there is no cross-filesystem global writer registry. Cloning a writable root to a
new path is refused. There is no rollback over a selected output prefix.

Retention is finite and nondestructive; exhausted budgets refuse. No automatic GC
or recovery exists. Lossy working crash orphans require proved-dead operator cleanup;
checkpoint/savepoint orphans stay charged. Logical local rescale is not physical
worker/Kafka membership rescale. Only compatible codec1→2 is implemented, not arbitrary
rolling upgrades. Static/default/v1/v2 paths and Phase10 physical-elasticity,
Phase12 production-HA and Phase13–16 broader durability debt remain separate.
Phase18 work requires new authorization.
