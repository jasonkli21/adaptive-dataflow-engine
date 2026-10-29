# Phase 15 implementation report — bounded keyed state and checkpoints v1

Verdict: **COMPLETE_BOUNDED_STATE_V1**, 2026-10-29 Pacific. This closes the
explicit bounded Phase15 product, not the full distributed/production PRD state
roadmap. All G0–G11 gates pass for this bounded local product; full distributed state
rescale and production HA remain deferred.

## Authority, source and environment

The user explicitly authorized implementation of the supplied Phase15 prompt,
completion in this session, no subagents, and one local commit. No subagent,
production Kafka, paid/cloud resource, push or PR was used. The supplied plan was
already untracked and is adopted/updated. The unrelated untracked bootstrap prompt
is preserved outside the commit. Tracked files were initially clean. HEAD exactly
matched preparation: `dc767c7c4e9e08c7b79ae034411fd9218f30a8e6`.

Accepted runtime evidence is **candidate-8**, frozen before documentation/sign-off.
Its272-file source archive SHA256 is
`d4382a1764c4451d25e3670fec456d7e02347ba9998b55cf2dd2818fae59c619`;
source-inventory file SHA256 is
`0ed8473d3ee38f4cb99d414e2f3a4545959b6f290209f047aded59a6d37bad56`.
The sorted124-file crate/Cargo runtime input map, encoded as canonical compact
sorted JSON, has SHA256
`a13385511c422cde2bddc33266d538abff250a5d5eb5cf8c6b815cf50696f7ab`.
All124 current runtime inputs match that frozen inventory. Later edits are
report/status/evidence documentation only. The pinned executable SHA256 is
`dc27fcaced04beadb82ff4c8f32030c5038806c21d16fccabf3dbac0d61e0226`.
The harness copies the executable before starting any reader so subsequent Cargo
feature unification cannot change the measured binary between sessions. The final
project commit is the single commit containing this report (`git rev-parse HEAD`);
its self-referential hash is deliberately not embedded here. Base/final SHA are
also supplied in the completion response.

Environment: aarch64-apple-darwin, Darwin27/macOS27, rustc1.99.1
(58cf8f9c3), Cargo1.99.1, JDK17, Python3.9.6, Homebrew OpenSSL3, debug build.
Pinned rdkafka0.39.0/rdkafka-sys4.10.0/librdkafka2.12.1. Broker is a fresh separate
Apache Kafka3.9.1 KRaft process with three-partition fixtures (two active and one
quiet), plus separate two-partition pressure fixtures. Cluster ID:
`CwTIjFZUQJe8ZmMJi287Cg`. Broker restart preserves that fixture cluster; each
reader/restore gets a new process and UUID. Trusted immutable uncompressed<=32KiB
Kafka history remains an explicit operator assumption; no topic UUID attestation
or cross-host durability is inferred.

## Implementation and reconciled decisions

[ADR-0022](../adr/0022-keyed-state-and-checkpoints-v1.md) refines architecture§18
and INV1/3/4/6/7/8. The default-off `stateful` feature and separate
`ade-state-stream` CLI admit a strict versioned descriptor. The existing Engine
still performs the shared native Arrow filter/projection. The single-owner loop
uses one common nonblocking frame pump for input and window output. There is no
second streaming execution loop, frontend/DataFusion dependency or PlanV1 change.

Signed windows use checked Euclidean alignment, inclusive close at frontier>=end,
checked Int64 sum/UInt64 count, and deterministic `(start,key)` close order. Every
assigned partition contributes to min/unknown progress; idle cannot close windows.
An input for an already closed interval produces an explicit closed-window-late
receipt without reopening state. This is distinct from the original partition
watermark `late` diagnostic. Stable output ID is job/key/start/end/version.

Each logical owner is an actual separate store instance with its own preallocated
hot vector, cold inventory and directory. Stable32-shard BLAKE3 routing gives one
exclusive owner per key, independent of Kafka partition. Cold sorted fixed records
are really evicted/read through/updated, with per-record checksums and quota that
includes rewrite overlap. No cardinality-sized cold index or close-output Vec is
retained. Working files are lossy session state and never selected as checkpoints.
A nonempty working root refuses a new attempt until the operator proves the old
process dead and removes its orphan session. This prevents crash garbage bypassing
the working-disk quota.

A cut pauses partition demand, confirms the current receipt, drains one close
frame at a time and settles all overdue windows before serializing. Open entries
encode pending event-time deadlines. Checkpoints persist only matched state,
confirmed next offsets, max times/watermarks/frontier, late counts, identities and
canonical shard inventory. No seen-only cursor or pending close enters a cut.
Any runtime/storage/publication error poisons the attempt. Files and manifest are
synced/re-read/verified, staging is synced and renamed to an immutable generation,
root is synced, then a synced pointer is atomically replaced and root synced again.
Success is emitted only after this sequence. Prior generations remain immutable;
quota exhaustion refuses rather than deleting evidence. An error after pointer
replacement can leave a valid new selection with no success event; manual restore
validates it. Corrupt CURRENT never silently falls back.

The stateful-only owned control poller drains Kafka main-queue statistics/errors
through synchronous I/O, uses a requested1MiB stack, and joins before consumer
close/drop. It never polls partition data or touches state/acknowledgment/output.
Default stateless polling remains unchanged. Before-cut and before-pointer source
verification refuse retention/config/identity drift. Regular file I/O and fsync
remain subject to OS latency; no hard real-time or hardware power-loss claim exists.

Restore verifies all selected bytes, ownership, versions, fingerprint and progress
before adoption/network assignment/output; Kafka then checks live source identity
and retained offset ranges before output. Saved offsets override initial offsets
as a complete vector. New session UUID, preserved logical job/time, no group offset
commit. Restore redistributes active and cold state1→2→4→1. This is local logical
rescale, not online physical worker membership or cross-host migration. Output is
honestly at least once: a demonstrated kill after close output and before
promotion replays the same stable IDs. Kafka provides no cross-partition total
order; with arbitrarily late inputs, acceptance can differ under a different legal
merge after restart. Equivalence is proved for the causally released witnessed
prefixes/actual ordered traces, not universal order-insensitive late-data results.
Stable IDs alone do not attest payload equality under different merge orders.

The plan's request for an independent reviewer conflicts with the user's explicit
no-subagent/same-session requirement. It is reconciled to documented same-session
self-review and independently written producer-ledger/Python oracles, not a claim
of a separate human/model review. Full distributed rescale, Phase16 adaptive
checkpointing, Phase17 transactions/savepoints, HA and general streaming SQL remain
outside authorization.

## G0–G11 acceptance and raw witnesses

Portable final artifacts are [acceptance.tar.gz](../evidence/phase15/acceptance.tar.gz)
and [manifest.json](../evidence/phase15/manifest.json). Archive SHA256:
`434003467c113e574e1e5d0cb8eacf978be833a07e49cec74726875bdb84747a`. Relative paths below refer to archive members. They require
no original private `/tmp` or ignored benchmark directory. All66 process cases
are checked against raw typed configs/traces/output/checkpoint bytes and the
independent producer ledger; authored PASS labels do not satisfy the verifier.

| Gate | Result | Raw independent witness |
|---|---|---|
| G0 authority/defaults | PASS | `validation/initial-inventory.json`, `validation/baseline.log` (7 tests), `validation/release-default-tree.log` (no rdkafka/frontend by default), full smoke; separately packaged fresh30-case Phase14 regression |
| G1 state/windows | PASS | `ledgers.json`, ordered `owner1`→`final-close` inputs/outputs; Python contract oracle verifies209 visible inputs,81 exact closed windows, negative times, filter/native projection, unknown quiet frontier, equality, one explicit closed-late arrival; `stateful.rs` signed property/sum/count overflow tests |
| G2 real tiering | PASS |384B hot control versus80 active windows, owner2/4 read-through updates, ample-memory uninterrupted output equality; `tiny_tier...`, `cold_partial_write...`, corrupt/partial working I/O and quota/cancellation tests in `validation/focused-10.log` |
| G3 matched cut | PASS | `partial_input_partial_close_and_failed_diagnostics...`, direct pending-input/pending-close cut refusal, every final generation checked against its confirmed producer prefix; `cancel-blocked` successful cut vectors never include stalled input |
| G4 publication/integrity | PASS |16 `kill-POINT`/restore pairs, covering writes/checksums/fsync/staging verification/generation rename/root sync/pointer write/sync/rename/durability/cleanup; prior generation independently checked, only whole old/new selected; corrupt/truncated files and checksum-consistent metadata attacks refuse |
| G5 restore equivalence | PASS | `owner2`, `owner4`, `owner1-return`, `late-open`, `final-close`, broker restart; exact saved offsets including legal aborted-transaction holes, fresh epochs, preserved known/unknown time; `window-kill`/`window-replay` demonstrate stable-ID duplicates |
| G6 logical redistribute | PASS | canonical state equality and separate owner files/hot allocations1→2→4→1 with cold windows; unit source-level owner-path checks plus live restored canonical accumulators/outputs |
| G7 identity/security | PASS | typed wrong job/window/hash/cluster/topic/filter/partition, retention-lost, pointer traversal, symlink, corrupt manifest/shard, truncated/oversized metadata and orphan-working cases all refuse before output; extra duplicate-owner/codec/version/count/permission attacks in Rust tests |
| G8 demand/cancellation | PASS | `backpressure` trace spans34311ms between blocked observations, fixed progress, backlog>500/partition,13317B native queue plateau,1600 exact resumed receipts; blocked-sink and write/promotion SIGINT refuse unmatched publication; `slow-checkpoint-control` shows5 stationary snapshots with control polls405→720/stat callbacks10→18/replyq peak0 |
| G9 real source/oracle | PASS | fresh isolated Kafka3.9.1, two active+one quiet partition, independent Java ledger, real abort/control holes, disconnect/restart/retention; all66 reached process contracts independently replay |
| G10 regressions/toolchain | PASS | locked fmt/check/Clippy; final all-feature workspace275 passed/0 failed/10 intentionally ignored with serial test scheduling;43 Python tests; full smoke; final-source original Phase14 campaign30/30 and strict offline replay |
| G11 source-bound evidence | PASS |272-file source archive/inventory,124-file runtime digest, immutable executable hash, commands/environment/producer ledger/checkpoints/raw negative contracts, preserved development candidates; standalone offline verifier plus checksum-consistent semantic forgery/version/hash/path refusals |

Measured managed domains: owner runs allocate384B hot; cold rewrite peak4864B,
including overlap. Owner2/4 have36/37 cold read-through hits; evictions104/105.
Pressure run allocates6144B hot and retains ten windows; terminal frame peak stays
<=2048B and one outstanding record, downstream channel0. Successful cut reservations
include retained generations and conservative64KiB staging metadata; final close
144034B and blocked-cancel245806B. I/O scratch is a fixed4096B charge and checkpoint
metadata a conservative131072B charge, not measured allocator/RSS. Native/thread,
Arrow, libc/OS/page-cache/filesystem allocation and trace-file domains remain
separate. No speedup, absolute latency, process RSS cap or power-loss measurement
is claimed. No historical Phase6–10 timing campaign was rerun/reclassified.

## Validation commands and qualifications

All Rust commands use `OPENSSL_DIR=/opt/homebrew/opt/openssl@3` and
`OPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3`. Offline uses the locked cached
registry. Local socket/process suites ran with approved loopback access.

| Command | Final result/raw log |
|---|---|
| `cargo fmt --all -- --check` | PASS; `validation/release-fmt.log` |
| `cargo check --workspace --all-targets --locked --offline` | PASS; `validation/release-check.log` |
| `cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings` | PASS; `validation/release-clippy.log` |
| `cargo test --workspace --all-features --locked --offline -- --test-threads=1` |275 passed,0 failed,10 ignored; `validation/release-tests.log` |
| `cargo test -p ade-stream --all-features --locked --offline` |23 passed (7 core,4 demand,12 new state),0 failed/ignored; `validation/focused-10.log` |
| `python3 -m unittest discover -s scripts/tests -p 'test_*.py'` |43 passed,0 failed; `validation/release-python.log` |
| `bash scripts/smoke.sh` | PASS; `validation/release-smoke.log`; mixed-engine one-sample warnings are not performance evidence |
| `python3 scripts/phase15/acceptance.py --kafka-home ... --java-home ... --output ...` |66/66 real process cases; exact argv in `commands.json` |
| `python3 scripts/phase15/verify_evidence.py --campaign ...` | PASS:66 cases,51 distinct admitted sessions,81 windows,272 source files; `validation/release-live-verifier.log` |
| `python3 scripts/phase15/verify_evidence.py --archive ... --manifest ...` | PASS:66 cases,51 sessions,81 windows;272 source files; `offline-final.log` alongside the package |
| final Phase14 acceptance/package/strict current verification |30/30,159 files,272 source files; `validation/release-phase14-offline.log` |

The first final parallel workspace attempt failed the existing Phase12
`concurrent_source_hashing_does_not_cancel_running_native_job` test with OWNER_BUSY
(its submit half succeeded; verify-sources startup collided with the metadata
lock). A package-only focused retry then failed its paused-stage handshake with a
mixed worker feature graph. No Phase12 code/test criterion was changed. Rebuilding
matching `--workspace --all-features` executables and running all tests serially
passed275/0/10, and the full final control-pump-source run passed again. These
failures and both confirmations remain in `validation/final-tests.log`,
`ops-contention-confirmation.log`, `matching-workspace-build.log`,
`final-tests-serial.log`, `release-build.log` and `release-tests.log`. The historical
CLI can still return retryable OWNER_BUSY under contention; no production HA debt
is closed. Large debug macOS links emitted an existing compact-unwind size warning
in build logs; Clippy's required warning gate passed.

The separate final stateless regression package is
[phase15-stateless-regression/acceptance.tar.gz](../evidence/phase15-stateless-regression/acceptance.tar.gz),
SHA256 `7cda3ed171e959486ff045459fd1f1c2b9e41d4fd2d5166d03a3c01ae5b783e2`.
Its development predecessor is retained separately. All preexisting Phase14
archives/manifests are unchanged.

Checksum-consistent copies with an inserted negative-case output and a forged
window sum are both refused by semantic checks after updating the copied member
and archive hashes. Unsupported manifest version and one-byte archive mutation
are refused before replay. Raw [tamper results](../evidence/phase15/archive-tamper.log),
[reproducer](../evidence/phase15/tamper-checks.py) and
[sidecar hashes](../evidence/phase15/validation-addendum.json) are public beside the
unchanged acceptance archive. `offline-final.log` and `package.log` are also
available there. Run the reproducer from the repository root; it changes only
copies under a temporary directory.

## Failed/development candidates and self-review

Every earlier source/binary/config/ledger/output/trace is retained in
`development/candidate-N.tar.gz`, with hashes in `development/inventory.json`.
The original ignored raw directories also remain locally, but portable review does
not depend on them. Compile/Clippy premeasurement failures remain in validation
logs. Nothing is relabeled as candidate8 evidence.

| Candidate | Disposition |
|---|---|
|1|Sandbox loopback bind refused before broker setup; authorized local rerun used a new directory.|
|2|Harness carried its oracle beyond an uncheckpointed receipt; corrected by forcing a cut before dependent restore.|
|3|All publication kill pairs reached; later window-kill fixture saw future partition data in a different valid arrival order and missed the intended close. Replay scenario moved before releasing later producer data.|
|4|Reached truncation refusal; harness expected the wrong string. Exact `shard length` contract now required.|
|5|Backpressure passed; outage failed under an overly narrow expected error. Contract now accepts only broker failure or the existing main-queue error refusal, with failed terminal/progress checks.|
|6|62 earlier process cases completed; superseded by actual separate owner-store allocations, orphan refusal and extra cancellation proofs.|
|7|65 cases and offline replay passed; self-review found native statistics could accumulate during slow synchronous I/O. Superseded by the owned control pump plus the new slow-checkpoint witness.|
|8|Final66-case source; no runtime edits after acceptance.|

Material self-review findings were fixed: estimated map charges became fixed hot
allocations; logical owners became distinct stores; orphan disk accumulation now
refuses admission; parent symlinks are checked at file opens; before-pointer source
verification was added; failed attempts cannot checkpoint; and native statistics
are pumped during synchronous state/checkpoint I/O. Attacks additionally challenged
state/offset cuts after partial writes, unknown/future time, duplicate owners,
working/checkpoint promotion confusion and nontransactional sink replay.

Residual P0/P1 for the bounded contract: **none confirmed**. P2 qualifications:
same-session self-review rather than an independent reviewer (explicit user
constraint); preexisting Phase12 contention/feature-graph test sensitivity;
linear cold scans/full checkpoints favor bounded simplicity over throughput;
operator-managed finite generation/trace storage and stale-lock/orphan cleanup;
local filesystem/hardware limits and trusted Kafka history. Full distributed
rescale remains PARTIAL; Phase10 physical elasticity, Phase12 production HA,
Phase13 cloud/HA and Phase14 historical sources/evidence remain unchanged.
No Phase16 authorization is inferred.

See the [operator guide](../phase-15-state-guide.md),
[support/refusal matrix](phase-15-support.json), and
[active plan](../plans/phase-15-implementation-plan.md).

## Files in the single project commit

```text
AGENTS.md
README.md
crates/ade-stream/Cargo.toml
crates/ade-stream/src/bin/ade-state-stream.rs
crates/ade-stream/src/kafka.rs
crates/ade-stream/src/lib.rs
crates/ade-stream/src/runtime.rs
crates/ade-stream/src/state/checkpoint.rs
crates/ade-stream/src/state/mod.rs
crates/ade-stream/src/state/store.rs
crates/ade-stream/tests/stateful.rs
docs/adr/0022-keyed-state-and-checkpoints-v1.md
docs/adr/README.md
docs/evidence/phase15-stateless-regression-development/acceptance.tar.gz
docs/evidence/phase15-stateless-regression-development/manifest.json
docs/evidence/phase15-stateless-regression/acceptance.tar.gz
docs/evidence/phase15-stateless-regression/manifest.json
docs/evidence/phase15/acceptance.tar.gz
docs/evidence/phase15/archive-tamper.log
docs/evidence/phase15/manifest.json
docs/evidence/phase15/offline-final.log
docs/evidence/phase15/package.log
docs/evidence/phase15/symlink-attacks.json
docs/evidence/phase15/tamper-checks.py
docs/evidence/phase15/validation-addendum.json
docs/phase-15-state-guide.md
docs/plans/phase-15-implementation-plan.md
docs/reports/phase-15-implementation-report.md
docs/reports/phase-15-support.json
scripts/phase15/Fixture.java
scripts/phase15/acceptance.py
scripts/phase15/oracle.py
scripts/phase15/package_evidence.py
scripts/phase15/verify_evidence.py
scripts/tests/test_phase15_evidence.py
```
