# Phase 14 independent review remediation — 2026-10-29

## Disposition and source pin

Reviewed implementation base: `fbafd01e781a5e3a034896fbdef02cadf1adb532`.

Review handoff: [`phase-14-independent-review-handoff.md`](../../phase-14-independent-review-handoff.md), SHA-256 `63e4ce08d9bdb1313946562bfada7cc29831ebfea18d8e3a3001605b3bfb3ac6`.

The independent review found no confirmed Phase 14 runtime defect. It accepted the bounded runtime with two evidence P2 findings. Both findings are fixed and revalidated below. No Rust runtime, `Cargo.toml`, or `Cargo.lock` input changed. The 119-file crate/Cargo input inventory still matches the accepted Phase 14 runtime digest `b399925d7749060972412040e58c475f41f4c6d0af6401e881b0a751e52b3590`.

| Finding | Disposition | New proof |
|---|---|---|
| F14-01 — malformed missing-partition descriptor was refused by JSON parsing | Fixed in the acceptance descriptor builder with separate scalar `offsets` and typed `explicit_starts`. The strict contract requires one `{ "partition": 3, "offset": 0 }` start and the source failure reason `assigned partition missing`. | Fresh real Kafka campaign case `missing-partition`; zero output. A malformed offset object fails the regression and offline contract. |
| F14-02 — negative evidence labels/exits were trusted without checking their semantics | Fixed with shared live/offline per-case contracts. They validate admission stage, failure class/reason, terminal/failure events, poison seen/forwarded cursors, output requirements and honest prefix output on sink/source failures. Raw backpressure traces now prove frozen progress, backlog, queue plateau and full resumed output conservation. | Nine focused Python regressions, all 30 live campaign cases, strict offline archive verification, and a checksum-consistent semantic-forgery refusal. |

The old portable archive was not edited. Its SHA-256 remains `d65c824568e092a8dbf8b632f3e420a79ea56dcc01b20399abc2d9338f1faad5`. It now verifies only with `--mode historical`, which returns `PASS_HISTORICAL_WITH_F14_01_LIMIT` and explicitly says the old missing-partition case is not a source-partition witness. Default/current mode refuses that archive. The fresh campaign has a separate strict portable package.

## Fresh source-bound campaign

Campaign root: `benchmarks/results/phase14/review-remediation-2026-10-29-campaign-2` (ignored local raw artifacts). Its source snapshot has 257 files, source-inventory SHA-256 `b1dd696994ba7e16d7403f4ce22fc4d45c3892693f8786437480e53412d6fa99`, and source archive SHA-256 `8d862b47d8be55137d7c4244a0efed7a8de27b0ae1544fbc884558e8b8c4ead2`.

The campaign ran the immutable all-feature executable `/tmp/ade-phase14-independent-review/ade-stream-pinned`, SHA-256 `9614eedc385a69f04479c3feff63293c0056b3a98e8cad108cdda847ac05e2f6`, against a new private Apache Kafka 3.9.1 loopback fixture. It passed **30/30** process cases. The corrected missing-partition trace is:

```json
{"failure_class":"source_identity_or_offset","reason":"source identity/offset: assigned partition missing","type":"failure"}
```

Its typed start is partition 3, offset 0; the output file is empty. The raw backpressure witness remained blocked 35.896684832999995 seconds, held a 7,937-byte queue plateau, observed broker backlog above 500 records on each partition in the final snapshots, then emitted all 1,600 expected receipts.

Portable package: [`acceptance.tar.gz`](../evidence/phase14-review-remediation/acceptance.tar.gz), SHA-256 `4d9f31739631babf844b743b7a7a9b967572e9890ba8daeb83430a2446a54212`; [`manifest.json`](../evidence/phase14-review-remediation/manifest.json), SHA-256 `135a6cf209b5348db111305405eb776297903db4f4803f772977ad0f9f94acfc`. The package contains 159 files, including the 257-file source archive. Strict offline verification returns `PASS` for 30 cases, 159 evidence files and 257 source files.

The first fresh harness attempt is preserved locally at `benchmarks/results/phase14/review-remediation-2026-10-29`. It stopped in fixture setup because the new descriptor wrapper supplied `bootstrap` twice for the TLS control. The wrapper was fixed before the passing campaign; no runtime change was made. Earlier review and phase candidates remain untouched.

## Validation

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | PASS |
| `OPENSSL_DIR=/opt/homebrew/opt/openssl@3 OPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3 cargo check --workspace --all-targets --locked --offline` | PASS |
| `OPENSSL_DIR=/opt/homebrew/opt/openssl@3 OPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3 cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings` | PASS |
| `OPENSSL_DIR=/opt/homebrew/opt/openssl@3 OPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3 cargo test --workspace --all-features --locked --offline` with user-authorized loopback access | 263 passed, 0 failed, 10 intentionally ignored. The first sandboxed attempt was blocked by loopback permission: 67 passed and 29 socket tests failed with `Operation not permitted`; the authorized rerun passed. |
| `PYTHONPYCACHEPREFIX=/tmp/ade14-pycache python3 -m unittest discover -s scripts/tests -p 'test_*.py'` | 35 passed, 0 failed; includes 9 Phase 14 evidence tests. |
| `bash scripts/smoke.sh` with user-authorized loopback access | PASS correctness/schema/process smoke. The one-iteration mixed-engine comparison emitted neutral/regression warnings and is not performance evidence. |
| `python3 scripts/phase14/verify_evidence.py --archive docs/evidence/phase14-review-remediation/acceptance.tar.gz --manifest docs/evidence/phase14-review-remediation/manifest.json --mode current` | PASS: 30 cases, 159 files, 257 source files. |
| `python3 scripts/phase14/verify_evidence.py --mode historical` | Qualified `PASS_HISTORICAL_WITH_F14_01_LIMIT`; old archive integrity and positive oracle replay only. |
| Checksum-consistent copy with an inserted oversize output row and updated copied member/archive hashes | REFUSED by the semantic `oversize` contract. The repository archive/manifest were not modified. |
| One-byte archive mutation with the original manifest | REFUSED at archive identity. |
| `python3 -O scripts/phase14/verify_evidence.py --archive docs/evidence/phase14/acceptance.tar.gz --manifest docs/evidence/phase14/manifest.json --mode historical` | REFUSED by the optimized-Python guard. |

No Phase 6–10 timing campaign was rerun or reclassified. Phase 10 physical membership, Phase 12 production HA, Phase 13 production cloud scope, and all future-phase boundaries remain unchanged. No production resource, paid service, push, pull request, or cloud action was used.
