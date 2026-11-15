# Phase 26 independent-review remediation — 2026-11-15

## Qualification

**Current status: `PARTIAL_PHASE26_POLICY_REMEDIATION_PENDING_REACCEPTANCE`.**
This additive report records the changes made after independent review of
`9134b4745c33029e27421b4d0a4c57c604c50ef2`. The implementation and original
candidate archive are preserved as historical evidence. No Phase 18–25 status
or prior Phase 26 result is reclassified.

The review defects were independently checked against the local source. F01–F04
are implemented with focused regressions; F05's campaign now removes its
intentional symlink after recording the refusal, and the verifier still rejects
unexpected symlinks. The original report's 2026-11-14 economic samples are not
new-source evidence and are not reused as current policy acceptance.

## Remediation record

| Finding | Change | Current proof / remaining acceptance |
|---|---|---|
| F01 current hit charged new-write economics | Expected-value mode validates the original source before opening the root. A candidate may inspect an authorized root when immediate `recompute_us > restore_us`; a verified hit is then used regardless of sunk write cost. A miss still uses the write-inclusive admission score. | Rust policy tests pass. New separate-process `f01-*` campaign cases cover an existing hit with negative write score, an empty initialized root, and restore slower than recompute. The campaign was not run locally because pinned `pyarrow` and `blake3` are unavailable. |
| F02 full-horizon incumbent value | New manifest v2 entries compute checked, floor-rounded linear residual value from `value(false)` and remaining TTL. Existing v1 entries are refused and require explicit exact-key purge, so old forecast fields are not silently reinterpreted. A rollback before creation assigns zero; malformed and zero horizons refuse. This is a stationary-use heuristic. | Pure tests cover creation, half-life, expiry minus one, expiry, rollback and invalid horizon. Campaign oracle now checks observed time and residual values; its poison-recovery case uses a legacy v1 manifest. Full campaign remains not run. |
| F03 greedy victim set | For at most 32 entries, policy uses exact meet-in-the-middle subset selection by minimum displaced value under both byte and entry constraints. Equal value refuses; ties prefer fewer bytes freed, then sorted key identity. | Unit tests reproduce the A/B/C witness, equal-value refusal and entry-count selection. Full quota/pinned-reader process campaign remains not run. |
| F04 no supported corruption repair | Added `purge --cache ROOT --key EXACT_KEY`: exact lowercase digest validation, exclusive root lock, root reattestation, private directory and exact regular child inventory, no-replace rename to bounded trash, root sync and checked cleanup. Normal lookup/fill/GC still fail closed. | Campaign now keeps one corrupt manifest across an unrelated hit, blocked fill and blocked GC, verifies exact-key purge and recovery, and tests invalid key, symlink and active-reader refusal. Not executed locally. |
| F05 CI seal sees retained root symlink | The campaign retains the `symlink-root` refusal receipt and unlinks only that known fixture before sealing. Evidence inventory was extracted to a standard-library-only module and continues to reject every symlink in the exported tree. | Dependency-free test rejects an unexpected evidence symlink; Python syntax and that unit test pass. Hosted workflow and full campaign have not been rerun on this source. |

## Executed checks

Host: macOS ARM64, stable Rust `1.99.1`, Python `3.9.6`.

| Command | Result |
|---|---|
| `cargo test -p ade-cache --features shared-v1 --locked` | PASS, 6 policy tests |
| `cargo check -p ade-cache --no-default-features --locked` | PASS |
| `cargo check -p ade-cache --features shared-v1 --all-targets --locked` | PASS |
| `cargo clippy -p ade-cache --features shared-v1 --all-targets --locked -- -D warnings` | PASS after fixing the initial lint findings. |
| `cargo build -p ade-cache --features shared-v1 --bin ade-cache --locked` | PASS |
| `cargo fmt --all --check` | PASS |
| `cargo check --workspace --all-targets --locked` | PASS |
| `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | PASS |
| `cargo test --workspace --all-features --locked` | BLOCKED by sandbox socket permissions: 67 passed, 29 failed when loopback binds returned `Operation not permitted`; these failures are unrelated to the changed cache code. |
| Manual CLI purge smoke with corrupt manifest and symlink target | PASS; exact corrupt key removed, symlink target refused with no stdout and outside sentinel unchanged. |
| Python AST parse for campaign, verifier, inventory and test | PASS |
| `python3 -B scripts/tests/test_phase26_verify.py` | PASS, unexpected symlink rejected |
| `scripts/phase26/campaign.py` / PyArrow oracle | NOT_RUN: pinned `pyarrow==24.0.0` and `blake3==1.0.8` are not installed. Installing into `/private/tmp` was attempted, but network DNS resolution failed and no local package cache was available. |
| Cross-process campaign, detached archive/seal and economics | NOT_RUN |
| Exact-final-source hosted CI | NOT_RUN; no new run receipt exists. The reviewed SHA's prior `rust` job failure remains historical and unchanged. |

The local worktree included an unrelated untracked Phase 0 prompt before these
changes; it was left untouched. This report and support matrix are additive
current-source qualification records. A new detached evidence bundle, source
inventory, policy economics campaign and exact-commit hosted CI run are still
required before any complete Phase 26 claim.
