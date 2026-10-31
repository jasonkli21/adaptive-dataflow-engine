# Phase17 current-source reacceptance — 2026-10-31

Status: **COMPLETE_BOUNDED_TRANSACTIONAL_STATE_V1** on the newly measured source
below, including G15. This additive record closes the review-remediation
`PARTIAL_VERIFICATION` prerequisite for Phase18. It does not relabel candidate5,
its 121-case archive, any predecessor, or the earlier NOT_RUN record.

The final Phase18 build and this fresh campaign have the same 147 Rust/Cargo input
hashes and identical transactional/stateful executables. The canonical input-set
SHA256 is `3b916fb9744a5aa66ef347497af2e034e160d75aaff0ce86619d45f2731a4b91`.
The raw inventory files use different newline formatting; their hashes are not
substituted for one another. Baseline is
`fbe0e2b7739fd7d61c45c58706faa56b63a945be`, plus the explicit Phase18 implementation.

- Source-inventory SHA256: `949140ec033f023ce0a89abf82f69f55d86960471da6ae7cd1f36ab79da8423b`.
- Raw matched-runtime inventory SHA256: `d3d01615eb8739d0561ad0074f23680264fb4fb6a6806e14e1f89ddd41c81f17`.
- Transactional executable: `1b81e47a33781e3aa6b41f3c716ba3baddf37cedb09168f23a95a8c450fc0b7f`.
- Stateful executable: `d1b6d9d93c0b4ceecfb6b59ab2d7adda7b26aa9ca3e525ff3e556116f156d598`.
- [Portable archive](../evidence/phase18-phase17-reacceptance/acceptance.tar.gz), SHA256
  `ace36b8fa54ef0a279b4050e04481c18a9d7da39764aecee29eed1db4cf99e05`;
  [manifest](../evidence/phase18-phase17-reacceptance/manifest.json).

All 137 transactional cases,152 independent reader observations,387 matched cuts,
70 admitted sessions and4 complete exports verify. G15 proves populated and empty
PAUSED selector loss, interrupted imports, exact selector restoration and stale
retired-head refusal before assignment/output. Actual SIGKILL at all 18 declared
publication boundaries, pause/resume, poisoned attempts, codec1→2, local logical
1→2→4→1 redistribution and no-replace savepoints retain their contracts.

Five fresh Phase16 prerequisite probes use actual Kafka 3.9.1 and a separate MinIO
process: expired replay origin with retained saved offsets, both vectors expired,
preflight/assignment retention race, effective full remote placement and full-copy
capacity veto before PUT. The wire proof has 848 signed requests and172 successful
remote objects. The matched-executable ENOSPC supplement uses an owned16MiB HFS+
image with zero available blocks and actual errno28. It preserves the old80-record,
80-entry cut and exact reader output; the image was detached. Import supplied the
actual retired predecessor head and checked it against the saved selected head.

The additive `scripts/phase18/phase17-criteria.json` changes only the original v4
criteria's stale starting-head provenance from bd5d1b9 to fbe0e2b. All gates,
refusal inventories, arithmetic predicates and quantitative criteria are unchanged.
The original criteria/archive remain untouched. Harness fixes add an explicit
criteria input, leading-dash-safe cluster IDs and the required import attestation.

The frozen source includes the current runtime; later documentation and Phase18
verifier hardening are separately inventoried. The final workspace gate passes319
Rust tests,0 failures,10 existing ignores, strict Clippy, locked check, formatting,
smoke and the feature matrix. Its frozen validation contains62 Python tests;
the final strengthened verification suite passes63. No runtime input changed.

Reproduce with the commands in the Phase18 report, then verify without services:

```sh
python3 scripts/phase17/verify_evidence.py \
  --archive docs/evidence/phase18-phase17-reacceptance/acceptance.tar.gz \
  --manifest docs/evidence/phase18-phase17-reacceptance/manifest.json
```

Exactly once remains the committed-visible window prefix in one pinned local job
lineage through CURRENT3 and the versioned reader. Trusted immutable Kafka history,
single-local-filesystem rename/fsync and one active imported continuation remain
operator assumptions. No external transactions, global cross-filesystem fencing,
automatic recovery/GC, physical rescale, disk-loss/cloud/production HA or hardware
power-loss guarantee is inferred. Same-session self-review and independent Python
oracles are disclosed; no separate reviewer or subagent was used.
