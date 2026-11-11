# Phase 20 acceptance evidence

- `acceptance.tar.gz` and `manifest.json` contain the matched source and
  executable inventory, raw Parquet, commands, terminal streams, round
  ledgers, selected cuts, all timing samples, and actual ENOSPC supplement.
- `verification.log` contains the detached verification counts and both
  rehashed semantic refusal witnesses.
- `toolchain.tar.gz` and `toolchain-manifest.json` contain the final format,
  workspace, no-default, smoke, and historical regression logs.
- `attempts/` preserves the unsuccessful and preliminary campaigns.
- `frozen-criteria.sha256` and `baseline-status.txt` preserve premeasurement
  thresholds and the original worktree status.

Use the commands in the [implementation
report](../../reports/phase-20-implementation-report.md) to verify the
archive with or without a checkout and executable. The Phase 19 archives
remain independent of this evidence.
