#!/usr/bin/env bash
# Run with access to local loopback for the existing distributed tests and smoke.
set -uo pipefail
out=$1
mkdir -p "$out"
status=0
run() {
  local name=$1
  shift
  local code=0
  "$@" > "$out/$name.log" 2>&1 || code=$?
  printf '%s\t%s\n' "$name" "$code" | tee -a "$out/exits.tsv"
  if [[ $code != 0 ]]; then status=1; fi
}
run format cargo fmt --all -- --check
run workspace-check cargo check --workspace --all-targets --locked
run workspace-clippy cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
run workspace-test cargo test --workspace --all-features --locked
run incremental-no-default cargo check -p ade-incremental --no-default-features --locked
run iterate-no-default cargo check -p ade-iterate --no-default-features --locked
run iterate-default-tree cargo tree -p ade-iterate --no-default-features --locked
run iterate-cyclic-tree cargo tree -p ade-iterate --features cyclic-v1 --locked
run smoke-build cargo build -p ade-bench --locked
run smoke bash scripts/smoke.sh
run python-regressions python3 -B -m unittest discover -s scripts/tests
original="$out/phase19-original-source"
mkdir -p "$original"
extract_original() {
  git archive 6cc1d19d713dcb544eb481d1c5a2d87935643e44 scripts/phase19 scripts/phase17/digest.py | tar -xf - -C "$original"
}
run phase19-extract-original extract_original
run phase19-historical python3 -B "$original/scripts/phase19/verify_evidence.py" \
  --archive docs/evidence/phase19/acceptance.tar.gz --manifest docs/evidence/phase19/manifest.json
run phase19-review-remediation-historical python3 -B scripts/phase19/verify_evidence.py \
  --archive docs/evidence/phase19-review-remediation/acceptance.tar.gz \
  --manifest docs/evidence/phase19-review-remediation/manifest.json
run prior-evidence git diff --exit-code c0e1c1faec96219fb79c864de1d6194bf5d8571e -- \
  docs/evidence/phase17 docs/evidence/phase18 docs/evidence/phase19 docs/evidence/phase19-review-remediation
exit "$status"
