#!/usr/bin/env bash
set -euo pipefail

bin=${1:-target/debug/ade-bench}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

"$bin" data generate synthetic --dataset-id phase25-portable-v1 --rows 2048 \
  --files 2 --row-group-rows 256 --key-cardinality 128 --seed 1307 \
  --output "$work/data" > "$work/manifest.json"
spec=benchmarks/specs/scan-filter-project.toml

"$bin" validate --engine ade-local --spec "$spec" --dataset "$work/data" \
  > "$work/legacy.json"
for policy in scalar arrow; do
  "$bin" validate --engine ade-local --engine-option "kernel_policy=$policy" \
    --spec "$spec" --dataset "$work/data" > "$work/$policy.json"
done

if "$bin" validate --engine ade-local --engine-option kernel_policy=force-avx2 \
  --spec "$spec" --dataset "$work/data" > "$work/forced.json" 2> "$work/forced.log"; then
  echo "unsupported AVX2 request unexpectedly succeeded" >&2
  exit 1
fi
grep -Fq 'unsupported native filter ISA: avx2' "$work/forced.log"

python3 - "$work" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
legacy = json.loads((root / "legacy.json").read_text())
for name in ("scalar", "arrow"):
    actual = json.loads((root / f"{name}.json").read_text())
    assert actual["validated"] is True, name
    assert actual["rows_output"] == legacy["rows_output"], name
    assert actual["result_fingerprint"] == legacy["result_fingerprint"], name
PY
