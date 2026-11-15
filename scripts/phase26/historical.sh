#!/usr/bin/env bash
# Replay the frozen campaign with its own source and verifier.
set -euo pipefail
repo=$(git rev-parse --show-toplevel)
revision=9134b4745c33029e27421b4d0a4c57c604c50ef2
evidence="$repo/docs/evidence/phase26"
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
mkdir "$work/scripts" "$work/bundle"

git -C "$repo" archive "$revision" scripts/phase26 | tar -xf - -C "$work/scripts"
tar -xzf "$evidence/candidate-final.tar.gz" -C "$work/bundle"
pin=$(cat "$evidence/candidate-final.seal-sha256")
historical="$work/scripts/scripts/phase26"
python_bin=${PYTHON:-python3}
"$python_bin" -B "$historical/verify.py" "$work/bundle" --pin "$pin" --release
"$python_bin" -B "$historical/source_inventory.py" "$repo" "$work/bundle/source" \
    --compare --revision "$revision"
"$python_bin" -B "$historical/tamper.py" "$work/bundle"
