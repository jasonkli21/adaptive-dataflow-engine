# Phase 26 evidence

The archive contains the exact macOS campaign executable and Linux cross-UID
executable, original Parquet and plans, a materialized prefix, runtime build
inputs, raw receipts, benchmark samples, and local check logs. The SHA-256 of
the archive's canonical file inventory is supplied as a separate pin. The
runtime input manifest can be checked against the final commit without
requiring a Git commit to embed its own SHA.

```bash
python3 -m pip install -r scripts/phase26/requirements.txt
bundle=$(mktemp -d)
tar -xzf docs/evidence/phase26/candidate-final.tar.gz -C "$bundle"
pin=$(cat docs/evidence/phase26/candidate-final.seal-sha256)
python3 scripts/phase26/verify.py "$bundle" --pin "$pin" --release
python3 scripts/phase26/tamper.py "$bundle"
python3 scripts/phase26/source_inventory.py "$PWD" "$bundle/source" \
  --compare --revision HEAD
```

The detached oracle reads original Parquet independently using PyArrow and
recomputes exact signed aggregate bags and the ordered cached prefix. It checks
policy arithmetic separately and matches durable events with their subprocess
receipts. Resealing the hostile fixtures exercises the semantic checks after
the outer inventory has already passed. Earlier failed bring-up and timing
samples are retained under `predecessors` with command logs.

To replay the bounded native cases on a fresh checkout, build with
`cargo build --locked -p ade-cache --features fault-injection --bin ade-cache`
and run `scripts/phase26/campaign.py` with the executable, a new output path,
and `--no-economics`. The stored macOS and Linux binaries are platform-specific.
Hosted CI receipts are external to this bundle.
