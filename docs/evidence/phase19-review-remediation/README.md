# Phase 19 review remediation evidence

This directory is additive. `acceptance.tar.gz` is a fresh source-matched run with its independent member inventory in `manifest.json`. `actual-enospc.json` records a separate real OS error 28 test on a disposable 32 MiB HFS+ image. `attempts/` preserves the intermediate failed harness/verifier candidates and their raw artifacts.

The original `docs/evidence/phase19/` archive and manifest are unchanged. The criteria and workload thresholds inside the new campaign are the original frozen Phase 19 criteria. The acceptance archive SHA-256 is `0aac3c3286396ab6a409167b221da7a1abe81c64d697c59377b95558bd172d4a`; the remediation source inventory SHA-256 is `5c941dd614e52ca57811fd9f6e776abc3d7b88d55481e8f03c55af88e3cd2c67`.

Verify the archive and source match from the repository root:

```sh
python3 -B scripts/phase19/verify_evidence.py \
  --archive docs/evidence/phase19-review-remediation/acceptance.tar.gz \
  --manifest docs/evidence/phase19-review-remediation/manifest.json \
  --repo . --bin target/release/ade-maintain \
  --encoder target/release/examples/baseline --tamper
```
