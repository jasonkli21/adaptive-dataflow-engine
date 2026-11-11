# Phase 20 review remediation evidence

This additive evidence set records the repaired detached verifier against the
immutable original acceptance archive. It does not replace that archive and
does not claim a fresh native Phase 20 campaign or full G0–G15 acceptance.

`attacks/` contains complete rehashed archive copies and member manifests for
each semantic mutation. The pristine input is the original
`docs/evidence/phase20/acceptance.tar.gz` with its original manifest. The
verifier output records the independently reconstructed counts and the reason
each mutated archive was refused.

See [`verification.log`](verification.log) for the replay and refusal results.
Consolidated repository check logs and their exit manifest are in `checks/`;
the transient parallel-load failure and its passing isolated retry are
preserved under `attempts/`.

The source/tool/fixture hashes, observed environment and run status are in
[`remediation-manifest.json`](remediation-manifest.json). The local replay used
the already available PyArrow 24 environment and a temporary BLAKE3 adapter
calling the cached Rust BLAKE3 implementation because network access prevented
installation of the pinned Python wheels. This is one local environment; the
independent Linux/Python 3.12 portability replay and a fresh source-matched
native campaign remain outstanding.
