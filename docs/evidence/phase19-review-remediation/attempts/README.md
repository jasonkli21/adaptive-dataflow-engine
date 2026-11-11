# Intermediate remediation attempts

These candidates are retained as review provenance. Their failed checkpoints are not acceptance results; the final matched archive is at the parent directory.

- `01-request-binding-conflict`: first new source test accidentally specified both `deltas` and `delta_tail`; the request correctly refused it.
- `02-reader-manifest-path`: the concurrent reader proof looked for a file named `manifest`; the on-disk selector is `MAINTAINED19`.
- `03-stale-provenance-check`: first full campaign was rejected because the verifier required the historical criteria baseline commit to equal the current campaign provenance commit. The verifier now records each baseline SHA without requiring the historical value.
- `04-packaged-verifier-hash`: packaging combined the just-edited verifier with a campaign that had pinned the prior verifier bytes.
- `05-partial-footer-check`: the initial verifier required no `result` bytes in any failed stream; the exact-minus-one cap can emit the footer bytes but not the terminating newline. The corrected check requires a nonzero exit and no complete success footer.
- `06-pre-lint-clean-source`: campaign completed before the new Rust test module was moved after helper definitions to satisfy workspace Clippy. The final campaign after that source change is the accepted archive.
- `07-interrupted-stale-release-binary-campaign.json`: a campaign was stopped after its 78 functional observations when it was detected that its release binary predated the CLI usage-text update. The final campaign rebuilt and matched that executable.
- `08-enospc-pre-final-binary.json`: an earlier actual ENOSPC trial passed using the predecessor release binary. The final, hash-matched ENOSPC witness is the parent directory's `actual-enospc.json`.

Raw `cases.json`, `samples.json`, `performance.json`, `provenance.json`, criteria and sequence traces are kept where available. Large repeated fixture, snapshot and output-ledger payloads are omitted from superseded candidates; the final accepted archive retains its complete fixtures, snapshots, ledgers and verifier inputs. The disposable `/private/tmp` work roots are excluded.
