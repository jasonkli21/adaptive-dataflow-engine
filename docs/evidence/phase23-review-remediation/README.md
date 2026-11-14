# Supplemental native-history witness

`native-history-witness.json` is the raw history-only event record from a live
Phase 23 manager and four managed worker processes. It records 32 successful
native registrations in FIFO mode followed by immediate `NATIVE_HISTORY_FULL`
submission refusal. This was a focused author-run correctness witness, not a
sealed full campaign and not independent G3/G5 acceptance.

At execution, `HEAD` was `a639a93ddb411525ad8cdbc124ca8f3eed97362f` with a dirty
worktree. The manager binary was freshly built from the remediation Rust source;
the batch, worker and fixture binaries had these SHA-256 digests:

| Binary | SHA-256 |
|---|---|
| `ade-resource` | `28350d1543c898ab1112d0856dba020d576d55972d970f8f5f0b33fa7492527d` |
| `ade-batch` | `44a60e6912e3a9e56e60809504b5237f9b14f2cfe3c73a8e5dce80dc1bd3aa20` |
| `ade-worker` | `070add5f84cad298212b02d72b45e2e92381d6621216edc5698b655b592bdd68` |
| `phase23-fixture` | `f7727fb21b91646dbecae7c6437e752a10e0033a63994173bc872879e7f44641` |

The raw case file SHA-256 is
`dd9917780ea8973c7e0f69f55590a28d39bce7e6e2dc270485088f099fe5ae96`.
The observed policy source SHA-256 at the run was
`9db6aba5006f2a64d4ce8efe1b1e88b006e5e1a09c9f4436241ab5e17af447a2`.
The checked-in candidate11 archive remains the only sealed historical campaign.
