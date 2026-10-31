# Phase17 transactional state operator guide

The default-off `transactional-state` feature provides a single local transactional
output sink for native Kafka keyed tumbling windows. Its visibility contract is
exactly-once **committed window output within a pinned job lineage**. Diagnostics,
input observations and uncommitted files are not business output. The v1/v2 stdout
paths remain trace-relative at least once and cannot be imported implicitly.

Build `cargo build -p ade-stream --features transactional-state --locked`.
The separate binary is `target/debug/ade-state-stream-txn`. The checked example
`benchmarks/examples/phase17-config3.json` documents every field. Supply absolute,
canonical, disjoint, precreated 0700 working/checkpoint directories. Files are 0600,
regular, single-link and opened without following symlinks. The working directory
must be empty; it contains lossy per-session independent owner stores. Use a new
checkpoint directory for every independently initialized job.

The v3 envelope embeds the unchanged native state v1 descriptor only to configure
the existing kernels and hot/cold stores. `limits.root_bytes`, `stage_bytes`,
`output_bytes`, `history_bytes` and `epochs` govern the new checkpoint/sink domain;
the embedded legacy checkpoint byte/generation limits do not publish v1 cuts.
Record/time cadence still uses the native fixed cadence fields. No policy can
bypass the transactional quotas. `codec` is an exact activation requirement;
`trusted_history` is the operator's pinned immutable topic-incarnation attestation,
not a broker assertion. Job, original offsets, source semantics and attestation
are fingerprinted; changing them refuses admission.

```
ade-state-stream-txn CONFIG3 run
ade-state-stream-txn CONFIG3 run --pause-after-records 1000
ade-state-stream-txn CONFIG3 resume
ade-state-stream-txn CONFIG3 resume --pause-after-ms 5000
ade-state-stream-txn CONFIG3 read
```

Only `read` writes business records to stdout. It takes a snapshot of CURRENT3,
verifies the complete authenticated parent chain, every canonical state record
and output frame, then emits whole committed records. An independent consumer
must use this reader contract. Directory LIST, provisional output files and stderr
are not supported sink interfaces. A slow/closed reader fails boundedly; it cannot
change the committed selector. Each read returns the retained committed prefix;
calling read twice is not a new delivery transaction to an arbitrary downstream
service. Use the epoch/offset identities to checkpoint an external reader if
building another system; no external exactly-once guarantee is supplied here.

A writer holds a kernel lock on the permanent WRITER3 inode. Never remove that
file. The ROOT3 identity binds the canonical checkpoint path; copying a writable
root and starting it elsewhere is refused. Use savepoint import for deliberate
continuations. Reader processes do not hold the writer lock. Atomic replacement
of CURRENT3 is the only visibility boundary. All output segments,32 state shards
and the epoch manifest are immutable and synced first. The manifest records the
parent hash, job/session/writer, codec/owners, offsets/frontier, late counts and
state/output hashes. There is no separately committed state pointer.

`ACTIVATED3` is a synced monotonic refusal witness, not a second selector. It is
written before the first epoch publication or import. If CURRENT3 is missing,
`read`, `run` and `resume` refuse whenever this witness or any object beyond the
fresh `ROOT3`/`WRITER3` controls exists. This includes interrupted first commits
and imports. Never remove the witness or guess a selector from directory contents.
Restore the exact selector only after independently proving its bytes and closure;
otherwise recover through a verified exact-digest savepoint into a new root.

SIGINT requests source-demand pause. The owner settles its current input and all
closed windows, verifies Kafka ranges, commits a PAUSED epoch, shuts down the owned
Kafka client/control pump and exits with a PAUSED diagnostic. That operation status
is also durable in the selected manifest. Resuming an unchanged already-PAUSED cut
is idempotent and does not consume another epoch. The public transaction handle
is terminal after PAUSED; `accept` and further commits require a fresh-process
resume. SIGKILL, I/O failure, failed
telemetry or a test hook is not a clean pause. After a failure, discard the attempt
and reopen CURRENT3. An error after rename may mean the new epoch committed;
never delete, roll back or replay it based on missing success telemetry. A corrupt
CURRENT3 or required object is fatal: no older-cut fallback or full replay exists.

## Savepoints

Use a separate canonical private directory for named exports. Retain the exact
BLAKE3 digest from EXPORTED telemetry independently of the export directory.

```
ade-state-stream-txn CONFIG3 export /absolute/private/exports name
ade-state-stream-txn FRESH_CONFIG3 import /absolute/private/exports/name EXACT_DIGEST RETIRED_PREDECESSOR_HEAD_BLAKE3
ade-state-stream-txn FRESH_CONFIG3 resume
```

An export is a self-contained immutable closure: all retained committed epochs,
all output history, every state shard, hashes and exact source progress, plus the
strict SAVEPOINT3 inventory. The named directory appears only after complete
sync. Export collision, unsafe name/path, corruption, foreign source/job/operator,
incomplete closure, excessive bytes and symlink/hardlink refuse. Failure leaves
an invalid stage orphan charged to the export budget; previous named exports are
unchanged. Import verifies the exact retained digest, all bytes and semantic
closure before adopting into an empty root. It requires the operator to supply the
retired predecessor's authoritative CURRENT3 `blake3`; this must match the
savepoint's selected head, so a newer supplied head refuses a stale savepoint
before target effects. This is an explicit operator attestation, not global
cross-root fencing. Import also requires matching codec/owners;
change those through an explicit transition afterward. Import selection is atomic;
an interrupted import requires a fresh root and preserves the source export.

A savepoint continuation retains the logical output prefix. Do not concurrently
run imported branches and merge their external output as one lineage. No system
can fence an independent copied filesystem after the operator explicitly authorizes
a new writer there; this product supports one active continuation under operator
control. The predecessor-head value is operator-supplied; it cannot prove that
another root is retired or that no branch remains active. Imports do not authorize
rollback over an existing selected output prefix.

The export label is published with an atomic no-replace rename. If the label
appears after preflight, export fails and preserves that destination. The
implementation requires the platform's atomic no-replace rename; unsupported
filesystems/platforms fail closed.

## Upgrade and logical rescale

Stop the writer first. A fresh transition process opens the exact old config:

```
ade-state-stream-txn OLD_CONFIG3 transition 2 2
# Write a config with codec=2, state.owners=2, identical semantic identity.
ade-state-stream-txn NEW_CONFIG3 resume
ade-state-stream-txn NEW_CONFIG3 transition 2 4
# Update owners to4, resume; then transition 2 1 and update to1.
```

Codec1 records contain32 canonical bytes plus their32-byte BLAKE3 checksum. Codec2
really transforms each to80 bytes:32 canonical bytes,8-byte checked window end,
8-byte ADE3REC2 tag and32-byte checksum. All semantic entries and progress must
match before the new epoch can activate. Older codec admission rejects new state.
Only1→2 is supported; downgrades, arbitrary operator/schema changes and replay of
a no-op transition refuse. A failed migration leaves the old selector authoritative.

A logical owner transition reconstructs all32 stable BLAKE3 key shards into separate
native owner hot/cold stores and writes/verifies the complete new snapshot. Every
entry, source offset and output prefix is preserved in one transition epoch. This
is local1/2/4-owner redistribution, not physical worker or Kafka partition rescale.

## Quotas, cleanup and failure assumptions

All capacities are finite and validated before effects. The checkpoint byte/count
budget includes retained generations, failed stages and selector temp files.
Staging reserves complete state plus output plus64KiB metadata and selector room
before writing. Per-epoch output and cumulative history have separate caps; no
visible history is silently dropped. Savepoint byte/count limits include failed
export orphans in the designated export root. Do not evade a quota by rotating
roots and claiming they are one accounting domain. Metadata/parser/record caps,
fixed32 shards and finite parent depth bound verification. Diagnostic writes have
a finite lifetime byte budget and use nonblocking I/O. An exhausted quota fails.

Hot/cold state, serialized provisional output and authenticated history/metadata
have separate accounting domains. Decoder/allocator/Arrow/client/kernel buffers
are external transients; this is not an RSS limit. Verification retains at most the
configured history and128 bounded manifests; it streams state records. No
background destructive GC exists. Increasing a permitted quota is explicit.
Otherwise stop the writer, retain required committed closure and operator-held
savepoints, and plan offline exact-object cleanup. Never remove committed history
or CURRENT3 to force a replay. After a killed process has been waited/proved dead,
lossy working-session directories may be removed explicitly; checkpoint orphans
remain charged until offline operator cleanup. WRITER3 is permanent.

Assumptions are a trusted OS user/private namespace and a single local filesystem
with atomic same-filesystem rename and functioning file/directory fsync. Rust's
[rename contract](https://doc.rust-lang.org/std/fs/fn.rename.html) and
[sync_all contract](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_all)
are distinct; a rename followed by failed sync is ambiguous and requires reopen.
Tests demonstrate process-crash behavior, not hardware/power-loss or disk-loss
survival. Source topics require immutable uncompressed <=32KiB history, fixed1–4
explicit partitions and exact original identity/offsets. Kafka metadata cannot
prove topic incarnation or undetected truncate/regrow; attestation remains mandatory.

No Kafka transactional output, consumer-group offset coupling, HTTP/S3 external
exactly-once, cross-service2PC, automatic recovery/GC, physical rescale, multi-host
HA or rolling arbitrary upgrade is supported. Prior Phase10–16 debt and timing
source identities remain separate. Phase18 is not authorized by this work.

Acceptance and offline reproduction commands are in the Phase17 report. Fault
options `--test-pause-at` / `--test-fail-at` are explicit research seams; they are
never clean-pause requests and must not be enabled in normal operation.
