# Maintained dataflows v1

`ade-maintain` applies bounded finite epochs to a local indexed multiset. It maintains `all_v1` and `filtered_v1` over one arrangement. Each view emits the count and signed sum by key. The filtered view includes values greater than or equal to the signed plan threshold. Keys with zero count are omitted.

## Build

```sh
cargo build --locked -p ade-incremental --features maintained-v1 --bin ade-maintain
```

Use Linux or macOS with a private directory on a local filesystem that supports atomic exclusive rename, hard links, fsync and kernel file locks. Make one empty state directory per active lineage with `mkdir -m 700`. Retain the exact plan and the expected chain for each requested epoch independently from the state directory. Source paths and state roots must be free of symlinks and parent-directory traversal.

## Bind a baseline and epochs

The plan is JSON with exactly these fields. Replace the example file dimensions and digest with those of your own immutable Parquet file.

```json
{
  "version": 1,
  "namespace": "inventory",
  "threshold": 0,
  "baseline": [
    {
      "name": "baseline.parquet",
      "bytes": 947,
      "rows": 12,
      "fingerprint": "0000000000000000000000000000000000000000000000000000000000000000"
    }
  ]
}
```

Each file contains `key: UInt64` followed by `value: Int64`, both non-null. Use the raw BLAKE3-256 file hash, byte size and decoded row count. The native catalog verifies exact files, schema, rows, decoder limits and postflight bytes. The schema and ordered file inventory are part of semantic identity.

The plan digest is BLAKE3-256 over `maintained-plan-v1`, a zero byte and the compact UTF-8 plan JSON. Canonical field order is the order shown above, including each file entry. The genesis chain is BLAKE3-256 over `maintained-genesis-v1`, a zero byte and the lowercase hex plan digest as ASCII. [oracle.py](../scripts/phase19/oracle.py) implements these identities without running the engine.

Number epoch files `00000001.json` through `00000032.json`. Each envelope has `version: 1`, the hex `plan` digest, the integer `epoch`, the hex `parent` chain and a `rows` array. A row is `[key, value, weight]`, with an unsigned 64-bit key and signed 64-bit value and weight. To replace a row, put its negative weight and the replacement's positive weight in the same batch. Equal rows combine before arithmetic. The resulting bag cannot contain negative multiplicities, and committed counts and sums must fit their respective types. An empty or net-zero batch still advances the epoch.

The chain for an epoch is BLAKE3-256 over `maintained-delta-v1`, a zero byte and the exact envelope bytes. Reordering rows or changing whitespace changes lineage. Keep original bytes for retries.

## Execute and inspect

Every command takes a strict request file. This example shows the structure for epoch zero. Replace the paths and chain with your own values.

```json
{
  "version": 1,
  "plan": "/data/inventory/plan.json",
  "root": "/data/inventory/state",
  "baseline": "/data/inventory/source",
  "epoch": 0,
  "chain": "0000000000000000000000000000000000000000000000000000000000000000",
  "policy": {"kind": "adaptive"}
}
```

```sh
ade-maintain explain request.json
ade-maintain bootstrap request.json
ade-maintain read request.json
ade-maintain verify request.json
```

`explain` checks available capabilities under a shared lease and emits the bounded decision without publication. `bootstrap` selects epoch zero. `apply` advances a selected root by one epoch. An empty root can use `apply` to rebuild any supported target from the complete history. Set the request `epoch` and `chain` to the desired target.

- Supply `baseline` as the directory containing the Parquet inventory and `deltas` as the directory containing the complete epoch history for full replay.
- Supply `delta_tail` as the exact target file and omit `deltas` to update from a verified parent alone. Omit `baseline` if it is unavailable or should remain unopened.
- Omit source paths to read or restore an already selected exact target. `read` emits ordered JSON lines for both views, then the selected epoch, chain and result ID. `verify` performs the same verification and emits only the footer. Consumers must check successful exit and the footer before treating streamed output as complete.
- Use `{"kind":"disabled"}` to select static full replay or `{"kind":"force","route":"incremental_update"}` to require a route. The other force values are `restore` and `full_recompute`. Invalid or unavailable forced choices fail closed. Forcing replay on the already selected exact target verifies independent equivalence without replacing committed objects.
- `resume` uses the same checked entry point as `apply`. It can finish an exact witnessed generation or continue from a selected parent. A verified retry of the selected target performs restore and applies zero deltas.

Rust callers can construct `Request` or parse it using `Request::from_path`, then use `run` and `read` with ADE's cancellation token. Callbacks execute synchronously and must honor the caller's own I/O deadline.

## Operations and limits

The kernel writer lease covers verification, staged object writes and publication. A conflicting reader or writer receives a busy error and can retry. A process loss before the durable witness leaves an unpublished stage. The next writer can clean it and recompute from the prior selected target. A process loss after the witness and before selection allows an exact resume. A process loss after selection allows an idempotent retry. If the requested epoch or digest differs from the witnessed generation, or root verification fails, preserve the directory for diagnosis and rebuild from complete input in a new empty root. Never copy a root to resume it. The root identity is tied to its directory and lease inodes.

The generation contains one shard object per populated shard, with both view aggregates in each key record. A sparse update decodes and rewrites only touched shards. Retained objects and generation metadata remain on disk. A read scans the committed view records in shard order and then ascending key order. File checksums protect selected contents, and the independently retained chain guards against requesting a rolled-back target. The durability envelope assumes a trusted operator, honest local filesystem and immutable source history. Arbitrary replacement of all local trust anchors requires external attestation.

Default limits are 131,072 hot entries, 64 MiB of current encoded state, 256 MiB of retained disk bytes, 16 MiB of output and 120 seconds. Override all five fields together under `limits` using `hot_entries`, `state_bytes`, `disk_bytes`, `output_bytes` and `timeout_ms`. The per-epoch caps are 2 MiB and 32,768 rows. Input has at most 64 Parquet files, one million rows and 64 MiB. The scan uses one synchronous Arrow batch of at most 1,024 rows, zero prefetch and a 1 MiB batch memory ledger. Fixed parser and decoder bounds apply separately from the logical arrangement and byte ledgers. The ledgers exclude allocator overhead, OS cache, block metadata and process RSS. Deadlines are checked between bounded operations. A blocked filesystem syscall can outlive a deadline.

This path supports local finite Parquet and delta files. General materialized SQL views, joins, Kafka CDC, remote arrangements, automatic garbage collection, physical rescale, production HA and external transactions remain outside the supported envelope.
