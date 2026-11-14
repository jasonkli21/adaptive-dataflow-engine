# Finite local Rust extensions

Phase 24 provides the optional Unix `local-v1` feature on `ade-ext-sdk`,
`ade-ext-vector` and `ade-ext-graph`. The Rust API hosts statically linked,
operator-installed code over immutable inputs. See the [release support
matrix](reports/phase-24-support.json) for current qualification.

## Build and run an independent extension

```sh
cargo test --locked --manifest-path examples/phase24-external-consumer/Cargo.toml
```

The example depends on `ade-ext-sdk` through a Cargo path dependency, uses the
public exports, and implements signed sensor calibration. It uses Arrow and
Parquet in tests for fixture creation and result inspection. The manifest has
its own workspace and lockfile. A copy can point its path dependency at an
absolute exported ADE source path. Rust source compatibility is the v1
contract for this checkout. There is no published package or binary plugin ABI.

Implement `Extension` and `RowKernel`. Version 1 requires the
`LocalReplayableRowMap` capability, a canonical lowercase ID, a positive
extension version, and an exact ordered non-null schema of up to eight
UInt64, Int64 or Float64 fields. Column zero is the strictly ascending
unique UInt64 `row_id`. Only declared Int64 and Float64 fields can change.
`negotiate` compares the expected and installed descriptors. `execute`
checks the model bytes, source pin and expected measured executable. The
projection starts with column zero and uses distinct field indices.

Create Parquet with flat primitive required columns, plain uncompressed
pages, no dictionary or external column chunks, at most 128 row groups and
a 64 KiB footer. Supply a canonical absolute path without symlink aliases.
Use an externally retained pin of the entire file, its length and its row
count. The file cap is 16 MiB and the decoded row cap is 32,768. The SDK
admits at most 256 rows per map batch, 128 KiB of conservative map array
accounting per batch, 4 KiB each for model bytes and inline prepared state,
4 MiB of retained native output and a 10-second absolute deadline. Native
managed memory has a separate 16 MiB limit and spill is disabled. Batch and
result limits can be lowered independently. Very small batches may exceed the
same result cap because retained Arrow arrays have per-batch overhead.

Pin the approved linked executable out of band. Linux hashes
`/proc/self/exe`. On macOS, keep the installed executable path immutable
while launching and running it. The implementation fingerprint combines the
pinned executable digest with the selected Rust extension type name. It
participates in the logical ID and appears in the success receipt, so distinct
extension types in one binary do not share that ID. This is provenance, not
cryptographic proof of callback behavior; the SDK accepts trusted operator
code and does not sandbox it. Do not use the logical ID as an authenticity
claim or an unrestricted cache key. The attempt UUID changes between retries.
File and model contents
are read under one trusted operator. Native temporary allocations, allocator
rounding, kernel-owned heap and OS RSS are outside the measured map charge.
Keep third-party code cooperative and bounded. In-process Rust code can
block, miscompute or terminate the whole process. Results are returned in
memory after successful execution and verification. Callers own downstream
writers, cancellation while writing and publication.

## Vector features and fixed affine scoring

`VectorV1` accepts `row_id, f0, f1, f2, f3, score`. The five values after
`row_id` are required Float64. Select one through four active leading
features. The exact JSON model contains a `weights` array of the selected
length and a finite `bias`. Choose `Normalize`, `Score`, or
`NormalizeThenScore`. Normalization scales by the maximum absolute feature
before computing the norm. A zero norm fails. Inactive fields and unmodified
slots retain their bits. Affine products and additions occur in increasing
feature order. Nonfinite inputs, intermediates or results fail. This covers
finite feature preparation and fixed linear batch scoring. General iterative
training, model serving and approximate nearest-neighbor indexing are deferred.

## Weak component sizes

The graph API takes a Phase 20 `Request` for
`WeakComponentsMinLabelV1`, a separately retained selected `Pin`, a
canonical private stage parent and an absolute deadline. For a fresh cyclic
root, run `operate(Command::Run, ...)` and retain the terminal `Selected`
pin outside the graph root. Call `component_sizes_v1` with that exact pin.
The helper verifies the complete terminal, stages one `(label, 1)` row
per vertex, runs native sum/count, checks the tallies and verifies the pin
and source again. It returns ordered components and an attempt receipt after
removing the temporary stage. Existing Phase 20 graph and root limits still
apply. The stage cap is 1 MiB and its aggregate uses a separate 16 MiB native
managed budget.

If the process dies while staging, an operator can inspect and remove
`ade24-stage-*` directories after confirming the attempt has exited. A new
call uses a fresh private stage and explicitly supplied pin. Returned data
has no independent on-disk publication or cluster durability.

## Reproduce the local evidence

```sh
python -m pip install -r scripts/phase24/requirements.txt
cargo build --locked -p ade-ext-graph --features fault-injection --example phase24-native
output="$(mktemp -d)/phase24"
seal="$(python scripts/phase24/campaign.py --binary target/debug/examples/phase24-native --output "$output")"
python scripts/phase24/verify.py "$output" --mode historical-archive --seal-pin "$seal"
python scripts/phase24/tamper.py "$output" --seal-pin "$seal"
```

The optional macOS `--enospc` argument creates, fills and detaches a
disposable HFS+ image. Building with `fault-injection` enables deliberate
test pauses and failures. For historical replay use the independently
retained seal pin. Current-source acceptance also requires the matching
binary, a clean checkout, an external release receipt and independently
pinned exact-SHA hosted CI receipt.

Streaming and stateful kernels, remote execution, shape-changing operations,
CEP, geospatial, arbitrary code uploads, dynamic plugins, cross-tenant
isolation and external transactions are outside this version.
