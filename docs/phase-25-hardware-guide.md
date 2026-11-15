# Opt-in native filter kernels

Build the ADE-local benchmark adapter with the optional feature.

```sh
cargo build --release --locked -p ade-bench --features filter-kernels-v1
target/release/ade-bench run --engine ade-local \
  --spec benchmarks/specs/scan-filter-project.toml --dataset DATASET \
  --engine-option kernel_policy=force-neon --warmups 1 --iterations 7 \
  --verify-data full --output result.json
```

Use `scalar` for the current explicit control, `arrow` for Arrow's typed
comparison control, or `force-neon` for the packed unsigned comparison on an
admitted little-endian ARM64 CPU. `force-avx2` returns an ISA refusal. Leaving
`kernel_policy` out uses the legacy implementation, including in a
feature-enabled build. The options apply only to the ADE-local
`scan_filter_project` case. The feature-disabled build rejects these options.

For Rust, enable `ade-exec/filter-kernels-v1` and use
`execute_with_filter_kernel(plan, config, cancellation, request)`. It uses
ordinary local pull execution and returns the completed output and a separate
audit. `ExecutionConfig` and the existing entry points keep their signatures.

## Resource and result behavior

The opt-in filter admits at most 262,144 rows and 64 MiB of reported Arrow input
buffers per batch. It reserves ADE-owned mask scratch before allocation and
checks cancellation between 8,192-row groups on the NEON path. The scalar and
Arrow mask calls, and the Arrow gather call common to all modes, check
cancellation at their bounded call boundaries. Retained output uses the
existing collection budget. Scratch and retained bytes are managed accounting.
Arrow internal allocations and operating-system peak RSS are measured separately.

The structured `phase25-filter-v1` line on explicit successful execution reports
grouped batch/row/vector counts, selected kernel and reason, scratch peak and
wall-time nanoseconds for the mask and gather. The original benchmark result
schema and independent synthetic oracle are used for query timing and result
validation.

## Calibrated policy

Select `kernel_policy=measured-auto` with optional paired
`kernel_profile=/path/to/profile.json` and `kernel_profile_blake3=<hex>`.
The detached campaign creates a sample-bound profile for the original
threshold-250 shape on macOS ARM64. Use the pin in the `profile-current` process
record. Profile age may be at most 90 days. A profile must match the exact
executable, public Mac model, OS release, dataset bytes, workload, and
execution settings. The executable pin also binds the Rust and Arrow build.
The caller is responsible for controlling the executable, profile and source
files during execution. A caller-supplied digest checks integrity within that
trust boundary.

Auto requires all seven paired comparisons against both controls to show at
least 5% filter and query gains. The candidate process wall time has to fit
within the allowed 5% margin including the measured cost of profile admission.
Child peak RSS has the same 5% margin. Missing, invalid, incompatible, stale,
unsupported and unmeasured inputs fall back with recorded reasons. A matched
profile with insufficient evidence also uses scalar.

## Reproduce the local campaign

Install the pinned reader dependencies in a Python environment. Use fresh
output and fixture paths. Keep the exact binary if you plan to verify its
archive later.

```sh
python -m pip install -r scripts/phase25/requirements.txt
cargo build --release --locked -p ade-bench --features filter-kernels-v1
cargo build --release --locked -p ade-exec --features filter-kernels-v1 --example phase25-study
python scripts/phase25/study.py target/release/examples/phase25-study \
  /tmp/phase25-micro.json --compare scalar arrow neon
seal=$(python scripts/phase25/campaign.py --bench target/release/ade-bench \
  --fixtures /tmp/phase25-fixtures --output /tmp/phase25-candidate)
python scripts/phase25/verify.py /tmp/phase25-candidate --seal-pin "$seal" \
  --bench target/release/ade-bench --fixtures /tmp/phase25-fixtures
python scripts/phase25/tamper.py /tmp/phase25-candidate --seal-pin "$seal" \
  --bench target/release/ade-bench --fixtures /tmp/phase25-fixtures
```

The script's profile generator is calibrated for the observed macOS product
class. A portable correctness run on x86 exercises the scalar and Arrow
controls and the hard capability refusal. Physical placement, NUMA, GPU and
code generation beyond the compiled NEON routine are outside this interface.
