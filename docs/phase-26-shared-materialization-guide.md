# Phase 26 shared materialization

Build the optional native CLI on Linux or macOS.

```bash
cargo build --locked -p ade-cache --features shared-v1 --bin ade-cache
bin="$PWD/target/debug/ade-cache"
work=$(mktemp -d)
chmod 700 "$work"
work=$(cd "$work" && pwd -P)
mkdir -m 700 "$work/cache"
"$bin" init --cache "$work/cache"
```

The operator supplies an immutable source directory, a PlanV1 file for each
job, and an independently approved BLAKE3 digest of the exact source
`catalog.json` bytes. The strict catalog has `version: 1`, a logical snapshot
fingerprint, a non-null Arrow field list, and an ordered list of Parquet files
with relative names, byte counts, row counts and BLAKE3 digests. The PlanV1
source must match the catalog exactly. Keep the pin in operator configuration.
Issue new plans and a new pin together when the snapshot changes.

For a runnable local example, generate two plans independently with PyArrow.

```bash
python3 -m pip install -r scripts/phase26/requirements.txt
PYTHONPATH="$PWD/scripts/phase26" python3 - "$work" <<'PY'
from pathlib import Path
import sys
from fixtures import digest, make_fixture, plan, write
work = Path(sys.argv[1])
source = make_fixture(work / "source", 8194, 5, 3)
write(work / "a.json", plan(source, 4000))
write(work / "b.json", plan(source, 4000, group=2, alias="total"))
(work / "pin").write_text(digest((source / "catalog.json").read_bytes()))
PY
pin=$(cat "$work/pin")
"$bin" run --source "$work/source" --catalog-pin "$pin" \
  --plan "$work/a.json" --policy always --cut 2 --cache "$work/cache"
"$bin" run --source "$work/source" --catalog-pin "$pin" \
  --plan "$work/b.json" --policy always --cut 2 --cache "$work/cache"
"$bin" inspect --cache "$work/cache"
"$bin" gc --cache "$work/cache"
```

The first job reports `miss` and `publish`. The second reports `hit` and runs
its sum/count over the cached prefix. Both return a typed schema, rows and a
versioned audit. The audit separates the original logical plan from the
rewritten native suffix and shows real operator row counts. The native source
preflight still reads and decodes original Parquet on a hit.

Cut 1 supports Scan → UInt64 Filter. Cut 2 adds a positional projection.
The suffix supports GroupSumCount and optional projections. Bounds include one
replayable local source, 64 files, 1,000,000 rows, 64 MiB physical source bytes,
64 PlanV1 nodes, and the native output/managed resource caps. Materialization
has a separate 64 MiB encoded maximum and 4 KiB metadata ceiling. Stage
reservation includes metadata and defaults to 4 MiB. Increase it with
`--stage-bytes` for a wide or less selective prefix. The initialized root
configuration caps logical file bytes and retained entry count. Defaults are
256 MiB and 32 entries. Scratch state consists of one stage and one trash
directory. System block and directory accounting varies by filesystem.

`disabled` is the default. `never` also executes the original plan without a
cache lookup. `always` retains a verified prefix when capacity permits.
`expected_value_v1` uses the optional `--calibration` JSON file with
`uses_milli`, `survival_bps`, `recompute_us`, `restore_us`, `write_us`,
`hold_us` and `invalidation_us`. Restore estimates include original-source
revalidation. Hold and invalidation estimates cover the declared retention
horizon. Use `--ttl-ms` to set the horizon. Missing and nonpositive forecasts
skip before opening the cache root. An admission may remove lower-value
entries before writing. A failed fill can leave completed evictions in place.

Use an absolute canonical path and a dedicated effective-UID-owned 0700 cache
directory on a local filesystem that supports no-replace rename and directory
fsync. Configure the filesystem without ACLs that grant other principals
access. Use a trusted immutable source and ordinary owner- or root-owned
ancestors. The source files may be owner- or root-owned and must be single-link,
non-symlink and free of group/other write permissions. An individual root is
one trusted operator namespace. The cache trusts that operator and its
filesystem. Source attestation before and after native access catches observed
replacement. Operating across an adversarial same-UID path swap is outside the
contract.

Run `inspect` after a failure to see approximate inventory by charged file
length, then `gc` to remove an abandoned stage or trash directory and expired
entries. These operations use the same pins as jobs. Eight reads may share a
root pin. An exclusive writer/GC waits no more than two seconds or the request
deadline. Restarting a process releases its locks. A corrupt entry refuses
reuse and should be retired under operator control with active jobs stopped.
The cache requires an honest filesystem and makes a process-crash recovery
claim. User-visible output is emitted only after native and provenance checks.
