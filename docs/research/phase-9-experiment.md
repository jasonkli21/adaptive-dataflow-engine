# Phase 9 release experiment — v1 (predeclared 2026-10-22)

Frozen before any Phase9 release measurements. Phase8's separate closure remains
in its report and immutable release. No Phase9 release timing has been inspected
at declaration. The implementation source snapshot containing this declaration,
runner and verifier must be clean, Git-bundled and built before execution. Main
remains at c87d2ba until the user's one final implementation commit; temporary
clean source snapshots establish the exact measured runtime tree.

Host: this macOS arm64 machine, 8 logical CPUs, stable installed Rust/locked
Cargo.lock; capture `ade-bench env` and binary SHA256. Localhost 2-process primary,
4-process limited confirmation; these are not separate cloud failure domains.
Before timing take 3 host readings 10 seconds apart: CPU idle >=70%, load1 <=8,
swapout-page increase <=1000 across the window, no other cargo/rustc/ade-bench/
ade-worker process. If readiness fails, retain it and postpone without measuring;
do not discard measured samples based on their latency. Capture readiness and
before/after host telemetry. No concurrent compilation or performance experiment.

All generators seed42, 4 files per side/input, join rowgroups4096, aggregate
rowgroups8192/snappy, original generator defaults otherwise (payload16).
InnerEquiJoin is non-null bag semantics, output cap1,000,000, independent DF SQL
reference and exact canonical multiplicity. GroupBySum retains its original oracle.
Every run is a fresh harness child; full file verification. Primary arms have one
warmup and FIVE measured children each; confirmation one warmup and THREE.
Arm ordering is fixed below and all samples included, no best-only result.
The harness query interval is the primary end-to-end job metric (includes prepare,
probe, ACK, exchange, join/final and cancellation; excludes cluster creation in
adapter setup and independent oracle). Record diagnostic coordinator wall time,
source verification, scan/probe/ACK, operator work (wall less source/sink, NOT CPU),
managed/source/frame/queue peaks, disk and transmitted bytes, and dispersion.

Join controls: spill exchange only, no recovery/prefetch/layout policy/copies;
32MiB local memory, 65536 build rows, automatic hash->grace, batches1024;
probe4096 rows/4files/8MiB API reads PER worker+side, metadata1MiB/decode1MiB,
broadcast4096 rows/1MiB state upper bound/4MiB amplified raw row bytes. Complete
coverage of the selected side and configured hash state must fit the reservation.
All modes perform identical probes (including forced controls). Build side auto,
right fallback; complete smaller side wins, ties right. Fixed hash1%N/stages2,3,4.
Full source domain <=1m rows and64MiB physical bytes PER side. Exchange ingress
per store <=1m rows/128MiB encoded/1mframes. Bounded final validation cap is global.

| Regime | Exact data | Primary arms | Exit |
|---|---|---|---|
| tiny | L65536/R128; both key domain65536 | forced_repartition, forced_broadcast, observed_v1 | observed broadcast/right; median <=1.15 * better forced |
| balanced | L65536/R65536; both domain1048576 | forced_repartition, observed_v1; forced_broadcast negative | observed repartition, both incomplete; median <=1.20 * repartition |
| pressure | L8192/R8192; both domain8192; hash512, memory262144 | auto and forced grace, forced repartition; hash_only negative | actual auto transition, exact rows, grace complete, bounded clean; quantify, no speed gate |
| low | GroupBySum100000 rows/cardinality16/uniform | fixed_hash, raw_partial, adaptive_v1 | adaptive retains hash; median <=1.15 * fixed |
| high | GroupBySum100000 rows/cardinality100000/uniform | fixed_hash, raw_partial, adaptive_v1 | actual adaptive hash->raw; median <=1.15 * better feasible forced |
| hot negative | L128/R128, both domain1; hash32,262144 memory, repartition | DF oracle and automatic bounded refusal | JoinSkew; no committed result, no performance win claimed |

Aggregate controls: static layout, spill exchange1MiB, task memory32MiB,
batch1024, prefetch0, recovery disabled for ALL three controls, prefix1024,
minimum reduction50%. Do not widen baseline aggregate spill domain. Record
actual transferred partial rows/bytes and all final work; raw may be worse.

Limited 4-process confirmation: tiny all3, balanced both, pressure auto,
low/high fixed+adaptive. Same guards on these measured controls (tiny better
forced, balanced1.20, low1.15fixed, high1.15fixed); four-process results never
substitute for primary gates. Exact gates apply separately; no aggregate score.

The primary run includes 13 arms *6 children =78; confirmation10 arms *4 =40;
118 total successful children,95 measured. Each regime first has a standalone
DF oracle. Expected negative arms are separate validated failures, never measured
successes. Preserve all raw result JSON, child stderr/outcomes, sidecars, manifests,
commands, environment, source/build identities, failed runs and readiness records.
The verifier checks the exact arm inventory, iteration/warmup cardinality, unique
PIDs, correctness/fingerprints, source/config equality, side observations/digests,
actual traffic/terminal/gather conservation and mechanism/budget facts before
computing medians. Tamper copied artifacts to prove rejection. CI uses correctness
only, never these timing gates. If any gate fails, report failure and preserve
it; any subsequent code change requires a new clean source run, without changing
v1 thresholds or dropping previous evidence.
