# Phase 25 native CPU experiment

The native pull runtime dispatches `LocalPlan::Filter` through the shared
`ade-exec::stateless` predicate. Its contiguous UInt64 input gives a narrow
SIMD candidate. The native hash aggregate at `Kind::Aggregate` has more
irregular table lookups, checked reductions, and spill interactions. Changing
the hash kernel would confound table layout and memory-policy changes with the
CPU comparison. The filter keeps the reference operator and Arrow's gather
common to every arm.

Arrow 59.3.1 exposes a typed scalar comparator through `kernels::cmp::lt`.
The return mask propagates predicate nulls and Arrow excludes them at gather.
Optimized Arrow output already contains ARM64 vector unsigned comparisons.
The explicit NEON candidate packs eight comparisons into a mask byte with four
two-lane comparisons, vector narrowing, bit weights and a vector reduction.
Tails use scalar unsigned comparisons. Arrow's borrowed validity bitmap
preserves nonzero offsets. See the [Arrow comparison API](https://arrow.apache.org/rust/arrow_ord/cmp/fn.lt.html)
and the [Arrow gather API](https://arrow.apache.org/rust/arrow_select/filter/fn.filter_record_batch.html).

## Exploratory arm selection

One warmup and seven measured fresh processes per arm used offset-three input
with the frozen sizes, null densities, thresholds and distributions. Every
trial validated selection against direct row indices before collecting mask
and full-filter elapsed time. The paired median mask figures below are
nanoseconds per batch. Full-filter includes mask generation and the shared
Arrow row gather.

| Rows | Null % | Threshold | Initial scalar mask | Initial Arrow mask | Pilot Arrow mask | Pilot NEON mask | Pilot Arrow full | Pilot NEON full |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 257 | 0 | 0 | 528 | 169 | 289 | 201 | 1,080 | 1,003 |
| 257 | 10 | 1 | 810 | 178 | 298 | 198 | 1,155 | 1,005 |
| 1,024 | 50 | 50 | 2,697 | 317 | 551 | 409 | 2,607 | 2,411 |
| 8,192 | 0 | 50 | 10,517 | 1,640 | 3,365 | 2,688 | 19,883 | 19,977 |
| 8,192 | 10 | 99 | 18,117 | 1,644 | 3,388 | 2,721 | 55,536 | 55,819 |
| 8,192 | 50 | 100 | 22,237 | 1,637 | 3,384 | 2,741 | 26,023 | 26,407 |
| 32,768 | 0 | 50 | 34,807 | 6,341 | 12,306 | 10,272 | 125,690 | 123,921 |

The prototype ran during a background dependency build. Paired estimates
show scope for reducing mask cost, especially at small sizes. Full-filter
medians on medium and large batches show that gather can absorb that gain.
The original study/prototype sources and all samples are in
[the attempts directory](../evidence/phase25/attempts/).

## Measurement boundaries

The whole-query experiment uses the original scan/filter/project case at
threshold 250 and five fixed threshold/batch variants. All arms use the same
Parquet bytes, ordinary ADE-local native operator and release executable.
Synthetic EventV1 requires nonnull source columns. Null density and offsets
are covered by the micro experiment and the adversarial batches. Fixture
generation and the independent pretrial reader put the files into the
warm-cache regime. Each trial runs in a new process. Query time is the
benchmark harness elapsed time, including common collection and the explicit
audit emission, before canonical oracle validation. Child process wall and
peak RSS include admission and the harness oracle. The reported p50/p95 and
min/max retain the whole observed distribution. A shape qualifies for an
auto offer only when all seven paired samples meet the plan's margin.

NUMA placement, dual-architecture economics, GPU transfer and JIT/fusion
remain deferred. No native placement experiment or price/power comparison ran.
