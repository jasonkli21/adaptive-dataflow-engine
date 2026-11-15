# Phase 26 whole-sequence experiment

## Protocol frozen before measurements

The primary selective regime uses a pinned 900,000-row, 16-column, four-file
Zstandard Parquet fixture. A UInt64 predicate selects approximately 1/128 of
rows. The projection retains two possible aggregate keys plus the signed value.
Eight independently launched native jobs alternate two distinct grouping
columns. The small 4,096-row, four-column, one-file regime uses one job and a
zero-use forecast. Each regime runs disabled, always and expected_value_v1 with
one warmup plus five measured, interleaved, fresh-root sequences per arm. The
whole-sequence timer includes root initialization where applicable, all child
launches and exits, receipt parsing, actual cache cleanup and subprocess RSS
collection. The original source is immutable and OS page cache is uncontrolled.
A warmup has its own root and cannot transfer committed cache state to a sample.

The selective study uses an operator forecast of seven eligible subsequent
uses, 90% survival, 50,000 µs recompute, 25,000 µs restore, 60,000 µs write/sync,
1,000 µs capacity rent and 2,000 µs invalidation cost. Values are explicit
scenario estimates and must not be read as measured calibration. The cheap arm
uses zero demand with the same coefficients. Source rehash/postflight and
result serialization are included. Per-job resource accounting uses BSD or GNU
`time` RSS for the executable, and reports managed peak separately. Median and
nearest-rank p95 are computed from all five samples. The release thresholds are
at least 10% adaptive sequence improvement in the selective regime and no more
than 20% adaptive slowdown without reuse. Actual eviction under an opposing LRU
ordering is independently mandatory. The campaign archives all measured and
warmup samples, including failed conclusions. The host/day is one experimental
unit.

Before running the timing campaign, the adaptive implementation was changed to
decline uncalibrated and nonpositive forecasts before opening a root. The
adaptive no-reuse arm therefore charges one native job with no cache
initialization. The always arm still creates, charges and removes its root.

## Measured result

The initial completed run overlapped local builds and cleared both guards
with adaptive/disabled median ratios of 0.635 and 1.023. Its selective forced
arm had an 11.752 s p95, illustrating run-to-run interference. Its full
receipts remain in the sealed archive. The quiet confirmation used the same
runtime executable and artifact shape after the workspace build and test
commands finished.

| Regime | Mode | Median whole sequence | p95 whole sequence |
| --- | --- | ---: | ---: |
| Selective reuse, eight jobs | Disabled | 2,935.580 ms | 2,943.272 ms |
| Selective reuse, eight jobs | Always | 1,921.270 ms | 1,930.714 ms |
| Selective reuse, eight jobs | Expected value | 1,922.855 ms | 1,943.703 ms |
| No reuse, one job | Disabled | 13.553 ms | 13.764 ms |
| No reuse, one job | Always | 64.571 ms | 67.455 ms |
| No reuse, one job | Expected value | 13.978 ms | 14.318 ms |

The confirmation shows 34.5% lower median time with reuse and 3.1% higher
median time without reuse. All adaptive no-reuse jobs declined before lookup.
The maximum measured job RSS was 18,497,536 bytes in the confirmation.
This is one ARM64 Apple M3 host with 24 GiB RAM and macOS 27.0.1. Core
verification still decodes the original source. The measurements include
the first materialization as well as all later opens, verification, reads,
native suffixes and cleanup, with no claim of host/day repeatability.
The committed [evidence bundle](../evidence/phase26/README.md) includes
the unrounded timing samples and per-job native and byte observations.

## 2026-11-15 policy review qualification

The values above belong to the original Phase 26 source and fixed-horizon
policy. They remain unchanged historical samples. The additive independent
review remediation separates existing-hit value from candidate-write value,
decays incumbent value across remaining TTL, and replaces greedy eviction with
an exact bounded victim subset. Consequently the archived expected-value
timings do not establish the economics of the remediated policy. A new matched
source/executable campaign is required; no current-source timing or release
claim is made. See the [review remediation report](../reports/phase-26-independent-review-remediation-2026-11-15.md).
