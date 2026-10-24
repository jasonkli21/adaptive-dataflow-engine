# Phase 8 unchanged-source stability confirmation — predeclaration

Measured source is frozen at ab7404b1c7095ce3e8b60d79f944a78cd9787c34,
runtime tree432cc7dd212ec41ad6fdec93bc97f957df5a514d. No runtime, matrix,
workload, policy, memory or numeric threshold changes are allowed in this study.

The first listener suite (A) remains FAILED, with all39 children correct but
skew work1.3799937472 and job1.0982230575. It included static producer work of
0.79–2.65seconds versus the preceding candidate's0.17–0.19seconds, and later
returned to normal ranges. We did not capture host telemetry during those
samples and cannot prove the cause. A post-run snapshot found load averages
18.32/51.02/39.29, CPU73.93% idle,24GiB RAM with4.5GiB compressed. This supports
investigating environmental instability; it does not invalidate or erase A.

Before collecting further samples, predeclare exactly two additional full
independent copies of the unchanged39-child matrix (B and C), each retaining
one warmup and three measured iterations, all original seeds/configs/thresholds.
Keep every correctness-valid measured child from A, B and C. There will be no
selection of the better suite or the best iterations. The primary pooled
comparisons use ALL NINE measured samples per arm (three per suite), and must
satisfy the original >=20% skew final-work improvement, no skew job-latency
increase and <=1.30x uniform overhead. In addition, BOTH B and C must each
independently pass all original gates. This is a stronger reproducibility rule,
not a replacement of A's failed outcome. If either B or C fails, do not keep
rerunning unchanged code seeking a lucky pass; leave this confirmation failed.

Before B and before C: no cargo/rustc/ade-bench/ade-worker processes from other
work may be active. Take three host readings10seconds apart; require CPU idle
>=70% each, one-minute load <=logical CPU count, and swap-in/out changes <=1000
16KiB pages between readings. Save all telemetry, including failed readiness
checks; wait for readiness rather than discard measured results. Do not alter
other applications or processes. Run no independent CPU workloads during each
matrix. This local-host readiness rule is newly declared; it was not present in
A or the earlier experiments and cannot retroactively validate them.

Every suite still runs the complete independent-oracle/source/config/layout/
terminal/byte/recovery verifier. The combined verifier must require matching
source/runtime identities,9 samples per arm and39 children per suite, compute
pooled medians from the raw per-suite lists, and publish per-suite plus pooled
outcomes. Keep all negative controls, static_packed attribution, warmups and
4-host confirmation. A Phase8 closure claim is limited to this supported local
workload and these exact reproducibility criteria. No production/cloud or
statistical significance claim follows from the small samples.
