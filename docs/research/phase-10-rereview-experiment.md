# Phase 10 supplementary attribution and confirmation — 2026-10-26

Predeclared before any new timing. Authority: remediation re-review F1–F4.
Historical source ee9f8da and the original 13-arm release remain immutable.

First replay archive SHA256
850b166a48b062558f32c617ce59f536d9081ab349d478ad6e8317a23b823877,
all 733 inventory entries, and each release with its own source/binaries. Do not
write verification.json into the historical archive. Retain FAILED and PASSED.

Freeze a clean source snapshot (including this declaration), Cargo.lock,
toolchain and binaries before measurement. The final single repository commit
may add documentation; its runtime/scripts must match the snapshot. Preserve the
snapshot in a Git bundle and source tar, without adding a second branch commit.

One complete matched campaign, no best-of repeats: 2 then 4 workers, imbalanced
then uniform; within each group legacy static, static_projected, leased_no_steal,
leased_v1 in that order. 16 arms; one warmup each, five measured children at two
workers, three at four: 80 children / 64 measured. The independent DataFusion
oracle, full dataset verification, manifest bytes, source files, resource/lease
limits, spec, timing and sidecar boundaries are unchanged. Reuse the exact frozen
4m-row/32-file manifests from release-2-ready-3. Match the original idle/load,
swap and process checks, intervals and postflight. Keep every readiness attempt;
a refusal is UNVERIFIED with zero measurements. Do not retry a completed FAIL.

Static_projected uses fixed modulo ownership with the shared projected per-file
operator, first-footer reuse, full content passes, raw checked contributions,
lazy spill, scoped hashing and packing. Lease service/ACK and typed transfer
fencing are present only in leased controls. Leased_no_steal versus leased_v1
matches those mechanisms too, differing only in assignment policy. This paired
control is necessary to isolate policy; static_projected also tests whether
optimized static ownership erases the end-to-end advantage. Report actual
preference/owner traces, all query and last-producer samples, medians/ranges and
per-worker source/decode/hash/control/routing/send/final-merge wall components.
Never sum parallel workers or add overlapping producer/final components into
query latency or claim these timers measure CPU.

Evaluate the same six numerical comparisons against static_projected separately:
imbalanced job/tail <=0.90, uniform job <=1.15 at EACH worker count. Report all six
legacy-static ratios too. Report leased_v1/no_steal ratios without inventing a
new threshold. Any missing correctness/source/lease/terminal/readiness evidence
is UNVERIFIED, never a timing pass. Any complete failed ratio remains FAIL.
Scheduler-attributable general gain is not established if the optimized-static
comparisons fail, regardless of the historical original PASS.

After historical replay and matched evaluation, perform ONE complete original
13-arm confirmation at the same clean source/binaries, fresh session/readiness,
unchanged 62 children/49 measured, three reached negatives and six original
thresholds. Reuse exact original manifests; no timing/lease/grace/resource tuning.
Keep any zero-measurement attempts and every measured sample. No replacement of
the historical result, no best-of campaign selection. This is a same-host local
confirmation on a fresh idle session; an independent host/day replication is
preferred but unavailable within this session and must remain explicitly
unverified. Report robustness honestly even if confirmation FAILS. Retain the
original declaration alongside this supplement in the final raw bundle.

Operating envelope: trusted loopback peers and fixed 2–4 processes on a host
that can schedule independent renewals before lease expiry. The admitted 80ms
minimum lease is a fail-closed boundary, not a CPU-starvation availability SLA.
Existing real-process disabled-renewal expiry/death/notice tests must still
return no output, cancel surviving tasks and remove owned temporary storage.
Preserve the two historical compiler-contention expiry failures. No guarantee
against multiple hostile peers, sustained CPU contention, spot interruption or
physical worker loss/replacement is made. Do not widen limits to pass timings.
CI remains correctness-only; no Phase12 authorization follows.
