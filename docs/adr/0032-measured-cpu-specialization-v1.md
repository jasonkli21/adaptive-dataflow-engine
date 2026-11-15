# ADR-0032 — Opt-in native CPU filter specialization

## Decision

Use the existing local pull Filter for a bounded little-endian ARM64 NEON
implementation of the unsigned predicate bitmap. Retain the existing Arrow row
gather. Keep the legacy scalar entry points and offer an explicit Arrow public
comparison control. Refuse forced capabilities unless the compiled mechanism and
executing CPU both support them.

Use exact shape calibration from paired native child observations. Bind trusted
profiles to verified dataset bytes, workload, executable and observable CPU class.
No profile, an incompatible profile or insufficient evidence selects scalar. The
local optimization makes no placement or recovery decisions.

## Alternatives

Arrow's comparison already vectorizes on the calibration host. It remains a
portable control and the reference for how much extra value the explicit packed
mask provides. The native pilot justified evaluating the narrow specialization.
A standalone engine or extension bypass would leave native ownership unproven.
There is no measured crossover justifying GPU transfer, executable code
generation, arbitrary fusion or scheduler changes.

## Architecture impact

Sections 7–8 and INV-3/4 constrain capability admission, managed scratch and
bounded batch work. Sections 14–15 and INV-5/9 keep selection auditable and
optional. The filter preserves logical identity and replayability under INV-1/8.
Arrow reuse and the native dependency direction preserve INV-6/10. Runtime
options take no part in PlanV1, logical dataset IDs or result hashes. The
managed reservation covers ADE-controlled bytes. Arrow allocator activity and
child process RSS are separate measurements.
