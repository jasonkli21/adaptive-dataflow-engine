# ADR-0032 — Opt-in native CPU filter specialization

## Decision

Use the existing local pull Filter for a bounded little-endian ARM64 NEON
implementation of the unsigned predicate bitmap. Retain the existing Arrow row
gather. Keep the legacy scalar entry points and offer an explicit Arrow public
comparison control. Refuse forced capabilities unless the compiled mechanism and
executing CPU both support them.

Keep `measured-auto` as an explicitly trusted, operator-supplied research hint.
Its current policy compares scalar with NEON only; Arrow is an explicit control,
not an auto-selectable candidate. A profile digest checks file integrity and is
not an issuer signature or evidence that the claimed measurements occurred.
Profile admission rehashes the data and executable on each run, so its cost is
part of the decision. No profile, an incompatible profile or insufficient
evidence selects scalar. The checked-in campaign found no robust whole-query
gain and the policy selected scalar. This is a negative research result, not a
completed measured-economics outcome. The local optimization makes no placement
or recovery decisions.

## Alternatives

Arrow's comparison already vectorizes on the calibration host. It remains a
portable measured control, but the current auto policy does not choose among
all three mechanisms. The native pilot justified evaluating the narrow
specialization, not promoting it or claiming Arrow-aware adaptive economics.
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
