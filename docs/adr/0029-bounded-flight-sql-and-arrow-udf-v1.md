# ADR-0029 — Bounded Flight SQL and Arrow batch functions

Accepted design for Phase 22 under Architecture §§5, 7, 8, 17, 20, 22, 23
and 26, particularly INV-1, INV-4 through INV-6, INV-8 and INV-10.
Release qualification is tracked separately.

Keep PlanV1 bytes, inferred properties and digests. A distinct Flight CMD Any
type `ade.gateway/QueryRequestV1` carries strict UTF-8 JSON, version 1,
target local, the registered input table, typed operations, a timeout in
milliseconds and an optional registered function reference. Server binding
uses the catalog, then PlanV1 inference and validation. Decimal strings carry
full-range UInt64 thresholds. JSON rejects duplicate/unknown fields and
out-of-range integers. Arrow fields carry exact names/order/nullability,
non-null UInt64/Int64/Float64/UInt32/UTF-8 and timezone-free epoch milliseconds.
Output order is native operator order. The server hashes the admitted native
plan and function identity separately from the random execution attempt.

An optional native map adapter wraps the verified scan. It has a typed
schema-preserving callback and a cancellation token, and downstream native
operators pull the validated result. One operator-owned Python subprocess
per input batch runs exact verified script bytes with a pinned ABI and no
inherited environment. ABI 1 exchanges a fixed header and one V5
uncompressed Arrow IPC stream. A batch index, identity, and count are echoed.
The input's row IDs and every column except secondary must remain unchanged.
The registry declares the same input/output schema. No automatic retry or
distributed/streaming/cyclic admission. The transform is nonreplayable.
The Python installation and its dependencies are operator-trusted. This is
process supervision for trusted code, with no hostile-code sandbox.

A new default-off gateway owns Flight, process supervision and retention.
Standard statement GetFlightInfo, GetSchema, SQL info and statement DoGet
use Apache Arrow Flight SQL. No general prepared statements or mutations.
A numeric loopback listener requires an operator bearer and a client
session capability. Bounded in-memory Flight frames are fully built before
ticket insertion. Random tickets expire, are session-bound, and repeated
reads reuse the same retained frames. A separate reader cap covers references
outliving expiry. Restart discards all tickets. Native spilling is disabled.

Extending serialized PlanV1 would widen every executor's admission and risk
ambiguity for old readers. Staging to Parquet/IPC would add another write,
rebind, verification, disk ownership and source lifecycle. An in-pipeline map
retains native accounting and avoids the second scan. A Flight-only client
transform would bypass ADE's native downstream execution. Memory retention
avoids filesystem result-publication races; resource exhaustion refuses the
result before ticket insertion. Predecessor qualifications remain independent.
