# ADR-0028 — Verified staging for bounded connectors

Accepted for the finite batch mechanism on 2026-11-11. This applies Architecture
§§5, 8, 10, 17, 21, 22 and 26. See the Phase 21 report for release qualification.

A separate `ade-connectors` crate owns source contracts, snapshot identity,
transport adapters, private staging and local publication. Its optional batch
integration admits a verified local binding through the existing schema-only
frontend and native PlanV1 executor. The execution crates know only their
original immutable files. Connector and catalog names remain independent of
the physical `input` binding. Runtime configuration and credentials are outside
the semantic snapshot. Pushdown is disabled.

Direct Arrow batches could save a write and read, but would require a new
frontend-to-executor interface for verified replay, resource ownership and
source binding. A remote PlanV2 scan could preserve split locality, but would
move transport and credentials into the physical layer or require a separate
runtime registry. Private staged Parquet reuses native bounded schema, row,
byte and digest checks at the cost of an extra write and rereads. Its lifetime
is the owning handle. No durable cache or optimizer policy is introduced.

Immutable byte pins are mandatory on replayable sources. Format adapters must
verify and interpret pinned metadata before resolving data pins. A Postgres
transaction or Flight ticket alone cannot promise a second identical read.
Kafka remains under ade-stream's identity and progress authority. Phase 18
and Phase 20 retain their existing qualifications and separate evidence.

One local sink key names one immutable Arrow IPC artifact. Publication uses a
same-directory atomic no-replace link after file sync, then directory sync.
The reader verifies its envelope and byte digest. A retry with identical bytes
can confirm an already published object. A different digest for the same key
fails. The operator controls the directory and retains any independently
trusted digest. This contract is local and relies on filesystem link/fsync
semantics. Abandoned temporary files after process death require operator
cleanup. No cross-system transaction is implied.
