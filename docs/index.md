# ADE documentation

Use the guide and ADR for the behavior you need. Check the qualification link
for the scope and evidence that presently support it. The
[PRD](product/prd.md#22-scope-boundary-core-vs-ecosystem) gives research goals,
and the [architecture invariants](architecture/architecture.md#31-architecture-invariants)
apply across implementations.

| Component | Start here | Code and tests | Qualification |
| --- | --- | --- | --- |
| Benchmark, local execution and queries | [Benchmark guide](../benchmarks/README.md), [query guide](phase-11-query-guide.md), [managed memory ADR](adr/0006-managed-local-memory-and-spill.md), [physical IR ADR](adr/0018-physical-ir-and-frontend-v1.md) | [ade-bench](../crates/ade-bench), [ade-exec](../crates/ade-exec), [ade-physical](../crates/ade-physical), [ade-frontend](../crates/ade-frontend), [native smoke](../scripts/smoke.sh) | [Local query report](reports/phase-11-implementation-report.md) |
| Exchange, replay, scheduling and remote shuffle | [Exchange ADR](adr/0010-static-unified-exchange.md), [recovery ADR](adr/0013-lineage-recovery-v1.md), [lease ADR](adr/0017-morsel-scheduler-v1.md), [S3 shuffle guide](phase-13-operations-guide.md) | [ade-distributed](../crates/ade-distributed), [process smoke](../scripts/smoke.sh) | [Scheduler](reports/phase-10-implementation-report.md), [remote shuffle](reports/phase-13-implementation-report.md) |
| Durable batch jobs | [Operator guide](phase-12-operations-guide.md), [ADR](adr/0019-durable-batch-operations.md) | [ade-ops](../crates/ade-ops) | [Operations limits](reports/phase-12-unsupported.json) |
| Streaming, state and checkpoints | [Kafka](phase-14-streaming-guide.md), [local state](phase-15-state-guide.md), [adaptive checkpoints](phase-16-adaptive-checkpoint-guide.md), [transactions and savepoints](phase-17-transactional-state-guide.md), [transaction ADR](adr/0024-transactional-output-savepoints-v1.md) | [ade-stream](../crates/ade-stream), [Kafka and local state tests](../crates/ade-stream/tests) | [Kafka](reports/phase-14-support.json), [state](reports/phase-15-support.json), [adaptive](reports/phase-16-support.json), [transaction reacceptance](reports/phase-17-reacceptance-2026-10-31.md) |
| Unified planner | [Guide](phase-18-unified-planner-guide.md), [ADR](adr/0025-unified-bounded-unbounded-planning-v1.md) | [ade-logical](../crates/ade-logical), [frontend tests](../crates/ade-frontend/tests) | [Support matrix](reports/phase-18-support.json) |
| Maintained views and graph iteration | [Finite views](phase-19-maintained-dataflows-guide.md), [BFS and components](phase-20-iterative-guide.md), [incremental ADR](adr/0026-incremental-maintained-dataflows-v1.md), [cyclic ADR](adr/0027-cyclic-dataflow-v1.md) | [ade-incremental](../crates/ade-incremental), [ade-iterate](../crates/ade-iterate) | [Views](reports/phase-19-support.json), [iteration](reports/phase-20-support.json) |
| Connectors | [Guide](phase-21-connectors-guide.md), [ADR](adr/0028-connector-catalog-v1.md) | [ade-connectors](../crates/ade-connectors), [campaign](../scripts/phase21) | [Support matrix](reports/phase-21-support.json) |
| Flight SQL and registered Arrow transforms | [Guide](phase-22-multilanguage-udf-guide.md), [ADR](adr/0029-bounded-flight-sql-and-arrow-udf-v1.md) | [ade-gateway](../crates/ade-gateway), [Python](../clients/python), [Java](../clients/java), [interop](../scripts/phase22) | [Support matrix](reports/phase-22-support.json) |
| Local resource management | [Guide](phase-23-resource-manager-guide.md), [ADR](adr/0030-bounded-local-multitenant-resource-manager-v1.md) | [ade-resource](../crates/ade-resource), [cohort campaign](../scripts/phase23) | [Support matrix](reports/phase-23-support.json) |
| Trusted Rust extensions | [Guide](phase-24-extensions-guide.md), [ADR](adr/0031-specialized-extension-sdk-v1.md) | [ade-ext-sdk](../crates/ade-ext-sdk), [ade-ext-vector](../crates/ade-ext-vector), [ade-ext-graph](../crates/ade-ext-graph), [standalone consumer](../examples/phase24-external-consumer) | [Support matrix](reports/phase-24-support.json) |
| ARM64 filter kernel | [Guide](phase-25-hardware-guide.md), [ADR](adr/0032-measured-cpu-specialization-v1.md) | [ade-exec kernels](../crates/ade-exec/src/filter_kernel), [standalone consumer](../examples/phase25-external-consumer) | [Support matrix](reports/phase-25-support.json) |
| Shared materialization | [Guide](phase-26-shared-materialization-guide.md), [ADR](adr/0033-cross-job-shared-materialization-v1.md) | [ade-cache](../crates/ade-cache), [process and detached checks](../scripts/phase26) | [Support matrix](reports/phase-26-support.json), [repo-wide remediation](reports/repo-wide-consolidated-findings.md) |

## Evidence

[Plans](plans) specify scoped deliverables and predeclared gates.
[Reports](reports/README.md) record historical or additive results against
specific snapshots. [Evidence](evidence) holds immutable samples, fixtures,
executables, independent verifiers and pins. A historical pass or failure
applies to its named source and fixture. Use the current support record before
claiming the same status for a later checkout.

For new evidence, keep a compact source and executable inventory, exact
commands, criteria, all required samples (including failures), detached
integrity pins and verification instructions. Prefer durable externally stored
large bundles when their access and pins can be verified by another reader.
Keep disposable scratch runs outside Git. Retain all older source-bound
records and archive bytes when updating a guide or recording a new result.

The previous [dated authorization and scope record](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/e3ed9a9f24a954c1ce790512730b0e4fb2d8ebbf/AGENTS.md)
is pinned for historical reference. Approval for a phase applies to the
corresponding task and does not carry forward.
