# Bounded ADE queries (physical IR v1)

Build with `cargo build --workspace`. Generate inputs with the existing lab:

```bash
target/debug/ade-bench data generate synthetic --dataset-id query-example --rows 1000 --files 2 --row-group-rows 128 --output /tmp/ade-query-events
target/debug/ade-query explain --dataset /tmp/ade-query-events --sql 'SELECT key, value_i64 FROM input WHERE secondary < 25'
target/debug/ade-query execute --dataset /tmp/ade-query-events --sql 'SELECT key, row_id, value_i64 FROM input ORDER BY key, row_id LIMIT 100'
target/debug/ade-query execute --dataset /tmp/ade-query-events --target distributed-aggregate --worker-bin target/debug/ade-worker --sql 'SELECT key, SUM(value_i64) AS sum, COUNT(*) AS count FROM input GROUP BY key'
target/debug/ade-bench data generate join --dataset-id query-join --left-rows 100 --right-rows 10 --left-key-domain 20 --right-key-domain 20 --output /tmp/ade-query-join
target/debug/ade-query execute --dataset /tmp/ade-query-join --target distributed-join --worker-bin target/debug/ade-worker --sql 'SELECT l.key, l.value_i64 AS left_value, r.value_i64 AS right_value FROM ade_left l INNER JOIN ade_right r ON l.key=r.key'
```

`plan-sql` and `explain` print the validated graph, source identities, native
target, semantic distribution, version and plan hash. `execute` prints diagnostics
and rows only after success and source postflight. The CLI default output cap is
1000 rows/16MiB; use `--max-output-rows`/`--max-output-bytes` within v1 ceilings.
A cap overflow is a failure, never silent truncation. Use sorted SQL LIMIT for a
prefix. `--manifest` selects an explicit manifest, `--sql-file` reads bounded SQL,
`--memory-limit-bytes`, `--timeout-ms` and `--workers` set native controls.
Execution performs no output file writes. Sources must remain immutable.

The Rust API has the same compilation and native execution path:

```rust,ignore
use ade_frontend::{Catalog, DataFrame, Target, execution::Options};
let mut catalog = Catalog::default();
catalog.register_manifest("input", root, &root.join("manifest.json"))?;
let plan = DataFrame::scan(&catalog, "input", Target::Local)?
    .filter_lt("secondary", 25)?
    .select(&[("key", "k"), ("value_i64", "v")])?
    .build()?;
let output = catalog.execute(&plan, Options::default())?;
let batches = output.batches();
// SQL alternative: catalog.plan_sql(sql, Target::Local).await?
```

`group_sum_count`, `sort_key_row_id`, `limit` and `output_cap` are also available.
`PlanV1::from_json` validates strict serialized plans; catalog binding independently
rejects forged identities. Native callers can invoke `ade_exec::physical::execute`
or `ade_distributed::physical::execute` without linking the frontend. The old
LocalPlan and distributed job APIs remain available.

| Target | Admitted shape | Explicit refusals |
|---|---|---|
| local | Events scan, selected columns/aliases, secondary < nonnegative UInt64 literal, one UInt64 grouping key + SUM(value_i64)/COUNT(*), key,row_id ascending sort and optional deterministic LIMIT | Expressions, casts/coercions outside exact integer literals, nullable inputs, unordered LIMIT, arbitrary sorts; sorted SELECT must contain exactly key,row_id,value_i64 and original names |
| distributed-aggregate | Unfiltered input scan → original key SUM(value_i64)/COUNT(*) → projection/aliases | Upstream filters/projections, downstream filters/sorts/limits, leased scheduler, sampled topology, non-fixed partials |
| distributed-join | Original ade_left/right scans → UInt64 key inner equijoin → key/left/right value projection/aliases | Local join, reverse source order, USING/outer/non-equi joins, extra predicates, arbitrary joined DAGs, recovery |

Aggregates and joins are bags; duplicate join pairs remain duplicated. SUM uses
checked i128 state and rejects final Int64 overflow. SQL COUNT is checked into
Int64; its source bound makes count overflow unreachable. SUM output schema is
nullable as required by SQL, although admitted groups have non-null values.
Sort requires strictly increasing source row_id, checked at preflight; reordered
selected columns are supported. General SQL, Python, streaming, writes and HA
remain out of scope. There is no DataFusion execution fallback.

Source ceilings are 64 files/1m rows/64MiB per registered table; metadata/footer
<=1MiB and declared uncompressed row group<=16MiB. Plans have <=64 nodes and
256KiB canonical serialization; SQL<=16KiB. Output ceiling is 1m rows/64MiB of
accounted Arrow buffers plus batch metadata. Managed task memory, exchange queues
and decoder/allocator transients are separate; no total RSS cap is promised.
Registration and execution verify all listed files; extra directory files cannot
enter a query. Source identities exclude absolute dataset-root paths.

Phase 10 remains PARTIAL with its six frozen timing failures and fixed physical
membership. Phase 11 makes no performance claim and does not change those gates.
