# Adaptive Dataflow Engine

Adaptive Dataflow Engine (ADE) is a Rust research engine for studying when
dataflow should pipeline, retain, spill or recompute intermediate results. It
runs bounded workloads over Arrow and Parquet and exposes policy decisions and
resource accounting so experiments can compare mechanisms against static controls.

| Area | Implemented scope |
| --- | --- |
| [Benchmarks](benchmarks/README.md) | Deterministic fixtures, an independent correctness oracle and isolated processes. |
| [Local batch and query frontend](docs/phase-11-query-guide.md) | Scans, filters, projections, spilling aggregates and sorting. Bounded SQL and Rust DataFrame lowering. |
| [Distributed batch and local operations](docs/index.md) | Aggregation and inner equijoins on fixed worker processes. Opt-in exchange, retention, single-final-worker recovery and scheduling experiments. |
| [Optional runtimes and integrations](docs/index.md) | Kafka, windowed state and checkpointing, maintained views, graph iteration, connectors, Flight SQL and language clients. |
| [Optional specialization and reuse](docs/index.md) | Trusted Rust extensions, ARM64 filter specialization and cross-job prefix reuse. |

The default paths use static batch execution. Native runtimes depend on the
physical plan contract, while SQL binding and the DataFusion benchmark baseline
sit at the frontend and benchmark boundaries. The opt-in services are scoped
to their documented operators, inputs, failure models and trust assumptions.
Several optional paths await current-source reacceptance. Their support records
are linked from the documentation index.
General SQL, physical worker elasticity and production high availability
are outside the supported envelope.

## Run a local query

Use a recent stable Rust toolchain (the declared minimum is Rust 1.94), Git and
a native linker. This shell example runs on macOS or Linux from the repository
root. The first build downloads and compiles dependencies.

~~~sh
cargo build --locked -p ade-bench -p ade-frontend --bins
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
target/debug/ade-bench data generate synthetic \
  --dataset-id quickstart --rows 1000 --files 2 --row-group-rows 128 \
  --output "$work/events"
target/debug/ade-query execute --dataset "$work/events" \
  --sql 'SELECT key, row_id, value_i64 FROM input ORDER BY key, row_id LIMIT 5'
target/debug/ade-bench validate --engine ade-local \
  --spec benchmarks/specs/aggregate-skew.toml --dataset "$work/events"
~~~

The query prints a deterministic prefix. The validation command checks a native
aggregate against an independent expected result and reports its fingerprint.

## Develop and verify

Use [the documentation index](docs/index.md) for operator guides, code ownership
and current qualification. [Architecture](docs/architecture/architecture.md)
covers runtime boundaries, and [the benchmark guide](benchmarks/README.md)
explains correctness checks and comparable measurements. Historical evidence and
its source identity are indexed under [reports](docs/reports/README.md).

~~~sh
cargo fmt --all --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p 'test_*.py'
cargo build --workspace --locked
bash scripts/smoke.sh
~~~

All-feature builds and process tests have additional native-library and fixture
requirements. See [contributing](CONTRIBUTING.md) and [CI](.github/workflows/ci.yml).
The repository currently has no project license.
