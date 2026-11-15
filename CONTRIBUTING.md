# Contributing

Start with [the component map](docs/index.md) and its supported workload,
operator guide and tests. Keep patches scoped to one behavior or invariant.
Explain the observed failure, how to reproduce it and which feature combinations
are affected. Add or update documentation when behavior, limits or operational
assumptions change. Record architecture decisions in [ADRs](docs/adr/README.md).

## Development

The Rust workspace declares Rust 1.94 and uses stable with rustfmt and Clippy
for everyday work. Use `Cargo.lock` and follow the native dependency installs
in [CI](.github/workflows/ci.yml) for the enabled features. Python verifier
unit tests use the standard library. Campaigns may require an isolated
Python 3.10+ environment with pinned packages, and Flight SQL interop also
uses Java 17 and Maven.

~~~sh
cargo fmt --all --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p 'test_*.py'
~~~

For a focused change, start with the affected crate's tests. For changes to an
admission or failure path, exercise the nearest real entry point and check both
the expected refusal and a valid control. Keep independent oracles, fault and
tamper tests distinct. Build the appropriate workers before process tests. Run
`cargo build --workspace --locked` before `bash scripts/smoke.sh`. The CI
workflow lists the external consumers and native process campaigns. Use
disposable services and output directories for live or fault tests.

When reporting research results, record exact source, binaries, fixture and
environment identities, raw samples and external pins. See [evidence management](docs/index.md#evidence).
Hosted CI and scoped live or economic campaigns answer separate questions.
List unavailable checks and the prerequisites needed to run them.

## Provenance and private reports

The repository has no project license. Identify the origin and permissions for
any third-party material before proposing it, including redistributable
executables, datasets and fixture bundles. Contact a maintainer privately about
security-sensitive reports and keep credentials or sensitive data out of reports
and test fixtures.
