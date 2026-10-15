# Contributing

ADE is currently an early research project. Contributions should preserve phase discipline and make experimental claims reproducible.

Before contributing, read `AGENTS.md`, the PRD, architecture, active phase plan, and relevant ADRs.

For code changes:

- include correctness tests;
- keep benchmark semantics engine-independent;
- do not add future-phase scope without an explicit plan/ADR update;
- record meaningful architectural decisions;
- keep generated datasets and benchmark results out of Git except small canonical examples;
- run formatting, checking, linting, and tests before submitting changes.

Performance changes should include the workload, environment, configuration, and full benchmark result rather than only a headline percentage.
