# AGENTS.md — Development guidance

Start with the user-authorized task and the affected component. Use
[docs/index.md](docs/index.md) to find its operator guide, accepted ADR and
current support record. For a contract or design change, read the relevant
sections of the [PRD](docs/product/prd.md) and
[architecture](docs/architecture/architecture.md), the scoped plan, accepted
ADRs, latest report, then code and tests. For a narrow fix, read the affected
contracts and callers and expand the read when a boundary changes.

## Authority and scope

- Apply PRD scope, architecture invariants, the scoped plan and accepted ADRs.
  An ADR can refine an architecture decision with explicit rationale.
  Updating the design does not extend implementation authorization.
- Current support matrices and operator guides qualify what is supported.
  Code and tests show behavior. Reconcile accidental drift with the appropriate
  source of authority. Dated reports and evidence apply to their pinned inputs.
- Keep phase boundaries and explicit user authorizations. See the docs index
  for current qualifications and past authorization records. New phases,
  physical membership changes and broader recovery require separate scope.
- Honor worktree, commit and delegation instructions for the current task.

## Runtime and evidence invariants

- Preserve static defaults and the dependency spine
  `ade-bench → ade-distributed → ade-exec → ade-physical`, with
  `ade-frontend → ade-physical`. Keep native code frontend-independent and
  preserve versioned PlanV1, manifests, child protocol and result contracts.
- Preserve pure policy/mechanism separation, explicit replayability and
  logical IDs across attempts. Recovery admits the documented single final-worker
  loss with one episode and attempt-safe gather commit. Producer or multiple
  losses, replay mismatch, poisoning or failed recovery cancel and clean up.
- Keep native memory, output, exchange, disk and process budgets explicit.
  A task ledger covers managed state. Error paths should fail closed before
  effects where possible and clean up according to the component's contract.
- Use stable Rust, structured library errors and narrow dependency features.
  Keep I/O and compute ownership clear. Document and test the invariant for
  any justified unsafe code.
- Keep independent correctness oracles and process, recovery, tamper and refusal
  witnesses. Keep all original benchmark criteria, source identities and negative
  results. Use fresh output paths and preserve frozen evidence bytes and pins.
  Record environment, source, executable, config, dataset, workload and samples.
  CI gates correctness. Report measurements only under matching performance criteria.

## Validate and document

Start with a regression at the affected entry point and run focused crate or
script tests. For substantial changes, use the current [CI workflow](.github/workflows/ci.yml)
and the workspace commands in [CONTRIBUTING.md](CONTRIBUTING.md). Keep feature-off,
all-feature, external-consumer and process checks distinct. Match test binaries
to build features. Report failed, blocked and unrun checks with their reasons.

Update the scoped guide for contract changes, the support matrix when backed by
new evidence, and the ADR for durable architecture decisions. Use dated
[reports](docs/reports/README.md) for implementation qualifications, deviations,
raw evidence pointers and remaining gates. Keep dated session details out of
this file.
