# Phase 23 independent review remediation — 2026-11-14

**Current status:** `PARTIAL_PHASE23_INDEPENDENT_REVIEW_REMEDIATION_PENDING_REACCEPTANCE`.
Candidate 11, its report, sealed archive, source inventory and raw samples remain
historical and unchanged. The support matrix now marks G0–G17 partial until the
remediated clean commit is matched to a fresh campaign and its required proofs.

The attached independent review found correctness defects and gaps in the
release verifier. This remediation fixes the two direct scheduler defects and
hardens source/CI binding and attempt receipt validation. It does not claim the
full independent live reacceptance demanded by G0–G17.

## Changes in this remediation

- Adaptive aging promotes a queued job at `min(configured_aging_ms,
  deadline_ms - 1)`. The original absolute deadline remains unchanged. This is
  progress protection for jobs that remain eligible; impossible quotas, source
  drift, unserviceable duration and unavailable cohorts remain outside a
  guarantee.
- Submission returns `NATIVE_HISTORY_FULL` when every cohort permitted by the
  configured policy has reached its immutable native registration limit. A
  previously queued job that becomes impossible is terminally failed with the
  same reason before ordinary deadline expiry. No native history is recycled.
- The checksummed ledger now carries a bounded operator-only decision receipt
  list for dispatch, expiry, permanent history refusal, preemption and standby
  transitions. Receipts bind policy version and config digest and include a
  bounded queue/work snapshot. The old empty-ledger encoding remains unchanged.
  Admission denial, all deferred quota comparisons, settlement and quarantine
  are not yet covered; G13 remains partial.
- Current-source verification checks a separately supplied receipt file against
  an out-of-archive SHA-256 pin. It requires the exact final commit, workflow,
  successful completed `rust`, `msrv`, `phase22-interop` and `phase23-local`
  checks. It also binds the candidate SHA and Git tree to a clean checkout and
  compares the campaign inventory across source, criteria and release documents.
  Historical-archive mode remains a separate integrity/semantic check and does
  not establish final-release acceptance.
- Attempt accounting is recomputed for terminal and cancellation receipts as
  well as successful results. Crash evidence binds its logical job ID and
  rejects nonterminal outcomes. The tamper suite now includes coordinated
  release-claim, cancellation-charge, crash-identity and impossible-transition
  mutations.

The CI receipt document's detached pin is a trust input supplied independently
of the candidate archive. The verifier validates its exact schema, SHA, required
job inventory and successful conclusions; the operator must obtain and pin the
receipt from the trusted repository CI service. A self-authored archive hash
does not establish CI provenance.

## Reacceptance state

The previous green hosted checks belong to `a639a93ddb411525ad8cdbc124ca8f3eed97362f`.
They do not attest this remediation commit. The implementation-session campaign
and candidate 11 measurements also remain historical. The following work is
still required before Phase 23 can be accepted:

- Run the full live manager/worker campaign from a clean checkout after this
  commit, using feature-matched executables and immutable independent fixtures.
- Complete adversarial source-swap, peer-cap/load, exact quota boundaries,
  permanent-history restart, sustained-arrival fairness, fault-stage and
  non-success accounting witnesses. Independently inspect all recorded outputs.
- Complete durable decision coverage and offline reconstruction for admission,
  defer, settlement and quarantine, or keep those criteria explicitly partial.
- Run disposable-volume ENOSPC on a supported host and the local workspace,
  MSRV and smoke gates on this exact commit.
- Obtain a separate exact-SHA CI receipt and detached pin after all required
  checks complete. No final-SHA hosted result is claimed here.

One focused live witness did complete: the updated `history` case ran against
the remediation manager with four supervised native workers and observed 32
successful FIFO registrations followed by immediate `NATIVE_HISTORY_FULL`
refusal. Its raw event is preserved at
[`native-history-witness.json`](../evidence/phase23-review-remediation/native-history-witness.json).
The manager binary was built from the changed Rust sources, but the checkout was
dirty and this history slice is not a sealed source-matched campaign; G3/G5
remain partial. A separate attempt to combine native and logical history
exhaustion in one queue was abandoned because native-history refusal correctly
takes effect before a 64-job logical-history cap can be reached.

The original source and evidence are not rewritten. Phase 18, 20, 21 and 22
qualifications remain unchanged. No Phase 24 authorization is inferred.

## Checks run for this remediation

- Direct stable `rustfmt --check` on the four changed Rust source files: pass.
- `cargo check --workspace --all-targets --offline --locked`: pass.
- `cargo test -p ade-resource --all-features --offline --locked`: 6 passed.
- `cargo test -p ade-resource --no-default-features --offline --locked`: 6 passed.
- `cargo clippy -p ade-resource --all-targets --all-features --offline --locked -- -D warnings`: pass.
- `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p 'test_*.py'`: 76 passed.
- Workspace all-feature Clippy was attempted, but the environment lacks `cmake`
  needed by the `rdkafka-sys` build script. The normal `cargo fmt` wrapper also
  could not complete because rustup tried to remove its protected
  `/Users/jasonkli/.cargo/bin/rustup-init` setup file; direct formatting and
  check-mode on the modified Rust files passed.
