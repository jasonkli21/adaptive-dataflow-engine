# ADR-0023 — Bounded adaptive checkpoints and placement v1

Status: Accepted for implementation, 2026-10-29. User authorization: implement
Phase16 in this session without subagents, one local commit, no push.

Refines architecture §§14–15/18 and INV1–10, implementing all five PRD Phase16
choices within the fixed Phase15 keyed-window/sink-confirmed cut. No Phase17,
physical rescale, automatic recovery, cloud/HA or transactional sink is inferred.
The preparation base is a89f1ae; the preexisting untracked Phase16 plan is adopted.

## Decision / assumption matrix (G0)

| Question | Decision / authority |
|---|---|
| Compatibility | Default features, strict Config/StreamConfigV1, v1 files and fixed CLI remain. New default-off adaptive-checkpoint and remote-checkpoint features; --adaptive-policy selects v2. No implicit migration; fresh v2 roots only. |
| Policy/mechanism | Pure deterministic checked integer enumeration; hard durability/risk/SLO/capacity constraints precede price. Bounded last observation/previous choice, sample minimum, cooldown/margin; safe declared static full fallback. Prices/risk/retention are configured, throughput/latency measured, recovery/cost estimated. |
| Cut | Existing demand owner pauses/verifies, completes ACK/closures; no partition data polls during I/O. Existing owned control pump survives. Failure poisons; diagnostic hooks precede publication. |
| Incremental | Canonical scan all32 shards; write only differing contents. Full logical index references exact immutable object generation/hash/size, bounded prior manifest chain (<=4). Placement change and maximum depth force full. No hardlinks, dirty-bit proofs or GC. |
| Selector | CURRENT2 is a strict hash-bound certificate selector, separate from CURRENT v1. Immutable v2 manifests and certificate, fsync/atomic local selection; remote certificate conditional PUT/HEAD/GET only after all required copies verify. No remote rename/latest/LIST. |
| Disk-loss restore | Operator retains the exact certificate bytes AND BLAKE3 from successful publication; explicit --certificate/--certificate-blake3 with empty local scratch. The hash is an operator capability, not an untrusted service assertion. Remote objects alone confer no eligibility. |
| Replication | Local, remote, both. Every required copy's complete reference closure verifies before certificate. Restore selects local or remote explicitly, never falls back; new policy cannot lower selected copy count/risk floor. Same-host service proves API/process isolation only. |
| Replay | Pre-output --recover may choose full replay only with trusted history attestation, original starts pinned separately, exact selected certificate consent, duplicate/conflict consent and fresh empty state. Always verify a selected generation first; never bypass corrupt CURRENT2. Keep old selection until next whole full cut. New UUID and explicit trace. |
| Bounds/orphans | Root byte/generation cap includes stages; durable append-before-request remote reservation journal charges attempted writes/requests/bodies, including failed/ambiguous orphans across restarts. New scratch remote writer requires new private budget namespace; disk-loss restore is read-only until a fresh namespace/full cut. No destructive automatic cleanup. |
| Source | Trusted immutable uncompressed<=32KiB Kafka topic incarnation is an explicit attestation; live cluster/inventory/retained range checks precede assignment/output. Kafka public metadata here cannot attest topic UUID or undetected truncate/regrow. |
| Delivery | Trace-relative at least once, preserving payload_fingerprint. Different legal partition merges can yield conflicting same-ID payloads; consent does not reconcile them. |

An S3 PUT timeout is ambiguous: abort/poison without retry/adoption, retain its
reservation. Exact HEAD/GET lengths and digest bind every object. Explicit
endpoint/bucket/region/prefix, environment-only credentials, signed requests,
verified TLS (literal-loopback HTTP opt-in only), no proxy/redirect/metadata
credentials, zero SDK retry, bounded response/error body, owned serial operation
with cancellation. Security patterns are isolated copies of Phase13 concepts;
stream does not depend on distributed or frontend.

Legacy v1 remains explicit old CLI only; v2 refuses CURRENT and old readers refuse
v2 CURRENT2 roots (no CURRENT to discover). No migration is claimed. Full manifests
reset dependency closure; previous generations stay immutable. Offline cleanup
requires a fenced dead writer, exact keys and retained current/prior closure.
Costs are a declared proxy, never provider billing or a throughput promise.

Validation obligations are G0–G12 in the active plan and handoff. Same-session
self-audit plus independent Python contract arithmetic/state oracle replaces no
separate reviewer: the user explicitly forbids subagents. All failed candidates
and historical archives retain their identities. Completion requires real Kafka
and real separately running S3 storage, not a filesystem/mock backend.

The required workspace gate twice reproduced the historical Phase12 contention
fixture's OWNER_BUSY startup race. Its test now retries only that existing
pre-hash refusal for at most2s with the same idempotency key/recipe. The8s prepared
pause, >=5s actual hash witness, overlap and succeeded/not-cancelled assertions
remain. Production operations code and contracts are unchanged; the failed runs
are retained, and no historical result is reclassified or HA debt closed.

## Independent review follow-up — 2026-10-30

The independent review of `2662c8d` identified two bounded contract gaps. Recovery
now fully verifies the selected checkpoint first, then obtains read-only live
metadata and low/high checks for both the original replay offsets and the saved
checkpoint offsets before cost ranking. An expired replay origin is excluded from
the candidate set; an eligible saved vector can select restore with an explicit
refusal reason for replay. If neither path is eligible, recovery fails before
assignment/output and reports both candidate reasons. `KafkaSource::connect`
rechecks the chosen vector immediately before assignment. These metadata checks
do not attest topic UUID or immutable history; the operator attestation remains
required.

Snapshot policy now determines exact placement/ancestry reuse eligibility before
enumeration. Only executable full/incremental actions receive scores. A placement
or durability-floor transition that requires full state is evaluated and capacity
checked as full before local staging or remote capability probes. The decision
trace records the reusable-placement set and candidates rejected because a
self-contained generation is required. The first successful cut after replay is
also forced full even if replay has already advanced beyond the previous record
count.

The original Phase16 acceptance archive, source inventory, failed candidates and
measurements remain immutable evidence for `2662c8d`. The additive review report
records this source's local verification and the live-service gates that were not
rerun; the prior archive is not post-fix acceptance evidence.
