# Phase16 adaptive checkpoint operator guide

This opt-in bounded research path executes the Phase15 native keyed windows with
adaptive cadence, full/changed-shard snapshots, local/S3/both placement, one/two
copies and a guarded pre-output replay/restore choice. It preserves trace-relative
at-least-once output. Compare payload_fingerprint whenever output_id repeats;
ID-only deduplication is unsafe across legal Kafka partition interleavings.

Build `cargo build -p ade-stream --features adaptive-checkpoint --locked` for local
v2, or `--features remote-checkpoint` for S3. On macOS set OPENSSL_DIR and
OPENSSL_ROOT_DIR to the installed OpenSSL3 directory. Default features remain empty;
`--features stateful` and the original fixed v1 CLI/config/files remain supported.
StreamConfigV1, PlanV1, old wire/result/manifest schemas are unchanged.

Use the strict Phase15 config with canonical private0700 working/checkpoint roots,
original starts (never manually replace with saved offsets), and at least262144
checkpoint bytes. Add `--adaptive-policy POLICY.json`. Every policy field is
required; policy<=16KiB, unknown/duplicate fields, unsafe values and incompatible
source identity refuse. A complete local example is [phase16-local-policy.json](../benchmarks/examples/phase16-local-policy.json); adjust original_starts to exactly match your source descriptor. The canonical complete schema is `adaptive::Policy` and
its typed nested structs. A reproducible descriptor generator is
`scripts/phase16/acceptance.py:policy`; its throwaway endpoint/namespace must be
replaced with independently provisioned operator values. There is no arbitrary
SDK/client-property escape hatch. Do not use a production topic/bucket implicitly.

Policy1 fields: mode static/adaptive; min/max_records1..1000000; min/max_ms100..3600000;
max_depth1..4; min_samples2..64; cooldown_ms0..3600000; hysteresis_nano0..10^12;
placements (unique local/remote/both); static_placement; constraints (min_copies1/2,
require_remote, configured local/remote/both/max_loss_ppm and failure_ppm0..1000000,
recovery_slo_ms1..86400000, retention_ms0..86400000); prices (integer configured
nano-USD proxy rates0..10^9); limits; optional remote; explicit recovery descriptor.
Cadence choices use the min or max pair; changed sink-confirmed input is required.
A stationary empty/restored cut never burns generations. Delayed I/O or backpressure
can delay timer observation; these are safe-point thresholds, not hard real-time SLOs.

Cost equation, using ceiling division: write = configured compute_ms price times
estimated canonical scan + physical local/remote write ms, plus physical-byte prices,
remote PUT/HEAD/GET proxy (3 per new shard plus6 manifest/certificate), and logical
bytes * copies * configured retention_ms * capacity price. Score =
ceil(write*1000000/cadence_records) + failure_ppm *
(ceil(cadence_records/2)*replay_record_price + estimated_restore_ms*compute_ms_price).
Hard copy/remote/risk/SLO and measured remaining capacity predicates reject candidates before ranking. Remote candidates conservatively reserve payload plus65560 metadata/probe bytes, changed-or32 shard objects plus4, and PUT/HEAD/GET/full-ancestry request/response envelopes before ranking; mechanism quotas independently veto every operation. Ties use
stable enumeration. Cooldown/hysteresis can retain an admissible previous action.
Invalid/insufficient calibration uses the declared static full control with
**estimated**1000B/s and1ms/record inputs, never fabricated measurements. Arithmetic
overflow or an inadmissible fallback fails closed. `model_inputs` and
`input_provenance` expose the exact measured-versus-estimated distinction.

Measured scan rate is canonical bytes/scan wall duration; local/remote write rates
are conservatively physical shard bytes/whole-cut wall duration (including hashing,
verification and sync overhead). A both-placement cut charges the whole duration
to both tier samples. Native record preparation is measured separately, excluding
source reread, window output, sink/diagnostic wait and native/client overhead.
All source connection, retrieval, full-chain verification, capability, metadata,
cleanup and abandoned-write costs are outside the narrow policy request proxy;
actual remote accounting separately reports every request/reservation. Recovery
latencies and prices are estimates/configuration, never provider billing, an
end-to-end latency prediction or an asserted speedup.

V2 uses separate CURRENT2 and immutable manifests/certificates. A complete32-shard
index binds canonical contents, original offsets, confirmed progress/frontier,
owner inventory, job fingerprint, policy digest, copy floor and generation. An
incremental scans all state and writes only changed shard payloads. References pin
exact immutable generation/size/hash and prior manifest; maximum4 ancestry edges,
periodic full, placement/floor changes force full. Every ancestor/state byte and
semantic invariant verifies before adoption. No hardlinks or v1 migration.
A corrupt CURRENT2 refuses; it never selects a prior generation automatically.

S3 has explicit endpoint, bucket, region, private prefix and unique nonnil namespace.
Only environment AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/optional AWS_SESSION_TOKEN
supply credentials. Never put them into configs, traces, shell history or evidence.
Verified HTTPS is default; HTTP requires dev_allow_http and a literal loopback IP.
No redirects, ambient proxy, metadata credentials, LIST, multipart, automatic write
retry or destructive GC. A real capability probe must reject a divergent second
conditional create and read back the original bytes. Failed/ambiguous PUT poisons;
no retry/adoption. HEAD/GET exact size/digest validates objects independently.

Both required copy closures verify before the immutable remote certificate PUT,
then exact certificate readback, local certificate sync and atomic CURRENT2 replace
plus root sync. Failed/ambiguous publication emits no checkpoint success. After
selector replacement an error/crash may leave a valid whole new selection with no
success; inspect it explicitly. Old generations remain valid. Object existence or
an uploaded orphan certificate is never a latest/recovery selection.

Restore explicitly:

```sh
ade-state-stream state.json --adaptive-policy policy.json --recover --restore-copy local
ade-state-stream state.json --adaptive-policy policy.json --recover --restore-copy remote
```

The selected copy verifies fully; there is no fallback. A required local copy can
restore while S3 is unavailable; new remote-required cuts must still probe/write
all required copies and fail on outage. Restore cannot lower the selected cut's
configured copy/remote/risk floor. Local owner1/2/4 redistribution remains logical,
not physical distributed rescale. Saved offsets are a complete vector; a fresh
UUID fences the session. Live Kafka checks precede assignment/output.

For remote-only disk-loss recovery, retain certificate.json and its exact BLAKE3
from a successful adaptive_decision/current selection outside the lost disk.
Treat the hash as an operator integrity capability, not service-supplied authority:

```sh
ade-state-stream state.json --adaptive-policy policy.json --recover --restore-copy remote \
  --certificate /private/operator/certificate.json --certificate-blake3 RETAINED_HASH
```

Use empty private scratch roots. No local shard or bucket listing is needed. The
remote certificate and every reachable object verify. A missing reservation ledger
marks disk-loss recovery; before writing remotely again choose a **fresh unique
namespace** in the new policy, forcing a self-contained full cut. Never reuse a
namespace after ledger loss/rollback. A copied/rolled-back accounting journal is
not an authoritative budget. The trusted operator fences writers and preserves the
ledger; production multi-writer/disk-loss accounting requires external durable
metadata and is deferred.

Recovery policy pins original_starts and trusted_immutable_history. Full replay
also requires allow_replay, accept_duplicate_and_conflicting_outputs and exact
replay_certificate_blake3 consent to the currently selected cut. Both recovery
estimates are explicitly configured. Verified logical bytes and confirmed records combine with configured read/request/compute/replay prices in a disclosed single-image recovery proxy. Replay only wins with lower estimated cost
and admissible SLO; it starts completely empty state at original offsets, preserving
old selection until a new full cut. Missing original retention or a wrong topic,
cluster, inventory, selector, origin or consent fails before output. To restore a
valid newer cut when original history expired, disable replay and use saved offsets.
There is no group recovery or hidden offset reset. Topic incarnation and immutable
uncompressed<=32KiB history remain trusted operator assumptions; current metadata
cannot prove a topic UUID or undetected truncate-and-regrow.

Limits: one cut/HTTP operation at a time, object<=1MiB, metadata<=32KiB,
remote objects<=8192, attempted write bodies<=256MiB, requests<=65536,
reserved response envelope<=64GiB (1MiB reserved per attempted request),
io_timeout100..5000ms, per-cut remote deadline120s, lifetime trace<=64MiB,
32 shards and<=5 manifests. Remote objects/write bytes/requests are reserved in a
synced atomic REMOTE-LEDGER **before** network effects, including divergent probes,
failures/ambiguous orphans. No refunds. Read-only restore has its own finite
session request/response budget; it cannot create objects. A partial ledger-next
or corrupt ledger refuses until fenced operator investigation. Working hot/cold
limits retain Phase15 accounting; retained+staging checkpoint disk/generation caps
include orphan stages. Charged object/metadata/I/O domains and library/native/OS/
page-cache allocations are separate; no process RSS or hardware power-loss cap.

No automatic GC. Offline, fence/prove the writer dead, retain CURRENT2 plus at
least one independently verified prior certificate and their entire dependency
closures, inventory only exact keys derived from those manifests and recorded
attempts, account late/ambiguous writes and service version retention, then review
an exact deletion list. This software does not issue cleanup DELETEs. Never delete
an ancestor pinned by a selected incremental or treat the lossy working tier as
recovery data. Quota exhaustion is an intentional stop requiring an operator.

Reproduce with Kafka3.9.1/JDK17 and a real separate MinIO binary (the tested pinned
source is RELEASE.2025-09-07T16-13-09Z; source-build module identity is archived):

```sh
python3 scripts/phase16/acceptance.py --kafka-home /path/kafka_2.13-3.9.1 \
  --java-home /path/jdk17 --minio /path/minio --service-python /path/python-with-boto3-and-requests \
  --output /private/tmp/phase16-new-campaign
python3 scripts/phase16/verify_evidence.py --campaign /private/tmp/phase16-new-campaign
python3 scripts/phase16/package_evidence.py --campaign /private/tmp/phase16-new-campaign \
  --destination docs/evidence/phase16-new
```

The harness provisions only its disposable local broker/service and kills only
owned children. Same-host MinIO proves API/process separation, not cross-host/AZ
or disk-loss durability. Physical rescale, production HA, automatic recovery/GC,
transactions/exactly-once, savepoints and general streaming SQL remain deferred.
