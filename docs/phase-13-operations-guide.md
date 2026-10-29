# Phase 13 remote-object batch v1

This is an explicit experimental static aggregate API/CLI, separate from
`ade-batch`. It uses a signed S3-compatible provider, immutable Parquet inputs,
2–4 fixed logical producer IDs and hash1 partitions. All producers exit before
fresh readers start. Reader width may be1..N, with disjoint sequential ownership
of the same N partitions. There is no live worker replacement or coordinator
restart/adoption. Defaults, legacy exchange modes and durable local operations
remain available under their existing contracts.

## Build and configure

```bash
cargo build --workspace --features ade-distributed/remote-s3 --locked
# Use the same worker build as the coordinator; all-feature tests use all-feature workers.
# Credentials are supplied only through this process's environment/provider creation:
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
# Optional AWS_SESSION_TOKEN is supported; never put credentials in RUN.json.
```

Provision a private bucket yourself on an explicitly authorized endpoint. ADE
creates/deletes only attempt objects, never buckets, IAM or lifecycle policies.
TLS and normal certificate validation are required. Explicit dev HTTP accepts
literal loopback IPs only. No redirects, ambient HTTP proxy, unsigned fallback
or worker-local filesystem fallback is permitted. The provider must support
conditional create and GET-after-PUT; every run probes a divergent second create
and verifies the original object. Raw provider diagnostics are redacted.

Use a manifest-listed `synthetic-events-v1` dataset. The config helper uses the
existing frontend only to compile the fixed query; core execution remains
frontend/DataFusion independent. It removes only an exact composed identity projection,
then native admission validates the resulting complete IR.

```bash
python3 scripts/phase13-config.py \
  --query-binary target/debug/ade-query --worker-binary target/debug/ade-worker \
  --dataset /absolute/dataset --scratch /absolute/new-private-scratch \
  --endpoint https://s3-compatible.example --bucket private-bucket \
  --prefix private-experiment --region us-east-1 --workers 4 --readers 2 \
  --output /absolute/new-run.json

target/debug/ade-remote /absolute/new-run.json             # cleanup is dry-run
# For a separate fresh run with explicit cleanup after all readers are reaped:
target/debug/ade-remote /absolute/new-run.json --apply-gc
```

Every invocation gets a fresh execution UUID; reusing the config is a new run,
not recovery of old objects. The native Rust entrypoint is
`ade_distributed::remote::execute(&Run, CancellationToken)`. It returns bounded,
fully verified Arrow batches, a receipt and a private quiescence capability.
The CLI returns receipt/rowcount/cleanup diagnostics; it does not implement a
new external sink. No success JSON is emitted on failure. The synchronous
`execute_observed` events are supervision hooks; observer errors abort the run.

Default limits per actor:256 total producer segments,16MiB cumulative attempted
object bodies,272 attempted creates,4096 requests,5s request/120s whole-run
budget and1 bounded read retry (maximum2). Frames<=1024rows/1MiB; JSON<=1MiB;
individual transfer terminal<=64KiB; all transfer metadata<=1MiB. Task state32MiB,
operator spill disabled; gather serialized writes<=64MiB and IR output caps<=
1m rows/64MiB accounted Arrow buffers. Sources retain IR1's64files/1mrows/64MiB,
1MiB footer and16MiB decoded row-group caps. At most4 worker requests concurrently;
one request per actor. HTTP response bodies<=1MiB each/64MiB per actor, including
SDK error responses. Queues, codec/network/Parquet transients and the local
ledger are distinct; none is a process RSS/free-disk or physical network cap.
Cooperative cancellation kills/reaps owned children; OS syscalls/teardown retain
normal non-preemptible system-call limitations.

## Failure, retention and cleanup

Eligibility requires the coordinator-pinned global manifest and exact terminal
set. The store's LIST results never authorize data. Readers verify hashes,
sequence/count/schema, positive counts, hash destination, context/attempt and
complete transfer chains before a successful final terminal. Gather commits the
complete final set and exact fingerprints before any result returns. Input files
and manifest hash again before success; immutable-file assumptions remain.

`ADE_REMOTE_ATTEMPT UUID` on stderr identifies an admitted private namespace.
Before global publication, after a producer/reader crash, timeout, cancellation,
source drift or corrupt data, no successful result returns. Failed/crashed runs
are not automatically retried or resumed. The service may finish a previously
received PUT after cancellation. Such an object is an orphan: there is no new
reader grant/global eligibility or successful output. A lost service can make
all work unavailable; no fallback or cloud durability is implied.

`Output::cleanup(false)` lists exact verified private keys and changes nothing.
`cleanup(true)` requires the private capability returned only after every owned
reader has exited/reaped. Changing display/serialized receipt metadata cannot
retarget that capability. Apply checks its request cap before deletion, removes
only recorded keys and is idempotent after successful completion. Partial delete
failure leaves debt and no false cleanup success. The CLI exposes no arbitrary
receipt/prefix/bucket deletion command and no live-consumer GC.

For failed/crashed orphans, preserve the execution UUID, source/config, service
trace and any manifests. Fence/stop all actors, establish provider I/O quiescence
or retention expiry, then have an operator remove only that validated private
execution's exact keys. There is deliberately no force-adopt/broad-prefix GC.
Versioned stores may retain old versions/delete markers; physical version
reclamation belongs to the bucket operator. No background TTL is installed.

## Reproduce the private fixture and gates

The acceptance fixture uses a separately running Moto5.1.16 service, reached
through `object_store`'s real signed S3 HTTP API. It proves process/storage
separation, not production authentication, host/AZ durability or HA. Use a fresh
private virtual environment and throwaway fixture credentials only:

```bash
python3 -m venv /private/tmp/ade-remote-fixture
/private/tmp/ade-remote-fixture/bin/pip install 'moto[server]==5.1.16' 'requests==2.32.5'
/private/tmp/ade-remote-fixture/bin/moto_server -H 127.0.0.1 -p 18913
# In another terminal, create only the fixture bucket with throwaway fixture keys:
/private/tmp/ade-remote-fixture/bin/python - <<'PYTHON'
import boto3
boto3.client('s3', endpoint_url='http://127.0.0.1:18913', region_name='us-east-1',
             aws_access_key_id='ade-fixture', aws_secret_access_key='ade-fixture-secret')\
    .create_bucket(Bucket='ade-phase13-test')
PYTHON
# Then start the bounded fault/trace proxy:
/private/tmp/ade-remote-fixture/bin/python scripts/phase13-service.py \
  --backend http://127.0.0.1:18913 --port 18914 --trace /absolute/requests.jsonl
unset AWS_SESSION_TOKEN
export AWS_ACCESS_KEY_ID=ade-fixture AWS_SECRET_ACCESS_KEY=ade-fixture-secret
export ADE_REMOTE_ENDPOINT=http://127.0.0.1:18914 ADE_REMOTE_BACKEND=http://127.0.0.1:18913
cargo build --workspace --all-features --locked
cargo test -p ade-distributed --all-features --locked --lib remote::
cargo test -p ade-distributed --all-features --locked --test remote -- \
  --ignored --nocapture --test-threads=1 --skip zz_real_service_restart
```

The separately selected `zz_real_service_restart_refuses_lost_intermediates`
test requires `ADE_REMOTE_SERVICE_PID` identifying YOUR private Moto process and
`ADE_MOTO_BIN` pointing to that pinned fixture executable. Run it last: it stops
that process, starts a fresh empty service and proves loss of committed remote
objects refuses output; it then stops the replacement. Never set a production
PID/endpoint. Default CI skips all external-service/process acceptance tests.

The source-bound paired experiment is declared in
[phase-13-experiment.md](research/phase-13-experiment.md). Set
`ADE_PHASE13_EVIDENCE` to a fresh ignored output directory and `ADE_REMOTE_WORKER`
to the matching absolute worker path, then run:

```bash
cargo test -p ade-bench --features remote-s3 --locked --test phase13 -- \
  --ignored --nocapture --test-threads=1
python3 scripts/phase13-verify.py /absolute/campaign/samples.json \
  /absolute/requests.jsonl --native-verifier /absolute/ade-remote \
  --output /absolute/verification.json
```

The verifier requires all32 warmup/measured runs and independently observed HTTP
requests/bytes, reconstructs configured cost, checks fresh cohorts and invokes
native receipt/source/config/feature verification. SHA256 inventories supply
source/binary/raw-log provenance; this is not signed attestation.
`ade-remote --verify-receipt` is offline structural/identity/cost verification,
not output reexecution or provider-billed verification. Preserve refused runs.

Prices and placement labels are CONFIGURED, counters/latencies MEASURED; nothing
is PROVIDER_BILLED. Object-body and admitted HTTP-body counters have distinct
scopes. Delete uses one-key S3 DeleteObjects POST, one request per key. Headers,
TLS and rejected/dropped transport bytes are not a provider traffic bill.
There is no automatic remote/local selector, real measured cross-AZ cost,
serverless runtime, remote-source admission, join, `ade-batch` remote flag or
production HA claim.
