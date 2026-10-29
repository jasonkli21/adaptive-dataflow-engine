# Phase 13 bounded remote experiment — declared before measurement

2026-10-28. Compare the existing local-materialized static aggregate with the
explicit remote_object path on exactly the same generated Parquet files, IR,
32MiB task budget, disabled operator spill and original hash1 distribution.
Debug/all-feature source/binary inventories must be frozen before acceptance.
These are local diagnostics, with no performance pass/fail threshold.

Two datasets: seed42, 100000 rows, 4 equal-sized row ranges/files, 4096-key domain,
row groups8192, Snappy, payload16. Balanced uniform versus hot_key, fraction0.99,
one hot key. Run each dataset at2/4 workers. One warmup and three measured complete
process-isolated native executions per arm: a fresh coordinator test process,
fresh workers and new scratch for every sample. Coordinator PIDs are recorded
and must all differ. Alternate local and remote for each
repetition; retain every warmup, sample, error and independent-oracle result.
No concurrent compilation/tests during the measured loop. The same host runs the
separate loopback Moto S3 service and bounded fault proxy. Record their exact
versions and request log; no cross-host/AZ/security/durability assertion.

Whole-run wall includes worker startup/teardown, source validation, native work,
exchange/gather and postflight. Remote includes two sequential fresh cohorts and
its extra source/manifest verification. This compares complete execution paths;
it does not isolate network time or hold simultaneous process lifetimes equal.
Remote GC is separately measured after quiescence, then added only to its declared
lifecycle total. Independent oracle and diagnostic serialization are outside wall.
The unchanged benchmark oracle validates captured batches; its replay-adapter
setup/query timers are NOT the observed native query times.

CONFIGURED illustrative prices, not a provider tariff: PUT5000, HEAD400, GET400,
DELETE0 nano-USD/request; read/write object-byte prices0; egress1 nano-USD/read byte;
storage byte-ms0; retention60000ms; labels loopback-compute/loopback-store. No
placement-based inference. Report all API attempts, successfully/partially read
object payload bytes, attempted PUT body bytes and retries. DELETE is the public
S3 DeleteObjects API (one validated key/HTTP POST per call), not necessarily an
HTTP DELETE. HTTP framing, TLS, error XML, and delete XML are outside object-body
byte counts. These costs are explicit model estimates, never PROVIDER_BILLED.

Acceptance: exact existing independent oracle for every sample, identical source
manifest identity between arms, real2/4 producer and fresh reader processes,
network PUT/HEAD/GET trace agreement, full terminal/gather/byte bounds, and raw
checksummed provenance. Display all medians/ranges and remote overhead. A slower
remote result is valid; restored work after producer exit is its demonstrated
value. Historical Phase6–10 timing gates and evidence remain unchanged.
