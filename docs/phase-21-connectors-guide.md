# Phase 21 connector catalog operator guide

Enable the applicable `ade-connectors` features explicitly. `batch` adds the
local SQL adapter, the IPC sink and `ade-connect`. The `s3`, `delta` and `kafka`
features can be built separately. `iceberg`, `postgres` and `flight` include
`batch`. The [support matrix](reports/phase-21-support.json) records acceptance
for each route.

## Local Parquet and atomic publication

Use a private stage parent and an existing operator-controlled output directory
on a local filesystem that provides atomic same-directory hard links and
durable file and directory sync. The sink root must be owned by the effective
user and must not be group- or world-writable. Publication is anchored to the
directory descriptor opened by `LocalSink`; filesystem actors with the same
user identity remain trusted. Provide independently trusted BLAKE3 pins for
the source descriptor and every Parquet file. Source descriptors have this
shape. Replace the example digests and lengths with trusted values.

```json
{
  "version": 1,
  "entry": {
    "version": 1,
    "name": "events",
    "connector": {"version": 1, "kind": "local_parquet", "namespace": "<64 lowercase hex digits>"},
    "schema": [{"name": "value", "data_type": "i64", "nullable": false}]
  },
  "authority": "<64 lowercase hex digits>",
  "splits": [{"key": "part.parquet", "bytes": 504, "rows": 3, "blake3": "<64 lowercase hex digits>"}],
  "replay": "pinned"
}
```

The namespace pins the logical source. The authority pins the upstream
snapshot statement. Compute the descriptor digest over compact UTF-8 JSON in
the displayed field order, including each nested structure, with no trailing
newline. Rust callers can use `ResolvedSnapshotV1::digest` to check a descriptor
against their pin. Supply split keys in ascending lexical order. Configure
operational paths, endpoint and authentication independently.

```sh
cargo build -p ade-connectors --features batch --bin ade-connect --locked
target/debug/ade-connect SOURCE_ROOT DESCRIPTOR_JSON EXPECTED_BLAKE3 STAGE_PARENT \
  'SELECT value FROM input' OUTPUT_DIR run_001
```

The catalog retains the name `events`. Native SQL binds the one staged relation
as `input`. The existing PlanV1 SQL subset and native resource limits apply.
The receipt carries the key, native plan digest, IPC payload digest and byte
length. Use `LocalSink::reopen` and compare the result to an independently
retained receipt when reconciling a retry. The reader verifies the envelope and
returns the Arrow IPC stream as bytes. Reusing a key with identical plan and
payload verifies the existing artifact. Different bytes fail with a conflict.
Temporary files from a killed process are invisible to the reader and require
operator cleanup after the process exits.

## Source contracts

- **Local and S3 Parquet.** Supply 1–64 ordered split pins, at most one million
  rows and 64 MiB per source. Parquet data can use uncompressed or Snappy
  pages. Local reads reject symlinks within the source root.
  S3 uses one signed whole-object GET per split with a 16 MiB object cap and a
  content digest independent of S3 metadata. Configure `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY` and optionally `AWS_SESSION_TOKEN`. S3 HTTPS uses a
  DNS hostname and a caller-pinned public IPv4 address. Plain HTTP is confined
  to explicitly configured numeric loopback. The reader rejects redirects and
  partial responses and performs no discovery or writes.
- **Iceberg.** Use a local, unpartitioned format-v1 table with one schema, an
  append-only snapshot ancestry and uncompressed Avro manifests and Parquet
  data. Pin the metadata file, selected manifest list, every manifest and the
  full active split inventory. Field IDs and native column types are checked
  before normalization into native Parquet. Snapshot selection uses the
  explicit ID. Row-level deletes, deleted manifest entries, encryption,
  branches other than a consistent main reference, evolved schemas and
  partition specifications are refused.
- **Delta.** Use a local unpartitioned protocol reader 1/writer 2 table with
  exact contiguous JSON commits from version zero through the selected version.
  Pin each log digest and the resolved active split inventory. Supported
  metadata types are non-null long, double and string, subject to native
  Parquet verification. Adds and removes of different files in a commit are
  applied to the active set. A checkpoint marker, deletion vectors, table
  features, same-path actions within one commit and metadata evolution refuse.
- **PostgreSQL.** Select a single ordinary table and an ordered list of
  non-null bigint results. The adapter checks identifiers, holds a
  `REPEATABLE READ READ ONLY` transaction during bounded extraction and
  publishes a one-shot stage after successful commit. This is caller-authorized
  capture, not catalog-managed discovery or authorization: the caller must
  authorize the exact endpoint, table, projection and schema before invoking
  capture. The TLS client verifies the configured server hostname.
  Remote credentials come from `ADE_CONNECTOR_PG_PASSWORD`. Raw traffic is capped before the client buffers PostgreSQL messages. Generic JDBC,
  arbitrary SQL, cross-table transactions and CDC are outside this route.
- **Arrow Flight.** A bounded `DoGet` uses an opaque ticket and materializes
  one response as a one-shot stage. This is caller-authorized capture, not
  catalog-managed discovery or authorization: the caller must authorize the
  exact server identity, ticket and schema before invoking capture. Schema,
  messages and batches are checked.
  Dictionary and compressed IPC, application metadata, stream schema changes,
  Flight SQL and write operations are refused. Remote TLS verifies the server
  hostname against Web PKI. Flight and PostgreSQL both offer explicit owned
  loopback fixture transport and an option to verify a fixture CA for
  `localhost`.
- **Kafka.** Pin `KafkaEntryV1.native_spec_blake3` to the already compiled
  native semantic spec and pass an existing `StreamConfigV1` as a compatibility
  assertion. The facade returns the existing stateless engine and Kafka source.
  Use `ade_stream::runtime::run` and close the source explicitly. Assigned and
  history-then-live streams are unbounded. Offset suggestions are nondurable.
  Finite Kafka range, stateful selection and an external transaction sink are
  outside this facade.

One-shot batches are capped at 65,536 rows and 16 MiB decoded Arrow and
staged Parquet. Flight also caps encoded messages at 1 MiB each and 16 MiB per stream and refuses schema
or protocol drift. Connector staging adds a write and native rereads.
Iceberg also decodes and normalizes its data files. The local sink caps IPC
payloads at 16 MiB. Native memory diagnostics cover the native ledger.
Arrow/Parquet client internals and process RSS have separate measurements
and limits. Connector diagnostics separate `download_bytes`,
`final_stage_bytes`, normalization writes, and the owned-file high-water
estimate. The source and final Parquet representations are each capped at
64 MiB; Iceberg normalization may temporarily hold portions of both
representations, so its disk high-water can exceed either individual cap.
Filesystem allocation blocks and directory metadata are outside this
file-length estimate. Request cancellation and deadlines cover network futures,
native decode batch boundaries and publication boundaries. An individual
kernel file operation can finish before cancellation is observed.

## Reproduce the evidence

Use macOS with an owned temporary directory, OpenJDK 17, Kafka 3.9.1,
PostgreSQL 17, SeaweedFS 4.34 and a Python 3.9 environment containing
`pyarrow==21.0.0`, `fastavro==1.12.1`, `deltalake==0.25.5`,
`pyiceberg==0.10.0`, `sqlalchemy==2.0.44`, `boto3==1.40.54` and
`blake3==1.0.8`. Use the fixture runner with dedicated loopback services
and a fresh output directory.

```sh
cargo build -p ade-connectors --all-features --bins --examples --locked
python3 scripts/phase21/campaign.py --output NEW_TMP_DIR \
  --binary "$PWD/target/debug/examples/phase21-probe" \
  --postgres PG_BIN_DIR --weed WEED_BINARY --java JAVA_HOME --kafka KAFKA_HOME
python3 scripts/phase21/verify.py NEW_ARCHIVE.tar.gz --pack NEW_TMP_DIR \
  --mode current-source --repo . --binary target/debug/examples/phase21-probe --tamper
```

Retain the archive SHA-256 in a separate trusted record. Offline replay
requires the archive, its pin, this revision of the verifier and its Python
libraries. The verifier checks raw semantic evidence and rejects rehashed
mutations. To inspect an older archive without live attribution, use
`--mode historical-archive`; this mode does not assert current source or binary
identity and does not close current-source release gates. Evidence for
predecessor phases keeps its original qualification.
