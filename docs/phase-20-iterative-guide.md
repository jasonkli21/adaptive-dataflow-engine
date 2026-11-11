# Finite cyclic graph jobs

Enable the command explicitly from the repository root.

```sh
cargo build --release --locked -p ade-iterate --features cyclic-v1
```

The request binds two ordered local Parquet inventories. Vertex files contain
`vertex: UInt64`. Edge files contain `src: UInt64, dst: UInt64`. All fields are
non-nullable and each row in its role is unique. Use an empty Parquet file with
the correct schema for an empty role. Self-loops and reciprocal directed edges
are legal. Every endpoint must occur in the vertex inventory.

| Algorithm kind | Parameters | Terminal row |
| --- | --- | --- |
| `reachability_bfs_v1` | `seed` is a supplied vertex ID | `vertex`, `distance` (UInt32 shortest-hop distance on directed edges) |
| `weak_components_min_label_v1` | No parameters | `vertex`, `component_min_vertex` (UInt64 minimum ID in the weak component) |

BFS emits the reachable vertices. Components emits every supplied vertex,
including isolated vertices. Both sort terminal rows by ascending vertex.
Acyclic PlanV1, streaming roots and maintained-view roots have their own
formats and entry points. Phase 20 admits only the finite local graph shapes
above.

## Request and commands

Build a UTF-8 JSON request with these fields.

| Field | Value |
| --- | --- |
| `version` | `1` |
| `plan` | The canonical `CyclicPlanV1`, as shown below |
| `inventory` | `{"vertices":[...],"edges":[...]}`; each entry has `name`, `bytes`, decoded `rows` and lowercase hex `blake3` |
| `source_dir` | Absolute path to the local graph files |
| `root` | Absolute path to the private job directory. Its parent exists before `run` |
| `expected` | Omit for `run`. Supply the independently retained exact pin for `resume`, `read` and `verify` |
| `checkpoint_every` | Number of executed rounds between checkpoints. Default `0` selects only a terminal snapshot |
| `limits` | Optional positive per-operation limits, up to the ceilings below |
| `mode` | `indexed` by default, or `scan_all` for the repeated-arc-scan baseline |
| `reverse` | Optional reverse traversal of the indexed current frontier |
| `trace` | Optional per-round credit and work records on stderr |

A BFS plan with seed 3 uses this physical graph.

```json
{
  "version": 1,
  "algorithm": {"kind": "reachability_bfs_v1", "seed": 3},
  "nodes": ["expand", "reduce", "delay"],
  "arcs": [
    {"from": 0, "to": 1, "schema": "bfs_candidate", "delay": 0},
    {"from": 1, "to": 2, "schema": "bfs_frontier", "delay": 0},
    {"from": 2, "to": 0, "schema": "bfs_frontier", "delay": 1}
  ]
}
```

Components uses `{"kind":"weak_components_min_label_v1"}` and the port
schemas `label_candidate` and `label_frontier`. Rust callers can construct
the same physical plan with `CyclicPlanV1::new(algorithm)`, decode the
bounded request with `Request::decode`, and call `ade_iterate::operate`.
A caller-supplied writer provides interruptible backpressure if it may block.
A callback that serializes diagnostics accounts for its own bytes.

```sh
target/release/ade-iterate explain request.json
target/release/ade-iterate run request.json 2> receipts.jsonl
target/release/ade-iterate resume pinned-request.json 2>> receipts.jsonl
target/release/ade-iterate read terminal-request.json > result.jsonl
target/release/ade-iterate verify terminal-request.json
```

`explain` validates the graph and prints the admitted plan and resource
summary without creating a root. `run` creates a fresh root. `resume` starts
at the pinned cut or verifies an already selected terminal result. A run
stopped by quota, timeout, SIGINT or round cap exits nonzero. The last selected
cut remains available if one was committed. `read` emits canonical NDJSON
and a final `kind: "complete"` record. `verify` checks the terminal state and
emits only that record. Treat the process exit status and a complete,
newline-terminated footer as one success condition. Partial streams from
a broken or stalled sink can contain earlier rows.

## Pins, interruption and replay

Retain the `pin` from each `selected` receipt on stderr outside the job root.
Copy the exact `sequence`, `round` and `chain` object into the next request's
`expected` field. A checkpoint represents the start of its `round` with a
canonical next frontier and zero outstanding credits. Terminal selection
requires a settled cut with no next frontier. A reader with a checkpoint pin
refuses before emitting rows.

If a process dies after a `prepared` receipt but before the corresponding
`selected` receipt, preserve both independently captured pins. After
confirming no writer is active, check for the candidate's append-only
`s20-NNNN` selector. Supply the prepared pin when that selector exists and
the previous selected pin when it does not. The runtime still validates the
exact pin, all parents, and the object bytes. A fenced attempt can be retried
after the other process exits. A changed, missing or forked chain requires
operator investigation. For deterministic replay, choose a fresh root with
the complete original source. Keep old generations under the existing root.

A root belongs to one operator-controlled lineage on an honest local
filesystem. The kernel lease uses one exclusive writer or shared readers.
Root mode is 0700 and stored files are 0600. Named files and path components
must be free of symlinks and parent traversal. On macOS, use the real
`/private/tmp` path for temporary jobs. External pin retention and backup
are operator responsibilities. The tested envelope covers process interruption,
I/O failure and local ENOSPC. Disk loss, arbitrary power loss, filesystem
tampering during operations, remote/distributed execution and external
transactions are outside this envelope.

## Bounds

| Setting | Default and maximum |
| --- | ---: |
| `source_bytes` | 64 MiB across both roles |
| `state_bytes` | 64 MiB managed graph/frontier backing storage |
| `disk_bytes` | 256 MiB retained serialized bytes, including staged files |
| `output_bytes` | 16 MiB of terminal NDJSON including the footer and newline |
| `deadline_ms` | 120,000 ms per operation |
| `rounds` | 4,096 total round advances across a lineage |
| `credits` | 1,024 queued plus executing feedback rows |
| `batch_rows` | 1,024 Arrow rows per decode batch |

Additional ceilings are 4,096 vertices, 65,536 directed edges, 32 files per
role, 64 KiB of request JSON, 1 MiB of Parquet footer metadata, 16 MiB of
declared uncompressed bytes per row group, 4,096 reduced feedback rows and
131,072 arc examinations per round, and 12,293 root entries. The memory
ledger covers runtime-managed buffers and records a conservative allocation
bound. The decode batch peak is reported separately. Parquet/Arrow internals,
allocator and OS cache overhead, filesystem block allocation and process RSS
are outside these ledgers. The CLI can interrupt a stalled pipe between
nonblocking writes. Time inside a blocked filesystem syscall remains subject
to the OS.

See [ADR-0027](adr/0027-cyclic-dataflow-v1.md), the [implementation
report](reports/phase-20-implementation-report.md) and the [evidence
directory](evidence/phase20/README.md) for the acceptance scope.
