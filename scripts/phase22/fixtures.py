"""Generate deterministic local Parquet and the predeclared independent row oracle."""
import json
import struct

import blake3
import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = [
    ("row_id", "Int(64, false)", pa.uint64()),
    ("key", "Int(64, false)", pa.uint64()),
    ("secondary", "Int(64, false)", pa.uint64()),
    ("value_i64", "Int(64, true)", pa.int64()),
    ("value_f64", "FloatingPoint(DOUBLE)", pa.float64()),
    ("category", "Int(32, false)", pa.uint32()),
    ("payload", "Utf8", pa.string()),
    ("event_time", "Timestamp(MILLISECOND, null)", pa.timestamp("ms")),
]
SQL = "SELECT row_id, secondary, value_i64, payload, event_time FROM input WHERE secondary < 3"
SELECT = ["row_id", "secondary", "value_i64", "payload", "event_time"]


def row(index):
    return (
        2**63 + index,
        (2**64 - 1) if index == 0 else index % 5,
        index % 19,
        [-2**63, 2**63 - 1, 2**53 + 7, -2**53 - 9][index % 4],
        [-0.0, float("inf"), -1.5, 2.5][index % 4],
        index % 3,
        ["café", "東京", "😀", "plain"][index % 4],
        -1001 + 789 * index,
    )


def hash_bytes(data):
    return blake3.blake3(data).hexdigest()


def encode_rows(rows, columns):
    indexes = [next(i for i, field in enumerate(SCHEMA) if field[0] == name) for name in columns]
    return {
        "schema": [{"name": SCHEMA[i][0], "type": SCHEMA[i][1], "nullable": False} for i in indexes],
        "rows": [
            [format(struct.unpack(">Q", struct.pack(">d", r[i]))[0], "x") if i == 4 else str(r[i]) for i in indexes]
            for r in rows
        ],
    }


def oracle(case):
    count = {"empty": 0, "single": 1, "boundary": 256}.get(case, 521)
    rows = [row(i) for i in range(count)]
    if case in ("udf", "repeat", "boundary", "empty", "single"):
        rows = [(r[0], r[1], r[2] % 7, *r[3:]) for r in rows]
    if case in ("sql", "plan", "udf", "repeat"):
        rows = [r for r in rows if r[2] < 3]
    columns = SELECT if case in ("sql", "plan", "udf", "repeat") else [name for name, *_ in SCHEMA]
    return encode_rows(rows, columns)


def fixture(root, count=521, *, large=False):
    root.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([pa.field(name, data_type, nullable=False) for name, _, data_type in SCHEMA])
    rows = [row(i) for i in range(count)]
    if large:
        rows = [(*r[:6], str(i) * 4096, r[7]) for i, r in enumerate(rows)]
    columns = [
        pa.array([r[i] for r in rows], type=field.type)
        for i, field in enumerate(schema)
    ]
    path = root / "fixture.parquet"
    pq.write_table(pa.Table.from_arrays(columns, schema=schema), path,
                   compression="NONE", use_dictionary=False, row_group_size=256)
    raw = path.read_bytes()
    identity = hash_bytes(raw)
    manifest = {
        "schema_version": 1, "dataset_version": 1, "required_schema": "synthetic-events-v1",
        "schema_fingerprint": hash_bytes(
            b"synthetic-events-v1:row_id=u64,key=u64,secondary=u64,value_i64=i64,value_f64=f64,category=u32,payload=utf8,event_time=timestamp_ms;all_nonnull"
        ),
        "logical_fingerprint": hash_bytes(f"phase22-fixture-v1:{count}:{large}".encode()),
        "format": "parquet", "row_count": count,
        "files": [{"relative_path": path.name, "bytes": len(raw), "rows": count, "fingerprint": identity}],
    }
    (root / "manifest.json").write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return path
