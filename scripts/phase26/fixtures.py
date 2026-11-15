"""Generate pinned native input independently with PyArrow, outside ADE."""
import json
from pathlib import Path

import blake3
import pyarrow as pa
import pyarrow.parquet as pq

U64 = 2**64 - 1


def canonical(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def digest(data):
    return blake3.blake3(data).hexdigest()


def write(path, value):
    path.write_bytes(canonical(value))


def source_type():
    return pa.uint64()


def make_fixture(root, rows, width, files, *, overflow=False):
    root.mkdir(mode=0o700)
    names = ["key", "value_i64", "secondary", "alt"] + [
        f"padding_{i}" for i in range(width - 4)
    ]
    schema = pa.schema(
        [pa.field(name, pa.int64() if i == 1 else source_type(), False)
         for i, name in enumerate(names)]
    )
    inventory = []
    ordered_hash = blake3.blake3()
    for part in range(files):
        start, stop = rows * part // files, rows * (part + 1) // files
        ix = range(start, stop)
        columns = [
            [0 if i % 5 else i % 71 for i in ix],
            ([2**63 - 1, 1] if overflow else [i % 2053 - 1026 for i in ix]),
            [0 if i == 0 else U64 if i == 1 else (i * 127) % 8192 for i in ix],
            [i % 13 for i in ix],
        ]
        for j in range(width - 4):
            columns.append([((i * (j + 3)) ^ (i // (j + 7))) % 16001 for i in ix])
        path = root / f"part-{part}.parquet"
        pq.write_table(pa.Table.from_arrays([pa.array(c, type=f.type)
                                           for c, f in zip(columns, schema)], schema=schema),
                       path, row_group_size=32_768, compression="zstd", use_dictionary=False)
        blob = path.read_bytes()
        hashed = digest(blob)
        ordered_hash.update(bytes.fromhex(hashed))
        inventory.append(dict(name=path.name, bytes=len(blob), rows=stop-start, fingerprint=hashed))
    fields = [dict(name=f.name, data_type="i64" if i == 1 else "u64", nullable=False)
              for i, f in enumerate(schema)]
    write(root / "catalog.json", dict(version=1, logical=ordered_hash.hexdigest(),
                                      schema=fields, files=inventory))
    return root


def field(name, kind, nullable=False):
    return dict(name=name, data_type=kind, nullable=nullable)


def plan(root, threshold, group=0, *, cut=2, projection=None, alias=None, cap=1_000_000,
         cap_bytes=67_108_864, replayable=True, suffix_limit=False):
    catalog = (root / "catalog.json").read_bytes()
    pin = digest(catalog)
    info = json.loads(catalog)
    fields = info["schema"]
    source = dict(id=0, table="input", manifest=pin, logical=info["logical"],
                  schema_identity=digest(canonical(fields)), schema=fields,
                  files=info["files"], replayable=replayable)
    props = dict(bounded=True, deterministic=True, replayable=replayable,
                 ordering=[], distribution="single")
    nodes = []

    def add(op, schema, **args):
        nodes.append(dict(id=len(nodes), operator=dict(op=op, **args), schema=schema,
                          properties=props))

    add("scan", fields, source=0)
    add("filter", fields, input=0, column=2, less_than=threshold)
    if cut == 2:
        if projection is None:
            projection = [(0, "key"), (1, "value_i64"), (3, "alt")]
        picked = [dict(fields[i], name=name) for i, name in projection]
        add("project", picked, input=1,
            columns=[dict(column=i, alias=name) for i, name in projection])
    else:
        picked = fields
    agg = [field("key", "u64"), field("sum", "i64", True), field("count", "i64")]
    add("group_sum_count", agg, input=cut, key=group,
        value=next(i for i, f in enumerate(picked) if f["data_type"] == "i64"))
    if alias is not None:
        add("project", [field(alias, "i64", True), agg[0]], input=len(nodes)-1,
            columns=[dict(column=1, alias=alias), dict(column=0, alias="key")])
    if suffix_limit:
        add("limit", nodes[-1]["schema"], input=len(nodes)-1, rows=1)
    return dict(version=1, target="local", sources=[source], nodes=nodes, output=len(nodes)-1,
                max_output_rows=cap, max_output_bytes=cap_bytes)
