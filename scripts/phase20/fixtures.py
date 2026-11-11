"""Deterministic independent finite graph and Parquet fixtures."""
import random
from pathlib import Path


def graph(name):
    if name == "empty":
        return [], []
    if name == "isolated":
        return list(range(4)), []
    if name == "mixed":
        return list(range(10)), [(0, 1), (0, 2), (1, 3), (2, 3), (3, 4),
                                 (4, 1), (3, 3), (5, 6), (6, 5), (7, 6),
                                 (8, 8), (4, 0)]
    if name in ("chain", "max_chain"):
        n = 1024 if name == "chain" else 4096
        return list(range(n)), [(i, i + 1) for i in range(n - 1)]
    if name == "max":
        n = 4096
        edges = [(0, i) for i in range(1, n)]
        edges += [(i, (i + d) % n) for d in range(1, 17) for i in range(1, n)][:65536 - len(edges)]
        return list(range(n)), edges
    n, degree = {"dense": (256, 32)}[name]
    return list(range(n)), [(i, (i + d) % n) for i in range(n) for d in range(1, degree + 1)]


def parquet_file(path, names, rows, nullable=False, signed=False):
    import pyarrow as pa
    import pyarrow.parquet as pq
    schema = pa.schema([pa.field(name, pa.int64() if signed else pa.uint64(), nullable=nullable)
                        for name in names])
    columns = list(zip(*rows)) if rows else [[] for _ in names]
    pq.write_table(pa.Table.from_arrays([pa.array(col, type=field.type)
                                        for col, field in zip(columns, schema)], schema=schema),
                   path, compression="NONE", use_dictionary=False, row_group_size=4096)


def inventory_entry(path, rows):
    from blake3 import blake3
    raw = Path(path).read_bytes()
    return {"name": Path(path).name, "rows": rows, "bytes": len(raw), "blake3": blake3(raw).hexdigest()}


def encode(path, vertices, edges, permutation=None):
    path.mkdir(parents=True, exist_ok=True)
    source = {}
    for role, names, rows in [("vertices", ["vertex"], [(v,) for v in vertices]),
                               ("edges", ["src", "dst"], list(edges))]:
        if permutation is not None:
            random.Random(permutation).shuffle(rows)
        files = [rows] if permutation is None else [rows[::2], rows[1::2]]
        entries = []
        for ordinal, part in enumerate(files):
            file = path / f"{role}-{ordinal}.parquet"
            parquet_file(file, names, part)
            entries.append(inventory_entry(file, len(part)))
        source[role] = entries if permutation is None else entries[::-1]
    return source


def seal(path, criteria):
    import hashlib
    import json
    from collections import Counter

    sealed = {}
    for name, expected in criteria["fixtures"].items():
        vertices, edges = graph(name)
        assert [len(vertices), len(edges)] == expected
        sealed[name] = {
            "inventory": encode(path / name, vertices, edges),
            "graph_sha256": hashlib.sha256(json.dumps([sorted(vertices), sorted(edges)],
                                                     separators=(",", ":")).encode()).hexdigest(),
        }
        degree = Counter(a for a, _ in edges)
        sealed[name]["out_degree_histogram"] = {str(d): count for d, count in sorted(Counter(degree[v] for v in vertices).items())}
    return sealed


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("--criteria", type=Path, default=Path(__file__).with_name("criteria.json"))
    args = parser.parse_args()
    print(json.dumps(seal(args.output, json.loads(args.criteria.read_text())), indent=2, sort_keys=True))
