"""Immutable input construction. The detached oracle reads the resulting Parquet."""
import argparse
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


HERE = Path(__file__).resolve().parent
CONTRACT = json.loads((HERE / "case-contract.json").read_bytes())
CRITERIA = json.loads((HERE / "criteria.json").read_bytes())
MODEL = json.dumps(CONTRACT["model"], separators=(",", ":")).encode()
SCALES = [(3.0, 4.0, 0.0, -0.0), (-5.0, 0.0, 12.0, 0.0),
          (1e300, -1e300, 1e300, 0.0), (1e-300, 2e-300, -1e-300, 0.0)]
ORDINARY = [*SCALES[:2], (1.0, -1.0, 1.0, 0.0), (1.0, 2.0, -1.0, 0.0)]


def write(path, fields, columns):
    schema = pa.schema([pa.field(name, kind, nullable=False) for name, kind in fields])
    pq.write_table(pa.Table.from_arrays(columns, schema=schema), path,
                   use_dictionary=False, compression="NONE", write_statistics=False,
                   row_group_size=256)


def generate(root):
    root.mkdir(parents=True, exist_ok=False)
    (root / "model.json").write_bytes(MODEL)
    (root / "model-1.json").write_bytes(b'{"weights":[2.0],"bias":0.25}')
    for kind, count in {"combined": 257, "score": 257, "empty": 0, "single": 1,
                        "large": 32768, "zero": 1, "nan": 1, "infinity": 1,
                        "overflow": 1, "duplicate-id": 257, "reordered-id": 257}.items():
        ids = list(range(count))
        vectors = [(ORDINARY if kind == "score" else SCALES)[i % 4] for i in ids]
        if kind in ("zero", "nan", "infinity", "overflow"):
            first = {"zero": -0.0, "nan": float("nan"), "infinity": float("inf"),
                     "overflow": float.fromhex("0x1.fffffffffffffp+1023")}[kind]
            vectors[0] = (first, 0.0, 0.0, 0.0)
        if kind == "duplicate-id":
            ids[-1] = ids[-2]
        elif kind == "reordered-id":
            ids[-2:] = reversed(ids[-2:])
        fields = [("row_id", pa.uint64()), *((f"f{i}", pa.float64()) for i in range(4)),
                  ("score", pa.float64())]
        columns = [ids, *([row[i] for row in vectors] for i in range(4)), [-7.0] * count]
        write(root / f"{kind}.parquet", fields, columns)
    graphs = CONTRACT["graph_cases"]
    for name in ("mixed", "reordered", "empty", "isolated", "single", "foreign-edge", "duplicate-vertex", "chain"):
        directory = root / name
        directory.mkdir()
        if name == "reordered":
            vertices = sorted(graphs["mixed"]["vertices"])
            edges = sorted(graphs["mixed"]["edges"])
        elif name == "foreign-edge":
            vertices, edges = [1, 2], [[1, 3]]
        elif name == "duplicate-vertex":
            vertices, edges = [1, 1], []
        elif name == "chain":
            vertices = list(range(CRITERIA["measurements"]["graph_vertices"]))
            edges = list(zip(vertices[1:], vertices))
        else:
            vertices, edges = graphs[name]["vertices"], graphs[name]["edges"]
        write(directory / "vertices.parquet", [("vertex", pa.uint64())], [vertices])
        write(directory / "edges.parquet", [("src", pa.uint64()), ("dst", pa.uint64())],
              [[edge[i] for edge in edges] for i in range(2)])


def inventory(root):
    return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(root.rglob("*")) if path.is_file()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    generate(args.output)
    print(json.dumps(inventory(args.output), indent=2))
