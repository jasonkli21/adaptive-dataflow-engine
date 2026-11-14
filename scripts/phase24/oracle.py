"""Detached reference workloads over the original Parquet data and pinned model."""
import argparse
import json
import math
from pathlib import Path

import pyarrow.parquet as pq


def vector(path, model, operation, dimensions):
    alpha = model["weights"][:dimensions]
    table = pq.read_table(path).to_pydict()
    ids, values = table["row_id"], []
    for i, previous in enumerate(table["score"]):
        features = [table[f"f{j}"][i] for j in range(4)]
        if operation in ("combined", "prep"):
            active = features[:dimensions]
            # Independent normalization: hypot uses compensated internal scaling.
            norm = math.hypot(*active)
            features[:dimensions] = [value / norm for value in active]
        if operation in ("combined", "score"):
            values.append(math.fsum([model["bias"], *(w * f for w, f in zip(alpha, features))]))
        elif operation == "prep":
            values.extend([*features, previous])
        else:
            values.append(previous)
    return {"ids": ids, "values": values}


def graph(directory):
    vertices = pq.read_table(directory / "vertices.parquet").column("vertex").to_pylist()
    edges = pq.read_table(directory / "edges.parquet").to_pydict()
    parent = dict(zip(vertices, vertices))

    def find(vertex):
        while parent[vertex] != vertex:
            parent[vertex] = parent[parent[vertex]]
            vertex = parent[vertex]
        return vertex

    for source, target in zip(edges["src"], edges["dst"]):
        lhs, rhs = sorted((find(source), find(target)))
        parent[rhs] = lhs
    counts = {}
    terminal = []
    for vertex in sorted(vertices):
        label = find(vertex)
        counts[label] = counts.get(label, 0) + 1
        terminal.append({"vertex": vertex, "component_min_vertex": label})
    components = [{"component_min_vertex": label, "size": size} for label, size in sorted(counts.items())]
    return {"components": components, "terminal": terminal}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("request", type=Path)
    args = parser.parse_args()
    request = json.loads(args.request.read_bytes())
    if "operation" in request:
        observed = vector(Path(request["path"]), json.loads(Path(request["model_path"]).read_bytes()),
                          request["operation"], request["dimensions"])
    else:
        observed = graph(Path(request["source_dir"]))
    print(json.dumps(observed, separators=(",", ":")))
