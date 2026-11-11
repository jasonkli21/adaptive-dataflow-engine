"""Independent BFS and union-find for the finite graph campaign."""
from collections import deque


def result(vertices, edges, algorithm):
    assert len(vertices) == len(set(vertices))
    assert len(edges) == len(set(edges))
    vertex_set = set(vertices)
    assert all(a in vertex_set and b in vertex_set for a, b in edges)
    if algorithm["kind"] == "reachability_bfs_v1":
        adjacency = {vertex: [] for vertex in vertices}
        for a, b in edges:
            adjacency[a].append(b)
        seed = algorithm["seed"]
        assert seed in adjacency
        distance = {seed: 0}
        queue = deque([seed])
        while queue:
            source = queue.popleft()
            for target in adjacency[source]:
                if target not in distance:
                    distance[target] = distance[source] + 1
                    queue.append(target)
        return [{"vertex": key, "distance": distance[key]} for key in sorted(distance)]
    assert algorithm["kind"] == "weak_components_min_label_v1"
    parent = {key: key for key in vertices}
    size = dict.fromkeys(vertices, 1)

    def representative(key):
        while parent[key] != key:
            parent[key] = parent[parent[key]]
            key = parent[key]
        return key

    for a, b in edges:
        x, y = representative(a), representative(b)
        if x != y:
            if size[x] < size[y]:
                x, y = y, x
            parent[y] = x
            size[x] += size[y]
    minima = {}
    for vertex in sorted(vertices):
        minima.setdefault(representative(vertex), vertex)
    return [{"vertex": key, "component_min_vertex": minima[representative(key)]}
            for key in sorted(vertices)]
