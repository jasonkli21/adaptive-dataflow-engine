"""Portable Phase 20 archive and semantic verifier. Requires PyArrow and BLAKE3."""
import argparse
from collections import Counter, defaultdict
import gzip
import hashlib
import json
import shutil
import statistics
import tarfile
import tempfile
from pathlib import Path, PurePosixPath

from blake3 import blake3
import pyarrow as pa
import pyarrow.parquet as pq

from oracle import result

BFS = "reachability_bfs_v1"
COMPONENTS = "weak_components_min_label_v1"


def read(path):
    return json.loads(path.read_text())


def raw(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def domain(name, value):
    return blake3(name.encode() + b"\0" + value).hexdigest()


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def json_lines(value):
    return [json.loads(line) for line in value.splitlines() if line.startswith("{")]


def selected(case, kind="selected"):
    return [e["pin"] for e in json_lines(case["stderr"]) if e["event"] == kind][-1]


def graph(directory, inventory):
    data = []
    for role, names in [("vertices", ["vertex"]), ("edges", ["src", "dst"])]:
        expected_schema = pa.schema([pa.field(name, pa.uint64(), nullable=False) for name in names])
        rows = []
        for entry in inventory[role]:
            path = directory / entry["name"]
            assert path.name == entry["name"]
            source = path.read_bytes()
            assert len(source) == entry["bytes"] and blake3(source).hexdigest() == entry["blake3"]
            table = pq.read_table(path)
            assert table.schema == expected_schema and table.num_rows == entry["rows"]
            columns = [table[name].to_pylist() for name in names]
            assert all(None not in column for column in columns)
            rows.extend(columns[0] if role == "vertices" else zip(*columns))
        data.append(sorted(rows))
    assert len(data[0]) == len(set(data[0])) and len(data[1]) == len(set(data[1]))
    return data


def ids(request):
    inventory = {role: [{key: item[key] for key in ("name", "bytes", "rows", "blake3")}
                        for item in request["inventory"][role]] for role in ("vertices", "edges")}
    field = lambda name, dtype: {"name": name, "data_type": dtype, "nullable": False}
    source = domain("ade-cycle20-source-v1", raw((
        [field("vertex", "u64")], [field("src", "u64"), field("dst", "u64")], inventory)))
    plan = request["plan"]
    algo = plan["algorithm"]
    bfs = algo["kind"] == BFS
    prefix = "bfs" if bfs else "label"
    assert plan == {"version": 1, "algorithm": {"kind": BFS, "seed": algo["seed"]} if bfs else {"kind": COMPONENTS},
                    "nodes": ["expand", "reduce", "delay"],
                    "arcs": [{"from": start, "to": end, "schema": f"{prefix}_{schema}", "delay": delay}
                             for start, end, schema, delay in
                             ((0, 1, "candidate", 0), (1, 2, "frontier", 0), (2, 0, "frontier", 1))]}
    output = [field("vertex", "u64"), field("distance", "u32") if bfs else field("component_min_vertex", "u64")]
    return source, domain("ade-cycle20-plan-v1", raw((plan, source, output, "simple_directed_strict_round_v1")))


def feedback(vertices, edges, algorithm, states_at=()):
    """Independent synchronous arithmetic checker, alongside BFS/union-find."""
    bfs = algorithm["kind"] == BFS
    outgoing = defaultdict(list)
    for a, b in edges:
        outgoing[a].append(b)
        if not bfs:
            outgoing[b].append(a)
    vertex_index = dict(zip(vertices, range(len(vertices))))
    labels = {algorithm["seed"]: 0} if bfs else dict(zip(vertices, vertices))
    active = set(labels)
    ledger, states = [], {}
    for round_num in range(4096):
        next_labels = {}
        work = 0
        for src in active:
            for dst in outgoing[src]:
                work += 1
                proposal = round_num + 1 if bfs else labels[src]
                if dst not in labels or proposal < labels[dst]:
                    next_labels[dst] = min(next_labels.get(dst, proposal), proposal)
        ledger.append((len(active), len(next_labels), work, len(edges) * (1 if bfs else 2)))
        labels.update(next_labels)
        active = set(next_labels)
        if round_num + 1 in states_at:
            states[round_num + 1] = {"next_round": round_num + 1, "values": [labels.get(v) for v in vertices],
                                     "frontier": sorted(vertex_index[v] for v in active), "in_flight": 0}
        if not active:
            break
    else:
        assert not active, "reference failed to converge"
    return ledger, states


def expected_footer(rows, algo, rounds):
    shape = "bfs" if algo["kind"] == BFS else "components"
    return {"kind": "complete", "rows": len(rows), "rounds": rounds,
            "result": domain(f"ade-cycle20-{shape}-result-v1", b"".join(raw(row) + b"\n" for row in rows))}


class Evidence:
    def __init__(self, work, criteria, fixture_lock):
        self.work = work
        self.criteria = criteria
        self.fixture_lock = fixture_lock
        self.cache = {}
        self.cases = {}
        self.requests = {}
        for path in sorted((work / "commands").glob("*.json")):
            record = read(path)
            key = path.stem
            assert record["name"] == key
            self.cases[key] = record
            try:
                self.requests[key] = read(work / "requests" / (key + ".json"))
            except json.JSONDecodeError:
                assert record["exit"] != 0
        self.counts = Counter()
        self.layout = read(work / "root-layout.json")

    def reference(self, request):
        name = Path(request["source_dir"]).name
        algorithm = request["plan"]["algorithm"]
        key = name, raw(request["inventory"]), raw(algorithm)
        if key not in self.cache:
            vertices, edges = graph(self.work / "fixtures" / name, request["inventory"])
            if name in self.fixture_lock:
                assert sha_bytes(raw([vertices, edges])) == self.fixture_lock[name]["graph_sha256"]
                assert [len(vertices), len(edges)] == self.criteria["fixtures"][name]
            rows = result(vertices, edges, algorithm)
            rounds, _ = feedback(vertices, edges, algorithm)
            self.cache[key] = vertices, edges, rows, rounds, expected_footer(rows, algorithm, len(rounds))
        return self.cache[key]

    def cases_and_credits(self):
        for key, case in self.cases.items():
            command = case["argv"][1]
            request = self.requests.get(key)
            ledger = [e["stats"] for e in json_lines(case["stderr"]) if e["event"] == "round"]
            if case["exit"] != 0:
                assert '"kind":"complete"' not in case["stdout"]
                self.counts["refusals_or_kills"] += 1
                if not ledger:
                    continue
            assert request
            vertices, edges, rows, rounds, footer = self.reference(request)
            source_id, plan_id = ids(request)
            if command in ("read", "verify"):
                if case["exit"] == 0:
                    expected = rows + [footer] if command == "read" else [footer]
                    assert case["stdout"].encode() == b"".join(raw(row) + b"\n" for row in expected), "terminal ledger differs"
                    assert len(case["stdout"].encode()) <= request.get("limits", {}).get("output_bytes", 16777216)
                    self.counts["terminal_reads"] += 1
            elif case["exit"] == 0:
                value = json.loads(case["stdout"])
                if command == "explain":
                    assert value[0] == request["plan"] and value[1]["pin"] is None
                    value = value[1]
                else:
                    assert value["terminal"] == footer and value["rounds_executed"] == len(ledger)
                    assert value["edge_work"] == sum(item["edges_examined"] for item in ledger)
                    if ledger:
                        assert selected(case) == value["pin"]
                    else:
                        assert request["expected"] == value["pin"]
                assert value["source"] == source_id and value["plan"] == plan_id
                assert value["vertices"] == len(vertices) and value["directed_edges"] == len(edges)
                assert value["managed_state_bytes"] <= request.get("limits", {}).get("state_bytes", 67108864)
                assert value["retained_bytes"] <= request.get("limits", {}).get("disk_bytes", 268435456)
                assert value["metadata_entries"] <= self.criteria["limits"]["root_entries"]
            start = (request.get("expected") or {}).get("round", 0) if command == "resume" else 0
            for round_num, item in enumerate(ledger, start):
                current, next_rows, proposals, scan = rounds[round_num]
                edges_examined = scan if request.get("mode") == "scan_all" else proposals
                assert item["round"] == round_num and item["feedback_round"] == round_num + 1
                assert item["frontier_rows"] == current and item["next_rows"] == next_rows
                assert (item["edges_examined"], item["proposals"], item["suppressed"]) == (
                    edges_examined, proposals, proposals - next_rows), "round work differs"
                peak = min(request.get("limits", {}).get("credits", 1024), next_rows)
                assert item["credits"] == {"produced": next_rows, "queued": 0, "executing": 0,
                                            "acknowledged": next_rows, "peak_queued": peak,
                                            "peak_in_flight": peak, "producer_open": False}, "credit conservation differs"
                self.counts["rounds"] += 1
                self.counts["feedback_rows"] += next_rows
        self.counts["commands"] += len(self.cases)

    def roots(self, alternate=None):
        roots = (self.work / "roots").iterdir() if alternate is None else [alternate]
        requests = {}
        pins = defaultdict(dict)
        for case, request in ((self.cases[key], req) for key, req in self.requests.items()):
            if not isinstance(request, dict) or "root" not in request:
                continue
            name = Path(request["root"]).name
            if case["argv"][1] == "run" and name not in requests:
                requests[name] = request
            for event in json_lines(case["stderr"]):
                if event["event"] in ("selected", "prepared"):
                    pin = event["pin"]
                    pins[name].setdefault(pin["sequence"], []).append(pin)
        for root in roots:
            if not root.is_dir():
                continue
            anchor_raw = (root / "CYCLE20").read_bytes()
            anchor = json.loads(anchor_raw)
            request = requests["job" if alternate else root.name]
            source, plan = ids(request)
            assert anchor["version"] == 1 and anchor["plan"] == plan and anchor["source"] == source
            chain = domain("ade-cycle20-anchor-v1", anchor_raw)
            name = "job" if alternate else root.name
            layout = self.layout[name]
            files = layout["files"]
            assert set(files) == {file.name for file in root.iterdir()}
            assert all((root / file).stat().st_size == meta["bytes"] for file, meta in files.items())
            assert anchor["location"] == {"device": layout["device"], "directory": layout["inode"],
                                           "lease": files["LEASE20"]["inode"]}
            retained = sum(dict((f["inode"], f["bytes"]) for f in files.values()).values())
            assert len(files) <= self.criteria["limits"]["root_entries"]
            assert retained <= self.criteria["limits"]["disk_bytes"]
            assert {"sequence": 0, "round": 0, "chain": chain} in pins[name][0]
            vertices, edges, _, _, footer = self.reference(request)
            selectors = sorted(root.glob("s20-*"))
            rounds_at = {read(path)["round"] for path in selectors}
            _, states = feedback(vertices, edges, request["plan"]["algorithm"], rounds_at)
            last_round = 0
            for i, selector in enumerate(selectors, 1):
                assert selector.name == f"s20-{i:04}"
                witness = selector.read_bytes()
                witness_name = f"w20-{i:04}"
                assert witness == (root / witness_name).read_bytes()
                assert files[selector.name] == files[witness_name] and files[witness_name]["links"] == 2
                obj = (root / f"o20-{i:04}").read_bytes()
                metadata, snapshot = json.loads(witness), json.loads(obj)
                round_num = metadata["round"]
                assert metadata["sequence"] == i and metadata["version"] == 1 and round_num > last_round
                assert metadata["parent"] == chain and metadata["bytes"] == len(obj)
                assert metadata["object"] == domain("ade-cycle20-object-v1", obj)
                assert snapshot["plan"] == plan and snapshot["source"] == source and snapshot["version"] == 1
                assert snapshot["cut"] == states[round_num], "checkpoint mathematical state differs"
                final = round_num == footer["rounds"]
                assert metadata["terminal"] == final and snapshot["result"] == (footer if final else None)
                chain = domain("ade-cycle20-witness-v1", witness)
                assert {"sequence": i, "round": round_num, "chain": chain} in pins[name][i]
                last_round = round_num
                self.counts["selected_cuts"] += 1
            head = {"sequence": len(selectors), "round": last_round, "chain": chain}
            for key, request in self.requests.items():
                if Path(request.get("root", "")).name != name:
                    continue
                case = self.cases[key]
                if case["exit"] == 0 and case["argv"][1] in ("read", "verify"):
                    assert request["expected"] == head and last_round == footer["rounds"]
            self.counts["roots"] += 1

    def faults(self):
        named = {name.split("-", 1)[1]: case for name, case in self.cases.items()}
        for point in self.criteria["crash_points"]:
            stopped = named["crash-" + point]
            assert stopped["exit"] == -9 and f"cycle20-fault {point} 2\n" in stopped["stderr"]
            candidate = selected(stopped, "prepared") if point in {"selected", "directory_synced"} else selected(stopped)
            resumed = named["crash-resume-" + point]
            assert self.requests[resumed["name"]]["expected"] == candidate
            assert resumed["pid"] != stopped["pid"]
            assert named["crash-not-terminal-" + point]["exit"] != 0
            assert named["crash-terminal-" + point]["exit"] == 0
            self.counts["kill_boundaries"] += 1
        for point in ("before_select", "selected"):
            name = "terminal-crash-" + point
            stopped = named[name]
            assert stopped["exit"] == -9 and f"cycle20-fault {point} 4\n" in stopped["stderr"]
            assert named[name + "-old-pin"]["exit"] != 0 and named[name + "-read"]["exit"] == 0
            if point == "selected":
                assert self.requests[named[name + "-read"]["name"]]["expected"] == selected(stopped, "prepared")
            else:
                assert self.requests[named[name + "-resume"]["name"]]["expected"] == selected(stopped)
            self.counts["terminal_kills"] += 1
        for algo in (BFS, COMPONENTS):
            name = "restart-" + algo
            stopped, resumed = named[name], named[name + "-resume"]
            assert stopped["exit"] == -9 and selected(stopped)["round"] == 2 and resumed["exit"] == 0
            assert len({stopped["pid"], resumed["pid"], named[name + "-terminal"]["pid"]}) == 3
            assert named[name + "-checkpoint-not-result"]["exit"] != 0
            assert named[name + "-terminal-verify"]["exit"] == 0
            assert named[name + "-reread"]["stdout"] == named[name + "-terminal"]["stdout"]
            assert named[name + "-replayed"]["stdout"] == named[name + "-terminal"]["stdout"]
        assert named["stalled-concurrent-reader"]["exit"] == 0
        for label, message in [
            ("stalled-stdout", "stalled stdout deadline"), ("stalled-concurrent-writer", "fenced"),
            ("concurrent-read", "fenced"), ("concurrent-resume", "fenced"),
            ("object-tamper", "digest mismatch"), ("witness-tamper", "parent mismatch"),
            ("selector-rollback", "selector differs"), ("root-clone", "binding changed"),
            ("external-root-file-alias", "external cycle20 file alias"),
            ("round-limit", "rounds before fixed point"), ("deadline", "INCOMPLETE_LIMIT: deadline"),
            ("cancel", "INCOMPLETE_CANCELLED"), ("short-write", "injected short write"),
            ("inode-swap-source_decoded", "graph file identity changed"),
            ("inode-swap-before_round", "graph file identity changed"),
            ("inode-swap-before_select", "graph file identity changed"),
            ("output-limit", "serialized output bytes"),
        ]:
            assert named[label]["exit"] != 0 and message in named[label]["stderr"]

    def timings(self):
        recorded = read(self.work / "performance.json")
        for name, target in self.criteria["performance"]["regimes"].items():
            pairs = recorded[name]["pairs"]
            assert len(pairs) == self.criteria["performance"]["warmups"] + self.criteria["performance"]["pairs"]
            assert pairs[0]["warmup"] and all(not pair["warmup"] for pair in pairs[1:])
            for pair in pairs:
                for mode in ("indexed", "scan_all"):
                    item = pair[mode]
                    run = self.cases[item["case"]]
                    value = json.loads(run["stdout"])
                    request = self.requests[run["name"]]
                    assert request["mode"] == mode and Path(request["source_dir"]).name == target["fixture"]
                    assert item["compute_ns"] == value["compute_ns"] > 0
                    assert item["wall_ns"] == run["elapsed_ns"] > item["compute_ns"]
                    assert item["edge_work"] == value["edge_work"] and item["rounds"] == value["rounds_executed"]
                    assert item["source_bytes"] == value["source_bytes"] and item["retained_bytes"] == value["retained_bytes"]
                    for key in ("cpu_user_ns", "cpu_system_ns", "file_input_blocks", "file_output_blocks"):
                        assert item[key] == run[key] >= 0
                    if "checkpoint_round" in target:
                        assert request["expected"]["round"] == target["checkpoint_round"] and run["argv"][1] == "resume"
            measured = pairs[1:]
            for mode in ("indexed", "scan_all"):
                for key in ("compute_ns", "wall_ns"):
                    values = [p[mode][key] for p in measured]
                    assert recorded[name]["stats"][f"{mode}_{key}"] == {
                        "median": statistics.median(values), "stdev": statistics.pstdev(values)}
            scan_work = sum(p["scan_all"]["edge_work"] for p in measured)
            index_work = sum(p["indexed"]["edge_work"] for p in measured)
            work_ratio = scan_work / index_work
            time_ratio = (statistics.median(p["indexed"]["compute_ns"] for p in measured) /
                          statistics.median(p["scan_all"]["compute_ns"] for p in measured))
            assert recorded[name]["scan_to_index_work"] == work_ratio >= target["min_scan_to_index_work"]
            assert recorded[name]["index_to_scan_median"] == time_ratio <= target["max_index_to_scan_median"]
            self.counts["measured_pairs"] += len(measured)


def sha_bytes(data):
    return hashlib.sha256(data).hexdigest()


def package(work, archive, manifest):
    files = sorted(p for p in work.rglob("*") if p.is_file())
    inventory = {p.relative_to(work).as_posix(): sha(p) for p in files}
    with archive.open("wb") as file, gzip.GzipFile(filename="", mode="wb", fileobj=file, mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w") as bundle:
            for path in files:
                assert not path.is_symlink()
                with path.open("rb") as raw_file:
                    item = tarfile.TarInfo(path.relative_to(work).as_posix())
                    item.size = path.stat().st_size
                    bundle.addfile(item, raw_file)
    manifest.write_text(json.dumps({"archive_sha256": sha(archive), "files": inventory}, indent=2) + "\n")


def unpack(archive, manifest, work):
    identity = read(manifest)
    assert sha(archive) == identity["archive_sha256"]
    files = identity["files"]
    with tarfile.open(archive, "r:gz") as bundle:
        for member in bundle:
            name = PurePosixPath(member.name)
            assert member.isfile() and not name.is_absolute() and ".." not in name.parts
            target = work / name
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("xb") as file:
                shutil.copyfileobj(bundle.extractfile(member), file)
            assert sha(target) == files[member.name]
    assert len(list(p for p in work.rglob("*") if p.is_file())) == len(files)


def validate(work, repo=None, binary=None):
    provenance = read(work / "provenance.json")
    criteria = read(work / "criteria.json")
    lock = read(work / "fixtures.lock.json")
    assert sha(work / "criteria.json") == provenance["criteria_sha256"]
    assert sha(work / "fixtures.lock.json") == provenance["fixture_lock_sha256"]
    assert provenance["baseline"] == criteria["baseline"] and provenance["baseline_tree"] == criteria["baseline_tree"]
    assert provenance["phase18"] == criteria["phase18_disposition"]
    assert provenance["source_sha256"] == sha_bytes(raw(provenance["source"]))
    if repo:
        for name, fingerprint in provenance["source"].items():
            assert sha(repo / name) == fingerprint, f"source differs: {name}"
    if binary:
        assert sha(binary) == provenance["binary_sha256"]
    evidence = Evidence(work, criteria, lock)
    evidence.cases_and_credits()
    evidence.roots()
    evidence.faults()
    evidence.timings()
    volume = work / "enospc"
    proof = read(volume / "enospc.json")
    assert proof["available_bytes"] == 0 and proof["binary_sha256"] == provenance["binary_sha256"]
    enospc = Evidence(volume, criteria, lock)
    failure = enospc.cases[proof["failed_case"]]
    assert failure["exit"] == 1 and "No space left on device (os error 28)" in failure["stderr"]
    assert "cycle20-fault enospc_continue 2" in failure["stderr"] and selected(failure)["round"] == 1
    assert enospc.cases[proof["previous_case"]]["exit"] == 1
    assert enospc.cases[proof["terminal_case"]]["exit"] == 0
    enospc.cases_and_credits()
    enospc.roots(volume / "selected-root")
    return evidence.counts + enospc.counts


def tamper(work, storage):
    for label, suffix, change in [
        ("forged terminal rows", "mixed-bfs-0-terminal", lambda c: c.update(
            stdout=c["stdout"].replace('"distance":0', '"distance":9', 1))),
        ("forged feedback credit", "mixed-bfs-0", lambda c: c.update(
            stderr=c["stderr"].replace('"acknowledged":2', '"acknowledged":1', 1))),
    ]:
        path = next((work / "commands").glob(f"????-{suffix}.json"))
        previous = path.read_bytes()
        case = json.loads(previous)
        change(case)
        assert raw(case) != raw(json.loads(previous))
        path.write_bytes(raw(case))
        try:
            archive, manifest = storage / "tampered.tar.gz", storage / "tampered.json"
            package(work, archive, manifest)
            with tempfile.TemporaryDirectory(dir=storage) as extracted:
                target = Path(extracted)
                unpack(archive, manifest, target)
                try:
                    validate(target)
                except AssertionError as error:
                    assert str(error) in {"terminal ledger differs", "credit conservation differs"}, str(error)
                    print(f"rehashed {label}: refused ({error})")
                else:
                    raise AssertionError(f"accepted rehashed {label}")
        finally:
            path.write_bytes(previous)


def main():
    if not __debug__:
        raise RuntimeError("run verifier with assertions enabled")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--pack", type=Path)
    parser.add_argument("--repo", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--tamper", action="store_true")
    args = parser.parse_args()
    if args.pack:
        package(args.pack, args.archive, args.manifest)
        return
    with tempfile.TemporaryDirectory(prefix="ade20-offline-") as directory:
        storage = Path(directory)
        work = storage / "extracted"
        work.mkdir()
        unpack(args.archive, args.manifest, work)
        print(json.dumps(dict(validate(work, args.repo, args.binary)), sort_keys=True))
        if args.tamper:
            tamper(work, storage)


if __name__ == "__main__":
    main()
