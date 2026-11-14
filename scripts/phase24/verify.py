"""Verify a detached Phase 24 campaign, with separate release and hosted CI pins."""
import argparse
import json
import math
import statistics
import struct
import uuid
from pathlib import Path

from blake3 import blake3
import pyarrow.parquet as pq

from fixtures import CONTRACT, CRITERIA, HERE, inventory
from oracle import graph, vector
from provenance import git, load, members, sha, source_inventory


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def packed(data):
    return json.dumps(data, separators=(",", ":")).encode()


def hash_domain(domain, data):
    return blake3(domain.encode() + b"\0" + data).hexdigest()


def raw_digest(path):
    return list(blake3(path.read_bytes()).digest())


def verify_numbers(observed, wanted):
    require(observed["ids"] == wanted["ids"], "vector row identity or multiplicity")
    values, reference = observed["values"], wanted["values"]
    require(len(values) == len(reference), "vector result cardinality")
    tolerance = CRITERIA["vector_comparison"]
    for value, expected in zip(values, reference):
        require(math.isfinite(value) and math.isclose(
            value, expected, abs_tol=tolerance["absolute_tolerance"], rel_tol=tolerance["relative_tolerance"]),
            "numerical oracle bound")
        if expected == 0 and value == 0:
            require(math.copysign(1, expected) == math.copysign(1, value), "signed-zero mismatch")


def vector_fixture(name):
    if name.startswith("measure-") or name == "stalled-reader":
        return "large"
    if name in ("prep", "split", "bad-model", "bad-source", "too-many-rows", "cancelled", "kernel-panic", "result-cap", "map-cap"):
        return "combined"
    return "single" if name == "dimension-one" else name


def verify_vector(root, directory, name, request, process, provenance, attempts):
    fixture = vector_fixture(name)
    source = root / "fixtures" / f"{fixture}.parquet"
    model = root / "fixtures" / ("model-1.json" if name == "dimension-one" else "model.json")
    require(Path(request["path"]).name == source.name, "vector fixture identity")
    require(Path(request["model_path"]).name == model.name, "model path identity")
    require(request["model_pin"] == ([0] * 32 if name == "bad-model" else raw_digest(model)), "model pin identity")
    require(request["source"] == {
        "bytes": source.stat().st_size,
        "rows": 32769 if name == "too-many-rows" else pq.read_metadata(source).num_rows,
        "digest": [0] * 32 if name == "bad-source" else raw_digest(source),
    }, "source pin identity")
    require(request["executable"] == provenance["binary_blake3"], "executable identity")
    case = CONTRACT["vector_cases"].get(name, {})
    op = name if name in ("score", "prep") else ("score" if name == "overflow" else "combined")
    if name.startswith("measure-vector-identity-"):
        op = "identity"
    require(request["operation"] == op and request["dimensions"] == (1 if name == "dimension-one" else 4)
            and request["batch_rows"] == case.get("batch", 256)
            and request.get("cancel", False) == (name == "cancelled"), "vector operation identity")
    require(request.get("panic", False) == (name == "kernel-panic")
            and request.get("result_bytes") == (1 if name == "result-cap" else None)
            and request.get("batch_bytes") == (1 if name == "map-cap" else None)
            and request.get("output_timeout_ms") == (50 if name == "stalled-reader" else None),
            "vector fault request identity")
    if process["exit"]:
        return None
    observed = load(directory / "stdout")
    parameters = load(model)
    require(parameters == ({"weights": [2.0], "bias": 0.25} if name == "dimension-one" else CONTRACT["model"]),
            "independent model parameter identity")
    wanted = vector(source, parameters, op, request["dimensions"])
    verify_numbers(observed, wanted)
    if process["operation"] == "reference":
        return None
    rows = len(wanted["ids"])
    require(rows == case.get("rows", 32768), "independent vector row count")
    metrics = observed["metrics"]
    require([m[:3] for m in metrics] == [["parquet-scan", 0, rows],
            ["map-batches", rows, rows], ["schema-adapter", rows, rows]], "vector native route")
    require(0 <= observed["model_read_ns"] <= observed["execute_ns"] <= process["elapsed_ns"]
            and all(0 <= m[3] <= observed["execute_ns"] for m in metrics), "vector native timings")
    receipt = observed["receipt"]
    add_attempt(receipt, attempts)
    require(receipt["executable"] == request["executable"] and receipt["source"] == raw_digest(source)
            and receipt["model"] == raw_digest(model), "vector receipt binding")
    require(receipt["route"] == "scan/map-batches/project" and receipt["extension_version"] == 1
            and receipt["batch_rows"] == request["batch_rows"]
            and 0 <= receipt["peak_map_array_bytes"] <= 131072
            and (rows == 0 or receipt["peak_map_array_bytes"] > 0)
            and 0 <= observed["managed_peak"] <= 16777216, "vector receipt route or ledger")
    result = blake3(b"ade-extension-result-v1\0")
    width = 5 if op == "prep" else 1
    for i, ident in enumerate(observed["ids"]):
        result.update(struct.pack("<Q" + "d" * width, ident, *observed["values"][i * width:(i + 1) * width]))
    require(receipt["result"] == list(result.digest()), "vector result digest")
    return receipt


def add_attempt(receipt, attempts):
    attempt = uuid.UUID(receipt["attempt"])
    require(attempt.version == 4 and attempt not in attempts, "attempt collision or format")
    attempts.add(attempt)


def graph_fixture(name):
    if name.startswith("measure-graph-"):
        return "chain"
    if name.startswith("graph-"):
        return name.split("-")[1]
    if name in ("foreign-edge-run", "duplicate-vertex-run"):
        return name[:-4]
    return "mixed"


def check_graph_request(root, name, request, fixture):
    require(Path(request["source_dir"]).name == fixture.name, "graph fixture identity")
    plan = request["plan"]
    require(request["version"] == 1 and plan == {
        "version": 1, "algorithm": {"kind": "weak_components_min_label_v1"},
        "nodes": ["expand", "reduce", "delay"],
        "arcs": [{"from": 0, "to": 1, "schema": "label_candidate", "delay": 0},
                 {"from": 1, "to": 2, "schema": "label_frontier", "delay": 0},
                 {"from": 2, "to": 0, "schema": "label_frontier", "delay": 1}]},
        "graph plan identity")
    require(request["trace"] and request["checkpoint_every"] == (0 if name.startswith("measure-graph-native") else 1)
            and request.get("limits", {}) == ({"rounds": 1} if name.startswith("incomplete") else {}),
            "graph operational request identity")
    for role in ("vertices", "edges"):
        path = fixture / f"{role}.parquet"
        require(request["inventory"][role] == [{
            "name": path.name, "bytes": path.stat().st_size, "rows": pq.read_metadata(path).num_rows,
            "blake3": "0" * 64 if name == "corrupt-source-run" and role == "vertices"
            else bytes(raw_digest(path)).hex()}], "graph source pin identity")
    root_name = Path(request["root"]).name
    if name.startswith(("graph-", "measure-graph-")):
        wanted_root = name.removesuffix("-run").removesuffix("-terminal")
    elif name.startswith(("foreign-edge", "duplicate-vertex", "corrupt-source", "corrupt-terminal", "incomplete")):
        wanted_root = name.removesuffix("-run")
    else:
        wanted_root = "graph-mixed"
    require(root_name == wanted_root, "graph root ownership")
    return root / "roots" / root_name


def trace(directory):
    return [json.loads(line) for line in (directory / "stderr").read_bytes().splitlines()
            if line.startswith(b"{")]


def selected_terminal(root, run, request, wanted):
    events = trace(run)
    rounds = [event["stats"] for event in events if event.get("event") == "round"]
    selected = [event["pin"] for event in events if event.get("event") == "selected"]
    summary = events[-1]
    require(bool(rounds) and len(rounds) == summary["rounds_executed"]
            and selected[-1] == request["expected"] == summary["pin"]
            and selected[-1]["round"] == len(rounds), "native cyclic trace or independently selected pin")
    require(rounds[-1]["next_rows"] == 0 and (not wanted["terminal"] or rounds[0]["frontier_rows"] == len(wanted["terminal"])),
            "graph frontier terminal state")
    for i, entry in enumerate(rounds):
        require(entry["round"] == i and entry["feedback_round"] == i + 1
                and (i == 0 or entry["frontier_rows"] == rounds[i - 1]["next_rows"]),
                "native graph feedback sequence")
    anchor = (root / "CYCLE20").read_bytes()
    chain = hash_domain("ade-cycle20-anchor-v1", anchor)
    require(json.loads(anchor)["source"] == summary["source"] and json.loads(anchor)["plan"] == summary["plan"],
            "graph root plan/source anchor")
    require(selected[0] == {"sequence": 0, "round": 0, "chain": chain}, "graph genesis pin")
    snapshot = None
    for i, pick in enumerate(selected[1:], 1):
        suffix = f"{i:04}"
        raw = (root / f"w20-{suffix}").read_bytes()
        obj = (root / f"o20-{suffix}").read_bytes()
        witness = json.loads(raw)
        snapshot = json.loads(obj)
        require(raw == (root / f"s20-{suffix}").read_bytes() and witness["parent"] == chain
                and witness["object"] == hash_domain("ade-cycle20-object-v1", obj)
                and witness["bytes"] == len(obj), "graph selected object chain")
        chain = hash_domain("ade-cycle20-witness-v1", raw)
        require({"sequence": i, "round": witness["round"], "chain": chain} == pick, "graph selected witness pin")
    require(snapshot is not None and snapshot["result"] == summary["terminal"]
            and snapshot["cut"]["frontier"] == [] and snapshot["cut"]["in_flight"] == 0,
            "graph terminal checkpoint")
    labels = [row["component_min_vertex"] for row in wanted["terminal"]]
    require(snapshot["cut"]["values"] == labels, "graph checkpoint labels")
    require(summary["vertices"] == len(labels), "graph source row count")
    if len(labels) >= 9:
        require(len(rounds) > 1, "missing multi-round graph execution")
    return summary


def verify_graph(root, directory, name, request, process, attempts):
    fixture = root / "fixtures" / graph_fixture(name)
    native = request["native"] if "native" in request else request
    selected_root = check_graph_request(root, name, native, fixture)
    if "native" in request:
        require(request["stage_bytes"] == (12 if name == "stage-cap" else 1048576), "graph stage cap identity")
    if process["exit"] or process["operation"] == "graph-run":
        if name in ("stage-kill", "stage-change", "stage-source-change"):
            verify_stage_ownership(directory, request, process)
            require(bool(process["stage_after"]) == (name == "stage-kill"), "stage fault cleanup")
        if name == "stage-source-change":
            changed = root / "roots" / "source-drift-fixture" / "mixed" / "vertices.parquet"
            old = bytearray((fixture / "vertices.parquet").read_bytes())
            old[8] ^= 1
            require(changed.read_bytes() == old, "graph postflight source drift witness")
        return
    wanted = graph(fixture)
    if process["operation"] == "reference":
        require(load(directory / "stdout") == wanted, "graph reference sample")
        return
    run_name = "graph-mixed" if name == "stage-restart" else name.removesuffix("-terminal")
    summary = selected_terminal(selected_root, root / "cases" / (run_name + "-run"), native, wanted)
    if process["operation"] == "graph-read":
        raw = (directory / "stdout").read_bytes().splitlines(keepends=True)
        require(raw and raw[-1].endswith(b"\n") and [json.loads(row) for row in raw[:-1]] == wanted["terminal"],
                "graph terminal serialization or rows")
        terminal = json.loads(raw[-1])
        require(terminal == summary["terminal"] and terminal["kind"] == "complete"
                and terminal["rows"] == len(raw) - 1
                and terminal["result"] == hash_domain("ade-cycle20-components-result-v1", b"".join(raw[:-1])),
                "fabricated or invalid graph terminal footer")
        return
    data = load(directory / "stdout")
    require(data["components"] == wanted["components"], "graph union-find oracle")
    result = data["receipt"]
    add_attempt(result, attempts)
    require(result["terminal"] == summary["terminal"] and result["selected"] == native["expected"]
            and result["plan"] == summary["plan"] and result["source"] == summary["source"], "graph terminal receipt binding")
    require(result["result"] == list(blake3(packed(wanted["components"])).digest()), "graph result digest")
    require(result["logical"] == list(blake3(packed(["component_sizes_v1", result["source"], result["plan"]])).digest()),
            "graph logical identity")
    rows, groups = len(wanted["terminal"]), len(wanted["components"])
    require(result["stage_rows"] == rows and 12 <= result["stage_bytes"] <= result["stage_bytes_cap"] == request["stage_bytes"]
            and result["aggregate_operators"] == [["parquet-scan", 0, rows], ["hash-aggregate", rows, groups],
                                                   ["schema-adapter", groups, groups]]
            and 0 <= result["aggregate_peak_managed_bytes"] <= 16777216, "graph native aggregate route or ledger")
    require(process["stage_after"] == process["stage_before"], "stage ownership or cleanup")
    if not name.startswith("measure-"):
        verify_stage_ownership(directory, request, process)
        stage = directory / "stage.parquet"
        require(raw_digest(stage) == result["stage"] and stage.stat().st_size == result["stage_bytes"],
                "graph stage digest")
        require(pq.read_table(stage).to_pydict() == {"key": [row["component_min_vertex"] for row in wanted["terminal"]],
                                                   "value_i64": [1] * rows}, "graph reduction stage content")


def verify_stage_ownership(directory, request, process):
    reached = process["reached"]
    require(reached is not None, "missing reached stage witness")
    stage = Path(reached["stage_file"])
    require(stage.parent.parent == Path(request["stage_parent"]) and stage.name == "labels.parquet"
            and stage.parent.name.startswith("ade24-stage-")
            and f"ade24 before aggregate {stage}" in (directory / "stderr").read_text(), "forged stage ownership")
    evidence = directory / "stage.parquet"
    if evidence.exists():
        require(reached["sha256"] == sha(evidence) and reached["bytes"] == evidence.stat().st_size, "stage witness bytes")
    else:
        require(process["stage_after"] == {str(stage.relative_to(stage.parent.parent)): reached["sha256"]},
                "killed private stage inventory")


def expected_cases(provenance):
    contract = {name: ("vector", case["exit"], case.get("stage")) for name, case in CONTRACT["vector_cases"].items()}
    for name, case in CONTRACT["graph_cases"].items():
        if name in ("mixed", "reordered", "empty", "isolated", "single"):
            contract.update({f"graph-{name}-run": ("graph-run", 0, None),
                             f"graph-{name}-terminal": ("graph-read", 0, None),
                             f"graph-{name}": ("graph-reduce", 0, None)})
        elif name in ("foreign-edge", "duplicate-vertex", "corrupt-source"):
            contract[f"{name}-run"] = ("graph-run", 1, case["stage"])
        elif name != "stage-enospc" or provenance["enospc"]:
            contract[name] = ("graph-reduce", case["exit"], case.get("stage"))
    contract["incomplete-run"] = ("graph-run", 1, "rounds before fixed point")
    contract["corrupt-terminal-run"] = ("graph-run", 0, None)
    if provenance["benchmarks"]:
        for i in range(6):
            for control in CRITERIA["measurements"]["controls"]:
                name = f"measure-{control}-{i}"
                op = "reference" if control.endswith("reference") else ("vector" if control.startswith("vector") else "graph-reduce")
                contract[name] = (op, 0, None)
                if control == "graph-native":
                    contract[name + "-run"] = ("graph-run", 0, None)
    return contract


def check_measurements(root, processes):
    measured = load(root / "measurements.json")
    require(set(measured) == set(CRITERIA["measurements"]["controls"]), "measurement inventory")
    for name, result in measured.items():
        samples = []
        for i in range(1, 6):
            case = f"measure-{name}-{i}"
            total = processes[case]["elapsed_ns"]
            if name == "graph-native":
                total += processes[case + "-run"]["elapsed_ns"]
            samples.append(total)
        require(result["samples_ns"] == samples and result["p50_ns"] == statistics.median(samples)
                and result["p95_ns"] == max(samples)
                and math.isclose(result["population_stdev_ns"], statistics.pstdev(samples), rel_tol=1e-12),
                "altered measurement samples or summary")


def historical(root, seal_pin):
    require(sha(root / "seal.json") == seal_pin, "detached evidence pin")
    require(load(root / "seal.json") == members(root), "archive member inventory")
    require(inventory(root / "fixtures") == load(HERE / "fixtures.lock.json"), "frozen fixtures or model changed")
    provenance = load(root / "provenance.json")
    require(provenance["baseline"] == CRITERIA["baseline"] and provenance["source"]["scripts/phase24/criteria.json"]
            == sha(HERE / "criteria.json") and provenance["source"]["scripts/phase24/case-contract.json"]
            == sha(HERE / "case-contract.json"), "criteria or source baseline")
    contract = expected_cases(provenance)
    require(set(contract) == {x.name for x in (root / "cases").iterdir()}, "required case inventory")
    processes, receipts, attempts = {}, {}, set()
    for name, (operation, exit_code, refusal) in contract.items():
        directory = root / "cases" / name
        clock = load(directory / "process.json")
        request = load(directory / "request.json")
        require(clock["name"] == name and clock["operation"] == operation
                and Path(clock["argv"][-1]).parent.name == name, "case label or execution route")
        actual = clock["argv"][2:] if clock["argv"][:2] == ["/usr/bin/time", "-l"] else clock["argv"]
        require((Path(actual[1]).name == "oracle.py" if operation == "reference"
                 else actual[:2] == [provenance["binary_path"], operation])
                and Path(actual[-1]).name == "request.json", "process executable or request identity")
        require(clock["exit"] == exit_code, "required exit disposition")
        require(clock["end_ns"] - clock["start_ns"] == clock["elapsed_ns"] > 0 and clock["pid"] > 0,
                "raw process clock or identity")
        if exit_code:
            raw = (directory / "stdout").read_bytes()
            withheld = bool(raw) and not raw.endswith(b"\n") if name == "stalled-reader" else not raw
            require(withheld and refusal in (directory / "stderr").read_text(),
                    "failure stage or leaked output")
        processes[name] = clock
        if "operation" in request:
            receipts[name] = verify_vector(root, directory, name, request, clock, provenance, attempts)
        else:
            verify_graph(root, directory, name, request, clock, attempts)
    lhs, rhs = receipts["combined"], receipts["split"]
    require(all(lhs[field] == rhs[field] for field in ("logical", "result", "source", "plan", "model"))
            and lhs["attempt"] != rhs["attempt"], "partition-invariant plan/results")
    if provenance["benchmarks"]:
        check_measurements(root, processes)
    if provenance["enospc"]:
        require(load(root / "volume" / "full.json") == {"errno": 28, "available": 0},
                "no actual ENOSPC witness")
        commands = load(root / "volume" / "commands.json")
        require([entry["argv"][1] for entry in commands] == ["create", "attach", "detach"]
                and all(entry["exit"] == 0 for entry in commands), "disposable-volume lifecycle")
    return provenance, len(contract), len(attempts)


def current(root, provenance, seal_pin, release, release_pin, ci, ci_pin, binary):
    require(all((release, release_pin, ci, ci_pin, binary)), "external release and CI pins and executable required")
    require(sha(release) == release_pin and sha(ci) == ci_pin, "detached release or CI receipt pin")
    attestation, hosted = load(release), load(ci)
    head, tree = git(root, "rev-parse", "HEAD"), git(root, "rev-parse", "HEAD^{tree}")
    require(not git(root, "status", "--porcelain=v1"), "current-source requires clean checkout")
    require(attestation == {"commit": head, "tree": tree, "seal_sha256": seal_pin, "ci_sha256": ci_pin,
                            "binary_sha256": provenance["binary_sha256"]}, "exact release commit/tree identity")
    require(source_inventory(root) == provenance["source"] and sha(binary) == provenance["binary_sha256"]
            and provenance["benchmarks"] and provenance["enospc"], "matching source, executable and full campaign")
    require(hosted.get("head_sha") == head and hosted.get("name") == "ci" and hosted.get("conclusion") == "success"
            and hosted.get("status") == "completed" and hosted.get("id", 0) > 0 and hosted.get("jobs") == {
                "rust": "success", "msrv": "success", "phase22-interop": "success", "phase23-local": "success"},
            "matching completed hosted CI receipt")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--mode", choices=("historical-archive", "current-source"), required=True)
    parser.add_argument("--seal-pin", required=True)
    parser.add_argument("--release", type=Path)
    parser.add_argument("--release-pin")
    parser.add_argument("--ci", type=Path)
    parser.add_argument("--ci-pin")
    parser.add_argument("--binary", type=Path)
    args = parser.parse_args()
    provenance, cases, attempts = historical(args.archive, args.seal_pin)
    if args.mode == "current-source":
        current(HERE.parents[1], provenance, args.seal_pin, args.release,
                args.release_pin, args.ci, args.ci_pin, args.binary)
    print(f"{args.mode}: {cases} observations and {attempts} checked attempts")
