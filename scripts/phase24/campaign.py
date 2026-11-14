"""Run pinned native cases and matched subprocess comparisons into a fresh directory."""
import argparse
import errno
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

from blake3 import blake3
import pyarrow.parquet as pq

from fixtures import CONTRACT, CRITERIA, HERE, generate, inventory
from provenance import dump, git, load, members, sha, source_inventory


def raw_digest(path):
    return blake3(path.read_bytes()).digest()


def source_pin(path):
    return {"digest": list(raw_digest(path)), "bytes": path.stat().st_size,
            "rows": pq.read_metadata(path).num_rows}


def graph_plan():
    return {"version": 1, "algorithm": {"kind": "weak_components_min_label_v1"},
            "nodes": ["expand", "reduce", "delay"],
            "arcs": [{"from": 0, "to": 1, "schema": "label_candidate", "delay": 0},
                     {"from": 1, "to": 2, "schema": "label_frontier", "delay": 0},
                     {"from": 2, "to": 0, "schema": "label_frontier", "delay": 1}]}


def pin(stderr):
    observed = [json.loads(line) for line in stderr.read_bytes().splitlines() if line.startswith(b"{")]
    cuts = [entry["pin"] for entry in observed if entry.get("event") == "selected"]
    if not cuts:
        raise ValueError("no observed selected cut")
    return cuts[-1]


def layout(directory):
    return {path.relative_to(directory).as_posix(): sha(path)
            for path in sorted(directory.rglob("*")) if path.is_file()}


def measure(samples):
    ordered = sorted(samples)
    return {"p50_ns": statistics.median(samples), "p95_ns": ordered[-1],
            "population_stdev_ns": statistics.pstdev(samples)}


class Campaign:
    def __init__(self, output, binary):
        self.output = output
        self.binary = binary
        self.executable = list(raw_digest(binary))
        self.fixtures = output / "fixtures"
        self.cases = output / "cases"
        self.roots = output / "roots"
        self.cases.mkdir(parents=True)
        self.roots.mkdir()
        generate(self.fixtures)
        if inventory(self.fixtures) != load(HERE / "fixtures.lock.json"):
            raise ValueError("generated files diverge from frozen fixture lock")

    def subprocess(self, name, operation, request, expected=0, fault=None):
        case = self.cases / name
        case.mkdir()
        request_path = case / "request.json"
        dump(request_path, request)
        out, err = case / "stdout", case / "stderr"
        if operation == "reference":
            argv = [sys.executable, str(HERE / "oracle.py"), str(request_path)]
        else:
            argv = [str(self.binary), operation, str(request_path)]
        if name.startswith("measure-") and platform.system() == "Darwin":
            argv = ["/usr/bin/time", "-l", *argv]
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        stage = request.get("stage_parent")
        before = layout(Path(stage)) if stage else {}
        if fault:
            env["ADE24_TEST_STAGE_CONTINUE"] = str(case / "continue")
        started = time.monotonic_ns()
        reached = None
        with out.open("wb") as stdout, err.open("wb") as stderr:
            pipe_stall = name == "stalled-reader"
            with subprocess.Popen(argv, stdout=subprocess.PIPE if pipe_stall else stdout, stderr=stderr, env=env) as proc:
                if fault:
                    for _ in range(2500):
                        matches = [line for line in err.read_text().splitlines()
                                   if line.startswith("ade24 before aggregate ")]
                        if matches:
                            stage_file = Path(matches[0].removeprefix("ade24 before aggregate "))
                            reached = {"stage_file": str(stage_file), "sha256": sha(stage_file),
                                       "bytes": stage_file.stat().st_size}
                            if fault == "kill":
                                proc.kill()
                            else:
                                shutil.copyfile(stage_file, case / "stage.parquet")
                                if fault == "change":
                                    image = bytearray(stage_file.read_bytes())
                                    image[8] ^= 1
                                    stage_file.write_bytes(image)
                                elif fault == "source-change":
                                    source = Path(request["native"]["source_dir"]) / "vertices.parquet"
                                    raw = bytearray(source.read_bytes())
                                    raw[8] ^= 1
                                    source.write_bytes(raw)
                                (case / "continue").touch()
                            break
                        if proc.poll() is not None:
                            break
                        time.sleep(0.004)
                    if reached is None:
                        proc.kill()
                try:
                    exit_code = proc.wait(timeout=15 if pipe_stall else 130)
                    if pipe_stall:
                        stdout.write(proc.stdout.read(4 * 1024 * 1024))
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                    raise
        ended = time.monotonic_ns()
        event = {"name": name, "operation": operation, "argv": argv, "exit": exit_code,
                 "pid": proc.pid, "start_ns": started, "end_ns": ended,
                 "elapsed_ns": ended - started, "stage_before": before,
                 "stage_after": layout(Path(stage)) if stage else {}, "reached": reached}
        dump(case / "process.json", event)
        if out.stat().st_size > 4 * 1024 * 1024 or err.stat().st_size > 4 * 1024 * 1024:
            raise ValueError(f"{name}: excessive output")
        if exit_code != expected or (expected and out.stat().st_size and not pipe_stall):
            raise ValueError(f"{name}: expected exit {expected}, got {exit_code}: {err.read_text()}")
        return event

    def vector_request(self, fixture="combined", operation="combined", batch_rows=256):
        path = self.fixtures / (fixture + ".parquet")
        model_path = self.fixtures / "model.json"
        return {"source": source_pin(path), "path": str(path), "executable": self.executable,
                "model_path": str(model_path), "model_pin": list(raw_digest(model_path)),
                "operation": operation, "dimensions": 4, "batch_rows": batch_rows}

    def vector_cases(self):
        for name, case in CONTRACT["vector_cases"].items():
            fixture = "combined" if name in ("prep", "split", "bad-model", "bad-source",
                                               "too-many-rows", "cancelled", "kernel-panic", "result-cap", "map-cap") else name
            if name == "stalled-reader":
                fixture = "large"
            elif name == "dimension-one":
                fixture = "single"
            op = name if name in ("prep", "score") else ("score" if name == "overflow" else "combined")
            request = self.vector_request(fixture, op, case.get("batch", 256))
            if name == "dimension-one":
                model = self.fixtures / "model-1.json"
                request.update(dimensions=1, model_path=str(model), model_pin=list(raw_digest(model)))
            elif name == "bad-model":
                request["model_pin"] = [0] * 32
            elif name == "bad-source":
                request["source"]["digest"] = [0] * 32
            elif name == "too-many-rows":
                request["source"]["rows"] = 32769
            elif name == "cancelled":
                request["cancel"] = True
            if name == "kernel-panic":
                request["panic"] = True
            elif name == "stalled-reader":
                request["output_timeout_ms"] = 50
            elif name == "result-cap":
                request["result_bytes"] = 1
            elif name == "map-cap":
                request["batch_bytes"] = 1
            self.subprocess(name, "vector", request, case["exit"])

    def graph_request(self, name, fixture):
        data = self.fixtures / fixture
        graph_inventory = {}
        for role in ("vertices", "edges"):
            path = data / (role + ".parquet")
            graph_inventory[role] = [{"name": path.name, "bytes": path.stat().st_size,
                                      "rows": pq.read_metadata(path).num_rows,
                                      "blake3": raw_digest(path).hex()}]
        return {"version": 1, "plan": graph_plan(), "inventory": graph_inventory,
                "source_dir": str(data), "root": str(self.roots / name), "checkpoint_every": 1,
                "trace": True}

    def run_graph(self, name, request, expected=0):
        self.subprocess(f"{name}-run", "graph-run", request, expected)
        return pin(self.cases / f"{name}-run" / "stderr") if not expected else None

    def graph_reduce(self, name, request, expected=0, cap=1024 * 1024, fault=None, parent=None):
        if parent is None:
            parent = self.roots / f"{name}-stage"
            parent.mkdir()
        return self.subprocess(name, "graph-reduce", {"native": request, "stage_parent": str(parent),
                                                      "stage_bytes": cap}, expected, fault)

    def graph_cases(self, enospc):
        for name in ("mixed", "reordered", "empty", "isolated", "single"):
            request = self.graph_request(f"graph-{name}", name)
            request["expected"] = self.run_graph(f"graph-{name}", request)
            self.subprocess(f"graph-{name}-terminal", "graph-read", request)
            self.graph_reduce(f"graph-{name}", request, fault="observe")
        good = load(self.cases / "graph-mixed-terminal" / "request.json")
        self.graph_reduce("wrong-pin", dict(good, expected=dict(good["expected"], chain="0" * 64)), 1)
        self.graph_reduce("missing-pin", dict(good, expected=None), 1)
        self.graph_reduce("stage-cap", good, 1, cap=12)
        self.graph_reduce("stage-kill", good, -9, fault="kill")
        self.graph_reduce("stage-change", good, 1, fault="change")
        drift_graph = self.roots / "source-drift-fixture" / "mixed"
        shutil.copytree(self.fixtures / "mixed", drift_graph)
        self.graph_reduce("stage-source-change", dict(good, source_dir=str(drift_graph)), 1, fault="source-change")
        self.graph_reduce("stage-restart", good, fault="observe", parent=self.roots / "stage-kill-stage")
        for name in ("foreign-edge", "duplicate-vertex"):
            request = self.graph_request(name, name)
            self.run_graph(name, request, 1)
        request = self.graph_request("incomplete", "mixed")
        request["limits"] = {"rounds": 1}
        self.run_graph("incomplete", request, 1)
        request["expected"] = pin(self.cases / "incomplete-run" / "stderr")
        self.graph_reduce("incomplete", request, 1)
        corrupt = self.graph_request("corrupt-source", "mixed")
        corrupt["inventory"]["vertices"][0]["blake3"] = "0" * 64
        self.run_graph("corrupt-source", corrupt, 1)
        request = self.graph_request("corrupt-terminal", "mixed")
        request["expected"] = self.run_graph("corrupt-terminal", request)
        checkpoint = Path(request["root"]) / f"o20-{request['expected']['sequence']:04}"
        data = bytearray(checkpoint.read_bytes())
        data[len(data) // 2] ^= 1
        checkpoint.write_bytes(data)
        self.graph_reduce("corrupt-terminal", request, 1)
        if enospc:
            self.disk_full(good)

    def disk_full(self, request):
        commands = []
        directory = self.output / "volume"
        directory.mkdir()
        image = directory / "phase24.dmg"
        mount = directory / "mount"
        mount.mkdir()

        def command(*argv):
            child = subprocess.run(argv, capture_output=True, text=True, timeout=60)
            commands.append({"argv": argv, "exit": child.returncode,
                             "stdout": child.stdout, "stderr": child.stderr})
            dump(directory / "commands.json", commands)
            child.check_returncode()

        command("hdiutil", "create", "-size", "32m", "-fs", "HFS+", "-volname", "ADE24", str(image))
        command("hdiutil", "attach", "-nobrowse", "-mountpoint", str(mount), str(image))
        try:
            if not os.path.ismount(mount) or mount.stat().st_dev == directory.stat().st_dev:
                raise ValueError("refusing to fill an unverified disposable filesystem")
            if os.statvfs(mount).f_bavail * os.statvfs(mount).f_frsize > 32 * 1024 * 1024:
                raise ValueError("refusing to fill a larger filesystem")
            failure = None
            stage_parent = mount / "stage"
            stage_parent.mkdir()
            with (mount / "filler").open("wb", buffering=0) as filler:
                for size in (1024 * 1024, 4096, 1):
                    while True:
                        try:
                            filler.write(bytes(size))
                        except OSError as error:
                            if error.errno != errno.ENOSPC:
                                raise
                            failure = error.errno
                            break
            stat = os.statvfs(mount)
            dump(directory / "full.json", {"errno": failure, "available": stat.f_bavail * stat.f_frsize})
            self.graph_reduce("stage-enospc", request, 1, parent=stage_parent)
        finally:
            command("hdiutil", "detach", str(mount))
            image.unlink()

    def benchmarks(self):
        samples = {}
        controls = CRITERIA["measurements"]["controls"]
        for control in controls:
            samples[control] = []
        for i in range(6):
            for control in controls:
                name = f"measure-{control}-{i}"
                if control.startswith("vector"):
                    op = "identity" if control.endswith("identity") else "combined"
                    command = "reference" if control.endswith("reference") else "vector"
                    receipt = self.subprocess(name, command, self.vector_request("large", op))
                    elapsed = receipt["elapsed_ns"]
                else:
                    request = self.graph_request(name, "chain")
                    if control.endswith("native"):
                        request["checkpoint_every"] = 0
                        request["expected"] = self.run_graph(name, request)
                        receipt = self.graph_reduce(name, request)
                        elapsed = receipt["elapsed_ns"] + load(self.cases / f"{name}-run" / "process.json")["elapsed_ns"]
                    else:
                        elapsed = self.subprocess(name, "reference", request)["elapsed_ns"]
                if i:
                    samples[control].append(elapsed)
        dump(self.output / "measurements.json", {name: {"samples_ns": times, **measure(times)}
                                                 for name, times in samples.items()})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--enospc", action="store_true")
    parser.add_argument("--no-benchmarks", action="store_true")
    args = parser.parse_args()
    root = HERE.parents[1]
    output = args.output.resolve()
    binary = args.binary.resolve(strict=True)
    campaign = Campaign(output, binary)
    dump(output / "provenance.json", {"baseline": CRITERIA["baseline"], "head_at_run": git(root, "rev-parse", "HEAD"),
                                     "tree_at_run": git(root, "rev-parse", "HEAD^{tree}"),
                                     "status_at_run": git(root, "status", "--porcelain=v1").splitlines(),
                                     "binary_path": str(binary), "binary_sha256": sha(binary), "binary_blake3": campaign.executable,
                                     "os": platform.platform(), "machine": platform.machine(), "python": sys.version,
                                     "pyarrow": pq.__version__ if hasattr(pq, "__version__") else __import__("pyarrow").__version__,
                                     "build": os.environ.get("ADE24_BUILD_IDENTITY", "unrecorded"),
                                     "source": source_inventory(root), "enospc": args.enospc,
                                     "benchmarks": not args.no_benchmarks})
    campaign.vector_cases()
    campaign.graph_cases(args.enospc)
    if not args.no_benchmarks:
        campaign.benchmarks()
    dump(output / "seal.json", members(output))
    print(sha(output / "seal.json"))


if __name__ == "__main__":
    main()
