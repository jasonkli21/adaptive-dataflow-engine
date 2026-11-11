"""Run independent, process-isolated Phase 20 correctness and timing evidence."""
import argparse
import copy
import hashlib
import json
import os
import platform
import resource
import select
import shutil
import signal
import statistics
import subprocess
import time
from pathlib import Path

from fixtures import encode, graph, inventory_entry, parquet_file, seal
from oracle import result

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
CRITERIA = json.loads((HERE / "criteria.json").read_text())
BFS = "reachability_bfs_v1"
COMPONENTS = "weak_components_min_label_v1"


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def capture(*args):
    return subprocess.check_output(args, cwd=REPO, text=True).strip()


def inputs():
    names = ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "rustfmt.toml", "scripts/smoke.sh"]
    names.extend(str(p.relative_to(REPO)) for p in (REPO / "crates").rglob("*")
                 if p.is_file() and (p.suffix == ".rs" or p.name == "Cargo.toml"))
    names.extend(str(p.relative_to(REPO)) for p in HERE.iterdir()
                 if p.suffix in (".py", ".json", ".sh", ".txt"))
    return {name: sha(REPO / name) for name in sorted(set(names))}


def plan(algorithm, seed):
    spec = {"kind": algorithm}
    if algorithm == BFS:
        spec["seed"] = seed
    prefix = "bfs" if algorithm == BFS else "label"
    return {"version": 1, "algorithm": spec, "nodes": ["expand", "reduce", "delay"],
            "arcs": [{"from": a, "to": b, "schema": f"{prefix}_{schema}", "delay": d}
                     for a, b, schema, d in [(0, 1, "candidate", 0), (1, 2, "frontier", 0), (2, 0, "frontier", 1)]]}


def frames(raw):
    return [json.loads(line) for line in raw.splitlines() if line.startswith("{")]


def events(case, kind):
    return [event for event in frames(case["stderr"]) if event["event"] == kind]


def pin(case, kind="selected"):
    return events(case, kind)[-1]["pin"]


def summary(case):
    return json.loads(case["stdout"])


def check_terminal(case, expected):
    assert case["exit"] == 0, case
    records = frames(case["stdout"])
    assert records[:-1] == expected and records[-1]["kind"] == "complete", case["name"]
    assert records[-1]["rows"] == len(expected) and case["stdout"].endswith("\n")
    return records[-1]


class Campaign:
    def __init__(self, binary, work):
        self.binary = binary
        self.work = work
        self.fixtures = work / "fixtures"
        self.roots = work / "roots"
        self.records = work / "commands"
        self.requests = work / "requests"
        for path in (self.roots, self.records, self.requests):
            path.mkdir(parents=True)
        self.lock = seal(self.fixtures, CRITERIA)
        assert self.lock == json.loads((HERE / "fixtures.lock.json").read_text())
        self.counter = 0

    def request(self, name, fixture, algo, seed=0, **changes):
        inventory = self.lock[fixture]["inventory"] if fixture in self.lock else changes.pop("inventory")
        return {"version": 1, "plan": plan(algo, seed), "inventory": inventory,
                "source_dir": str(self.fixtures / fixture), "root": str(self.roots / name),
                "trace": True, **changes}

    def launch(self, name, command, request, fault=None):
        self.counter += 1
        ident = f"{self.counter:04}-{name}"
        config = self.requests / f"{ident}.json"
        config.write_text(request if isinstance(request, str) else json.dumps(request) + "\n")
        argv = [str(self.binary), command, str(config)]
        env = {k: v for k, v in os.environ.items() if not k.startswith("ADE_CYCLE20_TEST_")}
        if fault:
            env.update(ADE_CYCLE20_TEST_POINT=fault[0], ADE_CYCLE20_TEST_ROUND=str(fault[1]))
            if len(fault) > 2:
                env["ADE_CYCLE20_TEST_CONTINUE"] = str(fault[2])
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        started = time.perf_counter_ns()
        process = subprocess.Popen(argv, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return ident, argv, started, process, fault, usage

    def finish(self, job, initial=b"", drain_before_wait=True):
        ident, argv, started, process, fault, before = job
        if not drain_before_wait:
            process.wait(timeout=15)
        stdout, stderr = process.communicate(timeout=130)
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        record = {"name": ident, "argv": argv, "pid": process.pid, "exit": process.returncode,
                  "elapsed_ns": time.perf_counter_ns() - started, "stdout": stdout.decode(),
                  "cpu_user_ns": round((after.ru_utime - before.ru_utime) * 1e9),
                  "cpu_system_ns": round((after.ru_stime - before.ru_stime) * 1e9),
                  "file_input_blocks": after.ru_inblock - before.ru_inblock,
                  "file_output_blocks": after.ru_oublock - before.ru_oublock,
                  "stderr": (initial + stderr).decode(), "armed": list(map(str, fault)) if fault else None}
        save(self.records / f"{ident}.json", record)
        return record

    def run(self, name, command, request, error=None, fault=None):
        case = self.finish(self.launch(name, command, request, fault))
        if error:
            assert case["exit"] != 0 and error in case["stderr"] and case["stdout"] == "", case
        else:
            assert case["exit"] == 0, case
        return case

    def reached(self, job):
        ident, _, _, process, fault, _ = job
        target = f"cycle20-fault {fault[0]} {fault[1]}\n".encode()
        observed = bytearray()
        until = time.monotonic() + 20
        while target not in observed:
            timeout = until - time.monotonic()
            if timeout <= 0 or not select.select([process.stderr], [], [], timeout)[0]:
                process.kill()
                raise AssertionError(f"{ident} fault marker timeout: {observed!r}")
            block = os.read(process.stderr.fileno(), 4096)
            if not block:
                raise AssertionError(f"{ident} exited before {target!r}: {observed!r}")
            observed.extend(block)
        return bytes(observed)

    def terminal(self, name, request, expected_rows):
        case = self.run(name, "read", request)
        footer = check_terminal(case, expected_rows)
        verified = self.run(name + "-verify", "verify", request)
        assert frames(verified["stdout"]) == [footer]
        return case

    def expected(self, fixture, request):
        return result(*graph(fixture), request["plan"]["algorithm"])

    def kill(self, name, command, request, fault):
        job = self.launch(name, command, request, fault)
        observed = self.reached(job)
        job[3].kill()
        case = self.finish(job, observed)
        assert case["exit"] == -signal.SIGKILL and not case["stdout"], case
        return case

    def basic(self):
        for fixture, algorithms in [
            ("empty", [(COMPONENTS, 0)]), ("isolated", [(BFS, 3), (COMPONENTS, 0)]),
            ("mixed", [(BFS, 0), (BFS, 3), (BFS, 9), (COMPONENTS, 0)]),
            ("chain", [(BFS, 0)]), ("dense", [(COMPONENTS, 0)]),
            ("max", [(BFS, 0), (COMPONENTS, 0)]), ("max_chain", [(BFS, 0)]),
        ]:
            for algo, seed in algorithms:
                name = f"{fixture}-{'bfs' if algo == BFS else 'components'}-{seed}"
                req = self.request(name, fixture, algo, seed)
                explanation = self.run(name + "-explain", "explain", req)
                assert not Path(req["root"]).exists() and json.loads(explanation["stdout"])[0] == req["plan"]
                started = self.run(name, "run", req)
                req["expected"] = pin(started)
                terminal = self.terminal(name + "-terminal", req, self.expected(fixture, req))
                assert summary(started)["terminal"] == frames(terminal["stdout"])[-1]
                if fixture in {"max", "empty", "isolated"}:
                    self.boundaries(name, req, started, terminal)

        permuted = encode(self.fixtures / "permuted", *graph("mixed"), permutation=20)
        for algo in (BFS, COMPONENTS):
            base = self.request("canonical-" + algo, "mixed", algo)
            changed = self.request("perturbed-" + algo, "permuted", algo, inventory=permuted,
                                   limits={"batch_rows": 1, "credits": 1}, reverse=True,
                                   checkpoint_every=1)
            a = self.run("canonical-" + algo, "run", base)
            b = self.run("perturbed-" + algo, "run", changed)
            assert summary(a)["plan"] != summary(b)["plan"]
            base["expected"], changed["expected"] = pin(a), pin(b)
            x = self.terminal("canonical-terminal-" + algo, base, self.expected("mixed", base))
            y = self.terminal("perturbed-terminal-" + algo, changed, self.expected("mixed", changed))
            assert x["stdout"] == y["stdout"]

    def boundaries(self, name, request, started, terminal):
        measured = summary(started)
        exact = {"source_bytes": measured["source_bytes"], "state_bytes": measured["managed_state_bytes"],
                 "disk_bytes": measured["retained_bytes"], "output_bytes": len(terminal["stdout"].encode())}
        self.run(name + "-exact-bounds", "read", {**request, "limits": exact})
        for key, message in [("source_bytes", "source bytes"), ("state_bytes", "managed state bytes"),
                             ("disk_bytes", "retained disk bytes"), ("output_bytes", "serialized output bytes")]:
            self.run(name + "-below-" + key, "read", {**request, "limits": {**exact, key: exact[key] - 1}}, message)
        footer_bytes = len(terminal["stdout"].splitlines(keepends=True)[-1].encode())
        self.run(name + "-exact-verify-footer", "verify", {**request, "limits": {"output_bytes": footer_bytes}})
        self.run(name + "-short-verify-footer", "verify", {**request, "limits": {"output_bytes": footer_bytes - 1}}, "serialized output bytes")

    def negative_input(self):
        vertices, edges = graph("mixed")
        for name, vs, es, error in [
            ("duplicate_vertex", vertices + [0], edges, "duplicate vertex"),
            ("duplicate_edge", vertices, edges + [edges[0]], "duplicate directed edge"),
            ("unknown_endpoint", vertices, edges + [(0, 200)], "unknown destination endpoint"),
            ("vertices_over_cap", list(range(4097)), [], "graph row cap"),
            ("edges_over_cap", *graph("max")[:1], graph("max")[1] + [(1, 4000)], "graph row cap"),
        ]:
            inv = encode(self.fixtures / name, vs, es)
            self.no_root(name, self.request(name, name, BFS, inventory=inv), error)
        for name in ("wrong_schema", "null_schema"):
            inv = encode(self.fixtures / name, vertices, edges)
            vertex = self.fixtures / name / inv["vertices"][0]["name"]
            parquet_file(vertex, ["vertex"], [(v,) for v in vertices], nullable=name == "null_schema",
                         signed=name == "wrong_schema")
            inv["vertices"][0] = inventory_entry(vertex, len(vertices))
            self.no_root(name, self.request(name, name, BFS, inventory=inv), "Parquet schema")
        inv = encode(self.fixtures / "absent", vertices, edges, permutation=19)
        (self.fixtures / "absent" / inv["edges"][1]["name"]).unlink()
        self.no_root("missing_file", self.request("missing_file", "absent", BFS, inventory=inv), "No such file")
        req = self.request("bad-input", "mixed", BFS)
        forged = copy.deepcopy(req)
        forged["inventory"]["vertices"][0]["blake3"] = "0" * 64
        self.no_root("forged_hash", forged, "BLAKE3 changed")
        drift = self.fixtures / "mutated-content"
        inventory = encode(drift, vertices, edges)
        target = drift / inventory["edges"][0]["name"]
        altered = bytearray(target.read_bytes())
        altered[16] ^= 1
        target.write_bytes(altered)
        self.no_root("mutated_content", self.request("mutated-content", "mutated-content", BFS,
                                                       inventory=inventory), "BLAKE3 changed")
        self.no_root("missing_seed", {**req, "plan": plan(BFS, 100)}, "BFS seed is absent")
        self.no_root("unknown_json", {**req, "unknown": 1}, "unknown field")
        self.run("duplicate_json", "run", json.dumps(req).replace("{", '{"version":1,', 1), "duplicate field")
        self.no_root("forced_mode", {**req, "mode": "unbounded"}, "unknown variant")
        self.no_root("traversal", {**req, "root": req["root"] + "/../escape"}, "parent path component")
        for name, mutator in [
            ("zero_delay", lambda p: p["arcs"][2].update(delay=0)),
            ("instant_cycle", lambda p: p["arcs"][0].update(to=0)),
            ("mismatched_port", lambda p: p["arcs"][2].update(schema="label_frontier")),
            ("dangling", lambda p: p["arcs"][2].update(to=9)),
            ("multiple_delays", lambda p: p["arcs"][1].update(delay=1)),
            ("nested_loop", lambda p: p["nodes"].append("delay")),
        ]:
            invalid = copy.deepcopy(req)
            mutator(invalid["plan"])
            self.no_root(name, invalid, "trailing characters" if name == "nested_loop" else "unsupported cyclic topology")
        for field, cap in CRITERIA["limits"].items():
            if field in {"source_bytes", "state_bytes", "disk_bytes", "output_bytes", "deadline_ms", "rounds", "credits", "batch_rows"}:
                self.no_root("above-" + field, {**req, "limits": {field: cap + 1}}, "limits outside")
        self.source_swaps()
        self.source_limit()
        self.file_and_request_limits(req)

    def no_root(self, name, request, error):
        self.run(name, "run", request, error)
        assert not Path(request["root"]).exists(), name

    def file_and_request_limits(self, req):
        raw_request = json.dumps(req)
        self.run("request-exact", "explain", raw_request.ljust(64 * 1024))
        self.run("request-above", "run", raw_request.ljust(64 * 1024 + 1), "request byte cap")
        name = "files-per-role"
        folder = self.fixtures / name
        inv = encode(folder, [0], [])
        original = folder / inv["edges"][0]["name"]
        for index in range(1, 33):
            target = folder / f"edges-{index}.parquet"
            shutil.copyfile(original, target)
            if index < 32:
                inv["edges"].append(inventory_entry(target, 0))
        request = self.request(name, name, BFS, inventory=inv)
        self.run("files-per-role-exact", "explain", request)
        above = copy.deepcopy(request)
        above["inventory"]["edges"].append(inventory_entry(folder / "edges-32.parquet", 0))
        self.no_root("files-per-role-above", above, "graph files per role")

    def source_swaps(self):
        for stage, round_num, expected_root in [("source_decoded", 0, False), ("before_round", 1, True), ("before_select", 2, True)]:
            name = "inode-swap-" + stage
            inv = encode(self.fixtures / name, *graph("mixed"))
            req = self.request(name, name, BFS, inventory=inv, checkpoint_every=1)
            sentinel = self.work / (name + ".continue")
            job = self.launch(name, "run", req, (stage, round_num, sentinel))
            observed = self.reached(job)
            target = self.fixtures / name / inv["vertices"][0]["name"]
            tmp = target.with_suffix(".tmp")
            shutil.copyfile(target, tmp)
            tmp.replace(target)
            sentinel.touch()
            refused = self.finish(job, observed)
            assert refused["exit"] == 1 and "graph file identity changed" in refused["stderr"] and not refused["stdout"]
            assert Path(req["root"]).exists() == expected_root
            if expected_root:
                self.run(name + "-old-cut", "read", {**req, "expected": pin(refused)}, "provisional")

    def source_limit(self):
        name = "source-byte-bound"
        inv = encode(self.fixtures / name, *graph("mixed"))
        path = self.fixtures / name / inv["vertices"][0]["name"]
        raw = path.read_bytes()
        metadata = int.from_bytes(raw[-8:-4], "little")
        start = len(raw) - metadata - 8
        padding = CRITERIA["limits"]["source_bytes"] - sum(e["bytes"] for entries in inv.values() for e in entries)
        with path.open("wb") as file:
            file.write(raw[:start])
            file.seek(padding, os.SEEK_CUR)
            file.write(raw[start:])
        inv["vertices"][0] = inventory_entry(path, inv["vertices"][0]["rows"])
        req = self.request(name, name, BFS, inventory=inv)
        self.run(name + "-exact", "explain", req)
        above = copy.deepcopy(req)
        above["inventory"]["vertices"][0]["bytes"] += 1
        self.no_root(name + "-above", above, "source byte cap")

    def interruption(self):
        for name, limit, fault, send, error in [
            ("round-limit", {"rounds": 1}, None, None, "rounds before fixed point"),
            ("deadline", {"deadline_ms": 700}, ("before_round", 1), None, "INCOMPLETE_LIMIT: deadline"),
            ("cancel", {}, ("before_round", 1), signal.SIGINT, "INCOMPLETE_CANCELLED"),
            ("short-write", {}, ("short_write", 2), None, "injected short write"),
            ("disk-quota", {"disk_bytes": 950}, None, None, "retained disk bytes"),
        ]:
            req = self.request(name, "mixed", BFS, checkpoint_every=1, limits=limit)
            if fault and fault[0] == "before_round":
                job = self.launch(name, "run", req, fault)
                observed = self.reached(job)
                if send is not None:
                    job[3].send_signal(send)
                case = self.finish(job, observed)
                assert case["exit"] == 1 and error in case["stderr"] and not case["stdout"], case
            else:
                case = self.run(name, "run", req, error, fault)
            req["expected"] = pin(case)
            req["limits"] = {}
            self.run(name + "-not-terminal", "read", req, "provisional")
            resumed = self.run(name + "-resume", "resume", req)
            req["expected"] = pin(resumed)
            self.terminal(name + "-terminal", req, self.expected("mixed", req))
        req = self.request("output-limit", "mixed", BFS, limits={"output_bytes": 1})
        stopped = self.run("output-limit", "run", req, "serialized output bytes")
        self.run("output-limit-not-terminal", "read", {**req, "expected": pin(stopped)}, "provisional")

    def crash_and_resume(self):
        for point in CRITERIA["crash_points"]:
            req = self.request("crash-" + point, "mixed", BFS, checkpoint_every=1)
            stopped = self.kill("crash-" + point, "run", req, (point, 2))
            before = pin(stopped)
            after = point in {"selected", "directory_synced"}
            if after:
                self.run("stale-pin-" + point, "read", {**req, "expected": before}, "selector differs")
            req["expected"] = pin(stopped, "prepared") if after else before
            self.run("crash-not-terminal-" + point, "read", req, "provisional")
            resumed = self.run("crash-resume-" + point, "resume", req)
            req["expected"] = pin(resumed)
            self.terminal("crash-terminal-" + point, req, self.expected("mixed", req))
        for point in ("before_select", "selected"):
            name = "terminal-crash-" + point
            req = self.request(name, "mixed", BFS, checkpoint_every=1)
            stopped = self.kill(name, "run", req, (point, 4))
            self.run(name + "-old-pin", "read", {**req, "expected": pin(stopped)},
                     "provisional" if point == "before_select" else "selector differs")
            if point == "selected":
                req["expected"] = pin(stopped, "prepared")
            else:
                resumed = self.run(name + "-resume", "resume", {**req, "expected": pin(stopped)})
                req["expected"] = pin(resumed)
            self.terminal(name + "-read", req, self.expected("mixed", req))
        for algo in (BFS, COMPONENTS):
            name = "restart-" + algo
            req = self.request(name, "chain", algo, checkpoint_every=2)
            stopped = self.kill(name, "run", req, ("before_round", 2))
            req.update(expected=pin(stopped), checkpoint_every=0, reverse=True, limits={"batch_rows": 17, "credits": 2})
            self.run(name + "-checkpoint-not-result", "verify", req, "provisional")
            complete = self.run(name + "-resume", "resume", req)
            req["expected"] = pin(complete)
            first = self.terminal(name + "-terminal", req, self.expected("chain", req))
            retry = self.run(name + "-idempotent", "resume", req)
            assert summary(retry)["pin"] == req["expected"] and summary(retry)["rounds_executed"] == 0
            assert self.run(name + "-reread", "read", req)["stdout"] == first["stdout"]
            replay = self.request(name + "-replay", "chain", algo)
            fresh = self.run(name + "-replay", "run", replay)
            replay["expected"] = pin(fresh)
            replayed = self.terminal(name + "-replayed", replay, self.expected("chain", req))
            assert replayed["stdout"] == first["stdout"]

    def lineage(self):
        req = self.request("fenced", "mixed", BFS, checkpoint_every=1)
        job = self.launch("fenced-writer", "run", req, ("before_round", 1))
        observed = self.reached(job)
        req["expected"] = [e["pin"] for e in frames(observed.decode()) if e["event"] == "selected"][-1]
        for command in ("read", "resume"):
            self.run("concurrent-" + command, command, req, "fenced by another process")
        job[3].kill()
        self.finish(job, observed)
        self.run("pin-omitted", "resume", {**req, "expected": None}, "exact pin is required")
        self.run("pin-round-changed", "resume", {**req, "expected": {**req["expected"], "round": 0}}, "chain/round differs")
        self.run("changed-plan", "resume", {**req, "plan": plan(BFS, 3)}, "root/plan/source binding changed")
        inv = encode(self.fixtures / "changed-source", *graph("mixed"), permutation=9)
        self.run("changed-source", "resume", {**req, "source_dir": str(self.fixtures / "changed-source"), "inventory": inv},
                 "root/plan/source binding changed")
        final = self.run("fenced-resume", "resume", req)
        req["expected"] = pin(final)
        root = Path(req["root"])
        selected = root / f's20-{req["expected"]["sequence"]:04}'
        witness = root / selected.name.replace("s20", "w20")
        obj = root / selected.name.replace("s20", "o20")
        for label, path, message in [("object-tamper", obj, "object digest mismatch"),
                                     ("witness-tamper", witness, "parent mismatch")]:
            raw = path.read_bytes()
            try:
                item = json.loads(raw)
                item["version"] = 2
                path.write_text(json.dumps(item, separators=(",", ":")))
                self.run(label, "read", req, message)
            finally:
                path.write_bytes(raw)
        selected.unlink()
        try:
            self.run("selector-rollback", "read", req, "selector differs")
        finally:
            os.link(witness, selected)
        linked = self.work / "outside-object-alias"
        os.link(obj, linked)
        try:
            self.run("external-root-file-alias", "read", req, "external cycle20 file alias")
        finally:
            linked.unlink()
        alias = self.roots / "fenced-alias"
        alias.symlink_to(root, target_is_directory=True)
        self.run("root-symlink", "read", {**req, "root": str(alias)}, "symlink path")
        alias.unlink()
        clone = self.roots / "fenced-clone"
        clone.mkdir(mode=0o700)
        for file in sorted(root.iterdir()):
            if file.name.startswith("s20-"):
                continue
            shutil.copy2(file, clone / file.name)
            if file.name.startswith("w20-"):
                os.link(clone / file.name, clone / file.name.replace("w20", "s20"))
        self.run("root-clone", "read", {**req, "root": str(clone)}, "root/plan/source binding changed")
        shutil.rmtree(clone)
        self.terminal("fenced-terminal", req, self.expected("mixed", req))

    def stalled(self):
        name = "stalled"
        req = self.request(name, "max", COMPONENTS)
        done = self.run(name + "-run", "run", req)
        req["expected"] = pin(done)
        job = self.launch(name + "-stdout", "read", {**req, "limits": {"deadline_ms": 1500}})
        assert select.select([job[3].stdout], [], [], 5)[0], "read did not reach stdout"
        self.run(name + "-concurrent-writer", "resume", req, "fenced by another process")
        concurrent = self.run(name + "-concurrent-reader", "read", req)
        check_terminal(concurrent, self.expected("max", req))
        blocked = self.finish(job, drain_before_wait=False)
        assert blocked["exit"] == 1 and "stalled stdout deadline" in blocked["stderr"]
        assert blocked["stdout"] and '"kind":"complete"' not in blocked["stdout"]
        self.terminal(name + "-recovered", req, self.expected("max", req))

    def performance(self):
        metrics = {}
        for name, regime in CRITERIA["performance"]["regimes"].items():
            algo = BFS if regime["algorithm"] == "bfs" else COMPONENTS
            fixture = regime["fixture"]
            pairs = []
            for i in range(CRITERIA["performance"]["warmups"] + CRITERIA["performance"]["pairs"]):
                pair = {}
                for mode in ("indexed", "scan_all") if i % 2 == 0 else ("scan_all", "indexed"):
                    label = f"perf-{name}-{i}-{mode}"
                    req = self.request(label, fixture, algo, regime.get("seed", 0), mode=mode)
                    command = "run"
                    if "checkpoint_round" in regime:
                        round_num = regime["checkpoint_round"]
                        stopped = self.run(label + "-prefix", "run", {**req, "checkpoint_every": round_num,
                                                                      "limits": {"rounds": round_num}},
                                           "rounds before fixed point")
                        req["expected"] = pin(stopped)
                        command = "resume"
                    case = self.run(label, command, req)
                    measured = summary(case)
                    self.terminal(label + "-terminal", {**req, "expected": pin(case)}, self.expected(fixture, req))
                    pair[mode] = {"case": case["name"], "compute_ns": measured["compute_ns"],
                                  "wall_ns": case["elapsed_ns"], "edge_work": measured["edge_work"],
                                  "rounds": measured["rounds_executed"],
                                  "source_bytes": measured["source_bytes"], "retained_bytes": measured["retained_bytes"],
                                  **{k: case[k] for k in ("cpu_user_ns", "cpu_system_ns", "file_input_blocks",
                                                          "file_output_blocks")}}
                pairs.append({"warmup": i == 0, **pair})
            metrics[name] = {"pairs": pairs, **ratios(pairs)}
            save(self.work / "performance.json", metrics)
            assert metrics[name]["scan_to_index_work"] >= regime["min_scan_to_index_work"], name
            assert metrics[name]["index_to_scan_median"] <= regime["max_index_to_scan_median"], name

    def hardware(self):
        def attempt(*args):
            p = subprocess.run(args, capture_output=True, text=True)
            return {"exit": p.returncode, "stdout": p.stdout, "stderr": p.stderr}

        return {"platform": platform.platform(), "arch": platform.machine(),
                "cpu_memory": attempt("sysctl", "machdep.cpu.brand_string", "hw.memsize"),
                "rustc": capture("rustc", "-Vv"), "cargo": capture("cargo", "-V"),
                "disk": attempt("df", "-h", str(self.work)), "mount": attempt("mount"),
                "python": platform.python_version(), "pyarrow": __import__("pyarrow").__version__,
                "blake3": __import__("blake3").__version__}


def root_layout(root):
    identity = root.stat()
    files = {}
    for file in sorted(root.iterdir()):
        meta = file.stat()
        files[file.name] = {"inode": meta.st_ino, "links": meta.st_nlink, "bytes": meta.st_size}
    return {"device": identity.st_dev, "inode": identity.st_ino, "files": files}


def ratios(pairs):
    measurements = [p for p in pairs if not p["warmup"]]
    stats = {}
    for mode in ("indexed", "scan_all"):
        for key in ("compute_ns", "wall_ns"):
            values = [p[mode][key] for p in measurements]
            stats[f"{mode}_{key}"] = {"median": statistics.median(values), "stdev": statistics.pstdev(values)}
    return {"scan_to_index_work": sum(p["scan_all"]["edge_work"] for p in measurements) /
                                  sum(p["indexed"]["edge_work"] for p in measurements),
            "index_to_scan_median": stats["indexed_compute_ns"]["median"] / stats["scan_all_compute_ns"]["median"],
            "stats": stats}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("work", type=Path)
    parser.add_argument("--checks-only", action="store_true")
    parser.add_argument("--hardware", type=Path)
    args = parser.parse_args()
    work = args.work.resolve()
    work.mkdir()
    source = inputs()
    binary = args.binary.resolve()
    driver = Campaign(binary, work)
    baseline = CRITERIA["baseline"]
    capture("git", "merge-base", "--is-ancestor", baseline, "HEAD")
    save(work / "provenance.json", {
        "baseline": baseline, "baseline_tree": capture("git", "rev-parse", baseline + "^{tree}"),
        "worktree_head": capture("git", "rev-parse", "HEAD"),
        "worktree": capture("git", "status", "--porcelain=v1"), "source": source,
        "source_sha256": hashlib.sha256(json.dumps(source, separators=(",", ":")).encode()).hexdigest(),
        "criteria_sha256": sha(HERE / "criteria.json"), "fixture_lock_sha256": sha(HERE / "fixtures.lock.json"),
        "phase18": CRITERIA["phase18_disposition"], "binary_sha256": sha(binary),
        "build": "cargo build -p ade-iterate --release --features fault-injection --locked",
        "environment": driver.hardware(),
        "host_sysctl": args.hardware.read_text() if args.hardware else None,
    })
    for stage in ("basic", "negative_input", "interruption", "crash_and_resume", "lineage", "stalled"):
        print(stage, flush=True)
        getattr(driver, stage)()
    if not args.checks_only:
        print("performance", flush=True)
        driver.performance()
    save(work / "root-layout.json", {root.name: root_layout(root) for root in driver.roots.iterdir()})
    assert source == inputs(), "source changed during campaign"
    assert sha(binary) == json.loads((work / "provenance.json").read_text())["binary_sha256"]
    print(f"{driver.counter} process observations", flush=True)


if __name__ == "__main__":
    main()
