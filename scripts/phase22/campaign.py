#!/usr/bin/env python3
"""Live single-host Phase 22 campaign. Never writes into an existing archive."""

import argparse
import base64
import concurrent.futures
import errno
import json
import os
from pathlib import Path
import platform
import resource
import secrets
import selectors
import signal
import struct
import subprocess
import sys
import time

import pyarrow as pa
import pyarrow.flight as flight
import pyarrow._flight as flight_extension
from google.protobuf.any_pb2 import Any

from ade_client import Client, Function, Query, QueryError, _Statement, _descriptor, _rpc
from cases import FAILURES, PLAN, plan
from faults import script as fault_script
from fixtures import SCHEMA, SQL, fixture, hash_bytes, oracle
from inventory import sha, source_inventory

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
CRITERIA = json.loads((HERE / "criteria.json").read_text())
SCRIPT = HERE / "map_secondary.py"
DIGEST = hash_bytes(SCRIPT.read_bytes())
SECRET = secrets.token_hex(32)
METADATA = [[0, "ADE"], [3, True], [10001, "ade.gateway/QueryRequestV1;local;map_secondary/1"]]


def b64(value):
    return base64.b64encode(value).decode()


def document(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def capture(argv, **kwargs):
    start = time.monotonic_ns()
    proc = subprocess.run([str(a) for a in argv], capture_output=True, text=True, **kwargs)
    return {"argv": [str(a) for a in argv], "status": proc.returncode, "stdout": proc.stdout,
            "stderr": proc.stderr, "wall_ns": time.monotonic_ns() - start}


def plan_bytes(case, digest):
    if case == "malformed_json":
        return b"{"
    if case == "invalid_utf8":
        return b'{"table":"\xff"}'
    base = json.dumps(plan(case, digest), separators=(",", ":")).encode()
    if case == "oversized":
        return base.ljust(CRITERIA["budgets"]["request_bytes"] + 1, b" ")
    if case == "duplicate":
        return b'{"version":1,' + base[1:]
    return base


def descriptor(raw):
    command = Any(type_url="ade.gateway/QueryRequestV1", value=raw)
    return flight.FlightDescriptor.for_command(command.SerializeToString())


def canonical(table):
    fields = {name: (type_name, data_type) for name, type_name, data_type in SCHEMA}
    schema = []
    columns = []
    for index, field in enumerate(table.schema):
        type_name, data_type = fields[field.name]
        assert field.type == data_type
        schema.append({"name": field.name, "type": type_name, "nullable": field.nullable})
        col = table.column(index)
        if pa.types.is_timestamp(data_type):
            col = col.cast(pa.int64())
        if pa.types.is_floating(data_type):
            columns.append([format(struct.unpack(">Q", struct.pack(">d", x))[0], "x") for x in col.to_pylist()])
        else:
            columns.append([str(value) for value in col.to_pylist()])
    return {"schema": schema, "rows": [list(row) for row in zip(*columns)]}


def save_arrow(path, table):
    with path.open("wb") as output, pa.ipc.new_stream(output, table.schema) as writer:
        writer.write_table(table)


def refused(call, case):
    try:
        call()
    except QueryError as exc:
        code, detail = FAILURES[case]
        message = str(exc)
        assert code == exc.code and detail in message, (case, exc.code, message)
        assert "PHASE22_REDACTION_SENTINEL" not in message and SECRET not in message
        return {"code": exc.code, "message": message, "returned_ticket": False}
    raise AssertionError(f"{case} unexpectedly succeeded")


def wait_for_file(marker):
    until = time.monotonic() + 6
    while not marker.exists() and time.monotonic() < until:
        time.sleep(0.01)
    return int(marker.read_text())


def reaped(pid):
    until = time.monotonic() + 5
    observations = []
    while time.monotonic() < until:
        proc = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True)
        state = proc.stdout.strip()
        if not observations or state != observations[-1]:
            observations.append(state)
        if proc.returncode != 0 or not state or state.startswith("Z"):
            return {"pid": pid, "exit_observed": True, "process_states": observations}
        time.sleep(0.03)
    raise AssertionError(f"worker {pid} still active: {observations}")


class Server:
    def __init__(self, args, root, label, *, script=SCRIPT, tmpdir=None):
        self.args = args
        self.root = root
        self.label = label
        self.script = script
        self.digest = hash_bytes(script.read_bytes())
        self.env = os.environ | {"ADE_GATEWAY_TOKEN": SECRET}
        if tmpdir:
            self.env["TMPDIR"] = str(tmpdir)

    def argv(self):
        return [str(self.args.binary), "--root", str(self.root), "--manifest", str(self.root / "manifest.json"),
                "--python", sys.executable, "--script", str(self.script), "--digest", self.digest]

    def __enter__(self):
        self.log = (self.args.output / f"server-{self.label}.stderr").open("w")
        self.process = subprocess.Popen(self.argv(), env=self.env, stdout=subprocess.PIPE, stderr=self.log, text=True)
        with selectors.DefaultSelector() as sel:
            sel.register(self.process.stdout, selectors.EVENT_READ)
            if not sel.select(15):
                self.kill()
                raise RuntimeError(f"gateway startup timeout for {self.label}")
        line = self.process.stdout.readline().strip()
        if not line.startswith("127.0.0.1:"):
            self.kill()
            self.log.close()
            raise RuntimeError(f"gateway {self.label} startup refused: {self.log.name}")
        self.port = int(line.rsplit(":", 1)[1])
        return self

    def kill(self):
        if self.process.returncode is None:
            try:
                os.kill(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            deadline = time.monotonic() + 8
            while True:
                pid, status, usage = os.wait4(self.process.pid, os.WNOHANG)
                if pid:
                    self.process.returncode = os.waitstatus_to_exitcode(status)
                    self.usage = {
                        "user_cpu_ns": int(usage.ru_utime * 1e9),
                        "system_cpu_ns": int(usage.ru_stime * 1e9),
                        "rss_peak_bytes": usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024),
                    }
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("gateway did not exit after SIGKILL")
                time.sleep(0.01)
        return self.process.returncode

    def client(self, **kwargs):
        return Client("127.0.0.1", self.port, SECRET, **kwargs)

    def __exit__(self, *_):
        exit_code = self.kill()
        self.log.close()
        self.process.stdout.close()
        document(self.args.output / f"server-{self.label}.json", {
            "argv": self.argv(), "pid": self.process.pid, "exit": exit_code,
            "socket": f"127.0.0.1:{self.port}", "function_digest": self.digest,
            "gateway_sha256": sha(self.args.binary), "process_usage": self.usage,
        })


def capture_success(client, info, name, output):
    table, receipt = client.fetch(info)
    expected = oracle(name)
    actual = canonical(table)
    assert actual == expected, (name, actual["schema"], expected["schema"], len(actual["rows"]))
    save_arrow(output / f"{name}.arrow", table)
    return {"receipt": receipt, "data": actual, "ticket": b64(info.endpoints[0].ticket.ticket),
            "schema": actual["schema"], "arrow_sha256": sha(output / f"{name}.arrow")}


def java(args, server, action, value=None, *, expected=0):
    classpath = str(ROOT / "clients/java/target/classes") + os.pathsep + args.java_classpath
    argv = [args.java, "--add-opens=java.base/java.nio=ALL-UNNAMED", "-cp", classpath,
            "ade.Example", "127.0.0.1", str(server.port), action]
    if value is not None:
        argv.append(value)
    result = capture(argv, env=server.env, timeout=45)
    assert result["status"] == expected, result
    return json.loads(result["stdout"]), result


def small_fixtures(args, record):
    for name, count in [("empty", 0), ("single", 1), ("boundary", 256)]:
        root = args.output / ("source-" + name)
        fixture(root, count)
        with Server(args, root, name) as server, server.client() as client:
            raw = plan_bytes(name, DIGEST)
            observed = capture_success(client, client.submit(descriptor(raw)), name, args.output)
            if name == "boundary":
                request = args.output / "java-boundary-request.json"
                request.write_bytes(raw)
                jvm, command = java(args, server, "plan", str(request))
                assert {key: jvm[key] for key in ("schema", "rows")} == observed["data"], jvm
                observed |= {"java": jvm, "java_command": command}
            record(name, observed | {"request": b64(raw)})


def preflight_negatives(args, server, client, record):
    cases = ("malformed_json", "oversized", "unknown_version", "duplicate", "unknown_field",
             "unsupported_target", "unknown_table", "invalid_utf8", "overflow", "wrong_abi",
             "wrong_digest", "unknown_function", "forged_path")
    for name in cases:
        raw = plan_bytes(name, DIGEST)
        observed = refused(lambda: client.submit(descriptor(raw)), name)
        if name in ("wrong_abi", "unknown_function", "unknown_version"):
            path = args.output / f"java-{name}-request.json"
            path.write_bytes(raw)
            jvm, command = java(args, server, "plan", str(path), expected=2)
            assert jvm == {"code": FAILURES[name][0]}, jvm
            observed |= {"java": jvm, "java_command": command}
        record(name, observed | {"request": b64(raw)})
    sql = "DROP TABLE input"
    rejected = refused(lambda: client.submit_sql(sql), "unsupported_sql")
    jvm, command = java(args, server, "sql", sql, expected=2)
    assert jvm == {"code": "INVALID_ARGUMENT"}, jvm
    record("unsupported_sql", rejected | {"request": sql, "java": jvm, "java_command": command})
    action = flight.Action("CreatePreparedStatement", b"")
    record("unsupported_write", refused(lambda: _rpc(lambda: list(client._flight.do_action(action, client._options))),
                                         "unsupported_write") | {"request": action.type})
    with flight.FlightClient(f"grpc://127.0.0.1:{server.port}") as raw:
        record("missing_auth", refused(lambda: _rpc(raw.get_flight_info, _descriptor(_Statement(query=SQL))),
                                       "missing_auth") | {"request": SQL})


def main_service(args, root, record):
    with Server(args, root, "main") as server, server.client() as client:
        preflight_negatives(args, server, client, record)
        for name in ["sql", "plan", "udf"]:
            if name == "sql":
                info = client.submit_sql(SQL)
                request = SQL
                jvm, command = java(args, server, "sql", SQL)
            else:
                raw = plan_bytes(name, DIGEST)
                sdk_query = Query(tuple(PLAN["operations"]), Function("map_secondary", 1, DIGEST) if name == "udf" else None)
                info = client.submit_plan(sdk_query)
                request = b64(Any.FromString(info.descriptor.command).value)
                path = args.output / f"java-{name}-request.json"
                path.write_bytes(raw)
                jvm, command = java(args, server, "plan", str(path))
            result = capture_success(client, info, name, args.output)
            assert {key: jvm[key] for key in ("schema", "rows")} == result["data"], jvm
            record(name, result | {"request": request, "java": jvm, "java_command": command})
        metadata = client.sql_info([0, 3, 10001])
        actual = [[r["info_name"], r["value"]] for r in metadata.to_pylist()]
        jvm, command = java(args, server, "metadata")
        assert actual == METADATA and jvm["rows"] == [[str(k), str(v).lower() if isinstance(v, bool) else v] for k, v in METADATA], jvm
        record("metadata", {"request": [0, 3, 10001], "data": actual, "java": jvm, "java_command": command})
        schema = client.schema(SQL)
        jvm, command = java(args, server, "schema", SQL)
        assert schema.names == [f["name"] for f in oracle("sql")["schema"]]
        assert jvm["schema"] == oracle("sql")["schema"]
        record("schema", {"request": SQL, "schema": jvm["schema"], "java_command": command})
        raw = plan_bytes("repeat", DIGEST)
        info = client.submit(descriptor(raw))
        first = capture_success(client, info, "repeat", args.output)
        repeat = capture_success(client, info, "repeat", args.output)
        assert first == repeat
        record("repeat", first | {"request": b64(raw), "repeat": repeat})
        ticket = info.endpoints[0].ticket
        with server.client() as foreign:
            record("cross_session", refused(lambda: foreign.open_ticket(ticket), "cross_session") |
                   {"ticket": b64(ticket.ticket), "different_session": True})
        forged = flight.Ticket(ticket.ticket[:-1] + bytes([ticket.ticket[-1] ^ 1]))
        record("forged_ticket", refused(lambda: client.open_ticket(forged), "forged_ticket") |
               {"ticket": b64(forged.ticket), "original_ticket": b64(ticket.ticket)})
        before = time.monotonic()
        time.sleep(CRITERIA["budgets"]["ticket_ttl_seconds"] + 0.2)
        record("stale", refused(lambda: client.open_ticket(ticket), "stale") |
               {"ticket": b64(ticket.ticket), "wait_ns": int((time.monotonic() - before) * 1e9)})
        return ticket


def source_negatives(args, root, record):
    artifact = args.output / "changed.py"
    artifact.write_bytes(SCRIPT.read_bytes())
    with Server(args, root, "artifact", script=artifact) as server, server.client() as client:
        artifact.write_bytes(b"# deliberate drift\n" + artifact.read_bytes())
        raw = plan_bytes("artifact_drift", DIGEST)
        record("artifact_drift", refused(lambda: client.submit(descriptor(raw)), "artifact_drift") |
               {"request": b64(raw), "changed_digest": hash_bytes(artifact.read_bytes()), "original_digest": DIGEST})
    path = root / "fixture.parquet"
    original = path.read_bytes()
    outside = args.output / "outside.parquet"
    outside.write_bytes(original)
    with Server(args, root, "source-negative") as server, server.client() as client:
        try:
            path.unlink()
            path.symlink_to(outside)
            record("symlink_escape", refused(lambda: client.submit_sql(SQL), "symlink_escape") |
                   {"request": SQL, "outside_sha256": sha(outside), "target": str(path.readlink())})
            path.unlink()
            changed = bytearray(original)
            changed[16] ^= 1
            path.write_bytes(changed)
            (args.output / "changed-source.parquet").write_bytes(changed)
            record("source_drift", refused(lambda: client.submit_sql(SQL), "source_drift") |
                   {"request": SQL, "changed_sha256": sha(path), "original_sha256": sha(outside)})
        finally:
            path.unlink(missing_ok=True)
            path.write_bytes(original)
    proc = capture(Server(args, root, "non-loopback").argv() + ["--listen", "0.0.0.0:0"],
                   env=os.environ | {"ADE_GATEWAY_TOKEN": SECRET}, timeout=5)
    assert proc["status"] == 1 and "numeric loopback address required" in proc["stderr"] and not proc["stdout"]
    record("non_loopback", proc)


def worker_negatives(args, root, record):
    names = [name for name in CRITERIA["cases"]["worker"] if name not in ("cancel", "gateway_crash")]
    for name in names:
        artifact = args.output / f"worker-{name}.py"
        marker = args.output / f"worker-{name}.pid"
        artifact.write_text(fault_script(name, marker))
        with Server(args, root, name, script=artifact) as server, server.client() as client:
            raw = plan_bytes(name, server.digest)
            result = refused(lambda: client.submit(descriptor(raw)), name)
            if name == "hang":
                result |= reaped(wait_for_file(marker))
            if name == "wrong_rows":
                events = [json.loads(line) for line in Path(server.log.name).read_text().splitlines()]
                assert events[-1]["worker"]["invocations"] == 2, events
                result["failed_after_first_batch"] = events[-1]
            record(name, result | {"request": b64(raw), "artifact": artifact.name, "digest": server.digest})


def interrupted(args, root, record):
    for name in ("cancel", "disconnect", "concurrent", "gateway_crash"):
        artifact = args.output / f"worker-{name}.py"
        marker = args.output / f"worker-{name}.pid"
        artifact.write_text(fault_script(name, marker))
        with Server(args, root, name, script=artifact) as server, server.client(timeout=1.5 if name in ("cancel", "concurrent") else 10) as client:
            raw = plan_bytes(name, server.digest)
            if name == "disconnect":
                # Terminate an independent client during its RPC so the TCP
                # connection is actually closed instead of waiting for GC.
                helper = """
import base64, os, sys
from ade_client import Client
from pyarrow.flight import FlightDescriptor
client = Client("127.0.0.1", int(sys.argv[1]), os.environ["ADE_GATEWAY_TOKEN"])
client.submit(FlightDescriptor.for_command(base64.b64decode(sys.argv[2])))
"""
                wire = descriptor(raw).command
                process = subprocess.Popen([sys.executable, "-c", helper, str(server.port), b64(wire)],
                                           env=server.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    pid = wait_for_file(marker)
                    process.kill()
                    stdout, stderr = process.communicate(timeout=5)
                    assert process.returncode == -signal.SIGKILL and not stdout and not stderr
                    record(name, {"code": "DISCONNECTED", "returned_ticket": False, "request": b64(raw),
                                  "artifact": artifact.name, "digest": server.digest,
                                  "client_exit": process.returncode, "stdout": stdout, "stderr": stderr} | reaped(pid))
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.communicate(timeout=5)
                continue
            with concurrent.futures.ThreadPoolExecutor(1) as pool:
                task = pool.submit(client.submit, descriptor(raw))
                pid = wait_for_file(marker)
                if name == "gateway_crash":
                    assert server.kill() == -signal.SIGKILL
                elif name == "concurrent":
                    with server.client() as second:
                        record(name, refused(lambda: second.submit_sql(SQL), name) |
                               {"request": SQL, "held_request": b64(raw), "artifact": artifact.name,
                                "digest": server.digest, "active_pid": pid})
                failure_case = "cancel" if name == "concurrent" else name
                evidence = refused(lambda: task.result(timeout=12), failure_case)
                if name != "concurrent":
                    record(name, evidence | reaped(pid) |
                           {"request": b64(raw), "artifact": artifact.name, "digest": server.digest})
                else:
                    reaped(pid)


def publication(args, root, ticket, record):
    with Server(args, root, "publication") as server, server.client() as client:
        record("restart", refused(lambda: client.open_ticket(ticket), "restart") | {"ticket": b64(ticket.ticket)})
        successes = [client.submit_sql(SQL) for _ in range(CRITERIA["budgets"]["ticket_slots"])]
        receipts = [json.loads(info.app_metadata) for info in successes]
        record("ticket_cap", refused(lambda: client.submit_sql(SQL), "ticket_cap") |
               {"request": SQL, "receipts": receipts})
    large = args.output / "source-overflow"
    fixture(large, 256, large=True)
    with Server(args, large, "overflow") as server, server.client() as client:
        query = "SELECT payload FROM input"
        observed = refused(lambda: client.submit_sql(query), "result_overflow")
        table, receipt = client.sql("SELECT row_id FROM input")
        assert table.num_rows == 256 and receipt["retention"]["ticket_count"] == 1
        record("result_overflow", observed | {"request": query, "next_receipt": receipt})


def full_filesystem(args, root, record):
    if args.full_mount is None:
        raise RuntimeError("acceptance requires --full-mount on a disposable mounted filesystem")
    mount = args.full_mount
    assert os.path.ismount(mount)
    marker = mount / "phase22-fill"
    try:
        attempts = []
        with marker.open("xb", buffering=0) as stream:
            for size in (1024 * 1024, 4096, 512):
                block = bytes(size)
                try:
                    for _ in range(64):
                        stream.write(block)
                except OSError as exc:
                    attempts.append({"write_bytes": size, "errno": exc.errno})
                    assert exc.errno == errno.ENOSPC
            os.fsync(stream.fileno())
        assert attempts[-1] == {"write_bytes": 512, "errno": errno.ENOSPC}
        stats = os.statvfs(mount)
        with Server(args, root, "full-filesystem", tmpdir=mount) as server, server.client() as client:
            table, receipt = client.sql(SQL)
            assert canonical(table) == oracle("sql")
            assert receipt["retention"]["spool_bytes"] == 0
            record("enospc", {"request": SQL, "attempts": attempts,
                              "available_blocks": stats.f_bavail, "fragment_bytes": stats.f_frsize,
                              "mount": str(mount), "receipt": receipt, "data": canonical(table),
                              "entries": sorted(path.name for path in mount.iterdir())})
    finally:
        marker.unlink(missing_ok=True)


def benchmarks(args, root):
    data = {}
    for mode in ("plan", "udf"):
        raw = args.output / f"benchmark-{mode}-request.json"
        raw.write_bytes(plan_bytes(mode, DIGEST))
        arrow = args.output / f"benchmark-native-{mode}.arrow"
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        command = capture([args.native, root, raw, sys.executable, SCRIPT, DIGEST, arrow], timeout=90)
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        assert command["status"] == 0, command
        with pa.ipc.open_stream(arrow) as stream:
            assert canonical(stream.read_all()) == oracle(mode)
        data[f"native_{mode}"] = {"command": command, "samples": json.loads(command["stdout"]),
                                  "user_cpu_ns": int((after.ru_utime - before.ru_utime) * 1e9),
                                  "system_cpu_ns": int((after.ru_stime - before.ru_stime) * 1e9)}
    with Server(args, root, "python-bench") as server, server.client() as client:
        samples = []
        for _ in range(CRITERIA["iterations"]):
            start = time.monotonic_ns()
            cpu = time.process_time_ns()
            table, receipt = client.sql(SQL)
            assert canonical(table) == oracle("sql")
            samples.append({"wall_ns": time.monotonic_ns() - start,
                            "client_cpu_ns": time.process_time_ns() - cpu, "receipt": receipt})
        data["python_sql"] = {"samples": samples}
    with Server(args, root, "java-bench") as server:
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        samples, command = java(args, server, "repeat_sql", SQL)
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        for sample in samples:
            assert {key: sample["observed"][key] for key in ("rows", "schema")} == oracle("sql")
        data["java_sql"] = {"command": command, "samples": samples,
                            "user_cpu_ns": int((after.ru_utime - before.ru_utime) * 1e9),
                            "system_cpu_ns": int((after.ru_stime - before.ru_stime) * 1e9)}
    document(args.output / "benchmark.json", data)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--java", required=True)
    parser.add_argument("--java-classpath", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--full-mount", type=Path)
    parser.add_argument("--interop-only", action="store_true", help="CI smoke, not a release archive")
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.binary = args.binary.resolve()
    args.native = args.native.resolve()
    args.output.mkdir()
    document(args.output / "inputs.json", source_inventory(ROOT))
    root = args.output / "source-main"
    fixture(root)
    observations = {}
    document(args.output / "environment.json", {
        "platform": platform.platform(), "machine": platform.machine(), "python": sys.version,
        "pyarrow": pa.__version__, "java": capture([args.java, "-version"]),
        "head": capture(["git", "rev-parse", "HEAD", "HEAD^{tree}"]),
        "status": capture(["git", "status", "--porcelain=v1", "--untracked-files=normal"]),
        "rust": capture(["rustc", "-Vv"]),
        "binary_sha256": sha(args.binary), "native_sha256": sha(args.native),
        "package_sha256": sha(Path(sys.modules["ade_client"].__file__)),
        "java_classpath_sha256": [sha(Path(path)) for path in args.java_classpath.split(os.pathsep)],
        "criteria_sha256": sha(HERE / "criteria.json"),
        "fixture_lock_sha256": sha(HERE / "fixture-lock.json"),
        "interpreter_sha256": sha(Path(sys.executable).resolve()),
        "java_executable_sha256": sha(Path(args.java).resolve()),
        "java_classes": {p.name: sha(p) for p in sorted((ROOT / "clients/java/target/classes/ade").glob("*.class"))},
        "pyarrow_flight_extension_sha256": sha(Path(flight_extension.__file__)),
        "machine_info": capture(["uname", "-a"]),
        "filesystem": capture(["df", "-h", str(args.output)]),
    })

    def record(case, value):
        assert case not in observations
        observations[case] = value
        document(args.output / "cases.json", observations)
        print(case, flush=True)

    ticket = main_service(args, root, record)
    small_fixtures(args, record)
    if args.interop_only:
        return
    source_negatives(args, root, record)
    worker_negatives(args, root, record)
    interrupted(args, root, record)
    publication(args, root, ticket, record)
    full_filesystem(args, root, record)
    benchmarks(args, root)
    required = {name for group, names in CRITERIA["cases"].items() if group != "evidence_tamper" for name in names}
    assert source_inventory(ROOT) == json.loads((args.output / "inputs.json").read_text())
    assert set(observations) == required, (required - observations.keys(), observations.keys() - required)


if __name__ == "__main__":
    main()
