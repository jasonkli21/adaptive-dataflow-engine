#!/usr/bin/env python3
"""Run disposable native workers, the real manager, and an independent literal oracle."""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import struct
import subprocess
import tempfile
import time
import uuid

REPO = Path(__file__).resolve().parents[2]
EVIDENCE = REPO / "docs/evidence/phase23"
LIMITS = dict(queued=16, running=2, slots=4, managed_bytes=768 * 1024**2,
              staging_bytes=1_048_576, retained_bytes=8_388_608,
              cumulative_write_bytes=536_870_912, cost_microunits=2_000_000)
TERMINAL = {"succeeded", "failed", "cancelled", "expired", "quarantined"}


def sha(path):
    result = hashlib.sha256()
    with path.open("rb") as data:
        for block in iter(lambda: data.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def json_file(path, data):
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def inventory():
    paths = ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "rustfmt.toml",
             ".github/workflows/ci.yml", "scripts/smoke.sh", "AGENTS.md", "README.md",
             "docs/product/prd.md", "docs/architecture/architecture.md",
             "docs/plans/phase-23-implementation-plan.md",
             "docs/adr/0030-bounded-local-multitenant-resource-manager-v1.md",
             "docs/phase-23-resource-manager-guide.md",
             "docs/reports/phase-23-implementation-report.md",
             "docs/reports/phase-23-independent-remediation-2026-11-14.md",
             "docs/reports/phase-23-support.json",
             "docs/evidence/phase23-review-remediation/README.md",
             "docs/evidence/phase23-review-remediation/native-history-witness.json"]
    paths += [str(p.relative_to(REPO)) for p in (REPO / "crates").rglob("*")
              if p.is_file() and (p.suffix == ".rs" or p.name == "Cargo.toml")]
    paths += [str(p.relative_to(REPO)) for p in (REPO / "scripts/phase23").glob("*.py")]
    return {name: sha(REPO / name) for name in sorted(paths)}


def recv_exact(sock, n):
    data = bytearray()
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise EOFError("closed frame")
        data.extend(chunk)
    return bytes(data)


def connect(path):
    sock = socket.socket(socket.AF_UNIX)
    sock.settimeout(10)
    sock.connect(str(path))
    return sock


def exchange(path, frame, *, prefix=None):
    with connect(path) as sock:
        sock.sendall((struct.pack(">I", len(frame)) if prefix is None else prefix) + frame)
        if prefix is not None:
            sock.shutdown(socket.SHUT_WR)
        size, = struct.unpack(">I", recv_exact(sock, 4))
        assert 0 < size <= 262_144
        return json.loads(recv_exact(sock, size))


class Case:
    def __init__(self, name, args, root):
        self.name, self.args, self.root = name, args, root
        self.root.mkdir(mode=0o700)
        self.processes, self.cohorts, self.worker_logs, self.events = [], [], [], []
        self.manager = None
        self.state = root / "manager"
        self.socket = self.state / "control.sock"
        self.manager_log = root / "manager.log"
        self.launches = 0

    def command(self, binary, *args):
        return subprocess.check_output([str(binary), *map(str, args)], text=True, timeout=30).strip()

    def child(self, binary, args, log, env=None, stdout=None):
        with log.open("a") as stderr:
            proc = subprocess.Popen([str(binary), *map(str, args)], env=env, stdout=stdout,
                                    stderr=stderr, start_new_session=True)
        self.processes.append(proc)
        return proc

    def close(self):
        for proc in reversed(self.processes):
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
        for proc in self.processes:
            proc.wait(timeout=10)

    def setup(self, *, mode="adaptive", worker_pause=False, env=None, running=2, weight=1, delay=7000):
        for i in range(2):
            root = getattr(self, "ops_base", self.root) / f"ops-{i}"
            data = json.loads(self.command(self.args.batch, "--state", root, "init"))
            store = str(uuid.UUID(data["result"]["store"]))
            workers, pids = [], []
            for j in range(2):
                scratch = self.root / f"w{i}-{j}"
                scratch.mkdir(mode=0o700)
                log = self.root / f"w{i}-{j}.log"
                self.worker_logs.append(log)
                worker_env = os.environ.copy()
                if worker_pause and i == 0:
                    worker_env["ADE_MANAGED_PAUSE"] = "producer"
                proc = self.child(self.args.worker,
                                  ["--managed", j, scratch, store, "127.0.0.1:0", "127.0.0.1:0"],
                                  log, worker_env, subprocess.PIPE)
                magic = proc.stdout.read(8)
                if magic != b"ADEOPS01":
                    raise RuntimeError(log.read_text())
                size, = struct.unpack("<I", proc.stdout.read(4))
                ready = json.loads(proc.stdout.read(size))
                assert ready["boot"]["store"] == store
                proc.stdout.close()
                workers.append(dict(id=j, control=ready["endpoint"]["control"],
                                    exchange=ready["endpoint"]["exchange"],
                                    instance=ready["boot"]["instance"], scratch_root=str(scratch)))
                pids.append(proc.pid)
            self.cohorts.append((dict(root=str(root), store=store, workers=workers), pids))
        config = dict(version=1, mode=mode, aging_ms=delay, standby_idle_ms=100,
                      preempt_after_ms=20, limits=LIMITS | {"running": running},
                      price={"version": 1, "currency": "USD", "microunits_per_slot_ms": 1},
                      cohorts=[c for c, _ in self.cohorts])
        config_path = self.root / "config.json"
        json_file(config_path, config)
        self.config = config
        self.admin = self.command(self.args.manager, "init", "--root", self.state, "--config", config_path)
        self.start(env)
        self.tenants, self.specs = [], {}
        for i in range(2):
            source = self.root / f"tenant-{i}"
            source.mkdir(mode=0o700)
            tenant = self.ok(self.admin, "provision", config=dict(
                source_root=str(source), weight=weight if i == 0 else 1, limits=LIMITS | {"running": 1}))
            self.tenants.append(tenant)
            for kind in ("aggregate", "join", "empty", "boundary"):
                self.specs[i, kind] = json.loads(self.command(self.args.fixture, kind, source / kind))
        return self

    def start(self, env=None):
        self.launches += 1
        proc = self.child(self.args.manager, ["serve", "--root", self.state], self.manager_log,
                          os.environ.copy() | (env or {}))
        self.manager = proc
        def inspect():
            if proc.poll() is not None:
                raise RuntimeError(self.manager_log.read_text())
            try:
                return self.ask(self.admin, "inspect")["ok"]
            except (OSError, EOFError, KeyError):
                return None
        self.await_(inspect, 8)

    def stop(self):
        self.manager.kill()
        self.manager.wait(timeout=10)
        return self.manager.returncode

    def await_(self, predicate, timeout=10):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            result = predicate()
            if result:
                return result
            time.sleep(.012)
        raise AssertionError(f"timed out in {self.name}: {self.manager_log.read_text()[-1600:]}")

    def ask(self, token, op, **kwargs):
        payload = dict(version=1, token=token, request=dict(op=op, **kwargs))
        return exchange(self.socket, json.dumps(payload).encode())

    def ok(self, token, op, **kwargs):
        reply = self.ask(token, op, **kwargs)
        assert reply["ok"], (self.name, op, reply)
        return reply["data"]

    def token(self, i):
        return self.tenants[i]["token"]

    def submit(self, i, key, kind="aggregate", **changes):
        return self.ok(self.token(i), "submit", key=key, spec=self.specs[i, kind] | changes)

    def wait(self, i, ident, states=TERMINAL):
        def check():
            status = self.ok(self.token(i), "status", job=ident)
            return status if status["state"] in states else None
        return self.await_(check, 24)

    def refusal(self, name, reply, code):
        assert reply == {"ok": False, "code": code}, (name, reply, code)
        self.events.append(dict(check=name, code=reply["code"]))
        return {"code": reply["code"]}

    def sample(self):
        pids = [self.manager.pid] + [p for _, group in self.cohorts for p in group]
        out = subprocess.check_output(["ps", "-o", "pid=,rss=,time=", "-p", ",".join(map(str, pids))],
                                      text=True)
        processes = [dict(pid=int(pid), rss_kib=int(rss), cpu_time=cpu)
                     for pid, rss, cpu in (line.split() for line in out.splitlines() if line.strip())]
        size = 0
        missing = 0
        for cohort, _ in self.cohorts:
            roots = [Path(cohort["root"])] + [Path(w["scratch_root"]) for w in cohort["workers"]]
            for root in roots:
                for folder, _, names in os.walk(root):
                    for name in names:
                        try:
                            size += (Path(folder) / name).stat().st_size
                        except FileNotFoundError:
                            missing += 1
        return dict(at_ns=time.monotonic_ns(), processes=processes, native_file_logical_bytes=size,
                    files_disappeared_during_sample=missing)

    def check_result(self, i, status, kind):
        assert status["state"] == "succeeded", status
        ident = status["id"]
        data = bytearray()
        while True:
            chunk = self.ok(self.token(i), "result", job=ident, offset=len(data))
            assert chunk["offset"] == len(data)
            data.extend(bytes.fromhex(chunk["hex"]))
            if len(data) == chunk["total"]:
                break
            assert len(data) < chunk["total"]
        output = self.root / (ident + ".ipc")
        output.write_bytes(data)
        ipc_hash = sha(output)
        asset = self.args.output / "ipc" / (ipc_hash + ".arrow")
        if not asset.exists():
            asset.parent.mkdir(exist_ok=True)
            asset.write_bytes(data)
        decoded = json.loads(self.command(self.args.fixture, "read", output))
        fixture = json.loads((EVIDENCE / "fixture.json").read_text())
        expected = [[5, 13, -7]] * 1025 if kind == "boundary" else fixture[kind]
        assert decoded["rows"] == expected, decoded
        assert decoded["blake3"] == chunk["blake3"]
        attempt = status["attempts"][-1]
        c = self.cohorts[attempt["cohort"]][0]
        marker_path = Path(c["root"]) / "jobs" / attempt["native_id"] / "COMMITTED.json"
        raw_marker = marker_path.read_bytes()
        marker = json.loads(raw_marker)
        assert marker["blake3"] == decoded["blake3"]
        assert marker["identity"]["store"] == c["store"]
        assert marker["bytes"] == len(data) and marker["rows"] == len(expected)
        assert marker["identity"]["instance"] == attempt["instance"]
        assert attempt["usage"]["committed_bytes"] == len(data) + len(raw_marker)
        assert marker_path.with_name(marker["file"]).read_bytes() == bytes(data)
        self.events.append(dict(check="result", tenant=i, kind=kind, status=status,
                                marker=marker, marker_hex=raw_marker.hex(), decoded=decoded, ipc_sha256=ipc_hash))

    def receipt(self):
        config = {k: v for k, v in self.config.items() if k != "cohorts"}
        return dict(name=self.name, config=config, events=self.events, launches=self.launches,
                    tenant_ids=[t["tenant"] for t in self.tenants],
                    cohorts=[dict(store=c["store"], pids=pids,
                                  workers=[{k: w[k] for k in ("control", "exchange", "instance", "id")}
                                           for w in c["workers"]]) for c, pids in self.cohorts])


def auth(case):
    case.setup()
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        copies = list(pool.map(lambda _: case.submit(0, "same"), range(4)))
    assert len({j["id"] for j in copies}) == 1
    a = copies[0]
    b = case.submit(1, "same", "join")
    assert a["id"] != b["id"]
    case.events.append(dict(check="idempotency", same=[j["id"] for j in copies], foreign=b["id"]))
    for op in ("status", "cancel", "result"):
        for target in (b["id"], str(uuid.uuid4())):
            opts = {"offset": 0} if op == "result" else {}
            case.refusal(f"foreign_{op}", case.ask(case.token(0), op, job=target, **opts), "NOT_FOUND")
    case.refusal("changed_key", case.ask(case.token(0), "submit", key="same",
                 spec=case.specs[0, "aggregate"] | {"work": 2}), "KEY_CONFLICT")
    case.refusal("forgery", case.ask("0"*64, "list"), "UNAUTHORIZED")
    case.refusal("tenant_admin", case.ask(case.token(0), "inspect"), "UNAUTHORIZED")
    assert {j["id"] for j in case.ok(case.token(0), "list")} == {a["id"]}
    case.refusal("foreign_source", case.ask(case.token(0), "submit", key="forbidden",
                                          spec=case.specs[1, "join"]), "SOURCE_SCOPE")
    path = case.root / "tenant-0/empty/input.parquet"
    moved = path.with_suffix(".old")
    path.rename(moved)
    path.symlink_to(moved)
    case.refusal("symlink", case.ask(case.token(0), "submit", key="symlink",
                                    spec=case.specs[0, "empty"]), "UNSAFE_PATH")
    path.unlink()
    moved.rename(path)
    case.refusal("oversized", exchange(case.socket, b"", prefix=struct.pack(">I", 262145)), "FRAME_TOO_LARGE")
    case.refusal("malformed", exchange(case.socket, b"{"), "ENCODING")
    case.refusal("truncated", exchange(case.socket, b"{", prefix=struct.pack(">I", 8)), "TRUNCATED_FRAME")
    with connect(case.socket) as slow:
        slow.sendall(b"\x00")
        size, = struct.unpack(">I", recv_exact(slow, 4))
        case.refusal("slow_peer", json.loads(recv_exact(slow, size)), "PEER_READ")
    for i, j, kind in [(0, a, "aggregate"), (1, b, "join")]:
        case.check_result(i, case.wait(i, j["id"]), kind)
    for kind in ("empty", "boundary"):
        j = case.submit(0, kind, kind)
        case.check_result(0, case.wait(0, j["id"]), kind)
    assert case.submit(0, "same")["id"] == a["id"]
    old = case.token(0)
    case.tenants[0] = case.ok(case.admin, "rotate", tenant=case.tenants[0]["tenant"])
    case.refusal("rotated", case.ask(old, "list"), "UNAUTHORIZED")
    assert len(case.ok(case.token(0), "list")) == 3
    case.ok(case.admin, "revoke", tenant=case.tenants[0]["tenant"])
    case.refusal("revoked", case.ask(case.token(0), "list"), "UNAUTHORIZED")


def preemption(case):
    case.setup(worker_pause=True, running=1)
    low = case.submit(0, "low")
    case.await_(lambda: any("ADE_MANAGED_REACHED producer" in p.read_text()
                            for p in case.worker_logs[:2]))
    active = case.wait(0, low["id"], {"running"})
    high = case.submit(1, "high", "join", priority=2)
    case.check_result(1, case.wait(1, high["id"]), "join")
    result = case.wait(0, low["id"])
    case.check_result(0, result, "aggregate")
    previous, fresh = result["attempts"]
    assert active["attempts"][0]["native_id"] == previous["native_id"] != fresh["native_id"]
    assert previous["instance"] != fresh["instance"]
    assert previous["usage"]["outcome"] == "preempted"
    assert previous["usage"]["charge"] > 0
    assert result["expires_ms"] == low["expires_ms"]
    low_root = Path(case.cohorts[previous["cohort"]][0]["root"])
    assert not (low_root / "jobs" / previous["native_id"] / "COMMITTED.json").exists()
    case.events.append(dict(check="preemption", old_native=previous["native_id"],
                            new_native=fresh["native_id"], worker_stage="producer"))


def overlap(case):
    # Both native coordinators pause briefly in a real post-claim gather stage.
    case.setup(env={"ADE_OPS_FAULT": "gather", "ADE_OPS_FAULT_PAUSE_MS": "350"})
    a = case.submit(0, "overlap-a", preemptible=False)
    b = case.submit(1, "overlap-b", "join", preemptible=False)
    def both():
        data = case.ok(case.admin, "inspect")
        if data["totals"]["running"] == 2 and all(j["state"] == "running" for j in data["ledger"]["jobs"]):
            return data
        return None
    observed = case.await_(both)
    case.events.append(dict(check="overlap", observation=observed, sample=case.sample()))
    for i, j, kind in [(0, a, "aggregate"), (1, b, "join")]:
        case.check_result(i, case.wait(i, j["id"]), kind)
    def idle():
        data = case.ok(case.admin, "inspect")
        return data if len(data["ledger"]["standby_transitions"]) == 2 else None
    state = case.await_(idle)
    assert [on for _, on in state["ledger"]["standby_transitions"]] == [True, False]
    case.events.append(dict(check="standby", transitions=state["ledger"]["standby_transitions"],
                            sample=case.sample()))


def crash(case, stage, native=False, kind="aggregate"):
    env = {"ADE_OPS_FAULT" if native else "ADE_RESOURCE_FAULT": stage}
    case.setup(env=env)
    spec = case.specs[0, kind]
    if stage == "submitted_before_reply":
        request = dict(version=1, token=case.token(0),
                       request=dict(op="submit", key="crash", spec=spec))
        stream = connect(case.socket)
        frame = json.dumps(request).encode()
        stream.sendall(struct.pack(">I", len(frame)) + frame)
    else:
        case.submit(0, "crash", kind)
    reached = ("ADE_OPS_REACHED " if native else "ADE_RESOURCE_REACHED ") + stage
    case.await_(lambda: reached in case.manager_log.read_text())
    killed_exit = case.stop()
    assert killed_exit == -signal.SIGKILL
    if stage == "submitted_before_reply":
        assert stream.recv(1) == b""
        stream.close()
    case.start()
    job = case.submit(0, "crash", kind)
    assert job["expires_ms"] == job["queued_ms"] + spec["deadline_ms"]
    result = case.wait(0, job["id"])
    case.events.append(dict(check="crash", job_id=job["id"], stage=stage, reached=reached,
                            killed_exit=killed_exit, status=result))
    if result["state"] == "succeeded":
        case.check_result(0, result, kind)
    else:
        assert result["state"] == "failed", result
        assert len(result["attempts"]) == 1
        attempt = result["attempts"][0]
        root = Path(case.cohorts[attempt["cohort"]][0]["root"])
        assert not (root / "jobs" / attempt["native_id"] / "COMMITTED.json").exists()
        follow = case.submit(0, "after-recovery", kind)
        case.check_result(0, case.wait(0, follow["id"]), kind)


def fault_preemption(case, stage):
    case.setup(worker_pause=True, running=1, env={"ADE_RESOURCE_FAULT": stage})
    low = case.submit(0, "low")
    case.await_(lambda: any("ADE_MANAGED_REACHED producer" in p.read_text() for p in case.worker_logs[:2]))
    active = case.wait(0, low["id"], {"running"})
    high = case.submit(1, "high", "join", priority=2)
    reached = "ADE_RESOURCE_REACHED " + stage
    case.await_(lambda: reached in case.manager_log.read_text())
    killed_exit = case.stop()
    assert killed_exit == -signal.SIGKILL
    case.start()
    case.check_result(1, case.wait(1, high["id"]), "join")
    result = case.wait(0, low["id"])
    case.check_result(0, result, "aggregate")
    a, b = result["attempts"]
    assert active["attempts"][0]["native_id"] == a["native_id"] != b["native_id"]
    assert a["usage"]["outcome"] == "preempted"
    case.events.append(dict(check="crash", job_id=low["id"], stage=stage, reached=reached,
                            killed_exit=killed_exit, status=result))


def commit_race(case):
    case.setup(running=1, env={"ADE_OPS_FAULT": "marker_published", "ADE_OPS_FAULT_PAUSE_MS": "500"})
    initial = case.submit(0, "race")
    case.await_(lambda: "ADE_OPS_REACHED marker_published" in case.manager_log.read_text())
    # The native marker is linked while native commit still holds its metadata
    # lock. Persist manager cancellation during that interval.
    cancelling = case.ok(case.token(0), "cancel", job=initial["id"])
    assert cancelling["intent"] == "cancel"
    terminal = case.wait(0, initial["id"])
    case.check_result(0, terminal, "aggregate")
    assert len(terminal["attempts"]) == 1
    case.events.append(dict(check="commit_race", reached="ADE_OPS_REACHED marker_published",
                            cancelled=cancelling, terminal=terminal))


def quarantine(case):
    case.setup(worker_pause=True)
    job = case.submit(0, "poison")
    case.await_(lambda: any("ADE_MANAGED_REACHED producer" in p.read_text() for p in case.worker_logs[:2]))
    before = case.wait(0, job["id"], {"running"})
    killed = case.cohorts[0][1][0]
    os.kill(killed, signal.SIGKILL)
    case.ok(case.token(0), "cancel", job=job["id"])
    terminal = case.wait(0, job["id"])
    assert terminal["state"] == "quarantined" and terminal["attempts"][0]["usage"] is None
    case.stop()
    case.start()
    held, neighbour = case.ok(case.admin, "inspect"), case.submit(1, "independent", "join")
    assert held["totals"]["running"] == 1
    case.check_result(1, case.wait(1, neighbour["id"]), "join")
    case.events.append(dict(check="quarantine", worker_stage="producer", killed_pid=killed,
                            before=before, after=terminal, reservation=held["totals"]))


def quotas(case):
    case.setup(running=1, worker_pause=True)
    low = case.submit(0, "nonpreemptible", preemptible=False)
    case.await_(lambda: any("ADE_MANAGED_REACHED producer" in p.read_text() for p in case.worker_logs[:2]))
    case.wait(0, low["id"], {"running"})
    for dimension, changed in {
        "managed_bytes": 1, "staging_bytes": 1, "slots": 1, "retained_bytes": 1,
        "cumulative_write_bytes": 1, "cost_microunits": 1,
    }.items():
        case.refusal("tighten_" + dimension, case.ask(case.admin, "limits", tenant=None,
                     limits=LIMITS | {"running": 1, dimension: changed}), "QUOTA_IN_USE")
    case.ok(case.admin, "limits", tenant=case.tenants[1]["tenant"], limits=LIMITS | {"queued": 1})
    high = case.submit(1, "high", "join", priority=2)
    assert case.wait(1, high["id"], {"queued"})["state"] == "queued"
    case.refusal("queue_full", case.ask(case.token(1), "submit", key="third",
                                       spec=case.specs[1, "join"]), "QUEUE_FULL")
    case.refusal("zero_work", case.ask(case.token(0), "submit", key="zero",
                    spec=case.specs[0, "aggregate"] | {"work": 0}), "INVALID_SPEC")
    case.refusal("negative_work", case.ask(case.token(0), "submit", key="negative",
                    spec=case.specs[0, "aggregate"] | {"work": -1}), "ENCODING")
    case.refusal("numeric_overflow", case.ask(case.token(0), "submit", key="overflow",
                    spec=case.specs[0, "aggregate"] | {"runtime_bytes": 2**64}), "ENCODING")
    case.refusal("stale_price", case.ask(case.admin, "price",
                    price={"version": 1, "currency": "USD", "microunits_per_slot_ms": 2}), "PRICE_PINNED")
    case.ok(case.token(0), "cancel", job=low["id"])
    result = case.wait(0, low["id"])
    assert result["state"] == "cancelled" and len(result["attempts"]) == 1
    case.check_result(1, case.wait(1, high["id"]), "join")
    tail = case.ok(case.admin, "inspect")
    assert tail["totals"]["running"] == 0
    case.events.append(dict(check="quota_release", cancelled=result, totals=tail["totals"]))


def drift(case):
    case.setup(running=1, worker_pause=True)
    blocker = case.submit(0, "blocker", preemptible=False)
    case.await_(lambda: any("ADE_MANAGED_REACHED producer" in p.read_text() for p in case.worker_logs[:2]))
    queued = case.submit(1, "drift", "join")
    expiring = case.submit(1, "expiring", "empty", deadline_ms=250)
    assert case.wait(1, expiring["id"])["state"] == "expired"
    directory = case.root / "tenant-1"
    moved = directory.with_name("moved")
    directory.rename(moved)
    directory.symlink_to(moved, target_is_directory=True)
    case.ok(case.token(0), "cancel", job=blocker["id"])
    failed = case.wait(1, queued["id"])
    assert failed["state"] == "failed" and failed["reason"] == "SOURCE_DRIFT"
    assert failed["attempts"][0]["native_id"] is None
    case.events.append(dict(check="source_drift", refused=failed, expired=expiring["id"]))


def fairness(case, weight):
    case.setup(worker_pause=True, running=1, weight=weight, delay=50)
    blocker = case.submit(1, "blocker", preemptible=False)
    case.await_(lambda: any("ADE_MANAGED_REACHED producer" in p.read_text() for p in case.worker_logs[:2]))
    jobs = [(i, case.submit(i, f"fair-{i}-{j}", priority=(0 if i == 0 else 2)))
            for j in range(5) for i in (0, 1)]
    # Keep an aged low-priority backlog contending with top-rank arrivals.
    time.sleep(.08)
    case.ok(case.token(1), "cancel", job=blocker["id"])
    for i, item in jobs:
        case.check_result(i, case.wait(i, item["id"]), "aggregate")
    state = case.ok(case.admin, "inspect")["ledger"]
    assert len(state["jobs"]) == 11
    case.events.append(dict(check="fairness", weights=[weight, 1], blocker=blocker["id"],
                            starts=state["jobs"], sample=case.sample()))


def measure(case, mode):
    case.setup(mode=mode)
    jobs, samples = [], []
    start = time.monotonic_ns()
    for j in range(10):
        i = j % 2
        kind = "aggregate" if i == 0 else "boundary"
        jobs.append((i, kind, case.submit(i, f"measure-{j}", kind, preemptible=False)))
        samples.append(case.sample())
    for i, kind, job in jobs:
        case.check_result(i, case.wait(i, job["id"]), kind)
        samples.append(case.sample())
    end = time.monotonic_ns()
    case.events.append(dict(check="measure", mode=mode, elapsed_ns=end - start, samples=samples))


def snapshot_crash(case):
    case.setup()
    case.stop()
    case.start({"ADE_RESOURCE_FAULT": "snapshot_before_rename"})
    request = dict(version=1, token=case.token(0),
                   request=dict(op="submit", key="snapshot", spec=case.specs[0, "aggregate"]))
    with connect(case.socket) as stream:
        frame = json.dumps(request).encode()
        stream.sendall(struct.pack(">I", len(frame)) + frame)
        reached = "ADE_RESOURCE_REACHED snapshot_before_rename"
        case.await_(lambda: reached in case.manager_log.read_text())
        killed = case.stop()
        assert stream.recv(1) == b""
    case.start()
    assert case.ok(case.token(0), "list") == []
    job = case.submit(0, "snapshot")
    result = case.wait(0, job["id"])
    case.check_result(0, result, "aggregate")
    case.events.append(dict(check="crash", stage="snapshot_before_rename", reached=reached,
                            killed_exit=killed, status=result, unpublished_discarded=True))


def corruption(case):
    case.setup()
    first = case.submit(0, "durable")
    case.check_result(0, case.wait(0, first["id"]), "aggregate")
    case.stop()
    ledger = case.state / "state.json"
    original = ledger.read_bytes()
    def refuse(code, env=None):
        proc = subprocess.run([str(case.args.manager), "serve", "--root", str(case.state)],
                              env=os.environ.copy() | (env or {}), capture_output=True,
                              text=True, timeout=5)
        assert proc.returncode == 1 and proc.stderr.strip() == "ade-resource: " + code
        case.events.append(dict(check="reopen_refusal", code=code, exit=proc.returncode))
    refuse("CLOCK_ROLLBACK", {"ADE_RESOURCE_CLOCK_TEST_ROLLBACK_MS": "120000"})
    ledger.write_bytes(original.replace(b'"clock_ms":', b'"clock_ms":0,"ignored":', 1))
    refuse("CORRUPT_LEDGER")
    ledger.unlink()
    refuse("PRIVATE_FILE")
    victim = case.root / "victim"
    victim.write_bytes(original)
    ledger.symlink_to(victim)
    refuse("PRIVATE_FILE")
    ledger.unlink()
    ledger.write_bytes(original)
    ledger.chmod(0o644)
    refuse("PRIVATE_FILE")
    ledger.chmod(0o600)
    case.start()
    assert case.submit(0, "durable")["id"] == first["id"]


def cohort_identity(case):
    case.setup(running=1, worker_pause=True)
    start = case.root / "config.json"
    alias = json.loads(start.read_text())
    alias["cohorts"][1]["workers"][1] = alias["cohorts"][0]["workers"][1]
    bad_config = case.root / "alias.json"
    json_file(bad_config, alias)
    rejected = subprocess.run([str(case.args.manager), "init", "--root", str(case.root / "invalid"),
                               "--config", str(bad_config)], text=True, capture_output=True, timeout=10)
    assert rejected.returncode == 1 and not (case.root / "invalid").exists()
    # Starting a sibling manager against the same two stores can initialize
    # metadata while idle, but the independent kernel cohort lease refuses serve.
    sibling = case.root / "sibling"
    case.command(case.args.manager, "init", "--root", sibling, "--config", start)
    blocked = subprocess.run([str(case.args.manager), "serve", "--root", str(sibling)],
                             text=True, capture_output=True, timeout=10)
    assert blocked.returncode == 1 and blocked.stderr.strip().endswith("COHORT_LEASE_BUSY"), blocked.stderr
    case.events.append(dict(check="cohort_lease", alias_refusal=rejected.stderr.strip(),
                            sibling_refusal=blocked.stderr.strip()))
    job = case.submit(0, "identity", preemptible=False)
    case.await_(lambda: any("ADE_MANAGED_REACHED producer" in p.read_text() for p in case.worker_logs[:2]))
    case.ok(case.token(0), "cancel", job=job["id"])
    assert case.wait(0, job["id"])["state"] == "cancelled"
    cohort, pids = case.cohorts[0]
    for pid in pids:
        os.kill(pid, signal.SIGKILL)
    old = cohort["workers"][0]
    replacement = case.child(case.args.worker, ["--managed", 0, old["scratch_root"],
                              cohort["store"], old["control"], old["exchange"]],
                             case.root / "replacement.log", stdout=subprocess.PIPE)
    assert replacement.stdout.read(8) == b"ADEOPS01"
    size, = struct.unpack("<I", replacement.stdout.read(4))
    newer = json.loads(replacement.stdout.read(size))
    replacement.stdout.close()
    assert newer["boot"]["instance"] != old["instance"]
    suspect = case.submit(0, "boot-changed", preemptible=False)
    result = case.wait(0, suspect["id"])
    assert result["state"] == "quarantined" and result["reason"] == "WORKER_IDENTITY"
    case.events.append(dict(check="boot_replacement", old=old["instance"], new=newer["boot"]["instance"],
                            pid=replacement.pid, status=result))


def history(case):
    case.setup(mode="fifo")
    for j in range(32):
        job = case.submit(j % 2, f"history-{j}", "empty", preemptible=False)
        assert case.wait(j % 2, job["id"])["state"] == "succeeded"
    refusal = case.refusal("native_history_full", case.ask(case.token(0), "submit",
                           key="native-history-full", spec=case.specs[0, "empty"]),
                           "NATIVE_HISTORY_FULL")
    case.events.append(dict(check="history", native_count=32, native_history_refusal=refusal))


def run_cases(args, names):
    cases = []
    with tempfile.TemporaryDirectory(prefix="a23-", dir="/private/tmp" if os.path.isdir("/private/tmp") else "/tmp") as tmp:
        for i, (name, fn) in enumerate(names.items()):
            if args.only and name not in args.only:
                continue
            case = Case(name, args, Path(tmp).resolve() / str(i))
            print(name, flush=True)
            try:
                fn(case)
                cases.append(case.receipt())
                json_file(args.output / "cases.json", cases)
            except Exception:
                if case.manager_log.exists():
                    print(case.manager_log.read_text(), flush=True)
                for path in case.worker_logs:
                    print(path.read_text(), flush=True)
                json_file(args.output / "failed-events.json", case.events)
                raise
            finally:
                case.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bin-dir", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--only", action="append")
    args = p.parse_args()
    args.bin_dir = args.bin_dir.resolve()
    args.manager = args.bin_dir / "ade-resource"
    args.batch = args.bin_dir / "ade-batch"
    args.worker = args.bin_dir / "ade-worker"
    args.fixture = args.bin_dir / "examples/phase23-fixture"
    args.output.mkdir(parents=True, exist_ok=False)
    for file in ("criteria.json", "fixture.json", "frozen.sha256"):
        shutil.copyfile(EVIDENCE / file, args.output / file)
    json_file(args.output / "source-inventory.json", inventory())
    json_file(args.output / "executables.json",
              {n: sha(getattr(args, n)) for n in ("manager", "batch", "worker", "fixture")})
    json_file(args.output / "host.json", dict(
        baseline=json.loads((EVIDENCE / "baseline.json").read_text())["source"],
        candidate_sha=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
        tree_sha=subprocess.check_output(["git", "rev-parse", "HEAD^{tree}"], cwd=REPO, text=True).strip(),
        clean_worktree=not bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=REPO, text=True).strip()),
        uname={key: getattr(os.uname(), key) for key in ("sysname","release","machine")},
        features=["workspace/all-features"],
        rustc=subprocess.check_output(["rustc", "--version"], text=True).strip(),
        cpu_count=os.cpu_count(),
    ))
    cases = dict(auth=auth, preemption=preemption, overlap=overlap,
                 commit_race=commit_race, quarantine=quarantine, quotas=quotas, drift=drift,
                 crash_snapshot_before_rename=snapshot_crash, corruption=corruption,
                 cohort_identity=cohort_identity, history=history)
    for weight in (1, 2):
        cases[f"fairness_{weight}"] = lambda case, weight=weight: fairness(case, weight)
    for mode in ("fifo", "adaptive"):
        for repeat in range(3):
            cases[f"measure_{mode}_{repeat}"] = lambda case, mode=mode: measure(case, mode)
    for stage in ("preempt_persisted_before_cancel", "drained_before_settlement"):
        cases["crash_" + stage] = lambda case, stage=stage: fault_preemption(case, stage)
    for stage in ("reserved_before_registration", "native_registered_before_mapping",
                  "mapped_before_start", "native_terminal_before_meter", "meter_persisted",
                  "submitted_before_reply"):
        cases["crash_" + stage] = lambda case, stage=stage: crash(case, stage)
    for stage in ("claimed_native", "gather", "prepublish", "marker_published"):
        for kind in ("aggregate", "join"):
            cases[f"crash_{kind}_{stage}"] = lambda case, stage=stage, kind=kind: crash(case, stage, True, kind)
    run_cases(args, cases)


if __name__ == "__main__":
    main()
