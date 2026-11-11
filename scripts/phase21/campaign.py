#!/usr/bin/env python3
"""Run the frozen Phase 21 cases against owned loopback services."""
import argparse
import concurrent.futures
import errno
import base64
import copy
import hashlib
import http.server
import io
import json
import os
import pathlib
import re
import selectors
import shutil
import socket
import socketserver
import struct
import subprocess
import sys
import threading
import time
import urllib.parse
import uuid

import blake3
import boto3
import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from deltalake import DeltaTable, write_deltalake
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[1]
FIRST = [3, -4, 3]
SECOND = [11, 8]
SQL = "SELECT value FROM input"


def encoded(obj):
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode()


def dump(path, obj):
    path.write_bytes(encoded(obj) + b"\n")


def digest(obj):
    return blake3.blake3(obj).hexdigest()


def sha(path):
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest() if hasattr(hashlib, "file_digest") else sha_stream(file)


def sha_stream(file):
    hasher = hashlib.sha256()
    for chunk in iter(lambda: file.read(1024 * 1024), b""):
        hasher.update(chunk)
    return hasher.hexdigest()


def table(values):
    return pa.Table.from_arrays([pa.array(values, pa.int64())], schema=pa.schema([pa.field("value", pa.int64(), nullable=False)]))


def entry(kind):
    return dict(version=1, name="events", connector=dict(version=1, kind=kind, namespace=digest(kind.encode())),
                schema=[dict(name="value", data_type="i64", nullable=False)])


def split(root, key):
    file = root / key
    return dict(key=key, bytes=file.stat().st_size, rows=pq.ParquetFile(file).metadata.num_rows,
                blake3=digest(file.read_bytes()))


def descriptor(kind, authority, splits):
    return dict(version=1, entry=entry(kind), authority=authority, splits=splits, replay="pinned")


def reserve_ports(count):
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket()
            sockets.append(sock)
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


class Campaign:
    def __init__(self, args):
        self.args = args
        self.root = args.output.resolve()
        self.root.mkdir(mode=0o700)
        self.raw = self.root / "raw"
        self.raw.mkdir()
        self.fixtures = self.root / "fixtures"
        self.fixtures.mkdir()
        self.stage = self.root / "stage"
        self.stage.mkdir()
        self.sink = self.root / "sink"
        self.sink.mkdir()
        self.services = self.root / "services"
        self.services.mkdir()
        self.children = []
        self.cases = {}
        self.setup = {}
        self.criteria = json.loads((HERE / "criteria.json").read_text())

        shutil.copyfile(HERE / "criteria.json", self.root / "criteria.json")
        self.environment()

    def environment(self):
        def command(*argv):
            return subprocess.check_output(argv, cwd=REPO, text=True).strip()

        files = command("git", "ls-files", "--cached", "--others", "--exclude-standard").splitlines()
        runtime = {path: sha(REPO / path) for path in sorted(set(files))
                   if path.endswith(".rs") or pathlib.Path(path).name in ("Cargo.toml", "Cargo.lock")
                   or path.startswith(("scripts/phase21/", "scripts/phase15/Fixture.java"))}
        dump(self.root / "source.json", runtime)
        dump(self.root / "environment.json", dict(
            baseline=command("git", "rev-parse", "HEAD"), tree=command("git", "rev-parse", "HEAD^{tree}"),
            branch=command("git", "branch", "--show-current"), status=command("git", "status", "--short"),
            rust=command("rustc", "-Vv"), cargo=command("cargo", "-V"), uname=list(os.uname()),
            hardware=command("sysctl", "hw.model", "hw.ncpu", "hw.memsize"),
            filesystem=command("df", "-T", "apfs", str(self.root)),
            cargo_lock_sha256=runtime["Cargo.lock"],
            cli_sha256=sha(self.args.binary.parents[1] / "ade-connect"),
            executable_sha256=sha(self.args.binary), features="ade-connectors --all-features",
            versions=dict(pyarrow=pa.__version__, fastavro=fastavro.__version__, boto3=boto3.__version__),
        ))

    def request(self, mode, **fields):
        return dict(mode=mode, stage=str(self.stage), sql=SQL, **fields)

    def call(self, command, label, data=None, env=None, timeout=40):
        result = subprocess.run(command, input=data, capture_output=True, env=env, timeout=timeout)
        (self.services / (label + ".stdout")).write_bytes(result.stdout)
        (self.services / (label + ".stderr")).write_bytes(result.stderr)
        if result.returncode != 0:
            raise RuntimeError(f"{label} failed ({result.returncode})")
        return result.stdout

    def spawn(self, command, label, env=None, stdout=None, stdin=None):
        with (self.services / (label + ".stderr")).open("wb") as err:
            if stdout is None:
                with (self.services / (label + ".stdout")).open("wb") as out:
                    child = subprocess.Popen(command, stdout=out, stderr=err, env=env, stdin=stdin)
            else:
                child = subprocess.Popen(command, stdout=stdout, stderr=err, env=env, stdin=stdin)
        self.children.append(child)
        return child

    def wait_port(self, port, child):
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            if child.poll() is not None:
                raise RuntimeError("service exited before listening")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    return
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("service did not listen")

    def run(self, name, request, env=None, kill=False, advance=None):
        case = self.raw / name
        case.mkdir()
        path = case / "request.json"
        dump(path, request)
        command = [str(self.args.binary), str(path)]
        if request["mode"] == "cli":
            descriptor_path = case / "descriptor.json"
            dump(descriptor_path, request["snapshot"])
            command = [str(self.args.binary.parents[1] / "ade-connect"), request["root"], str(descriptor_path),
                       request["expected"], request["stage"], request["sql"], request["sink"], request["key"]]
        if name.startswith("timing_") and sys.platform == "darwin":
            command = ["/usr/bin/time", "-l", *command]
        before = time.monotonic_ns()
        with (case / "stdout").open("wb") as out, (case / "stderr").open("wb") as err:
            if kill:
                with subprocess.Popen(command, stdout=out, stderr=subprocess.PIPE, env=env) as process:
                    pid = process.pid
                    try:
                        with selectors.DefaultSelector() as ready:
                            ready.register(process.stderr, selectors.EVENT_READ)
                            marker = process.stderr.readline() if ready.select(10) else b""
                        if marker != b"REACHED\n":
                            raise RuntimeError(f"{name} did not reach crash cut")
                        process.kill()
                        _, rest = process.communicate(timeout=10)
                        err.write(marker + rest)
                        code = process.returncode
                    finally:
                        if process.poll() is None:
                            process.kill()
            elif advance:
                with subprocess.Popen(command, stdout=out, stderr=subprocess.PIPE, env=env) as process:
                    pid = process.pid
                    try:
                        with selectors.DefaultSelector() as ready:
                            ready.register(process.stderr, selectors.EVENT_READ)
                            marker = process.stderr.readline() if ready.select(10) else b""
                        if marker != b"HISTORY_RECEIPTS\n":
                            raise RuntimeError("Kafka history barrier not reached")
                        time.sleep(.4)
                        if process.poll() is not None:
                            raise RuntimeError("Kafka idle incorrectly terminated before live append")
                        advance()
                        _, rest = process.communicate(timeout=15)
                        err.write(marker + rest)
                        code = process.returncode
                    finally:
                        if process.poll() is None:
                            process.kill()
            else:
                with subprocess.Popen(command, stdout=out, stderr=err, env=env) as child:
                    pid = child.pid
                    try:
                        code = child.wait(30)
                    finally:
                        if child.poll() is None:
                            child.kill()
        elapsed = time.monotonic_ns() - before
        stage_parent = pathlib.Path(request.get("stage", self.stage))
        live_stages = sorted(p.name for p in stage_parent.iterdir())
        effects = dict(code=code, pid=pid, elapsed_ns=elapsed, live_stages=live_stages)
        if kill:
            directory = pathlib.Path(request["sink"])
            effects["selected"] = (directory / (request["key"] + ".ade-ipc")).exists()
            effects["pending"] = sorted(path.name for path in directory.glob(".ade-sink-*"))
        dump(case / "exit.json", effects)
        self.cases[name] = code
        print(name, code, flush=True)
        # Only this disposable campaign owns stage directories left by killed children.
        for stage in live_stages:
            if not kill or not stage.startswith(".ade-stage-"):
                raise RuntimeError("unexpected stage left after case")
            shutil.rmtree(stage_parent / stage)
        if code != 0:
            return None
        return json.loads((case / "stdout").read_bytes())

    def local(self):
        root = self.fixtures / "local"
        root.mkdir()
        file = root / "part.parquet"
        pq.write_table(table(FIRST), file, compression="NONE")
        snapshot = descriptor("local_parquet", digest(b"fixture-local-v1"), [split(root, file.name)])
        base = self.request("local", root=str(root), snapshot=snapshot, expected=digest(encoded(snapshot)))
        pub = dict(base, sink=str(self.sink), key="once")
        self.run("local", pub)
        self.run("local_cli", dict(pub, mode="cli", key="cli"))
        self.run("local_retry", pub)
        self.run("local_conflict", dict(pub, sql="SELECT value AS renamed FROM input"))
        self.run("local_abort", dict(pub, key="aborted", abort=True))
        self.run("local_wrong_snapshot", dict(base, expected="0" * 64))
        saved = file.read_bytes()
        file.write_bytes(bytes([saved[0] ^ 1]) + saved[1:])
        self.run("local_mutation", base)
        file.unlink()
        target = root / "outside.parquet"
        target.write_bytes(saved)
        file.symlink_to(target)
        self.run("local_symlink", base)
        file.unlink()
        file.write_bytes(saved)
        codec = root / "unsupported.parquet"
        pq.write_table(table(FIRST), codec, compression="gzip")
        snap = descriptor("local_parquet", digest(b"fixture-codec-v1"), [split(root, codec.name)])
        self.run("local_codec", dict(base, snapshot=snap, expected=digest(encoded(snap))))
        footer = root / "footer.parquet"
        footer.write_bytes(saved[:-8] + struct.pack("<I", 2 * 1024 * 1024) + saved[-4:])
        bad_split = dict(snapshot["splits"][0], key=footer.name, blake3=digest(footer.read_bytes()))
        bad = descriptor("local_parquet", digest(b"fixture-footer-v1"), [bad_split])
        self.run("local_footer", dict(base, snapshot=bad, expected=digest(encoded(bad))))
        bad = copy.deepcopy(snapshot)
        bad["splits"][0]["rows"] += 1
        self.run("local_row_count", dict(base, snapshot=bad, expected=digest(encoded(bad))))
        bad = copy.deepcopy(snapshot)
        bad["entry"]["schema"][0]["data_type"] = "u64"
        self.run("local_schema", dict(base, snapshot=bad, expected=digest(encoded(bad))))
        self.run("sink_kill_prepared", dict(pub, key="pre", pause="prepared"), kill=True)
        if (self.sink / "pre.ade-ipc").exists():
            raise RuntimeError("uncommitted sink selected")
        self.run("sink_kill_committed", dict(pub, key="post", pause="committed"), kill=True)
        self.run("sink_restart", dict(pub, key="post"))
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            jobs = []
            for side in ("a", "b"):
                stage = self.root / f"stage-{side}"
                stage.mkdir()
                jobs.append(pool.submit(self.run, f"sink_concurrent_{side}", dict(pub, key="race", stage=str(stage))))
            for job in jobs:
                job.result()
        self.enospc(pub)
        benchmark = root / "benchmark.parquet"
        count = self.criteria["timing"]["rows"]
        pq.write_table(table(FIRST + list(range(count - len(FIRST)))), benchmark,
                       compression="NONE", use_dictionary=False)
        snapshot = descriptor("local_parquet", digest(b"fixture-local-timing-v1"), [split(root, benchmark.name)])
        timed = dict(base, snapshot=snapshot, expected=digest(encoded(snapshot)), summarize=True)
        for i in range(self.criteria["timing"]["pairs"]):
            ordered = ("baseline", "local") if i % 2 == 0 else ("local", "baseline")
            for mode in ordered:
                self.run(f"timing_{i}_{mode}", dict(timed, mode=mode))
        return file

    def enospc(self, request):
        if sys.platform != "darwin":
            return
        image = self.root / "owned-hfs.dmg"
        mount = self.root / "mounted-sink"
        mount.mkdir()
        try:
            self.call(["hdiutil", "create", "-size", "32m", "-fs", "HFS+", "-volname", "ADE21", image], "enospc-create")
            self.call(["hdiutil", "attach", "-nobrowse", "-mountpoint", mount, image], "enospc-attach")
            try:
                if not mount.is_mount() or mount.stat().st_dev == self.root.stat().st_dev:
                    raise RuntimeError("ENOSPC requires a separate mounted image")
                errors = []
                with (mount / "filler").open("wb", buffering=0) as file:
                    for size in (65536, 4096, 512, 1):
                        block = bytes(size)
                        while True:
                            try:
                                file.write(block)
                            except OSError as error:
                                if error.errno != errno.ENOSPC:
                                    raise
                                errors.append(error.errno)
                                break
                free = os.statvfs(mount)
                self.run("sink_enospc", dict(request, sink=str(mount), key="full"))
                dump(self.services / "enospc.json", dict(errors=errors, free=free.f_bavail * free.f_frsize,
                     visible=(mount / "full.ade-ipc").exists()))
            finally:
                self.call(["hdiutil", "detach", mount], "enospc-detach")
        except Exception as error:
            self.setup["enospc"] = {"error": str(error)}
        finally:
            if not mount.is_mount() and image.exists():
                image.unlink()

    def certificates(self):
        root = self.root / "tls"
        if root.exists():
            return root / "ca.pem", root / "foreign.pem", root / "server.pem", root / "server.key"
        root.mkdir(mode=0o700)
        ca, foreign, leaf, key = [root / name for name in ("ca.pem", "foreign.pem", "server.pem", "server.key")]
        for name, cert in (("ca", ca), ("foreign", foreign)):
            self.call(["openssl", "req", "-new", "-x509", "-nodes", "-newkey", "rsa:2048",
                       "-subj", "/CN=ADE21 " + name, "-days", "2", "-keyout", root / (name + ".key"), "-out", cert],
                      "tls-" + name)
        csr = root / "server.csr"
        self.call(["openssl", "req", "-new", "-nodes", "-newkey", "rsa:2048", "-subj", "/CN=localhost",
                   "-keyout", key, "-out", csr], "tls-csr")
        key.chmod(0o600)
        config = root / "leaf.conf"
        config.write_text("basicConstraints=critical,CA:FALSE\nsubjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n")
        self.call(["openssl", "x509", "-req", "-in", csr, "-CA", ca, "-CAkey", root / "ca.key",
                   "-CAcreateserial", "-days", "2", "-extfile", config, "-out", leaf], "tls-sign")
        fixture = self.fixtures / "tls"
        fixture.mkdir()
        for cert in (ca, foreign, leaf):
            shutil.copyfile(cert, fixture / cert.name)
        self.call(["openssl", "verify", "-CAfile", ca, leaf], "tls-independent-verify")
        return ca, foreign, leaf, key

    def delta(self):
        root = self.fixtures / "delta"
        write_deltalake(str(root), table(FIRST), engine="rust")
        t = DeltaTable(str(root))
        t.delete("value < 0")
        logs = sorted((root / "_delta_log").glob("*.json"))
        commits = [digest(path.read_bytes()) for path in logs]
        requests = []
        for i, name in enumerate(("delta_v0", "delta_v1")):
            ref = DeltaTable(str(root), version=i)
            files = [split(root, key) for key in sorted(ref.files())]
            oracle = ref.to_pyarrow_table().column("value").to_pylist()
            pin = dict(version=i, commits=commits[:i + 1], files=files)
            snap = descriptor("delta", digest(encoded(pin)), files)
            request = self.request("delta", root=str(root), entry=snap["entry"], pin=pin, expected=digest(encoded(snap)))
            dump(self.fixtures / (name + ".reference.json"), oracle)
            self.run(name, request)
            requests.append(request)
        changed = logs[1].with_suffix(".gap")
        logs[1].rename(changed)
        self.run("delta_gap", requests[1])
        changed.rename(logs[1])
        data = logs[0].read_bytes()
        actions = [json.loads(line) for line in data.splitlines()]
        next(action["protocol"] for action in actions if "protocol" in action)["minReaderVersion"] = 3
        logs[0].write_bytes(b"\n".join(map(encoded, actions)) + b"\n")
        bad = copy.deepcopy(requests[0])
        bad["pin"]["commits"][0] = digest(logs[0].read_bytes())
        bad["expected"] = digest(encoded(descriptor("delta", digest(encoded(bad["pin"])), bad["pin"]["files"])))
        self.run("delta_protocol", bad)
        (self.raw / "delta_protocol" / "changed.json").write_bytes(logs[0].read_bytes())
        logs[0].write_bytes(data)
        marker = root / "_delta_log" / "_last_checkpoint"
        dump(marker, {"version": 0, "size": 3})
        self.run("delta_checkpoint", requests[0])
        shutil.copyfile(marker, self.raw / "delta_checkpoint" / "checkpoint.json")
        marker.unlink()
        actions = [json.loads(line) for line in data.splitlines()]
        next(action["add"] for action in actions if "add" in action)["deletionVector"] = {"storageType": "i"}
        logs[0].write_bytes(b"\n".join(map(encoded, actions)) + b"\n")
        bad["pin"]["commits"][0] = digest(logs[0].read_bytes())
        bad["expected"] = digest(encoded(descriptor("delta", digest(encoded(bad["pin"])), bad["pin"]["files"])))
        self.run("delta_deletion_vector", bad)
        (self.raw / "delta_deletion_vector" / "changed.json").write_bytes(logs[0].read_bytes())
        logs[0].write_bytes(data)

        def bad_delta(name, actions):
            raw = b"\n".join(map(encoded, actions)) + b"\n"
            logs[0].write_bytes(raw)
            request = copy.deepcopy(requests[0])
            request["pin"]["commits"][0] = digest(raw)
            request["expected"] = digest(encoded(descriptor("delta", digest(encoded(request["pin"])), request["pin"]["files"])))
            self.run(name, request)
            (self.raw / name / "changed.json").write_bytes(raw)
            logs[0].write_bytes(data)

        first = [json.loads(line) for line in data.splitlines()]
        protocol = next(action for action in first if "protocol" in action)
        bad_delta("delta_duplicate", [protocol, *first])
        bad = copy.deepcopy(first)
        next(action["add"] for action in bad if "add" in action)["path"] = "../part.parquet"
        bad_delta("delta_traversal", bad)
        # A real third Delta transaction re-adds the original immutable file.
        readd = next(action for action in first if "add" in action)
        raw = encoded(readd) + b"\n"
        (root / "_delta_log" / "00000000000000000002.json").write_bytes(raw)
        ref = DeltaTable(str(root), version=2)
        files = [split(root, key) for key in sorted(ref.files())]
        pin = dict(version=2, commits=commits + [digest(raw)], files=files)
        snap = descriptor("delta", digest(encoded(pin)), files)
        request = self.request("delta", root=str(root), entry=snap["entry"], pin=pin, expected=digest(encoded(snap)))
        dump(self.fixtures / "delta_readd.reference.json", ref.to_pyarrow_table().column("value").to_pylist())
        self.run("delta_readd", request)

    def iceberg(self):
        root = self.fixtures / "iceberg"
        cat = SqlCatalog("fixture", uri="sqlite:///:memory:", warehouse=self.fixtures.as_uri())
        cat.create_namespace("data")
        ice = cat.create_table("data.events", Schema(NestedField(1, "value", LongType(), required=True)),
                               location=root.as_uri(), properties={"format-version": "1",
                               "write.avro.compression-codec": "null", "write.parquet.compression-codec": "uncompressed"})
        for values in (FIRST, SECOND):
            ice.append(table(values))

        def key(uri):
            return pathlib.Path(urllib.parse.unquote(urllib.parse.urlparse(uri).path)).relative_to(root).as_posix()

        def pin_file(path):
            return dict(key=path, blake3=digest((root / path).read_bytes()))

        metadata = key(ice.metadata_location)
        requests = []
        for i, cut in enumerate(ice.metadata.snapshots):
            ref = ice.scan(snapshot_id=cut.snapshot_id)
            files = [split(root, k) for k in sorted(key(task.file.file_path) for task in ref.plan_files())]
            manifests = sorted(key(m.manifest_path) for m in cut.manifests(ice.io))
            pin = dict(snapshot_id=cut.snapshot_id, metadata=pin_file(metadata), manifest_list=pin_file(key(cut.manifest_list)),
                       manifests=[pin_file(k) for k in manifests], files=files)
            snap = descriptor("iceberg", digest(encoded(pin)), files)
            request = self.request("iceberg", root=str(root), entry=snap["entry"], pin=pin, expected=digest(encoded(snap)))
            name = f"iceberg_s{i}"
            dump(self.fixtures / (name + ".reference.json"), ref.to_arrow().column("value").to_pylist())
            self.run(name, request)
            requests.append(request)
        file = root / metadata
        saved = file.read_bytes()
        updated = json.loads(saved)
        updated["default-spec-id"] = 1
        dump(file, updated)
        self.run("iceberg_metadata_mutation", requests[1])
        bad = copy.deepcopy(requests[1])
        bad["pin"]["metadata"] = pin_file(metadata)
        bad["expected"] = digest(encoded(descriptor("iceberg", digest(encoded(bad["pin"])), bad["pin"]["files"])))
        self.run("iceberg_partition", bad)
        shutil.copyfile(file, self.raw / "iceberg_partition" / "changed.json")
        file.write_bytes(saved)
        manifest = requests[1]["pin"]["manifest_list"]["key"]
        file = root / manifest
        saved = file.read_bytes()
        file.write_bytes(saved[:-4])
        bad["pin"]["metadata"] = pin_file(metadata)
        bad["pin"]["manifest_list"] = pin_file(manifest)
        bad["expected"] = digest(encoded(descriptor("iceberg", digest(encoded(bad["pin"])), bad["pin"]["files"])))
        self.run("iceberg_manifest_truncated", bad)
        shutil.copyfile(file, self.raw / "iceberg_manifest_truncated" / "changed.avro")
        file.write_bytes(saved)

        def bad_metadata(name, edit):
            file = root / metadata
            raw = file.read_bytes()
            updated = json.loads(raw)
            edit(updated)
            dump(file, updated)
            request = copy.deepcopy(requests[0])
            request["pin"]["metadata"] = pin_file(metadata)
            request["expected"] = digest(encoded(descriptor("iceberg", digest(encoded(request["pin"])), request["pin"]["files"])))
            self.run(name, request)
            shutil.copyfile(file, self.raw / name / "changed.json")
            file.write_bytes(raw)

        bad_metadata("iceberg_v2", lambda meta: meta.update({"format-version": 2}))
        bad_metadata("iceberg_uri", lambda meta: meta["snapshots"][0].update({"manifest-list": "https://example.invalid/list.avro"}))
        bad_metadata("iceberg_schema_reuse", lambda meta: meta["schema"]["fields"][0].update({"type": "double"}))
        data_file = root / requests[0]["pin"]["files"][0]["key"]
        data_bytes = data_file.read_bytes()
        data_file.write_bytes(bytes([data_bytes[0] ^ 1]) + data_bytes[1:])
        self.run("iceberg_data_mutation", requests[0])
        data_file.write_bytes(data_bytes)

        def rewritten_avro(raw, mutate):
            reader = fastavro.reader(io.BytesIO(raw))
            rows = list(reader)
            mutate(rows)
            output = io.BytesIO()
            meta = {k: v for k, v in reader.metadata.items() if not k.startswith("avro.")}
            fastavro.writer(output, reader.writer_schema, rows, codec="null", metadata=meta)
            return output.getvalue()

        bad = copy.deepcopy(requests[0])
        manifest = root / bad["pin"]["manifests"][0]["key"]
        manifest_list = root / bad["pin"]["manifest_list"]["key"]
        original_manifest, original_list = manifest.read_bytes(), manifest_list.read_bytes()
        changed_manifest = rewritten_avro(original_manifest, lambda rows: rows[0].update(status=2))
        changed_list = rewritten_avro(original_list, lambda rows: rows[0].update(manifest_length=len(changed_manifest)))
        manifest.write_bytes(changed_manifest)
        manifest_list.write_bytes(changed_list)
        bad["pin"]["manifests"][0] = pin_file(bad["pin"]["manifests"][0]["key"])
        bad["pin"]["manifest_list"] = pin_file(bad["pin"]["manifest_list"]["key"])
        bad["expected"] = digest(encoded(descriptor("iceberg", digest(encoded(bad["pin"])), bad["pin"]["files"])))
        self.run("iceberg_deleted_entry", bad)
        (self.raw / "iceberg_deleted_entry" / "changed.avro").write_bytes(changed_manifest)
        manifest.write_bytes(original_manifest)
        manifest_list.write_bytes(original_list)

    def s3(self, file):
        ports = reserve_ports(10)
        volume = self.root / "s3-data"
        volume.mkdir()
        creds = dict(AWS_ACCESS_KEY_ID="ade" + uuid.uuid4().hex, AWS_SECRET_ACCESS_KEY=uuid.uuid4().hex + uuid.uuid4().hex)
        env = dict(os.environ, **creds)
        env.pop("AWS_SESSION_TOKEN", None)
        config = self.root / "s3-private.json"
        dump(config, {"identities": [{"name": "fixture", "credentials": [{"accessKey": creds["AWS_ACCESS_KEY_ID"],
             "secretKey": creds["AWS_SECRET_ACCESS_KEY"]}], "actions": ["Admin"]}]})
        child = self.spawn([str(self.args.weed), "mini", f"-dir={volume}", "-ip=127.0.0.1", "-ip.bind=127.0.0.1",
                            "-master.telemetry=false", "-admin.ui=false", "-bucket=ade-connectors",
                            f"-s3.config={config}", *(f"-{key}={port}" for key, port in zip(
                                ("s3.port", "master.port", "master.port.grpc", "filer.port", "filer.port.grpc",
                                 "volume.port", "volume.port.grpc", "admin.port", "admin.port.grpc", "s3.port.grpc"), ports))],
                           "s3", env)
        port = ports[0]
        self.wait_port(port, child)
        url = f"http://127.0.0.1:{port}"
        client = boto3.client("s3", endpoint_url=url, region_name="us-east-1",
                              aws_access_key_id=creds["AWS_ACCESS_KEY_ID"], aws_secret_access_key=creds["AWS_SECRET_ACCESS_KEY"],
                              config=Config(proxies={}, retries={"max_attempts": 0}, s3={"addressing_style": "path"}))
        data = file.read_bytes()
        for _ in range(25):
            try:
                client.put_object(Bucket="ade-connectors", Key=file.name, Body=data)
                break
            except client.exceptions.NoSuchBucket:
                time.sleep(0.2)
        snapshot = descriptor("s3_parquet", digest(b"fixture-s3-v1"), [split(file.parent, file.name)])
        request = self.request("s3", url=url, snapshot=snapshot, expected=digest(encoded(snapshot)))
        self.run("s3", request, env)
        self.run("s3_unsafe_endpoint", dict(request, url=f"http://localhost:{port}/"), env)
        unsafe = copy.deepcopy(request)
        unsafe["snapshot"]["splits"][0]["key"] = "../part.parquet"
        self.run("s3_unsafe_key", unsafe, env)
        self.run("s3_auth", request, dict(env, AWS_SECRET_ACCESS_KEY="invalid-fixture-secret"))
        client.put_object(Bucket="ade-connectors", Key=file.name, Body=bytes([data[0] ^ 1]) + data[1:])
        self.run("s3_mutation", request, env)
        benchmark = file.parent / "benchmark.parquet"
        client.put_object(Bucket="ade-connectors", Key=benchmark.name, Body=benchmark.read_bytes())
        for i in range(self.criteria["timing"]["remote_pairs"]):
            ordered = ("baseline", "s3") if i % 2 == 0 else ("s3", "baseline")
            for mode in ordered:
                kind = "s3_parquet" if mode == "s3" else "local_parquet"
                snap = descriptor(kind, digest(b"fixture-remote-timing-v1"), [split(file.parent, benchmark.name)])
                timed = dict(request, mode=mode, root=str(file.parent), snapshot=snap,
                             expected=digest(encoded(snap)), summarize=True)
                self.run(f"timing_remote_{i}_{mode}", timed, env)


        class Fault(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                if self.path == "/trap":
                    self.server.trapped += 1
                    self.send_error(500)
                elif self.server.mode == "stall":
                    self.server.requests.append("stall")
                    time.sleep(2)
                elif self.server.mode == "truncated":
                    self.server.requests.append("truncated")
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(data)))
                    self.send_header("Last-Modified", "Wed, 11 Nov 2026 10:00:00 GMT")
                    self.send_header("ETag", '"fixture"')
                    self.end_headers()
                    self.wfile.write(data[:len(data) // 2])
                else:
                    self.server.requests.append("redirect")
                    self.send_response(302)
                    self.send_header("Location", f"http://localhost:{self.server.server_port}/trap")
                    self.end_headers()

        with http.server.ThreadingHTTPServer(("127.0.0.1", 0), Fault) as server:
            server.mode, server.requests, server.trapped = "redirect", [], 0
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                url = f"http://127.0.0.1:{server.server_port}"
                self.run("s3_redirect", dict(request, url=url), env)
                server.mode = "truncated"
                self.run("s3_truncated", dict(request, url=url), env)
                server.mode = "stall"
                self.run("s3_timeout", dict(request, url=url, timeout_ms=1500), env)
            finally:
                server.shutdown()
                thread.join()
            dump(self.services / "s3-faults.json", dict(requests=server.requests, redirected=server.trapped))
        self.setup["s3"] = dict(binary_sha256=sha(self.args.weed), pid=child.pid, port=port,
                                version=self.call([self.args.weed, "version"], "s3-version").decode().strip())
        for file in (self.services / "s3.stdout", self.services / "s3.stderr"):
            data = file.read_bytes()
            for value in creds.values():
                data = data.replace(value.encode(), b"[fixture credential redacted]")
            file.write_bytes(data)
        config.unlink()

    def postgres(self):
        port, = reserve_ports(1)
        user = "ade_fixture"
        pg = self.args.postgres
        data = self.root / "pg-data"
        self.call([pg / "initdb", "-D", data, "-U", user, "--auth=trust", "--no-locale"], "pg-init")
        ca, foreign, leaf, key = self.certificates()
        child = self.spawn([pg / "postgres", "-D", data, "-h", "127.0.0.1", "-p", str(port), "-k", str(self.root),
                            "-c", "ssl=on", "-c", f"ssl_cert_file={leaf}", "-c", f"ssl_key_file={key}"], "postgres")
        self.wait_port(port, child)
        psql = [pg / "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1", "-p", str(port), "-U", user, "postgres"]
        self.call(psql + ["-c", """CREATE TABLE sample (value bigint NOT NULL); INSERT INTO sample VALUES (3),(-4),(3);
            CREATE TABLE nulls (value bigint); INSERT INTO nulls VALUES (NULL);
            CREATE TABLE wrong (value numeric); INSERT INTO wrong VALUES (3);
            CREATE TABLE many (value bigint NOT NULL); INSERT INTO many SELECT generate_series(1,65537);"""], "pg-fixture")
        self.call(psql + ["-c", "SELECT value FROM sample ORDER BY value"], "pg-oracle")
        request = self.request("postgres", port=port, user=user, table="sample", entry=entry("postgres"))
        self.run("postgres", request)
        self.run("postgres_tls", dict(request, ca=str(ca)))
        self.run("postgres_tls_wrong_ca", dict(request, ca=str(foreign)))
        with subprocess.Popen(psql, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as writer:
            writer.stdin.write(b"BEGIN; UPDATE sample SET value=99 WHERE value=3; DELETE FROM sample WHERE value=-4; INSERT INTO sample VALUES (40); SELECT 'held';\n")
            writer.stdin.flush()
            try:
                with selectors.DefaultSelector() as ready:
                    ready.register(writer.stdout, selectors.EVENT_READ)
                    if not ready.select(5) or writer.stdout.readline() != b"held\n":
                        raise RuntimeError("Postgres writer lock was not reached")
                self.run("postgres_concurrent", request)
            finally:
                writer.stdin.write(b"ROLLBACK;\n")
                writer.stdin.close()
                writer.wait(5)
        for name, relation in (("postgres_null", "nulls"), ("postgres_type", "wrong"),
                               ("postgres_injection", "sample; drop table sample"), ("postgres_limit", "many")):
            self.run(name, dict(request, table=relation))
        with subprocess.Popen(psql, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as lock:
            lock.stdin.write(b"BEGIN; LOCK TABLE sample IN ACCESS EXCLUSIVE MODE; SELECT 'held';\n")
            lock.stdin.flush()
            try:
                with selectors.DefaultSelector() as ready:
                    ready.register(lock.stdout, selectors.EVENT_READ)
                    if not ready.select(5) or lock.stdout.readline() != b"held\n":
                        raise RuntimeError("Postgres DDL lock was not reached")
                self.run("postgres_cancel", dict(request, timeout_ms=350))
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    failed = pool.submit(self.run, "postgres_loss", request)
                    end = time.monotonic() + 5
                    while time.monotonic() < end:
                        killed = subprocess.check_output(psql + ["-c", """SELECT pg_terminate_backend(pid)
                          FROM pg_stat_activity WHERE application_name='ade-connectors' AND wait_event_type='Lock'"""])
                        if b"t" in killed.splitlines():
                            break
                        time.sleep(.05)
                    else:
                        raise RuntimeError("Postgres loss did not reach blocked export")
                    failed.result()
                    dump(self.services / "postgres-loss.json", {"backend_terminated": True})
            finally:
                lock.stdin.write(b"ROLLBACK;\n")
                lock.stdin.close()
                lock.wait(5)
        self.postgres_mid_export(request, port)
        self.postgres_wire_cap(request)
        self.setup["postgres"] = dict(binary_sha256=sha(pg / "postgres"), pid=child.pid, port=port,
                                      version=self.call([pg / "postgres", "--version"], "pg-version").decode().strip())

    def postgres_mid_export(self, request, upstream_port):
        class Disconnect(socketserver.BaseRequestHandler):
            def handle(self):
                with socket.create_connection(("127.0.0.1", upstream_port), timeout=5) as upstream:
                    def send_frontend():
                        try:
                            while True:
                                chunk = self.request.recv(8192)
                                if not chunk:
                                    return
                                upstream.sendall(chunk)
                        except OSError:
                            pass

                    thread = threading.Thread(target=send_frontend, daemon=True)
                    thread.start()
                    with upstream.makefile("rb") as wire:
                        while True:
                            header = wire.read(5)
                            if len(header) != 5:
                                break
                            size, = struct.unpack(">I", header[1:])
                            if not 4 <= size <= 65536:
                                raise RuntimeError("unexpected fixture backend frame")
                            self.request.sendall(header + wire.read(size - 4))
                            if header[:1] == b"D":
                                self.server.rows += 1
                                if self.server.rows == 260:
                                    break
                    self.request.shutdown(socket.SHUT_RDWR)
                    thread.join(5)

        with socketserver.TCPServer(("127.0.0.1", 0), Disconnect) as server:
            server.rows = 0
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                self.run("postgres_mid_export", dict(request, port=server.server_address[1], table="many"))
            finally:
                server.shutdown()
                thread.join()
            dump(self.services / "postgres-mid-export.json", {"forwarded_data_rows": server.rows})

    def postgres_wire_cap(self, request):
        class Oversize(socketserver.BaseRequestHandler):
            def handle(self):
                self.request.settimeout(5)
                if not self.request.recv(4096):
                    return
                self.server.reached = True
                self.request.sendall(b"R" + struct.pack(">I", 32 * 1024 * 1024))
                try:
                    for _ in range(416):
                        self.request.sendall(bytes(65536))
                except (ConnectionResetError, BrokenPipeError):
                    pass

        with socketserver.TCPServer(("127.0.0.1", 0), Oversize) as server:
            server.reached = False
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                self.run("postgres_wire_cap", dict(request, port=server.server_address[1]))
            finally:
                server.shutdown()
                thread.join()
            dump(self.services / "postgres-wire.json", {"oversize_frame_reached": server.reached})

    def flight(self):
        marker = self.root / "flight-mutation"
        child = self.spawn([sys.executable, HERE / "flight_fixture.py", marker], "flight", stdout=subprocess.PIPE)
        with selectors.DefaultSelector() as ready:
            ready.register(child.stdout, selectors.EVENT_READ)
            if not ready.select(15):
                raise RuntimeError("Flight port announcement timeout")
            port = int(child.stdout.readline())
        request = self.request("flight", port=port, ticket="fixture", entry=entry("flight"))
        self.run("flight", request)
        marker.touch()
        self.run("flight_ticket_mutation", request)
        self.run("flight_schema", dict(request, ticket="wrong"))
        self.run("flight_schema_drift", dict(request, ticket="drift"))
        self.run("flight_oversize", dict(request, ticket="large"))
        self.run("flight_timeout", dict(request, ticket="stall", timeout_ms=350))
        self.run("flight_cancel", dict(request, ticket="stall", cancel_ms=350))
        ca, foreign, leaf, key = self.certificates()
        marker.unlink()
        secure = self.spawn([sys.executable, HERE / "flight_fixture.py", marker, leaf, key],
                            "flight-secure", stdout=subprocess.PIPE)
        with selectors.DefaultSelector() as ready:
            ready.register(secure.stdout, selectors.EVENT_READ)
            if not ready.select(15):
                raise RuntimeError("Flight TLS port announcement timeout")
            secure_port = int(secure.stdout.readline())
        self.run("flight_tls", dict(request, port=secure_port, ca=str(ca)))
        self.run("flight_tls_wrong_ca", dict(request, port=secure_port, ca=str(foreign)))
        self.setup["flight"] = dict(port=port, pid=child.pid, pyarrow=pa.__version__)

    def kafka(self):
        port, controller = reserve_ports(2)
        cluster = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("=")
        address = f"127.0.0.1:{port}"
        java = self.args.java / "bin" / "java"
        cp = str(self.args.kafka / "libs" / "*")
        classes = self.root / "java-classes"
        classes.mkdir()
        self.call([self.args.java / "bin" / "javac", "-cp", cp, "-d", classes, REPO / "scripts/phase15/Fixture.java"], "kafka-javac")
        props = self.root / "broker.properties"
        props.write_text(f"""process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:{controller}
listeners=PLAINTEXT://{address},CONTROLLER://127.0.0.1:{controller}
advertised.listeners=PLAINTEXT://{address}
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs={self.root / 'kafka-data'}
auto.create.topics.enable=false
offsets.topic.replication.factor=1
message.max.bytes=32768
""")
        self.call([java, "-cp", cp, "kafka.tools.StorageTool", "format", "--cluster-id=" + cluster, "--config", props], "kafka-format")
        broker = self.spawn([java, "-Xmx512m", "-cp", cp, "kafka.Kafka", props], "kafka")
        self.wait_port(port, broker)
        topic = "ade21-events"
        admin = [java, "-cp", str(classes) + os.pathsep + cp, "Fixture", address]
        self.call(admin + ["create", topic, "2"], "kafka-create")
        events = [dict(version=1, key=i, value_i64=value, event_time_ms=i + 10) for i, value in enumerate(FIRST + SECOND)]
        payload = b"".join(str(i % 2).encode() + b"\t" + encoded(event) + b"\n" for i, event in enumerate(events))
        records = payload.splitlines(keepends=True)
        prefix = self.call(admin + ["produce", topic], "kafka-history-ledger", b"".join(records[:3]))

        def append_live():
            tail = self.call(admin + ["produce", topic], "kafka-live-ledger", b"".join(records[3:]))
            (self.services / "kafka-ledger.stdout").write_bytes(prefix + tail)
        starts = [dict(partition=i, offset=0) for i in range(2)]
        source = dict(cluster=cluster, topic=topic, topic_partitions=[0, 1], original_starts=starts,
                      immutable_history="phase21-owned-immutable", mode=dict(kind="assigned"))
        logical = dict(version=1, computation=dict(kind="kafka", source=source, job="phase21-stateless",
                       key_less_than=1000, allowance_ms=10, projection=["value_i64", "key", "event_time_ms"],
                       window_ms=None, late_policy=None, hash_version=1, timers=[]), sink="receipts")
        config = dict(version=1, bootstrap=address, expected_cluster_id=cluster, topic=topic, starts=starts,
                      security=dict(mode="loopback_dev"), key_less_than=1000, allowance_ms=10,
                      payload_bytes=4096, client_queue_kib=16, sink_stall_ms=5000, timers=[])
        request = dict(mode="kafka", logical=logical, config=config, stop_records=len(events))
        history = copy.deepcopy(request)
        history["logical"]["computation"]["source"]["mode"] = dict(kind="history_then_live",
            cut=[dict(partition=0, offset=2), dict(partition=1, offset=1)])
        history["signal_after"] = 3
        self.run("kafka_history_live", history, advance=append_live)
        self.run("kafka_assigned", request)
        self.run("kafka_idle", dict(request, stop_records=len(events) + 1))
        self.run("kafka_pin", dict(request, wrong_pin=True))
        wrong = copy.deepcopy(request)
        wrong["config"]["expected_cluster_id"] = "wrong-cluster"
        wrong["logical"]["computation"]["source"]["cluster"] = "wrong-cluster"
        self.run("kafka_wrong_cluster", wrong)
        self.run("kafka_restart", request)
        self.call(admin + ["retain", topic, "0", "1"], "kafka-retain")
        self.run("kafka_retention", request)
        self.call(admin + ["expand", topic], "kafka-expand")
        self.run("kafka_expansion", request)
        finite = copy.deepcopy(request)
        finite["logical"]["computation"]["source"]["mode"] = dict(kind="range",
            end=[dict(partition=0, offset=3), dict(partition=1, offset=2)])
        self.run("kafka_range", finite)
        self.setup["kafka"] = dict(pid=broker.pid, port=port, cluster=cluster, version="3.9.1",
                                   java_sha256=sha(java),
                                   jars={p.name: sha(p) for p in sorted((self.args.kafka / "libs").glob("*.jar"))})

    def finish(self):
        for child in reversed(self.children):
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
        dump(self.root / "services.json", self.setup)
        dump(self.root / "case-exits.json", self.cases)


def main():
    parser = argparse.ArgumentParser()
    for name in ("output", "binary", "postgres", "weed", "java", "kafka"):
        parser.add_argument("--" + name, type=pathlib.Path, required=True)
    args = parser.parse_args()
    campaign = Campaign(args)
    failures = {}
    try:
        local_file = campaign.fixtures / "local" / "part.parquet"
        for name in ("local", "delta", "iceberg", "s3", "postgres", "flight", "kafka"):
            try:
                if name == "s3":
                    campaign.s3(local_file)
                else:
                    getattr(campaign, name)()
            except Exception as error:
                failures[name] = f"{type(error).__name__}: {error}"
                print(name, failures[name], file=sys.stderr, flush=True)
    finally:
        campaign.finish()
        dump(campaign.root / "setup-failures.json", failures)
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
