"""Capture fresh-process correctness, faults and whole-sequence economics."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import select
import shutil
import signal
import subprocess
import time

from fixtures import U64, digest, make_fixture, plan, write

FORECAST = dict(uses_milli=7000, survival_bps=9000, recompute_us=50_000,
                restore_us=25_000, write_us=60_000, hold_us=1000, invalidation_us=2000)
MODES = ("disabled", "always", "expected_value_v1")


class Campaign:
    def __init__(self, binary, out):
        self.binary = binary.resolve()
        self.out = out.resolve()
        self.log = self.out / "events.jsonl"
        self.roots = self.out / "roots"
        self.requests = self.out / "requests"
        self.fixtures = self.out / "fixtures"
        self.artifacts = self.out / "artifacts"
        for p in (self.roots, self.requests, self.fixtures, self.artifacts):
            p.mkdir(mode=0o700)

    def receipt(self, label, argv, pid, status, start, stdout, stderr, rss=None):
        end = time.perf_counter_ns()
        record = dict(label=label, argv=[str(a).replace(str(self.out), "$OUT") for a in argv],
                      pid=pid, exit=status, started_ns=start, ended_ns=end, wall_ns=end-start,
                      stdout=stdout, stderr=stderr, rss_bytes=rss)
        with self.log.open("a") as file:
            file.write(json.dumps(record, separators=(",", ":")) + "\n")
        return record

    def call(self, label, argv, *, status=0, error=None, measure=False):
        command = [self.binary, *map(str, argv)]
        if measure:
            command = (["/usr/bin/time", "-l"] if platform.system() == "Darwin"
                       else ["/usr/bin/time", "-f", "peak_kib=%M"]) + command
        start = time.perf_counter_ns()
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as p:
            out, err = p.communicate(timeout=125)
            pid = json.loads(out).get("audit", {}).get("pid", p.pid) if out else p.pid
            rss = None
            if measure:
                pattern = r"(\d+)\s+maximum resident set size" if platform.system() == "Darwin" else r"peak_kib=(\d+)"
                match = re.search(pattern, err)
                if match:
                    rss = int(match[1])
                    if platform.system() != "Darwin":
                        rss *= 1024
            rec = self.receipt(label, argv, pid, p.returncode, start, out, err, rss)
        assert rec["exit"] == status, rec
        if measure:
            assert rss is not None, rec
        if error:
            assert not out and error in err, rec
        return json.loads(out) if out else None

    def pause(self, argv, point, *, release=None):
        env = dict(os.environ, ADE_CACHE_PAUSE=point)
        if release:
            env["ADE_CACHE_RESUME"] = str(release)
        start = time.perf_counter_ns()
        p = subprocess.Popen([self.binary, *map(str, argv)], env=env,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if not select.select([p.stderr], [], [], 20)[0]:
            p.kill()
            streams = p.communicate()
            raise RuntimeError(("pause not reached", argv, streams))
        line = p.stderr.readline()
        if line != f"phase26_pause {point}\n".encode():
            p.kill()
            raise RuntimeError(("unexpected pause", line, p.communicate(), argv))
        return p, argv, start, line

    def finish(self, label, pending, status, *, action=None, error=None):
        p, argv, start, line = pending
        if action:
            p.send_signal(action)
        out, err = p.communicate(timeout=20)
        rec = self.receipt(label, argv, p.pid, p.returncode, start,
                           out.decode(), (line + err).decode())
        assert rec["exit"] == status, rec
        if error:
            assert error in rec["stderr"], rec
        if status:
            assert not rec["stdout"], rec
        return json.loads(out) if out else None

    def native(self, name, root, threshold, group=0, **kw):
        file = self.requests / (name + ".json")
        write(file, plan(root, threshold, group, **kw))
        return file

    def cache(self, label, *, quota=268_435_456, entries=32, parent=None):
        root = (parent or self.roots) / label
        root.mkdir(mode=0o700)
        self.call(label + "-init", ["init", "--cache", root, "--quota-bytes", quota,
                                   "--max-entries", entries])
        return root

    def args(self, request, source, cache=None, *, mode="always", offer=None,
             ttl=3_600_000, stage=2**22, cut=2, pin=None, timeout=30000):
        args = ["--timeout-ms", timeout, "run", "--plan", request, "--source", source,
                "--catalog-pin", pin or digest((source / "catalog.json").read_bytes()),
                "--policy", mode, "--cut", cut, "--ttl-ms", ttl, "--stage-bytes", stage]
        if cache is not None:
            args += ["--cache", cache]
        if offer is not None:
            args += ["--calibration", offer]
        return args

    def offer(self, name, **changes):
        path = self.requests / (name + ".forecast.json")
        write(path, FORECAST | changes)
        return path

    def preserve(self, cache, key, name):
        shutil.copytree(cache / key, self.artifacts / name)

    def semantic_and_fault(self):
        source = make_fixture(self.fixtures / "multi", 8194, 5, 3)
        a = self.native("alpha", source, 4000)
        b = self.native("beta", source, 4000, 2, alias="total")
        root = self.cache("main")
        for name, req in (("alpha", a), ("beta", b)):
            self.call("native-" + name, self.args(req, source, root, mode="disabled"))
        fill = self.call("fill-alpha", self.args(a, source, root))
        key = fill["audit"]["prefix_key"]
        self.preserve(root, key, "prefix")
        hit = self.call("hit-beta", self.args(b, source, root))
        assert hit["audit"]["actions"] == ["hit"] and hit["audit"]["prefix_key"] == key
        self.call("never-beta", self.args(b, source, root / "absent", mode="never"))
        assert set(fill["audit"]["actions"]) == {"miss", "publish"}

        for name, threshold, options, group in (
            ("zero", 0, {}, 0),
            ("uint64-max", U64, {}, 0),
            ("one", 1, {}, 0),
            ("projection-order", 4000, {"projection": [(3, "alt"), (1, "value_i64"), (0, "key")]}, 2),
            ("projection-alias", 4000, {"projection": [(0, "name"), (1, "signed"), (3, "alt")]}, 0),
            ("filter-cut", 4000, {"cut": 1}, 0),
        ):
            req = self.native(name, source, threshold, group, **options)
            args = self.args(req, source, root, cut=options.get("cut", 2))
            answer = self.call("fill-" + name, args)
            assert answer["audit"]["actions"] == ["miss", "publish"] and answer["audit"]["prefix_key"] != key
            self.call("hit-" + name, args)

        empty = make_fixture(self.fixtures / "empty", 0, 4, 1)
        e = self.native("empty", empty, U64)
        self.call("fill-empty", self.args(e, empty, root))
        self.call("hit-empty", self.args(e, empty, root))
        overflow = make_fixture(self.fixtures / "overflow", 2, 4, 1, overflow=True)
        # Build a checked overflow in one group by selecting both input rows.
        p = overflow / "part-0.parquet"
        import pyarrow as pa
        import pyarrow.parquet as pq
        tab = pq.read_table(p)
        tab = tab.set_column(2, tab.schema.field(2), pa.array([0, 0], type=pa.uint64()))
        pq.write_table(tab, p)
        self.repin(overflow, 0)
        o = self.native("overflow", overflow, U64)
        self.call("overflow-stage", self.args(o, overflow, root), status=1, error="arithmetic overflow")
        self.call("overflow-native", self.args(o, overflow, mode="disabled"), status=1, error="arithmetic overflow")

        for name, opts, error in (
            ("row-cap", {"cap": 1}, "output cap"),
            ("byte-cap", {"cap_bytes": 8}, "output cap"),
            ("nonreplayable", {"replayable": False}, "snapshot"),
            ("limit", {"suffix_limit": True}, "PlanV1"),
        ):
            req = self.native(name, source, 4000, **opts)
            self.call(name, self.args(req, source, root), status=1, error=error)

        request = plan(source, 4000)
        node = dict(id=len(request["nodes"]),
                    operator=dict(op="filter", input=request["output"], column=0, less_than=2),
                    schema=request["nodes"][-1]["schema"], properties=request["nodes"][-1]["properties"])
        request["nodes"].append(node)
        request["output"] = node["id"]
        unsupported = self.requests / "unsupported.json"
        write(unsupported, request)
        self.call("valid-unsupported-suffix", self.args(unsupported, source, root), status=1,
                  error="unsupported native cut")
        self.call("valid-native-bypass", self.args(unsupported, source, root, mode="disabled"))

        nullable = self.fixtures / "nullable"
        shutil.copytree(source, nullable)
        file = nullable / "part-0.parquet"
        table = pq.read_table(file)
        values = table[0].to_pylist()
        values[0] = None
        table = table.set_column(0, table.schema.field(0).with_nullable(True),
                                 pa.array(values, type=pa.uint64()))
        pq.write_table(table, file)
        self.repin(nullable, 0)
        req = self.native("nullable", nullable, 4000)
        self.call("physical-nullability", self.args(req, nullable, root), status=1,
                  error="invalid source")
        source2 = self.fixtures / "reordered"
        shutil.copytree(source, source2)
        catalog = json.loads((source2 / "catalog.json").read_bytes())
        catalog["files"].reverse()
        write(source2 / "catalog.json", catalog)
        req = self.native("reordered", source2, 4000)
        self.call("reordered-miss", self.args(req, source2, root))
        old_pin = digest((source / "catalog.json").read_bytes())
        self.call("stale-pin", self.args(a, source2, root, pin=old_pin), status=1, error="snapshot")

        self.source_drift(source, root, a, b)
        self.corruption(source, root, b, key)
        self.crashes(source, a, b)
        self.eviction(source, a, b)
        self.security(source, a)
        self.call("final-main-hit", self.args(b, source, root))

    @staticmethod
    def repin(root, index):
        catalog = json.loads((root / "catalog.json").read_bytes())
        file = catalog["files"][index]
        data = (root / file["name"]).read_bytes()
        file.update(bytes=len(data), fingerprint=digest(data))
        write(root / "catalog.json", catalog)

    def source_drift(self, source, cache, a, b):
        path = source / "part-0.parquet"
        contents = path.read_bytes()
        damaged = bytearray(contents)
        damaged[32] ^= 1
        path.write_bytes(damaged)
        self.call("same-size-source-mutation", self.args(b, source, cache), status=1, error="snapshot")
        path.write_bytes(contents)
        retired = source / "retired"
        path.rename(retired)
        self.call("revoked-original", self.args(b, source, cache), status=1, error="I/O")
        path.symlink_to(retired.name)
        self.call("source-symlink", self.args(b, source, cache), status=1, error="I/O")
        path.unlink()
        retired.rename(path)

        for case in ("same-size", "identical-inode-swap", "catalog-drift"):
            release = self.roots / ("continue-" + case)
            args = self.args(b, source, cache)
            pending = self.pause(args, "reader_pinned", release=release)
            catalog = source / "catalog.json"
            original_catalog = catalog.read_bytes()
            if case == "same-size":
                path.write_bytes(damaged)
            elif case == "identical-inode-swap":
                path.rename(retired)
                path.write_bytes(contents)
            else:
                catalog.write_bytes(original_catalog + b" ")
            release.touch()
            self.finish("postflight-" + case, pending, 1, error="snapshot")
            release.unlink()
            if case == "identical-inode-swap":
                path.unlink()
                retired.rename(path)
            else:
                path.write_bytes(contents)
                catalog.write_bytes(original_catalog)
        self.call("hit-after-restore", self.args(a, source, cache))

    def corruption(self, source, cache, req, key):
        directory = cache / key
        payload = directory / "payload.parquet"
        manifest = directory / "entry.json"
        bytes_ = payload.read_bytes()
        meta = manifest.read_bytes()
        other = next(p for p in cache.iterdir() if p.is_dir() and p.name != key)
        variants = [
            ("footer", bytes_[:-4] + b"BAD!", None),
            ("body", bytes_[:32] + bytes([bytes_[32] ^ 1]) + bytes_[33:], None),
            ("manifest-key", None, {"key": "f" * 64}),
            ("manifest-rows", None, {"rows": 0}),
            ("manifest-schema", None, {"schema": "1" * 64}),
            ("manifest-version", None, {"version": 2}),
            ("operator-version", None, {"semantics": "wrong-native-version"}),
            ("manifest-snapshot", None, {"snapshot": "2" * 64}),
            ("missing-payload", False, None),
        ]
        for name, data, fields in variants:
            if fields:
                write(manifest, json.loads(meta) | fields)
            if data is False:
                payload.unlink()
            elif data:
                payload.write_bytes(data)
            self.call(name, self.args(req, source, cache), status=1, error="refuse")
            payload.write_bytes(bytes_)
            payload.chmod(0o600)
            manifest.write_bytes(meta)
        data = (other / "payload.parquet").read_bytes()
        payload.write_bytes(data)
        write(manifest, json.loads(meta) | {"bytes": len(data), "digest": digest(data)})
        self.call("rehashed-cross-entry", self.args(req, source, cache), status=1, error="payload lineage")
        payload.write_bytes(bytes_)
        manifest.write_bytes(meta)

    def crashes(self, source, a, b):
        for point, expected in (
            ("data_written", "publish"), ("data_synced", "publish"),
            ("manifest_synced", "publish"), ("before_suffix", "publish"),
            ("entry_selected", "hit"), ("entry_synced", "hit"),
        ):
            root = self.cache("crash-" + point)
            pending = self.pause(self.args(a, source, root), point)
            self.finish("kill-" + point, pending, -9, action=signal.SIGKILL)
            reply = self.call("reopen-" + point, self.args(b, source, root))
            assert expected in reply["audit"]["actions"]
            inventory = self.call("inventory-" + point, ["inspect", "--cache", root])
            assert len(inventory["entries"]) == 1 and inventory["stage_bytes"] == 0
        root = self.cache("writers")
        release = self.roots / "release-writer"
        first = self.pause(self.args(a, source, root), "manifest_synced", release=release)
        self.call("gc-while-stage", ["--timeout-ms", 80, "gc", "--cache", root], status=1, error="deadline")
        args = self.args(b, source, root)
        start = time.perf_counter_ns()
        second = subprocess.Popen([self.binary, *map(str, args)], stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, text=True)
        time.sleep(.05)
        assert second.poll() is None
        release.touch()
        winner = self.finish("writer-first", first, 0)
        output, stderr = second.communicate(timeout=5)
        loser = self.receipt("writer-same-key", args, second.pid, second.returncode, start, output, stderr)
        assert loser["exit"] == 0 and json.loads(output)["audit"]["actions"] == ["hit"]
        assert json.loads(output)["audit"]["payload_digest"] == winner["audit"]["payload_digest"]
        expired = self.cache("gc-crash")
        self.call("fill-expiring", self.args(a, source, expired, ttl=100))
        time.sleep(.12)
        pending = self.pause(["gc", "--cache", expired], "eviction_selected")
        self.finish("kill-gc", pending, -9, action=signal.SIGKILL)
        inv = self.call("gc-crash-inventory", ["inspect", "--cache", expired])
        assert inv["trash_bytes"] > 0 and not inv["entries"]
        self.call("gc-restart", ["gc", "--cache", expired])
        inv = self.call("gc-clean-inventory", ["inspect", "--cache", expired])
        assert inv["trash_bytes"] == 0

        root = self.cache("cancel")
        pending = self.pause(self.args(a, source, root), "data_written")
        self.finish("cancel-staging", pending, 1, action=signal.SIGINT, error="job cancelled")
        inv = self.call("cancel-inventory", ["inspect", "--cache", root])
        assert inv["stage_bytes"] == 0 and not inv["entries"]
        self.call("cancel-recovered", self.args(a, source, root))
        pending = self.pause(self.args(b, source, root), "before_suffix")
        self.finish("kill-reader", pending, -9, action=signal.SIGKILL)
        self.call("hit-after-reader-kill", self.args(b, source, root))
        readers = [self.pause(self.args(b, source, root), "reader_pinned") for _ in range(8)]
        try:
            self.call("reader-slot-cap", self.args(b, source, root, timeout=80),
                      status=1, error="deadline")
        finally:
            for i, reader in enumerate(readers):
                self.finish(f"release-slot-{i}", reader, -9, action=signal.SIGKILL)
        root = self.cache("pin-expiry")
        self.call("pin-expiry-fill", self.args(a, source, root, ttl=500))
        pending = self.pause(self.args(b, source, root), "reader_pinned")
        time.sleep(.55)
        self.call("expired-reader-blocks-gc", ["--timeout-ms", 80, "gc", "--cache", root],
                  status=1, error="deadline")
        self.finish("cancel-expired-reader", pending, 1, action=signal.SIGINT, error="cancelled")
        self.call("gc-after-expired-reader", ["gc", "--cache", root])

    def eviction(self, source, a, b):
        root = self.cache("evict", quota=330_000, entries=2)
        low = self.offer("low", uses_milli=3000)
        high = self.offer("high", uses_milli=30_000)
        medium = self.offer("medium", uses_milli=10_000)
        zero = self.offer("zero-offer", uses_milli=0)
        bogus = self.offer("bad-offer", uses_milli=U64)
        c = self.native("gamma", source, 3000)
        d = self.native("delta", source, 2000)
        e = self.native("epsilon", source, 1000)
        def args(req, offer, ttl=3600000, stage=125_000, timeout=30000):
            return self.args(req, source, root, mode="expected_value_v1", offer=offer,
                             stage=stage, ttl=ttl, timeout=timeout)

        hi = self.call("evict-fill-high-oldest", args(c, high))
        self.call("evict-fill-low-newest", args(a, low))
        low_access = self.call("evict-touch-low", args(b, low))
        self.call("evict-missing-calibration", args(d, None))
        self.call("evict-zero-uses", args(d, zero))
        self.call("evict-invalid-coefficient", args(d, bogus), status=1, error="coefficient")
        release = self.roots / "resume-reader"
        reader = self.pause(args(b, low), "reader_pinned", release=release)
        self.call("pinned-quota-writer", args(d, medium, timeout=80), status=1, error="deadline")
        self.call("pinned-gc", ["--timeout-ms", 80, "gc", "--cache", root], status=1, error="deadline")
        self.call("parallel-reader", args(b, low))
        release.touch()
        self.finish("pinned-reader", reader, 0)
        out = self.call("evict-medium-removes-low", args(d, medium, stage=235_000))
        assert "evict" in out["audit"]["actions"] and out["audit"]["decision"]["victims"] == [low_access["audit"]["prefix_key"]], out
        inv = self.call("evict-inventory", ["inspect", "--cache", root])
        assert hi["audit"]["prefix_key"] in {e["key"] for e in inv["entries"]}
        self.call("evict-infeasible", args(e, low, stage=330_000))
        root = self.cache("tiny-stage")
        self.call("stage-quota", self.args(a, source, root, stage=4120), status=1, error="capacity")
        self.call("stage-quota-inventory", ["inspect", "--cache", root])

    def security(self, source, a):
        unsafe = self.roots / "unsafe"
        unsafe.mkdir(mode=0o755)
        self.call("unsafe-root-mode", self.args(a, source, unsafe), status=1, error="access refused")
        parent = self.roots / "unsafe-ancestor"
        parent.mkdir(mode=0o700)
        child = parent / "child"
        child.mkdir(mode=0o700)
        parent.chmod(0o777)
        self.call("unsafe-ancestor-mode", self.args(a, source, child), status=1, error="access refused")
        root = self.cache("security")
        alias = self.roots / "symlink-root"
        alias.symlink_to(root.name, target_is_directory=True)
        self.call("symlink-root", self.args(a, source, alias), status=1, error="access refused")
        self.call("traversal-root", self.args(a, source, child / ".." / root.name), status=1, error="access refused")
        out = self.call("security-fill", self.args(a, source, root))
        payload = root / out["audit"]["prefix_key"] / "payload.parquet"
        os.link(payload, self.roots / "unexpected-link")
        self.call("cache-hardlink", self.args(a, source, root), status=1, error="access refused")
        (self.roots / "unexpected-link").unlink()
        self.call("forged-tenant-label", self.args(a, source, root) + ["--tenant-id", "owner"],
                  status=2, error="unexpected argument")

    def economics(self):
        sequences = []
        for regime, rows, width, files, jobs, threshold, uses in (
            ("selective", 900000, 16, 4, 8, 64, 7000),
            ("no-reuse", 4096, 4, 1, 1, 64, 0),
        ):
            source = make_fixture(self.fixtures / regime, rows, width, files)
            reqs = [self.native(regime + "-a", source, threshold)]
            if jobs > 1:
                reqs.append(self.native(regime + "-b", source, threshold, 2, alias="total"))
            offer = self.offer(regime, uses_milli=uses)
            for sample in range(6):
                order = MODES[sample % 3:] + MODES[:sample % 3]
                for mode in order:
                    label = f"econ-{regime}-{sample}-{mode}"
                    start = time.perf_counter_ns()
                    needs_root = mode == "always" or mode == "expected_value_v1" and uses > 0
                    cache = self.cache(label) if needs_root else None
                    job_labels = []
                    for i in range(jobs):
                        job_label = f"{label}-{i}"
                        self.call(job_label, self.args(reqs[i % len(reqs)], source, cache,
                                                        mode=mode, offer=offer), measure=True)
                        job_labels.append(job_label)
                    if cache:
                        shutil.rmtree(cache)
                    sequences.append(dict(regime=regime, sample=sample, mode=mode,
                                          jobs=job_labels, wall_ns=time.perf_counter_ns() - start))
        write(self.out / "sequences.json", sequences)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("binary", type=Path)
    cli.add_argument("output", type=Path)
    cli.add_argument("--no-economics", action="store_true")
    cli.add_argument("--economics-only", action="store_true")
    args = cli.parse_args()
    args.output.mkdir(mode=0o700)
    campaign = Campaign(args.binary, args.output)
    shutil.copy2(campaign.binary, campaign.out / "ade-cache")
    write(campaign.out / "host.json", dict(platform=platform.platform(), machine=platform.machine(),
          python=platform.python_version(), binary_sha256=hashlib.sha256(campaign.binary.read_bytes()).hexdigest()))
    if not args.economics_only:
        campaign.semantic_and_fault()
    if not args.no_economics:
        campaign.economics()
    print(campaign.out)


if __name__ == "__main__":
    main()
