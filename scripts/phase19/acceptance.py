"""Run matched native Phase 19 children against disposable local roots."""
import argparse
import hashlib
import json
import os
import platform
import selectors
import shutil
import signal
import statistics
import subprocess
import time
from pathlib import Path

from oracle import check, compact, delta, expected_decision, from_fixture, identity, plan_digest, strict, verify_trace

REPO = Path(__file__).resolve().parents[2]
CRITERIA = strict((Path(__file__).parent / "criteria.json").read_bytes())
MAX = 2**63 - 1
MIN = -2**63


def sha(data):
    return hashlib.sha256(data).hexdigest()


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(compact(value) + b"\n")


def run_capture(args):
    return subprocess.check_output(args, cwd=REPO, text=True).strip()


def optional_capture(args):
    proc = subprocess.run(args, cwd=REPO, text=True, capture_output=True, timeout=10)
    return {"exit": proc.returncode, "stdout": proc.stdout.strip(), "stderr": proc.stderr.strip()}


class Fixture:
    def __init__(self, parent, encoder, label, rows, epochs, nullable=False, file_parts=None):
        self.dir = parent / label
        self.dir.mkdir(parents=True)
        self.plan = self.dir / "plan.json"
        self.baseline = self.dir / "baseline.parquet"
        self.deltas = self.dir / "deltas"
        self.deltas.mkdir()
        file_parts = file_parts or [rows]
        self.baselines = []
        self.specs = []
        for index, part in enumerate(file_parts):
            path = self.dir / ("baseline.parquet" if index == 0 else f"baseline-{index}.parquet")
            temp = self.dir / f"encoder-input-{index}.tmp"
            with temp.open("w+") as ledger:
                for k, v in part:
                    ledger.write(f"{k},{v}\n")
                ledger.seek(0)
                args = [encoder, path] + (["nullable"] if nullable else [])
                result = subprocess.run(args, stdin=ledger, capture_output=True, check=True, timeout=90)
            temp.unlink()
            self.baselines.append(path)
            self.specs.append(strict(result.stdout))
        self.spec = self.specs[0]
        descriptor = {"version": 1, "namespace": label, "threshold": CRITERIA["threshold"],
                      "baseline": self.specs}
        self.plan.write_bytes(compact(descriptor))
        self.plan_id = plan_digest(descriptor)
        self.chains = [identity("maintained-genesis-v1", self.plan_id.encode())]
        self.epoch_bytes = []
        for epoch, changes in enumerate(epochs, 1):
            blob = self.envelope(epoch, changes, self.chains[-1])
            self.epoch_bytes.append(blob)
            self.delta_path(epoch).write_bytes(blob)
            self.chains.append(delta(blob, self.plan_id, epoch, self.chains[-1])[0])
        if not nullable:
            self.model, self.raw_rows, self.changes = from_fixture(
                self.plan.read_bytes(), [path.read_bytes() for path in self.baselines], self.epoch_bytes, check_baseline=False)
            check([m["chain"] for m in self.model] == self.chains, "independent fixture chains")

    def delta_path(self, epoch):
        return self.deltas / f"{epoch:08}.json"

    def envelope(self, epoch, rows, parent):
        return compact({"version": 1, "plan": self.plan_id, "epoch": epoch, "parent": parent, "rows": rows})


class Campaign:
    def __init__(self, out, binary, encoder):
        self.out = out
        self.bin = binary
        self.encoder = encoder
        self.work = out / "work"
        self.work.mkdir(parents=True)
        self.fixtures = out / "fixtures"
        self.cases = []
        self.samples = []
        self.serial = 0

    def root(self, name):
        self.serial += 1
        path = self.work / f"{self.serial:04}-{name}"
        path.mkdir(mode=0o700)
        return path

    def request(self, fixture, root, epoch, policy="adaptive", sources=True, **override):
        doc = {
            "version": 1, "plan": str(fixture.plan), "root": str(root), "epoch": epoch,
            "chain": fixture.chains[epoch] if epoch < len(fixture.chains) else "0" * 64,
            "limits": dict(CRITERIA["limits"]),
            "policy": {"kind": policy} if policy in ("adaptive", "disabled")
            else {"kind": "force", "route": policy},
        }
        if sources:
            doc.update(baseline=str(fixture.dir), deltas=str(fixture.deltas))
        for key, value in override.items():
            if key == "limits":
                doc["limits"].update(value)
            elif value is None:
                doc.pop(key, None)
            else:
                doc[key] = value
        request = self.work / "request.json"
        write(request, doc)
        return request

    def execute(self, command, req, env=None, extra=()):
        start = time.perf_counter_ns()
        result = subprocess.run([self.bin, command, req, *extra], capture_output=True, timeout=125, env=env)
        return result, time.perf_counter_ns() - start

    def apply(self, fixture, root, epoch, policy="adaptive", command="apply", **kw):
        req = self.request(fixture, root, epoch, policy, **kw)
        p, wall = self.execute(command, req)
        check(p.returncode == 0, f"{command} epoch {epoch} {policy}: {p.stderr!r}")
        trace = strict(p.stdout)
        expected = fixture.model[epoch]
        check((trace["result"], trace["chain"]) == (expected["result"], expected["chain"]), "native result identity")
        return trace, wall

    def read(self, fixture, root, epoch, ledger=None):
        req = self.request(fixture, root, epoch, sources=False)
        result, _ = self.execute("read", req)
        check(result.returncode == 0, f"reader refused epoch {epoch}: {result.stderr!r}")
        rows = [strict(line) for line in result.stdout.splitlines()]
        footer = rows.pop()
        expected = fixture.model[epoch]
        check(rows == expected["views"], f"independent view bag epoch {epoch}")
        check((footer["epoch"], footer["chain"], footer["result"], footer["view_rows"]) ==
              (epoch, expected["chain"], expected["result"], len(rows)), "reader selected the wrong generation")
        if ledger:
            p = self.out / "ledgers" / f"{ledger}.ndjson"
            p.parent.mkdir(exist_ok=True)
            p.write_bytes(result.stdout)
        return {"sha256": sha(result.stdout), "footer": footer}

    def read_named_view(self, fixture, root, epoch, view):
        req = self.request(fixture, root, epoch, sources=False)
        result, _ = self.execute("read-view", req, extra=(view,))
        check(result.returncode == 0, f"reader refused view {view}: {result.stderr!r}")
        rows = [strict(line) for line in result.stdout.splitlines()]
        footer = rows.pop()
        expected = [row for row in fixture.model[epoch]["views"] if row["view"] == view]
        check(rows == expected, f"independent named view bag {view}")
        check(footer["views_read"] == 1 and footer["view_rows"] == len(rows), "named reader counters")
        return {"view": view, "sha256": sha(result.stdout), "footer": footer}

    def refuse(self, name, request, reason, command="apply", env=None):
        p, _ = self.execute(command, request, env)
        record = {"name": name, "command": command, "request": strict(request.read_bytes()),
                  "exit": p.returncode, "stderr": p.stderr.decode(), "stdout": p.stdout.decode()}
        self.record(record)
        check(p.returncode != 0 and not p.stdout and reason in record["stderr"],
              f"{name}: expected {reason!r}, observed {record}")
        return record

    def record(self, record):
        self.cases.append(record)
        write(self.out / "cases.json", self.cases)

    def proof(self, name, root):
        directory = self.out / "snapshots" / name
        directory.mkdir(parents=True)
        for item in sorted(root.iterdir()):
            if item.name.startswith("g-"):
                shutil.copytree(item, directory / item.name)
            elif item.name.startswith("w-"):
                shutil.copyfile(item, directory / item.name)

    def pause(self, command, request, point, resume=None):
        env = os.environ | {"ADE19_STOP_AT": point}
        if resume is not None:
            env["ADE19_CONTINUE_FILE"] = str(resume)
        child = subprocess.Popen([self.bin, command, request], stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, env=env)
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(child.stderr, selectors.EVENT_READ)
                check(selector.select(30), f"fault point {point} timed out")
                line = child.stderr.readline().decode().strip()
                check(line == f"fault reached {point}", f"fault point {point} missed: {line!r}")
        except BaseException:
            child.kill()
            out, error = child.communicate(timeout=10)
            self.record({"name": "fault_missed", "point": point, "exit": child.returncode,
                         "stderr": error.decode(), "stdout": out.decode()})
            raise
        return child, line

    def stop(self, child, name, point, marker, expected_exit, reason=None):
        stdout, stderr = child.communicate(timeout=30)
        record = {"name": name, "point": point, "marker": marker, "exit": child.returncode,
                  "stderr": stderr.decode(), "stdout": stdout.decode()}
        self.record(record)
        check(child.returncode == expected_exit and not stdout and
              (reason is None or reason in record["stderr"]), f"fault {name}: {record}")
        return record

    def basic(self):
        tiny_rows = [(0, -1), (0, 0), (0, 0), (0, 2), (128, 0), (128, 1), (1, 0), (9, -3)]
        first = [(0, -1, -1), (0, 1, 1), (0, 0, -1), (128, 1, -1), (128, -2, 1),
                 (500, 0, 3), (500, 0, -1), (9, -3, -1), (9, -3, 1)]
        third = [(500, 0, -2), (1, 0, -1), (1, -5, 1), (0, 0, 1)]
        tiny = Fixture(self.fixtures, self.encoder, "sequence", tiny_rows, [first, [], third])
        multi = Fixture(self.fixtures, self.encoder, "multi-baseline", tiny_rows,
                        [[(500, 0, 1)]], file_parts=[tiny_rows[:3], tiny_rows[3:6], tiny_rows[6:]])
        extreme = Fixture(self.fixtures, self.encoder, "extreme", [(99, 0)], [
            [(99, 0, MAX), (99, 0, MAX), (2**64 - 1, MIN, 1), (2**64 - 1, MAX, 1)],
            [(99, 0, MIN), (2**64 - 1, MIN, -1), (2**64 - 1, MAX, -1),
             (2**64 - 1, -1, 1), (2**64 - 1, 0, 1)],
            [(99, 0, -MAX), (2**64 - 1, -1, -1), (2**64 - 1, 0, -1)],
        ])
        root = self.root("sequence")
        traces = [self.apply(tiny, root, 0, command="bootstrap")[0]]
        self.read(tiny, root, 0, "sequence-0")
        traces.append(self.apply(tiny, root, 1, "incremental_update")[0])
        first_read = self.read(tiny, root, 1, "sequence-1")
        retry = self.apply(tiny, root, 1, sources=False)[0]
        check(retry["decision"]["selected"] == "restore" and retry["metrics"]["delta_rows_applied"] == 0,
              "retry must restore exact epoch")
        self.refuse("forced_illegal", self.request(tiny, root, 1, "incremental_update"), "no legal maintained route")
        self.refuse("unavailable_replay", self.request(tiny, root, 1, "disabled", sources=False), "no legal maintained route")
        self.refuse("gap", self.request(tiny, root, 3), "fork, gap or rollback")
        self.refuse("wrong_chain", self.request(tiny, root, 1, sources=False, chain="9"*64),
                    "different chain")
        self.refuse("forced_illegal", self.request(tiny, self.root("empty-force"), 0, "restore"), "no legal maintained route")
        modified = self.work / "modified-plan.json"
        descriptor = strict(tiny.plan.read_bytes())
        write(modified, descriptor | {"threshold": 1})
        self.refuse("semantic_plan_change", self.request(tiny, root, 1, sources=False, plan=str(modified)),
                    "semantic plan", "read")
        for name, doc, reason in [
            ("duplicate_json_field", tiny.plan.read_bytes().replace(b'"version":1', b'"version":1,"version":1'), "duplicate field"),
            ("unknown_field", compact(descriptor | {"forged_decision": "restore"}), "unknown field"),
            ("overlong_plan", b" " * (32768 + 1), "file bytes"),
            ("depth", b'{"version":1,"namespace":' + b'['*200 + b'0' + b']'*200 + b'}', "invalid type"),
        ]:
            modified.write_bytes(doc)
            self.refuse(name, self.request(tiny, root, 1, sources=False, plan=str(modified)), reason)

        original = tiny.delta_path(1).read_bytes()
        for name, bad, reason in [
            ("fork", tiny.envelope(1, first, "7"*64), "parent mismatch"),
            ("gap", tiny.envelope(2, first, tiny.chains[0]), "parent mismatch"),
            ("changed_delta", tiny.envelope(1, list(reversed(first)), tiny.chains[0]), "pinned source chain"),
            ("duplicate_json_field", original.replace(b'"epoch":1', b'"epoch":1,"epoch":1'), "duplicate field"),
        ]:
            tiny.delta_path(1).write_bytes(bad)
            self.refuse(name, self.request(tiny, root, 1), reason)
        tiny.delta_path(1).unlink()
        self.refuse("missing_delta", self.request(tiny, root, 1), "No such file")
        tiny.delta_path(1).write_bytes(original)
        check(self.read(tiny, root, 1) == first_read, "refused work altered selection")

        # Publish a neutral epoch, then consume only an independently attested tail.
        trace, _ = self.apply(tiny, root, 2, sources=False, delta_tail=str(tiny.delta_path(2)))
        check(trace["metrics"]["shards_written"] == trace["metrics"]["keys_updated"] == 0, "neutral epoch work")
        self.record({"name": "empty_epoch", "trace": trace})
        traces.append(trace)
        self.read(tiny, root, 2, "sequence-2")
        traces.append(self.apply(tiny, root, 3, sources=False, delta_tail=str(tiny.delta_path(3)))[0])
        self.read(tiny, root, 3, "sequence-3")
        self.proof("sequence", root)
        rebuild = self.root("same-target-full")
        full, _ = self.apply(tiny, rebuild, 3, "disabled")
        self.read(tiny, rebuild, 3, "sequence-full-3")
        self.proof("sequence-full", rebuild)
        write(self.out / "sequence.json", {"traces": traces, "replay": full, "retry": retry})

        # Explicit baseline binding is strict; only omission selects parent-only continuation.
        multi_root = self.root("multi-baseline")
        self.apply(multi, multi_root, 0, command="bootstrap")
        for index, path in enumerate(multi.baselines):
            saved = path.with_suffix(path.suffix + ".saved")
            path.rename(saved)
            try:
                name = ("baseline_missing_first", "baseline_missing_middle", "baseline_missing_last")[index]
                bad_request = self.request(multi, multi_root, 1, sources=False,
                                           baseline=str(multi.dir), delta_tail=str(multi.delta_path(1)))
                self.refuse(name, bad_request, "No such file")
                preserved = self.read(multi, multi_root, 0)
                self.record({"name": f"{name}_preserved_head", "head": preserved})
                if index == 1:
                    explain = self.request(multi, multi_root, 0, "restore", delta_tail=None)
                    self.refuse("baseline_missing_explain_restore", explain,
                                "No such file", command="explain")
            finally:
                saved.rename(path)
        continuation = self.request(multi, multi_root, 1, sources=False,
                                    delta_tail=str(multi.delta_path(1)))
        continued, _ = self.execute("apply", continuation)
        check(continued.returncode == 0 and strict(continued.stdout)["decision"]["selected"] == "incremental_update",
              "omitted baseline permits verified parent-only continuation")
        self.read(multi, multi_root, 1)
        bad_restore_path = multi.baselines[0]
        backup = bad_restore_path.with_suffix(".hidden")
        bad_restore_path.rename(backup)
        try:
            self.refuse("baseline_missing_restore", self.request(multi, multi_root, 1, "restore"),
                        "No such file", command="explain")
        finally:
            backup.rename(bad_restore_path)
        readers = []
        request_path = self.request(multi, multi_root, 1, sources=False)
        for view in ("all_v1", "filtered_v1"):
            process = subprocess.Popen([self.bin, "read-view", request_path, view],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            readers.append((view, process))
        view_records = []
        for view, process in readers:
            stdout, stderr = process.communicate(timeout=30)
            check(process.returncode == 0, f"named reader failed: {stderr!r}")
            rows = [strict(line) for line in stdout.splitlines()]
            footer = rows.pop()
            expected = [row for row in multi.model[1]["views"] if row["view"] == view]
            check(rows == expected and footer["views_read"] == 1
                  and footer["result"] == multi.model[1]["result"], "separate named reader oracle")
            view_records.append({"view": view, "stdout": stdout.decode(), "footer": footer,
                                 "manifest_sha256": sha((multi_root / "g-00000001" / "MAINTAINED19").read_bytes())})
        write(self.out / "view_readers.json", view_records)
        unknown, _ = self.execute("read-view", request_path, extra=("unknown_v1",))
        check(unknown.returncode != 0 and not unknown.stdout, "unknown view refuses before output")
        self.record({"name": "unknown_view", "exit": unknown.returncode,
                     "stderr": unknown.stderr.decode(), "stdout": unknown.stdout.decode()})

        eroot = self.root("signed-extremes")
        for epoch in range(4):
            trace, _ = self.apply(extreme, eroot, epoch, "incremental_update" if epoch else "disabled")
            self.read(extreme, eroot, epoch, f"extreme-{epoch}")
            if epoch == 2:
                self.record({"name": "min_signed_weight", "trace": trace})
        self.proof("extreme", eroot)

        variants = [
            ("negative_multiplicity", [[7, 1, -1]], "negative or overflowing row multiplicity"),
            ("count_overflow", [[1, 0, MAX], [1, 0, MAX], [1, 1, 1]], "view count overflow"),
            ("sum_overflow", [[2, MAX, 2]], "view sum overflow"),
        ]
        for name, rows, reason in variants:
            test = self.root(name)
            self.apply(tiny, test, 0, command="bootstrap")
            data = tiny.envelope(1, rows, tiny.chains[0])
            bad = self.work / "bad-delta.json"
            bad.write_bytes(data)
            self.refuse(name, self.request(tiny, test, 1, sources=False, delta_tail=str(bad),
                                           chain=identity("maintained-delta-v1", data)), reason)
            self.read(tiny, test, 0)

        self.filesystem_cases(tiny)
        self.crashes(tiny)

    def filesystem_cases(self, fixture):
        root = self.root("legacy")
        (root / "CURRENT3").write_text("foreign")
        self.refuse("legacy_root", self.request(fixture, root, 0), "legacy root")
        actual = self.root("symlink-target")
        link = self.work / "link"
        link.symlink_to(actual, target_is_directory=True)
        self.refuse("symlink_root", self.request(fixture, link, 0), "symlink path")
        for quota, limit, reason in [
            ("hot_quota", {"hot_entries": 1}, "hot entries"),
            ("state_quota", {"state_bytes": 75}, "arrangement encoded bytes"),
            ("disk_quota", {"disk_bytes": 512}, "projected disk budget"),
        ]:
            self.refuse(quota, self.request(fixture, self.root(quota), 0, limits=limit), reason)

        for label, rows in (("empty-output", []), ("one-row-output", [(1, 1)]),
                            ("many-row-output", [(i, i % 7) for i in range(40)])):
            output_fixture = Fixture(self.fixtures, self.encoder, label, rows, [])
            output_root = self.root(label)
            self.apply(output_fixture, output_root, 0, command="bootstrap")
            normal_request = self.request(output_fixture, output_root, 0, sources=False)
            normal, _ = self.execute("read", normal_request)
            check(normal.returncode == 0, "unbounded-size reference read")
            exact = len(normal.stdout)
            for cap in (1, 127, 128, exact - 1):
                limited = self.request(output_fixture, output_root, 0, sources=False,
                                       limits={"output_bytes": cap})
                p, _ = self.execute("read", limited)
                record = {"name": "output_quota", "fixture": label, "cap": cap,
                          "actual_bytes": exact, "exit": p.returncode,
                          "stderr": p.stderr.decode(), "stdout": p.stdout.decode()}
                self.record(record)
                check(p.returncode != 0 and b"byte quota exceeded" in p.stderr
                      and (not p.stdout.endswith(b"\n") or b'"result"' not in p.stdout.splitlines()[-1]),
                      f"bounded output refused without a success footer: {record}")
            exact_request = self.request(output_fixture, output_root, 0, sources=False,
                                         limits={"output_bytes": exact})
            exact_result, _ = self.execute("read", exact_request)
            check(exact_result.returncode == 0 and len(exact_result.stdout) == exact,
                  "exact serialized output byte limit")
            self.record({"name": f"{label}_exact_output_limit", "cap": exact,
                         "exit": exact_result.returncode, "stdout": exact_result.stdout.decode()})

            verify_reference, _ = self.execute("verify", normal_request)
            check(verify_reference.returncode == 0, "verify command completed")
            verify_footer = strict(verify_reference.stdout)
            check(set(verify_footer) == {"epoch", "chain", "result", "view_rows", "views_read", "hot_entries_peak"}
                  and verify_footer["views_read"] == 2
                  and ((verify_footer["view_rows"] == 0) if label == "empty-output"
                       else (verify_footer["view_rows"] > 0)),
                  "verify emits only the shared-view footer")
            verify_exact = len(verify_reference.stdout)
            verify_exact_request = self.request(output_fixture, output_root, 0, sources=False,
                                                limits={"output_bytes": verify_exact})
            verify_exact_result, _ = self.execute("verify", verify_exact_request)
            verify_short_request = self.request(output_fixture, output_root, 0, sources=False,
                                                limits={"output_bytes": verify_exact - 1})
            verify_short_result, _ = self.execute("verify", verify_short_request)
            check(verify_exact_result.returncode == 0 and len(verify_exact_result.stdout) == verify_exact
                  and verify_short_result.returncode != 0
                  and b"byte quota exceeded" in verify_short_result.stderr,
                  "verify footer follows the same exact byte cap")

        nullable = Fixture(self.work, self.encoder, "nullable", [(0, 1)], [], nullable=True)
        self.refuse("schema", self.request(nullable, self.root("schema"), 0), "schema/row mismatch")
        near_root = self.root("near-minimum")
        exact = fixture.model[0]
        near, _ = self.apply(fixture, near_root, 0, limits={
            "hot_entries": 11, "state_bytes": sum(len(s or b"") for s in exact["shards"]),
            "output_bytes": 128 * len(exact["views"]), "disk_bytes": 2 * 32768 + 16 * 128 + 60 * fixture.spec["rows"],
        })
        self.record({"name": "near_minimum", "trace": near})
        readonly = self.root("readonly")
        readonly.chmod(0o500)
        self.refuse("read_only_root", self.request(fixture, readonly, 0), "private directory mode")
        readonly.chmod(0o700)
        original = fixture.baseline.read_bytes()
        wrong = bytearray(original)
        wrong[-1] ^= 1
        fixture.baseline.write_bytes(wrong)
        self.refuse("changed_baseline", self.request(fixture, self.root("drift"), 0), "fingerprint drift")
        fixture.baseline.write_bytes(original)

        for name, mutation, reason in [
            ("selector_rollback", lambda p: shutil.rmtree(p / "g-00000001"), "selector rollback"),
            ("corrupt_shard", lambda p: self.flip(p / "g-00000000" / "s-001.bin"), "shard fingerprint drift"),
            ("truncated_manifest", lambda p: (p / "g-00000001" / "MAINTAINED19").write_bytes(b"{"), "EOF"),
        ]:
            root = self.root(name)
            self.apply(fixture, root, 0)
            self.apply(fixture, root, 1)
            mutation(root)
            self.refuse(name, self.request(fixture, root, 1, sources=False), reason, "read")
        root = self.root("clone-source")
        self.apply(fixture, root, 0)
        clone = self.root("cloned")
        # Preserve hard-link structure so the location attestation is exercised.
        shutil.copytree(root, clone, dirs_exist_ok=True)
        witness = clone / "w-00000000"
        witness.unlink()
        os.link(clone / "g-00000000" / "MAINTAINED19", witness)
        self.refuse("root_clone", self.request(fixture, clone, 0, sources=False), "foreign root", "read")
        lease = root / "LEASE19"
        retired = self.work / "retired-lease"
        lease.rename(retired)
        lease.touch(mode=0o600)
        self.refuse("stale_writer_lease", self.request(fixture, root, 0, sources=False), "foreign root", "read")

    @staticmethod
    def flip(path):
        data = bytearray(path.read_bytes())
        data[-1] ^= 1
        path.write_bytes(data)

    def crashes(self, fixture):
        for point in CRITERIA["fault_points"]:
            root = self.root(point)
            self.apply(fixture, root, 0, command="bootstrap")
            req = self.request(fixture, root, 1, "incremental_update")
            child, marker = self.pause("apply", req, point)
            try:
                if point == "shard_partial":
                    self.refuse("concurrent_writer", self.request(fixture, root, 1), "lease busy")
                    self.refuse("reader_while_provisional", self.request(fixture, root, 0, sources=False),
                                "lease busy", "read")
            finally:
                child.kill()
                self.stop(child, "interruption", point, marker, -signal.SIGKILL)
            selected = 1 if point in ("selected", "selection_synced") else 0
            prior = self.read(fixture, root, selected)
            resume, _ = self.apply(fixture, root, 1, command="resume")
            if point == "witness_synced":
                check(resume["recovered_publication"], "witnessed stage did not resume")
            next_trace, _ = self.apply(fixture, root, 2)
            later = self.read(fixture, root, 2, f"crash-{point}-2")
            signed, _ = self.apply(fixture, root, 3, sources=False, delta_tail=str(fixture.delta_path(3)))
            final = self.read(fixture, root, 3, f"crash-{point}-3")
            self.record({"name": "orphan_stage" if point == "shard_partial" else "resume_after_kill",
                         "point": point, "before": prior, "resume": resume, "next": next_trace, "after": later,
                         "signed": signed, "final": final})
            if point == "witness_synced":
                self.proof("crash-witness", root)

        root = self.root("short-write")
        self.apply(fixture, root, 0)
        self.refuse("short_write", self.request(fixture, root, 1, "incremental_update"),
                    "write zero", env=os.environ | {"ADE19_SHORT_WRITE": "shard_partial"})
        self.read(fixture, root, 0)

        root = self.root("staged-manifest-drift")
        self.apply(fixture, root, 0)
        resume = self.work / "tamper-stage-resume"
        child, marker = self.pause("apply", self.request(fixture, root, 1, "incremental_update"),
                                   "manifest_synced", resume)
        try:
            self.flip(root / ".stage19" / "MAINTAINED19")
            resume.touch()
            self.stop(child, "staged_manifest_drift", "manifest_synced", marker, 1, "staged manifest drift")
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=10)
        self.read(fixture, root, 0)

        original = fixture.baseline.read_bytes()
        for name in ("mid_read_source_change", "baseline_aba", "cancel", "deadline"):
            root = self.root(name)
            resume = self.work / f"resume-{name}"
            limits = {"timeout_ms": 300} if name == "deadline" else {}
            child, marker = self.pause("bootstrap", self.request(fixture, root, 0, limits=limits),
                                       "baseline_scanned", resume)
            try:
                if name == "mid_read_source_change":
                    self.flip(fixture.baseline)
                    reason = "fingerprint drift"
                elif name == "baseline_aba":
                    self.flip(fixture.baseline)
                    fixture.baseline.write_bytes(original)
                    reason = "baseline identity changed"
                elif name == "cancel":
                    child.send_signal(signal.SIGINT)
                    time.sleep(0.05)
                    reason = "cancel"
                else:
                    time.sleep(0.4)
                    reason = "deadline"
                resume.touch()
                self.stop(child, name, "baseline_scanned", marker, 1, reason)
            finally:
                if child.poll() is None:
                    child.kill()
                    child.communicate(timeout=10)
                fixture.baseline.write_bytes(original)
            check(not list(root.glob("g-*")), f"{name} published invalid work")

    def performance(self):
        regimes = CRITERIA["regimes"]
        loaded = {}
        for label in ("sparse", "dense"):
            spec = regimes[label]
            keys, rows = spec["keys"], spec["rows"]
            data = ((i % keys, (i // keys % 9) - 4 if label == "sparse" else 0) for i in range(rows))
            changed = range(spec["changed_keys"]) if label == "dense" else (0, 37, 170, 513)
            delta_rows = [row for key in changed for row in [(key, 0, -1), (key, 5, 1)]]
            loaded[label] = Fixture(self.fixtures, self.encoder, label, data, [delta_rows])
        loaded["restored"] = loaded["sparse"]
        for label, spec in regimes.items():
            fixture = loaded[label]
            parent = int(label == "restored")
            policies = ["adaptive"] + spec["static"]
            rounds = CRITERIA["warmups_per_regime_policy"] + CRITERIA["samples_per_regime_policy"]
            for turn in range(rounds):
                ordered = policies[turn % 3:] + policies[:turn % 3]
                if turn % 2:
                    ordered.reverse()
                for policy in ordered:
                    self.sample(fixture, label, parent, policy, turn)
        report = {}
        for label, spec in regimes.items():
            medians = {p: statistics.median([x["wall_ns"] for x in self.samples
                                             if x["regime"] == label and x["policy"] == p and x["round"] > 0])
                       for p in ["adaptive"] + spec["static"]}
            ratio = (medians["incremental_update"] / medians["full_recompute"] if label == "sparse"
                     else medians["adaptive"] / min(medians[p] for p in spec["static"]))
            report[label] = {"medians_ns": medians, "ratio": ratio}
        write(self.out / "performance.json", report)
        print(json.dumps(report, sort_keys=True))
        return report

    def sample(self, fixture, label, parent, policy, turn):
        root = self.root(label)
        self.apply(fixture, root, 0, command="bootstrap")
        if parent == 1:
            self.apply(fixture, root, 1, "incremental_update")
        name = f"{label}-{policy}"
        req = self.request(fixture, root, 1, policy)
        child, wall = self.execute("apply", req)
        record = {"regime": label, "fixture": fixture.dir.name, "policy": policy, "round": turn,
                  "parent_epoch": parent, "wall_ns": wall, "exit": child.returncode,
                  "stderr": child.stderr.decode(), "stdout": child.stdout.decode()}
        self.samples.append(record)
        write(self.out / "samples.json", self.samples)
        check(child.returncode == 0, f"sample failed {record}")
        trace = strict(child.stdout)
        selection = expected_decision(fixture.model, [fixture.spec["bytes"]], fixture.spec["rows"],
                                      fixture.raw_rows, fixture.changes, 1, parent, policy)
        verify_trace(trace, fixture.model[1], 1, selection)
        if policy == "adaptive":
            check(trace["decision"]["selected"] == CRITERIA["regimes"][label]["expected"], "regime route")
        record["read"] = self.read(fixture, root, 1, name if turn == 0 else None)
        if turn == 0:
            self.proof(name, root)
        write(self.out / "samples.json", self.samples)
        shutil.rmtree(root)

    def inventory(self):
        files = [p for p in (REPO / "crates").rglob("*") if p.is_file()
                 and (p.suffix == ".rs" or p.name == "Cargo.toml")]
        files += list((REPO / "scripts/phase19").glob("*.py"))
        files += [REPO / p for p in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml",
                                    "scripts/phase19/criteria.json", "scripts/phase17/digest.py")]
        source = {str(p.relative_to(REPO)): sha(p.read_bytes()) for p in sorted(files)}
        write(self.out / "provenance.json", {
            "baseline_commit": run_capture(["git", "rev-parse", "HEAD"]),
            "baseline_tree": run_capture(["git", "rev-parse", "HEAD^{tree}"]),
            "source": source, "source_sha256": sha(compact(source)),
            "executables": {"ade-maintain": sha(self.bin.read_bytes()), "baseline": sha(self.encoder.read_bytes())},
            "criteria_sha256": sha((REPO / "scripts/phase19/criteria.json").read_bytes()),
            "platform": platform.platform(), "uname": list(platform.uname()),
            "cpu_count": os.cpu_count(), "disk": list(shutil.disk_usage(self.work)),
            "filesystem": run_capture(["df", "-k", str(self.work)]),
            "hardware": ({k: optional_capture(["sysctl", "-n", k]) for k in ("hw.model", "hw.memsize")}
                         if platform.system() == "Darwin" else {}),
            "rustc": run_capture(["rustc", "--version"]), "cargo": run_capture(["cargo", "--version"]),
            "features": ["maintained-v1", "fault-injection"],
            "release_env": {"CARGO_PROFILE_RELEASE_LTO": "false", "CARGO_PROFILE_RELEASE_CODEGEN_UNITS": "16"},
            "phase18_acceptance": CRITERIA["phase18_acceptance"],
        })
        shutil.copyfile(REPO / "scripts/phase19/criteria.json", self.out / "criteria.json")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--bin", type=Path, default=REPO / "target/release/ade-maintain")
    parser.add_argument("--encoder", type=Path, default=REPO / "target/release/examples/baseline")
    parser.add_argument("--functional-only", action="store_true")
    args = parser.parse_args()
    campaign = Campaign(args.out.resolve(), args.bin.resolve(), args.encoder.resolve())
    campaign.inventory()
    campaign.basic()
    print(f"functional observations {len(campaign.cases)}", flush=True)
    if not args.functional_only:
        campaign.performance()


if __name__ == "__main__":
    main()
