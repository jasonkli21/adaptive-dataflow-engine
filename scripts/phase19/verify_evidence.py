"""Verify a portable Phase 19 archive against independently reconstructed results."""
import argparse
import collections
import hashlib
import io
import json
import statistics
import struct
import tarfile
from pathlib import Path, PurePosixPath

from oracle import check, compact, expected_decision, from_fixture, identity, strict, verify_trace

CRITERIA = (Path(__file__).parent / "criteria.json").read_bytes()


def sha(data):
    return hashlib.sha256(data).hexdigest()


def verify_members(members, inventory):
    check(set(members) == set(inventory), "archive member inventory")
    for name, content in members.items():
        check(sha(content) == inventory[name], f"archive member checksum {name}")


def archive_members(path, manifest):
    data = path.read_bytes()
    check(len(data) <= 32 * 1024 * 1024 and sha(data) == manifest["archive_sha256"], "archive sha256")
    result = {}
    total = 0
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
        for member in tar:
            name = PurePosixPath(member.name)
            total += member.size
            check(member.isfile() and not name.is_absolute() and ".." not in name.parts
                  and member.name not in result and len(result) < 8192
                  and total <= 128 * 1024 * 1024, "unsafe archive inventory")
            result[member.name] = tar.extractfile(member).read()
    verify_members(result, manifest["members"])
    return result


class Verifier:
    def __init__(self, members):
        self.members = members
        self.criteria = strict(CRITERIA)
        self.fixtures = {}

    def fixture(self, name):
        prefix = f"fixtures/{name}/"
        inventory = [(n, v) for n, v in sorted(self.members.items()) if n.startswith(prefix + "deltas/")]
        check([n for n, _ in inventory] == [f"{prefix}deltas/{i:08}.json" for i in range(1, len(inventory)+1)],
              "delta inventory")
        plan_bytes = self.members[prefix + "plan.json"]
        plan = strict(plan_bytes)
        files = [self.members[prefix + f["name"]] for f in plan["baseline"]]
        model, raw, changes = from_fixture(plan_bytes, files, [v for _, v in inventory])
        return plan, model, raw, changes

    def validate(self, repo=None, binary=None, encoder=None):
        data = self.members
        check(data["criteria.json"] == CRITERIA, "frozen criteria")
        provenance = strict(data["provenance.json"])
        check(len(provenance["baseline_commit"]) == 40
              and all(c in "0123456789abcdef" for c in provenance["baseline_commit"])
              and provenance["phase18_acceptance"] == self.criteria["phase18_acceptance"]
              and provenance["source_sha256"] == sha(compact(provenance["source"]))
              and provenance["criteria_sha256"] == sha(CRITERIA)
              and provenance["features"] == ["maintained-v1", "fault-injection"],
              "runtime provenance")
        for path, digest in provenance["source"].items():
            parts = PurePosixPath(path)
            check(not parts.is_absolute() and ".." not in parts.parts, "source inventory path")
            if repo:
                check(sha((repo / path).read_bytes()) == digest, f"matched source {path}")
            if path in data:
                check(sha(data[path]) == digest, f"packaged source {path}")
        for key, path in (("ade-maintain", binary), ("baseline", encoder)):
            if path:
                check(sha(path.read_bytes()) == provenance["executables"][key], f"matched executable {key}")
        for name in ("sequence", "extreme", "sparse", "dense", "multi-baseline",
                     "empty-output", "one-row-output", "many-row-output"):
            self.fixtures[name] = self.fixture(name)
        self.verify_cases()
        self.verify_samples()
        self.verify_snapshots()
        return {
            "source_sha256": provenance["source_sha256"],
            "cases": len(strict(data["cases.json"])),
            "samples": len(strict(data["samples.json"])),
            "snapshots": len({n.split("/")[1] for n in data if n.startswith("snapshots/")}),
            "performance": strict(data["performance.json"]),
        }

    def verify_cases(self):
        cases = strict(self.members["cases.json"])
        required = set(self.criteria["negative_cases"]) - {
            "rehashed_delta_tamper", "rehashed_view_tamper", "rehashed_decision_tamper"
        }
        check(required.issubset({c["name"] for c in cases}), "minimum case inventory")
        for record in cases:
            expected = self.criteria["refusal_reasons"].get(record["name"])
            if record["name"] == "output_quota":
                expected = ["byte quota exceeded", "view output bytes"]
            if expected:
                failed = record["exit"] != 0 and any(word in record["stderr"] for word in expected)
                if record["name"] == "output_quota" and record.get("cap") is not None:
                    output = record["stdout"].encode()
                    no_success_footer = not output or not (
                        output.endswith(b"\n") and b'"result"' in output.splitlines()[-1])
                    check(failed and len(output) <= record["cap"] and no_success_footer,
                          "bounded read failed without a successful footer")
                else:
                    check(failed and not record["stdout"], "reached refusal")
        names = {c["name"] for c in cases}
        check({"baseline_missing_first", "baseline_missing_middle", "baseline_missing_last",
               "baseline_missing_explain_restore", "baseline_missing_restore"}.issubset(names),
              "explicit incomplete baseline refusal inventory")
        for record in cases:
            if record["name"].startswith("baseline_missing_") and record["name"].endswith("preserved_head"):
                check(record["head"]["footer"]["epoch"] == 0, "failed baseline admission preserved old head")
        for label in ("empty-output", "one-row-output", "many-row-output"):
            check(f"{label}_exact_output_limit" in names, "exact output cap witness")
        view_records = strict(self.members["view_readers.json"])
        check([r["view"] for r in view_records] == ["all_v1", "filtered_v1"]
              and len({r["manifest_sha256"] for r in view_records}) == 1,
              "separate named readers use one committed manifest")
        _, multi_model, _, _ = self.fixtures["multi-baseline"]
        for record in view_records:
            rows = [strict(line) for line in record["stdout"].splitlines()]
            footer = rows.pop()
            expected_rows = [r for r in multi_model[1]["views"] if r["view"] == record["view"]]
            check(rows == expected_rows and footer == record["footer"]
                  and footer["views_read"] == 1 and footer["result"] == multi_model[1]["result"],
                  "independent named view reader reconstruction")
        _, model, _, _ = self.fixtures["sequence"]
        phases = self.criteria["fault_points"]
        kills = [c for c in cases if c["name"] == "interruption"]
        check([r["point"] for r in kills] == phases, "interruption inventory")
        for record in kills:
            check(record["marker"] == f'fault reached {record["point"]}'
                  and record["exit"] == -9 and not record["stdout"], "reached process kill")
        resumes = [c for c in cases if c["name"] in ("orphan_stage", "resume_after_kill")]
        check([r["point"] for r in resumes] == phases, "resume inventory")
        for record in resumes:
            point = record["point"]
            selected = int(point in ("selected", "selection_synced"))
            self.check_read(record["before"], model[selected], selected)
            self.check_read(record["after"], model[2], 2, f"crash-{point}-2")
            self.check_read(record["final"], model[3], 3, f"crash-{point}-3")
            check(record["resume"]["result"] == model[1]["result"]
                  and record["next"]["result"] == model[2]["result"]
                  and record["signed"]["result"] == model[3]["result"]
                  and record["signed"]["decision"]["selected"] == "incremental_update"
                  and record["resume"]["recovered_publication"] == (point == "witness_synced"),
                  "restarted target closure")
        sequence = strict(self.members["sequence.json"])
        for epoch, trace in enumerate(sequence["traces"]):
            check(trace["result"] == model[epoch]["result"]
                  and trace["decision"]["selected"] == ("full_recompute" if not epoch else "incremental_update"),
                  "separate-process sequence trace")
            self.check_ledger(f"sequence-{epoch}", model[epoch], epoch)
        for name, epoch in (("replay", 3), ("retry", 1)):
            trace = sequence[name]
            check(trace["result"] == model[epoch]["result"]
                  and trace["decision"]["selected"] == ("full_recompute" if name == "replay" else "restore"),
                  "replay and retry result identity")
        self.check_ledger("sequence-full-3", model[3], 3)
        _, extreme, _, _ = self.fixtures["extreme"]
        for epoch, state in enumerate(extreme):
            self.check_ledger(f"extreme-{epoch}", state, epoch)

    @staticmethod
    def shard_hot(obj):
        groups = struct.unpack_from("<Q", obj, 8)[0]
        pos, entries = 16, groups
        for _ in range(groups):
            n = struct.unpack_from("<I", obj, pos + 40)[0]
            entries += n
            pos += 44 + n * 16
        check(pos == len(obj), "oracle shard record closure")
        return entries

    @classmethod
    def render_read(cls, state, epoch, view=None):
        hot = max((cls.shard_hot(s) for s in state["shards"] if s is not None), default=0)
        output_rows = [row for row in state["views"] if view is None or row["view"] == view]
        footer = {"epoch": epoch, "chain": state["chain"], "result": state["result"],
                  "view_rows": len(output_rows), "views_read": 2 if view is None else 1,
                  "hot_entries_peak": hot}
        return b"".join(compact(row) + b"\n" for row in output_rows + [footer]), footer

    def check_ledger(self, label, state, epoch):
        rendered, _ = self.render_read(state, epoch)
        check(self.members[f"ledgers/{label}.ndjson"] == rendered, f"independent output ledger {label}")

    def check_read(self, read, state, epoch, ledger=None):
        rendered, footer = self.render_read(state, epoch)
        check(read == {"sha256": sha(rendered), "footer": footer}, "independent committed reader")
        if ledger:
            check(self.members[f"ledgers/{ledger}.ndjson"] == rendered, f"independent raw output ledger {ledger}")

    def verify_samples(self):
        samples = strict(self.members["samples.json"])
        regimes = self.criteria["regimes"]
        seen = collections.Counter()
        timings = collections.defaultdict(list)
        limits = self.criteria["limits"]
        for row in samples:
            label, policy, turn = row["regime"], row["policy"], row["round"]
            spec = regimes[label]
            parent = int(label == "restored")
            fixture = "sparse" if parent else label
            check((row["fixture"], row["parent_epoch"], row["exit"], row["stderr"]) ==
                  (fixture, parent, 0, "") and row["wall_ns"] > 0, "matched child execution")
            check(policy in ["adaptive"] + spec["static"], "sample policy")
            seen[label, policy, turn] += 1
            plan, model, raw, changes = self.fixtures[fixture]
            check(sum(f["rows"] for f in plan["baseline"]) == spec["rows"]
                  and len({key for key, _ in changes[0]}) == spec["changed_keys"], "frozen fixture size")
            selection = expected_decision(model, [f["bytes"] for f in plan["baseline"]], spec["rows"],
                                          raw, changes, 1, parent, policy)
            trace = strict(row["stdout"])
            verify_trace(trace, model[1], 1, selection)
            route = selection[0]
            if policy == "adaptive":
                check(route == spec["expected"], "predeclared route")
            metrics = trace["metrics"]
            check(metrics["state_bytes"] == sum(len(s or b"") for s in model[1]["shards"])
                  and metrics["hot_entries_peak"] <= limits["hot_entries"]
                  and metrics["root_bytes"] <= limits["disk_bytes"]
                  and metrics["decoded_batch_peak"] <= 1024 * 1024
                  and metrics["baseline_rows"] == (spec["rows"] if route == "full_recompute" else 0)
                  and metrics["view_updates"] == 2 * metrics["keys_updated"], "observed bounded work")
            if route == "restore":
                check(metrics["shards_written"] == metrics["delta_rows_applied"] == 0
                      and metrics["checkpoint_verified_bytes"] == metrics["state_bytes"], "restore work")
            else:
                check(metrics["arrangement_instances"] == 1 and metrics["defined_views"] == 2,
                      "one arrangement defines both views")
            if label == "sparse" and route == "incremental_update":
                work = self.criteria["work"]
                check(metrics["keys_updated"] == work["sparse_max_keys_updated"]
                      and metrics["shards_written"] <= work["sparse_max_shards_written"]
                      and metrics["baseline_rows"] == work["sparse_baseline_rows_decoded"], "sparse work")
            self.check_read(row["read"], model[1], 1, f"{label}-{policy}" if turn == 0 else None)
            if turn > 0:
                timings[label, policy].append(row["wall_ns"])
        rounds = self.criteria["samples_per_regime_policy"] + self.criteria["warmups_per_regime_policy"]
        expected = {(label, policy, turn) for label, spec in regimes.items()
                    for policy in ["adaptive"] + spec["static"] for turn in range(rounds)}
        check(set(seen) == expected and all(n == 1 for n in seen.values()), "raw paired sample coverage")
        report = strict(self.members["performance.json"])
        for label, spec in regimes.items():
            medians = {p: statistics.median(timings[label, p]) for p in ["adaptive"] + spec["static"]}
            if label == "sparse":
                ratio = medians["incremental_update"] / medians["full_recompute"]
                bound = self.criteria["sparse_ratio_max"]
            else:
                ratio = medians["adaptive"] / min(medians[p] for p in spec["static"])
                bound = self.criteria["other_regime_ratio_max"]
            check(report[label] == {"medians_ns": medians, "ratio": ratio}, "raw median and ratio")
            check(ratio <= bound, f"performance gate {label}: {ratio} > {bound}")

    def verify_snapshots(self):
        expected = {
            "sequence": ("sequence", [0, 1, 2, 3]),
            "sequence-full": ("sequence", [3]),
            "crash-witness": ("sequence", [0, 1, 2, 3]),
            "extreme": ("extreme", [0, 1, 2, 3]),
        }
        for label, spec in self.criteria["regimes"].items():
            for policy in ["adaptive"] + spec["static"]:
                expected[f"{label}-{policy}"] = ("sparse" if label == "restored" else label, [0, 1])
        observed = {path.split("/")[1] for path in self.members if path.startswith("snapshots/")}
        check(set(expected) == observed, "snapshot coverage")
        for name, (fixture, epochs) in expected.items():
            self.verify_snapshot(name, self.fixtures[fixture][1], epochs)

    def verify_snapshot(self, name, model, epochs):
        prefix = f"snapshots/{name}/"
        files = {p[len(prefix):]: data for p, data in self.members.items() if p.startswith(prefix)}
        previous = None
        location = None
        manifests = {}
        for epoch in epochs:
            selector = f"g-{epoch:08}"
            encoded = files.pop(f"{selector}/MAINTAINED19")
            check(files.pop(f"w-{epoch:08}") == encoded, "selector durable witness bytes")
            m = strict(encoded)
            state = model[epoch]
            check((m["version"], m["epoch"], m["previous"]) == (1, epoch, previous),
                  "selector chain and predecessor")
            check(all(m[k] == state[k] for k in ("plan", "chain", "history", "result")), "selector semantic closure")
            location = m["location"] if location is None else location
            check(m["location"] == location and all(v > 0 for v in location.values()), "root location")
            previous = identity("maintained-manifest-v1", encoded)
            manifests[epoch] = m
            check(len(m["objects"]) == len(state["shards"]), "shard inventory width")
            for index, o in enumerate(m["objects"]):
                payload = state["shards"][index]
                check((payload is None) == (o is None), "missing arrangement shard")
                if o:
                    check(o["hash"] == state["hashes"][index] and len(payload) == o["bytes"]
                          and o["view_rows"] == state["view_counts"][index] and o["hot_entries"] == self.shard_hot(payload),
                          "independent arrangement and view shard")
                    if o["epoch"] == epoch:
                        check(files.pop(f"{selector}/s-{index:03}.bin") == payload, "independent arrangement bytes")
                    else:
                        check(o["epoch"] in manifests and manifests[o["epoch"]]["objects"][index] == o,
                              "immutable shard reuse")
        check(not files, "unclaimed snapshot payload")


def tamper(members, verified):
    """Mutate payloads after validation and recompute every outer member checksum."""
    cases = []
    mutations = {
        "rehashed_delta_tamper": "fixtures/sequence/deltas/00000001.json",
        "rehashed_view_tamper": "ledgers/sequence-3.ndjson",
        "rehashed_decision_tamper": "samples.json",
    }
    for name, path in mutations.items():
        modified = dict(members)
        if name == "rehashed_delta_tamper":
            document = strict(modified[path])
            document["rows"][1][1] += 1
            modified[path] = compact(document)
        elif name == "rehashed_view_tamper":
            rows = modified[path].splitlines()
            row = strict(rows[0])
            row["sum"] += 1
            rows[0] = compact(row)
            modified[path] = b"\n".join(rows) + b"\n"
        else:
            rows = strict(modified[path])
            trace = strict(rows[0]["stdout"])
            trace["decision"]["candidates"][1]["work_units"] += 1
            rows[0]["stdout"] = compact(trace).decode()
            modified[path] = compact(rows)
        verify_members(modified, {member: sha(blob) for member, blob in modified.items()})
        attack = Verifier(modified)
        attack.fixtures = dict(verified.fixtures)
        try:
            if name == "rehashed_delta_tamper":
                attack.fixtures["sequence"] = attack.fixture("sequence")
            if name == "rehashed_decision_tamper":
                attack.verify_samples()
            else:
                attack.verify_cases()
        except (ValueError, OverflowError, KeyError, struct.error) as error:
            cases.append({"name": name, "member": path, "old_sha256": sha(members[path]),
                          "forged_sha256": sha(modified[path]), "refusal": str(error)})
        else:
            raise ValueError(f"rehashed attack passed {name}")
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--repo", type=Path)
    parser.add_argument("--bin", type=Path)
    parser.add_argument("--encoder", type=Path)
    parser.add_argument("--tamper", action="store_true")
    args = parser.parse_args()
    manifest = strict(args.manifest.read_bytes())
    members = archive_members(args.archive, manifest)
    verifier = Verifier(members)
    report = verifier.validate(args.repo, args.bin, args.encoder)
    if args.tamper:
        report["tamper"] = tamper(members, verifier)
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
