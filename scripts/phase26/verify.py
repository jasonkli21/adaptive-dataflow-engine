"""Verify detached Phase 26 bytes, original-source oracle and causal receipts."""
import argparse
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import re
import statistics

import blake3
import pyarrow.compute as pc
import pyarrow.parquet as pq

from fixtures import canonical, digest, write

SEMANTICS = "ade.shared-prefix.v1/scan-filter-lt-project-1/parquet-1"
MODES = {"disabled", "always", "expected_value_v1"}


def sha(path):
    hasher = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1 << 20), b""):
            hasher.update(block)
    return hasher.hexdigest()


def load(path):
    return json.loads(path.read_bytes())


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def file_inventory(root):
    result = {}
    for path in sorted(root.rglob("*")):
        require(not path.is_symlink(), "artifact symlink")
        if path.is_file() and path != root / "seal.json":
            result[str(path.relative_to(root))] = sha(path)
    return result


def seal(root):
    write(root / "seal.json", file_inventory(root))
    return sha(root / "seal.json")


def verify_seal(root, pin):
    require(sha(root / "seal.json") == pin, "external seal pin")
    require(load(root / "seal.json") == file_inventory(root), "artifact inventory")


def cli_arg(event, name, default=None):
    argv = event["argv"]
    return argv[argv.index(name) + 1] if name in argv else default


def vpath(root, event, name):
    raw = cli_arg(event, name)
    require(raw is not None and raw.startswith("$OUT/"), f"detached {name} path")
    path = root / raw.removeprefix("$OUT/")
    require(path.is_relative_to(root) and ".." not in path.parts, f"unsafe {name} path")
    return path


@lru_cache(maxsize=32)
def admitted_inventory(source):
    cat = source / "catalog.json"
    catalog = load(cat)
    declared = catalog.pop("files")
    for entry in declared:
        path = source / entry["name"]
        require(path.is_relative_to(source) and ".." not in path.parts, "catalog path")
        data = path.read_bytes()
        require(len(data) == entry["bytes"] and digest(data) == entry["fingerprint"], "original payload identity")
        p = pq.ParquetFile(path)
        require(p.metadata.num_rows == entry["rows"], "original row count")
        schema = [dict(name=f.name, data_type={"uint64": "u64", "int64": "i64"}[str(f.type)],
                       nullable=f.nullable) for f in p.schema_arrow]
        require(schema == catalog["schema"], "original physical schema")
    pin = digest(cat.read_bytes())
    return dict(id=0, table="input", manifest=pin, logical=catalog["logical"],
                schema_identity=digest(canonical(catalog["schema"])), schema=catalog["schema"],
                files=declared, replayable=True)


@lru_cache(maxsize=48)
def prefix_table(source, threshold, column, selections):
    source_id = admitted_inventory(source)
    names = [f["name"] for f in source_id["schema"]]
    projected = [names[i] for i, _ in selections]
    chunks = []
    for file in source_id["files"]:
        table = pq.read_table(source / file["name"], columns=list(dict.fromkeys([*projected, names[column]])))
        rows = table.filter(pc.less(table[names[column]], threshold))
        chunks.extend(zip(*(rows[name].to_pylist() for name in projected)))
    return tuple(chunks)


def oracle(source, plan, cut):
    nodes = plan["nodes"]
    filter_op = nodes[1]["operator"]
    if cut == 2:
        projection = tuple((c["column"], c["alias"]) for c in nodes[2]["operator"]["columns"])
    else:
        projection = tuple((i, f["name"]) for i, f in enumerate(nodes[1]["schema"]))
    # An explicitly typed scalar represents UINT64_MAX without a signed coercion.
    import pyarrow as pa
    rows = prefix_table(source, pa.scalar(filter_op["less_than"], type=pa.uint64()), filter_op["column"], projection)
    agg = nodes[cut + 1]["operator"]
    sums = {}
    for row in rows:
        k = row[agg["key"]]
        sum_, count = sums.get(k, (0, 0))
        sums[k] = sum_ + row[agg["value"]], count + 1
    grouped = [(key, sum_, count) for key, (sum_, count) in sums.items()]
    for node in nodes[cut + 2:]:
        op = node["operator"]
        if op["op"] == "project":
            grouped = [tuple(row[c["column"]] for c in op["columns"]) for row in grouped]
        elif op["op"] == "filter":
            grouped = [row for row in grouped if row[op["column"]] < op["less_than"]]
        else:
            raise ValueError("unsupported oracle suffix")
    return rows, grouped


def value(c, producing):
    if c is None:
        return None
    from fractions import Fraction
    demand = Fraction(c["uses_milli"], 1000) * Fraction(c["survival_bps"], 10000)
    savings = max(0, c["recompute_us"] - c["restore_us"])
    return int(demand * savings) - c["hold_us"] - c["invalidation_us"] - c["write_us"] * producing


def policy_oracle(decision, calibration, mode, reservation, limits, header_bytes):
    score = value(calibration, True)
    require(score == decision["candidate_us"], "policy candidate score")
    entries = decision["alternatives"]
    if mode == "expected_value_v1" and score is None:
        expected, victims = "missing_calibration", []
    elif mode == "expected_value_v1" and score <= 0:
        expected, victims = "nonpositive_value", []
    else:
        expected, victims = "insufficient_capacity", []
        remaining = {e["key"]: e for e in entries}
        eligible = [e for e in entries if e["expired"] or
                    (mode == "expected_value_v1" and e["remaining_us"] is not None)]
        def penalty(e):
            return 0 if e["expired"] else max(0, e["remaining_us"])
        paid = 0
        for alternative in [*sorted(eligible, key=lambda e: (penalty(e), e["key"])), None]:
            charge = sum(e["bytes"] for e in remaining.values()) + reservation + header_bytes
            if charge <= limits["quota_bytes"] and len(remaining) < limits["max_entries"]:
                expected = "admit"
                break
            if alternative is None:
                break
            paid += penalty(alternative)
            if mode == "expected_value_v1" and paid >= score:
                expected = "protected_value"
                break
            key = alternative["key"]
            del remaining[key]
            victims.append(key)
        if expected != "admit":
            victims = []
    require(decision["reason"] == expected and decision["victims"] == victims, "policy selection")


def receipt_index(root):
    events = [json.loads(line) for line in (root / "events.jsonl").read_text().splitlines()]
    index = {}
    for event in events:
        name = event["label"]
        require(name not in index and event["pid"] > 1 and event["wall_ns"] > 0, "process receipt")
        require(event["ended_ns"] - event["started_ns"] == event["wall_ns"], "receipt clock")
        if event["exit"] == 0:
            require(bool(event["stdout"]), "missing success output")
            event["value"] = json.loads(event["stdout"])
        else:
            require(not event["stdout"], "failed job published output")
            event["value"] = None
        index[name] = event
    return events, index


def plan_json(plan):
    return canonical(plan | {"nodes": [
        {name: node[name] for name in ("id", "operator", "schema", "properties")}
        for node in plan["nodes"]
    ]})


def rewritten_identity(plan, cut, audit):
    node = plan["nodes"][cut]
    key = audit["prefix_key"]
    schema = node["schema"]
    payload = audit["payload_digest"]
    cache_source = dict(id=0, table="input", manifest=key, logical=payload,
                        schema_identity=digest(canonical(schema)), schema=schema,
                        files=[dict(name="payload.parquet", bytes=audit["payload_bytes"],
                                    rows=audit["payload_rows"], fingerprint=payload)], replayable=True)
    scan = dict(id=0, operator=dict(op="scan", source=0), schema=schema, properties=node["properties"])
    suffix = []
    for node in plan["nodes"][cut+1:]:
        op = node["operator"]
        suffix.append(node | dict(id=node["id"] - cut, operator=op | dict(input=op["input"] - cut)))
    return digest(plan_json(plan | dict(sources=[cache_source], nodes=[scan, *suffix],
                                        output=plan["output"] - cut)))


def verify_jobs(root, events):
    configs = {}
    fills = {}
    jobs = {}
    for e in events:
        if "init" in e["argv"] and e["exit"] == 0:
            path = cli_arg(e, "--cache")
            limits = dict(quota_bytes=int(cli_arg(e, "--quota-bytes")), max_entries=int(cli_arg(e, "--max-entries")))
            configs[path] = limits
        if "run" not in e["argv"] or e["exit"] != 0:
            continue
        job_label = e["label"]
        out = e["value"]
        source = vpath(root, e, "--source")
        plan = load(vpath(root, e, "--plan"))
        cut = int(cli_arg(e, "--cut"))
        mode = cli_arg(e, "--policy")
        source_id = admitted_inventory(source)
        require(plan["sources"] == [source_id] and source_id["manifest"] == cli_arg(e, "--catalog-pin"),
                "plan catalog identity")
        audit = out["audit"]
        require(audit["original_plan"] == digest(plan_json(plan)) and audit["snapshot"] == source_id["manifest"],
                "audit plan/catalog binding")
        require(audit["mode"] == mode and audit["pid"] == e["pid"], "execution identity")
        prefix, result = oracle(source, plan, cut)
        require(out["schema"] == plan["nodes"][-1]["schema"] and
                sorted(map(tuple, out["rows"])) == sorted(result), f"original-source result oracle: {job_label}")
        actions = audit["actions"]
        source_count = sum(f["rows"] for f in source_id["files"])
        source_bytes = sum(f["bytes"] for f in source_id["files"])
        actual_result = audit["result"]
        scan = [op for op in actual_result if op["name"] == "parquet-scan"]
        require(len(scan) == 1 and any(op["name"] == "hash-aggregate" for op in actual_result),
                "native suffix path")
        if mode in {"disabled", "never"}:
            require(actions == ["bypass"] and audit["prefix_key"] is None and not audit["prefix"],
                    "disabled policy performed cache action")
        elif audit["scope"] is None:
            require(actions == ["skip"] and audit["prefix_key"] is None and not audit["prefix"],
                    "economic pre-admission side effect")
        else:
            scope = audit["scope"]
            native_prefix = plan | dict(nodes=plan["nodes"][:cut+1], output=cut,
                                        max_output_rows=1000000, max_output_bytes=67108864)
            require(re.fullmatch("[0-9a-f]{64}", scope) is not None, "security domain identity")
            k = blake3.blake3(derive_key_context=SEMANTICS)
            k.update(scope.encode())
            k.update(plan_json(native_prefix))
            require(k.hexdigest() == audit["prefix_key"], "derived prefix key")
        if "hit" in actions or "publish" in actions:
            require(audit["payload_rows"] == len(prefix) == scan[0]["output_rows"] and audit["payload_bytes"] > 12
                    and re.fullmatch("[0-9a-f]{64}", audit["payload_digest"]) is not None,
                    "native cached scan vs oracle prefix")
            require(audit["rewritten_plan"] == rewritten_identity(plan, cut, audit),
                    "derived native suffix identity")
            require(all(op["name"] != "filter" for op in actual_result), "prefix ran on cached suffix")
            entry = cli_arg(e, "--cache"), audit["prefix_key"]
            if "hit" in actions:
                require(actions == ["hit"] and not audit["prefix"], "hit did prefix work")
                allowed_unacknowledged = {"reopen-entry_selected", "reopen-entry_synced"}
                prior = fills.get(entry)
                if prior:
                    require(prior["audit"]["payload_digest"] == audit["payload_digest"], "hit payload changed")
                else:
                    require(job_label in allowed_unacknowledged, "hit without committed producer")
                require(audit["ledger"]["source_fingerprint_bytes"] == 2 * source_bytes,
                        "hit omitted source verification")
            else:
                prefix_scan = [op for op in audit["prefix"] if op["name"] == "parquet-scan"]
                filters = [op for op in audit["prefix"] if op["name"] == "filter"]
                require(actions[0] == "miss" and actions[-1] == "publish", "commit causality")
                require(len(prefix_scan) == len(filters) == 1 and prefix_scan[0]["output_rows"] == source_count
                        and filters[0]["input_rows"] == source_count and filters[0]["output_rows"] == len(prefix),
                        "genuine prefix scan/filter audit")
                fills[entry] = out
                require(audit["ledger"]["source_fingerprint_bytes"] >= 3 * source_bytes,
                        "fill omitted source verification")
        else:
            require(scan[0]["output_rows"] == source_count and not audit["prefix"] and
                    any(op["name"] == "filter" for op in actual_result), "native bypass audit")
        calibration = load(vpath(root, e, "--calibration")) if cli_arg(e, "--calibration") else None
        if audit["decision"]:
            require(calibration == audit["calibration"], "policy input binding")
            decision = audit["decision"]
            for entry in decision["alternatives"]:
                prior = fills.get((cli_arg(e, "--cache"), entry["key"]))
                require(prior is not None, "unobserved retained entry")
                require(entry["remaining_us"] == value(prior["audit"]["calibration"], False)
                        and entry["bytes"] > prior["audit"]["payload_bytes"], "retention input binding")
            limits = configs.get(cli_arg(e, "--cache"), dict(quota_bytes=0, max_entries=0))
            policy_oracle(decision, calibration, mode, int(cli_arg(e, "--stage-bytes")), limits,
                          len(canonical(dict(version=1, limits=limits))))
        jobs[job_label] = out
    return jobs


def verify_artifact(root, events, jobs):
    filled = jobs["fill-alpha"]["audit"]
    alpha = events["fill-alpha"]
    prefix, _ = oracle(vpath(root, alpha, "--source"), load(vpath(root, alpha, "--plan")), 2)
    artifact = root / "artifacts" / "prefix"
    meta = load(artifact / "entry.json")
    file = artifact / "payload.parquet"
    payload = pq.ParquetFile(file)
    key = filled["prefix_key"]
    require(key == meta["key"] and meta["scope"] == filled["scope"] and
            meta["snapshot"] == filled["snapshot"] and meta["semantics"] == SEMANTICS and
            payload.metadata.created_by == "ADE shared-prefix v1 " + key,
            "artifact lineage")
    require(meta["rows"] == payload.metadata.num_rows == len(prefix) and
            meta["digest"] == digest(file.read_bytes()) == filled["payload_digest"] and
            meta["bytes"] == file.stat().st_size == filled["payload_bytes"], "artifact digest or rows")
    table = payload.read()
    require(table.schema.names == ["key", "value_i64", "alt"] and
            all(not f.nullable for f in table.schema) and
            tuple(zip(*(col.to_pylist() for col in table.columns))) == prefix,
            "ordered independent prefix artifact oracle")


def scenarios(root, events, jobs):
    fill = events["fill-alpha"]
    hit = events["hit-beta"]
    require(fill["pid"] != hit["pid"] and fill["ended_ns"] <= hit["started_ns"] and
            jobs["fill-alpha"]["audit"]["original_plan"] != jobs["hit-beta"]["audit"]["original_plan"] and
            jobs["fill-alpha"]["audit"]["prefix_key"] == jobs["hit-beta"]["audit"]["prefix_key"],
            "independent cross-job hit")
    for point in ("data_written", "data_synced", "manifest_synced", "before_suffix", "entry_selected", "entry_synced"):
        kill = events["kill-" + point]
        restore = events["reopen-" + point]
        require(kill["exit"] == -9 and f"phase26_pause {point}" in kill["stderr"]
                and kill["ended_ns"] <= restore["started_ns"], "reached kill and restart")
        require(jobs["reopen-" + point]["audit"]["actions"] ==
                (["hit"] if point.startswith("entry_") else ["miss", "publish"]), "restart publication")
        inv = events["inventory-" + point]["value"]
        require(inv["stage_bytes"] == 0 and len(inv["entries"]) == 1, "restart cleanup")
    negatives = {
        "same-size-source-mutation": "snapshot", "postflight-same-size": "snapshot",
        "postflight-identical-inode-swap": "snapshot", "postflight-catalog-drift": "snapshot",
        "stale-pin": "snapshot", "physical-nullability": "invalid source",
        "rehashed-cross-entry": "payload lineage",
        "manifest-version": "identity", "operator-version": "identity", "cache-hardlink": "access refused",
        "unsafe-root-mode": "access refused", "unsafe-ancestor-mode": "access refused",
        "pinned-quota-writer": "deadline", "pinned-gc": "deadline", "expired-reader-blocks-gc": "deadline",
        "overflow-stage": "arithmetic overflow", "row-cap": "output cap", "stage-quota": "capacity",
        "gc-while-stage": "deadline", "cancel-staging": "cancelled",
    }
    for name, error in negatives.items():
        ev = events[name]
        require(ev["exit"] == 1 and error in ev["stderr"], f"negative runtime witness: {name}")
    if "reader-slot-cap" in events:
        cap = events["reader-slot-cap"]
        require(cap["exit"] == 1 and "deadline" in cap["stderr"], "reader slot bound")
        occupants = [events[f"release-slot-{i}"] for i in range(8)]
        require(len({e["pid"] for e in occupants}) == 8 and all(
                e["exit"] == -9 and e["started_ns"] < cap["started_ns"]
                < cap["ended_ns"] < e["ended_ns"] for e in occupants), "reader slots not occupied")
        require(events["forged-tenant-label"]["exit"] == 2, "caller supplied a tenant authority")
    pinned = events["pinned-reader"]
    contender = events["pinned-quota-writer"]
    require(pinned["started_ns"] < contender["started_ns"] < contender["ended_ns"] < pinned["ended_ns"]
            and pinned["pid"] != contender["pid"], "reader and writer did not overlap")
    witness = jobs["evict-medium-removes-low"]["audit"]
    high = jobs["evict-fill-high-oldest"]["audit"]["prefix_key"]
    low = jobs["evict-touch-low"]["audit"]["prefix_key"]
    entries = {e["key"]: e for e in witness["decision"]["alternatives"]}
    require(events["evict-fill-high-oldest"]["ended_ns"] < events["evict-touch-low"]["ended_ns"]
            and entries[high]["remaining_us"] > entries[low]["remaining_us"]
            and witness["decision"]["victims"] == [low] and witness["evicted_bytes"] == entries[low]["bytes"],
            "expected-value action contrary to LRU")
    inv = events["evict-inventory"]["value"]
    require(low not in {e["key"] for e in inv["entries"]} and high in {e["key"] for e in inv["entries"]}
            and inv["committed_bytes"] == sum(e["bytes"] for e in inv["entries"])
            and inv["committed_bytes"] + inv["config_bytes"] <= inv["limits"]["quota_bytes"],
            "actual eviction and quota accounting")


def economics(root, events, jobs):
    sequences = load(root / "sequences.json")
    groups = {}
    pairs = set()
    for s in sequences:
        regime, mode, sample = s["regime"], s["mode"], s["sample"]
        require(regime in {"selective", "no-reuse"} and mode in MODES and 0 <= sample <= 5
                and (regime, mode, sample) not in pairs, "economics treatment identity")
        pairs.add((regime, mode, sample))
        count = 8 if regime == "selective" else 1
        labels = [f"econ-{regime}-{sample}-{mode}-{i}" for i in range(count)]
        require(s["jobs"] == labels and s["wall_ns"] >= sum(events[name]["wall_ns"] for name in labels),
                "whole-sequence timing exclusions")
        if mode == "always" or mode == "expected_value_v1" and regime == "selective":
            require(s["wall_ns"] >= sum(events[name]["wall_ns"] for name in
                    [f"econ-{regime}-{sample}-{mode}-init", *labels]), "init omitted from economics")
        for i, name in enumerate(labels):
            e = events[name]
            audit = jobs[name]["audit"]
            require(e["rss_bytes"] > 0 and (i == 0 or events[labels[i-1]]["ended_ns"] <= e["started_ns"]),
                    "economics child isolation/RSS")
            expected = ["bypass"] if mode == "disabled" else ["miss", "publish"] if i == 0 else ["hit"]
            if regime == "no-reuse" and mode == "expected_value_v1":
                expected = ["skip"]
            require(audit["actions"] == expected, "economics treatment changed")
        if sample:
            groups.setdefault(regime, {}).setdefault(mode, []).append(s["wall_ns"] / 1_000_000)
    require(len(pairs) == 36, "incomplete economics campaign")
    summary = {regime: {mode: dict(p50_ms=statistics.median(times), p95_ms=max(times))
                        for mode, times in modes.items()} for regime, modes in groups.items()}
    rel = {regime: modes["expected_value_v1"]["p50_ms"] / modes["disabled"]["p50_ms"]
           for regime, modes in summary.items()}
    return dict(wall=summary, adaptive_ratio=rel,
                passes=(rel["selective"] <= .90 and rel["no-reuse"] <= 1.20))


def supplements(root):
    inputs = load(root / "source/inputs.json")
    files = inputs["files"]
    require(len(files) > 20 and {"Cargo.lock", "Cargo.toml", "crates/ade-cache/src/store.rs"} <= files.keys(),
            "incomplete runtime source closure")
    require(hashlib.sha256(canonical(files)).hexdigest() == inputs["build_inputs_sha256"],
            "runtime identity")
    for name, hashed in files.items():
        path = Path(name)
        require(not path.is_absolute() and ".." not in path.parts
                and sha(root / "source" / "files" / path) == hashed, "locked source drift")
    disk = root / "enospc"
    raw, index = receipt_index(disk)
    jobs = verify_jobs(disk, raw)
    marker = load(disk / "enospc.json")
    failed = index["kernel-enospc"]
    require(marker["available_bytes"] == 0 and marker["errno"] == 28 and failed["exit"] == 1
            and "phase26_pause before_data_write" in failed["stderr"] and "Some(28)" in failed["stderr"],
            "real kernel ENOSPC witness")
    require(jobs["enospc-old-hit"]["audit"]["prefix_key"] == marker["old_key"]
            and jobs["enospc-old-hit"]["audit"]["actions"] == ["hit"]
            and jobs["enospc-new-fill"]["audit"]["actions"] == ["miss", "publish"]
            and jobs["enospc-new-hit"]["audit"]["actions"] == ["hit"], "ENOSPC retry causality")
    cross = (root / "linux-cross-uid.log").read_text()
    owner = re.search(r"^owner_uid=(\d+)$", cross, flags=re.M)
    other = re.search(r"^other_uid=(\d+)$", cross, flags=re.M)
    require(owner and other and owner[1] != other[1] and sha(root / "ade-cache-linux") in cross,
            "separate OS principal identity")
    for case in ("other_inspect_owner", "other_gc_owner", "owner_inspect_other", "owner_gc_other"):
        require(case + ' exit=1 {"action":"refuse","error":"access refused","version":1}' in cross,
                "cross-UID access refusal")
    require("other_raw_metadata_denied=true" in cross, "kernel refused foreign metadata read")
    return dict(build_inputs_sha256=inputs["build_inputs_sha256"], files=len(files),
                disk_full=28, uid=[int(owner[1]), int(other[1])])


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("archive", type=Path)
    choice = cli.add_mutually_exclusive_group(required=True)
    choice.add_argument("--pin")
    choice.add_argument("--seal", action="store_true")
    cli.add_argument("--release", action="store_true")
    args = cli.parse_args()
    root = args.archive.resolve()
    if args.seal:
        print(seal(root))
        return
    verify_seal(root, args.pin)
    host = load(root / "host.json")
    require(host["binary_sha256"] == sha(root / "ade-cache"), "recorded executable identity")
    events, indexed = receipt_index(root)
    jobs = verify_jobs(root, events)
    verify_artifact(root, indexed, jobs)
    scenarios(root, indexed, jobs)
    result = dict(receipts=len(events), original_source_oracle_results=len(jobs))
    if (root / "sequences.json").exists():
        result["economics"] = economics(root, indexed, jobs)
    if args.release:
        require("economics" in result, "release economics receipts absent")
        result["supplements"] = supplements(root)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
