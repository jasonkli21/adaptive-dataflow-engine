"""Verify detached Phase 26 bytes, original-source oracle and causal receipts."""
import argparse
from functools import lru_cache
from itertools import combinations
import json
from pathlib import Path
import re
import statistics
import sys

import blake3
import pyarrow.compute as pc
import pyarrow.parquet as pq

from fixtures import canonical, digest, write
from evidence_inventory import file_inventory, sha
from source_inventory import verify_source

SEMANTICS = "ade.shared-prefix.v1/scan-filter-lt-project-1/parquet-1"
MODES = {"disabled", "always", "expected_value_v1"}


def load(path):
    return json.loads(path.read_bytes())


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


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
        eligible = [e for e in entries if e["expired"] or
                    (mode == "expected_value_v1" and e["remaining_us"] is not None)]
        byte_need = max(0, sum(e["bytes"] for e in entries) + reservation + header_bytes
                        - limits["quota_bytes"])
        count_need = max(0, len(entries) + 1 - limits["max_entries"])
        best = None
        if byte_need == 0 and count_need == 0:
            best = (0, 0, [])
        else:
            require(len(eligible) <= 16, "exhaustive policy oracle fixture exceeds 16 eligible entries")
            for size in range(count_need, len(eligible) + 1):
                for subset in combinations(eligible, size):
                    freed = sum(e["bytes"] for e in subset)
                    if freed < byte_need:
                        continue
                    loss = sum(0 if e["expired"] else max(0, e["remaining_us"] or 0) for e in subset)
                    rank = (loss, freed, sorted(e["key"] for e in subset))
                    if best is None or rank < best:
                        best = rank
        if best is None:
            expected, victims = "insufficient_capacity", []
        else:
            loss, _, keys = best
            if mode == "expected_value_v1" and loss >= score:
                expected, victims = "protected_value", []
            else:
                expected, victims = "admit", keys
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
                created, expires, observed = entry["created_ms"], entry["expires_ms"], entry["observed_ms"]
                horizon = expires - created
                require(horizon > 0 and entry["expired"] == (observed >= expires),
                        "retention horizon or expiry")
                calibration_ = prior["audit"]["calibration"]
                if calibration_ is None:
                    require(entry["remaining_us"] is None, "uncalibrated retention input")
                else:
                    remaining_ms = expires - observed if created <= observed < expires else 0
                    expected_value = max(0, value(calibration_, False)) * remaining_ms // horizon
                    require(entry["remaining_us"] == expected_value, "residual retention input")
                require(
                        entry["bytes"] > prior["audit"]["payload_bytes"], "retention input binding")
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
    require(jobs["f01-profitable-hit-negative-write"]["audit"]["actions"] == ["hit"]
            and not jobs["f01-profitable-hit-negative-write"]["audit"]["prefix"],
            "existing hit incorrectly charged candidate write")
    empty = jobs["f01-empty-negative-write"]["audit"]
    require(empty["actions"] == ["miss", "skip"] and not empty["prefix"]
            and empty["decision"]["reason"] == "nonpositive_value"
            and events["f01-empty-inventory"]["value"]["entries"] == []
            and jobs["f01-slow-restore"]["audit"]["actions"] == ["skip"],
            "miss admission or uneconomic hit policy")
    require(jobs["f04-unrelated-hit"]["audit"]["actions"] == ["hit"]
            and events["f04-new-fill-blocked"]["exit"] == 1
            and events["f04-gc-blocked"]["exit"] == 1
            and events["f04-purge-corrupt-key"]["exit"] == 0
            and jobs["f04-fill-after-purge"]["audit"]["actions"] == ["miss", "publish"],
            "explicit corrupt-entry recovery")
    for name, error in (("f04-invalid-purge-key", "exact lowercase cache key"),
                        ("f04-symlink-purge", "access refused"),
                        ("f04-purge-pinned-refused", "deadline")):
        require(events[name]["exit"] == 1 and error in events[name]["stderr"],
                f"safe purge refusal: {name}")
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
        "nonreplayable": "cache requires a replayable source snapshot",
        "limit": "invalid request: PlanV1",
        "valid-unsupported-suffix": "unsupported native cut or suffix",
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
        require(ev["exit"] == 1 and not ev["stdout"] and error in ev["stderr"],
                f"negative runtime witness: {name}")
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


def supplements(root, binary_sha256):
    disk = root / "enospc"
    raw, index = receipt_index(disk)
    jobs = verify_jobs(disk, raw)
    marker = load(disk / "enospc.json")
    failed = index["kernel-enospc"]
    require(marker.get("binary_sha256") == binary_sha256, "ENOSPC executable differs from campaign")
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
    return dict(disk_full=28, uid=[int(owner[1]), int(other[1])])


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("archive", type=Path)
    choice = cli.add_mutually_exclusive_group(required=True)
    choice.add_argument("--pin")
    choice.add_argument("--seal", action="store_true")
    cli.add_argument("--release", action="store_true", help="check current release prerequisites")
    cli.add_argument("--source", type=Path, help="Git checkout for the current release")
    cli.add_argument("--revision", help="exact 40-character release commit SHA")
    cli.add_argument("--binary", type=Path, help="current executable to match against the sealed copy")
    cli.add_argument("--ci-receipt", type=Path, help="externally retrieved exact-commit CI receipt")
    cli.add_argument("--ci-pin", help="independently pinned SHA-256 of the CI receipt")
    args = cli.parse_args()
    release_inputs = (args.source, args.revision, args.binary, args.ci_receipt, args.ci_pin)
    if args.release:
        if not args.pin or not all(release_inputs) or not re.fullmatch("[0-9a-f]{40}", args.revision):
            cli.error("--release requires --pin, --source, full --revision, --binary, --ci-receipt and --ci-pin")
    elif any(release_inputs):
        cli.error("release inputs require --release")
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
        require(result["economics"]["passes"], "release economics thresholds did not pass")
        require(sha(args.binary) == host["binary_sha256"], "release executable differs from archive")
        result["source"] = verify_source(args.source, root / "source", args.revision)
        # Reuse the same exact-commit hosted-receipt contract used by Phase 23.
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from phase23.verify import verify_ci_receipt
        receipt = verify_ci_receipt(args.ci_receipt, args.ci_pin, args.revision)
        result["ci_run_id"] = receipt["run_id"]
        result["supplements"] = supplements(root, host["binary_sha256"])
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
