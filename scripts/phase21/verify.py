#!/usr/bin/env python3
"""Package and verify Phase 21 witnesses with a separately retained SHA-256 pin."""
import argparse
import copy
import hashlib
import io
import json
import pathlib
import re
import statistics
import subprocess
import tarfile
import urllib.parse

import blake3
import fastavro
import pyarrow as pa
import pyarrow.parquet as pq

HERE = pathlib.Path(__file__).resolve().parent
FIXTURE_SCHEMA = [{"name": "value", "data_type": "i64", "nullable": False}]
HEX = re.compile(r"^[a-f0-9]{64}$")
MAX_BYTES = 64 * 1024 * 1024


def require(condition, message):
    if not condition:
        raise ValueError(message)


def parse(data):
    def unique(pairs):
        obj = {}
        for key, value in pairs:
            require(key not in obj, f"duplicate JSON key {key}")
            obj[key] = value
        return obj
    return json.loads(data, object_pairs_hook=unique)


def b3(data):
    return blake3.blake3(data).hexdigest()


def sha(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def pack(work):
    paths = ["criteria.json", "environment.json", "source.json", "services.json",
             "case-exits.json", "setup-failures.json"]
    for folder in ("raw", "fixtures", "services", "sink"):
        paths.extend(path.relative_to(work).as_posix() for path in (work / folder).rglob("*")
                     if path.is_file() and not path.is_symlink())
    return {name: (work / name).read_bytes() for name in sorted(set(paths))}


def encode_archive(path, entries):
    with tarfile.open(path, "w:gz") as archive:
        index = {name: sha(data) for name, data in entries.items()}
        for name, data in sorted(dict(entries, **{"inventory.json": canonical(index)}).items()):
            header = tarfile.TarInfo(name)
            header.size = len(data)
            header.mode = 0o600
            archive.addfile(header, io.BytesIO(data))
    return sha(path.read_bytes())


def load_archive(path, pin):
    require(path.stat().st_size <= MAX_BYTES, "archive size cap")
    require(isinstance(pin, str) and HEX.fullmatch(pin) and sha(path.read_bytes()) == pin, "independent archive pin")
    contents = {}
    total = 0
    with tarfile.open(path, "r:gz") as archive:
        for member in archive:
            name = pathlib.PurePosixPath(member.name)
            total += member.size
            require(member.isfile() and not name.is_absolute() and ".." not in name.parts
                    and name.as_posix() == member.name and member.name not in contents
                    and total <= MAX_BYTES and len(contents) < 8192, "unsafe archive member")
            contents[member.name] = archive.extractfile(member).read()
    index = parse(contents.pop("inventory.json"))
    require(index == {name: sha(data) for name, data in contents.items()}, "archive inventory digests")
    return contents


def read(entries, key):
    return parse(entries[key])


def file_key(uri):
    return urllib.parse.unquote(urllib.parse.urlparse(uri).path).split("/iceberg/", 1)[1]


def parquet(entries, folder, split):
    raw = entries[f"fixtures/{folder}/{split['key']}"]
    require(len(raw) == split["bytes"] and b3(raw) == split["blake3"], "Parquet byte pin")
    values = pq.read_table(pa.BufferReader(raw)).column("value").to_pylist()
    require(len(values) == split["rows"], "Parquet row count")
    return values


def check_delta(entries, request, values):
    pin = request["pin"]
    active = {}
    for i, digest in enumerate(pin["commits"]):
        raw = entries[f"fixtures/delta/_delta_log/{i:020}.json"]
        require(b3(raw) == digest, "Delta log pin")
        for line in raw.splitlines():
            action = parse(line)
            if "add" in action:
                add = action["add"]
                active[add["path"]] = add["size"]
            if "remove" in action:
                del active[action["remove"]["path"]]
    require(pin["version"] + 1 == len(pin["commits"]) and
            active == {s["key"]: s["bytes"] for s in pin["files"]}, "Delta active set")
    require(sorted(value for part in pin["files"] for value in parquet(entries, "delta", part)) ==
            sorted(values), "Delta row oracle")


def check_iceberg(entries, request, values):
    pin = request["pin"]
    def content(file):
        raw = entries[f"fixtures/iceberg/{file['key']}"]
        require(b3(raw) == file["blake3"], "Iceberg metadata pin")
        return raw
    metadata = parse(content(pin["metadata"]))
    snapshot = next(s for s in metadata["snapshots"] if s["snapshot-id"] == pin["snapshot_id"])
    require(file_key(snapshot["manifest-list"]) == pin["manifest_list"]["key"], "Iceberg cut")
    manifests = list(fastavro.reader(io.BytesIO(content(pin["manifest_list"]))))
    require({file_key(m["manifest_path"]) for m in manifests} == {p["key"] for p in pin["manifests"]},
            "Iceberg manifest list")
    active = {}
    for manifest in pin["manifests"]:
        for item in fastavro.reader(io.BytesIO(content(manifest))):
            data = item["data_file"]
            if item["status"] in (0, 1):
                key = file_key(data["file_path"])
                require(key not in active, "duplicate Iceberg file")
                active[key] = (data["file_size_in_bytes"], data["record_count"])
    require(active == {s["key"]: (s["bytes"], s["rows"]) for s in pin["files"]}, "Iceberg active set")
    require(sorted(value for part in pin["files"] for value in parquet(entries, "iceberg", part)) ==
            sorted(values), "Iceberg row oracle")


def check_kafka(output, request, ledger, name):
    caps = output["capabilities"]
    require(not caps["bounded"] and caps["native_offsets"] and not caps["checkpoint_restore"]
            and caps["replay"] == "trace_relative_kafka", "Kafka capability semantics")
    receipts = output["receipts"]
    phase = request["logical"]["computation"]["source"]["mode"]
    expected = {(p, o): event for p, o, event in ledger}
    require(len(receipts) == len(expected), f"{name} receipt cardinality")
    session = output["progress"]["session"]
    for receipt in receipts:
        event = expected.pop((receipt["partition"], receipt["offset"]))
        require(receipt["session"] == session and receipt["version"] == 1 and not receipt["late"]
                and receipt["event_time_ms"] == event["event_time_ms"]
                and receipt["projected"] == {k: event[k] for k in ("key", "value_i64", "event_time_ms")},
                f"{name} receipt oracle")
    require(output["stop"] == "STOPPED_BY_OPERATOR_TEST_LIMIT"
            and output["progress"]["forwarded_records"] == len(ledger), "Kafka progress")
    for partition in (0, 1):
        cursor = output["progress"]["partitions"][str(partition)]
        require(cursor["next_offset_suggestion"] == 1 + max(o for p, o, _ in ledger if p == partition),
                "Kafka next-offset authority")
    confirmed = [t["record"] for t in output["trace"] if t["type"] == "unified_confirmed"]
    require([(r["partition"], r["offset"]) for r in confirmed] ==
            [(r["partition"], r["offset"]) for r in receipts], "Kafka confirmed coverage")
    if name == "kafka_history_live":
        require(phase == {"kind": "history_then_live", "cut": [{"partition": 0, "offset": 2},
                                                               {"partition": 1, "offset": 1}]}, "Kafka cut")
        require(all(r["phase"] == ("historical" if r["offset"] < (2, 1)[r["partition"]] else "live")
                    for r in confirmed), "Kafka historical/live classification")
    if name == "kafka_idle":
        require(output["metrics"]["pending_polls"] >= 5, "Kafka idle proof")
    return session


def check_native(output, request, name, mode, values):
    diagnostics = output["diagnostics"]
    expected_rows = request["snapshot"]["splits"][0]["rows"] if name.startswith("timing_") else len(values)
    require(diagnostics["runtime"]["native_entrypoint"] == "ade_exec::physical::execute"
            and diagnostics["ir_version"] == 1 and diagnostics["target"] == "local"
            and diagnostics["runtime"]["metrics"]["output_rows"] == expected_rows, f"{name} native output")
    require(sorted(output["rows"]) == sorted(values), f"{name} row oracle")
    if mode == "baseline":
        require("connector" not in diagnostics, "baseline staging leaked")
        return
    connector = diagnostics["connector"]
    caps = connector["capabilities"]
    require(caps["bounded"] and not caps["native_offsets"] and not caps["checkpoint_restore"]
            and caps["pushdown"] == "none", "bounded capability semantics")
    kind = {"local": "local_parquet", "s3": "s3_parquet"}.get(mode, mode)
    require(connector["entry"]["connector"]["kind"] == kind and connector["entry"]["schema"] == FIXTURE_SCHEMA
            and connector["pushdown"] == "none" and connector["staged_bytes"] > 0
            and connector["replay"] == ("one_shot" if mode in ("postgres", "flight") else "pinned"),
            f"{name} connector provenance")
    require(diagnostics["sources"][0]["logical"] == connector["snapshot"], f"{name} source binding")
    if "expected" in request:
        require(request["expected"] == connector["snapshot"], f"{name} pinned snapshot")
    if "snapshot" in request:
        require(b3(canonical(request["snapshot"])) == connector["snapshot"]
                and connector["staged_bytes"] == sum(s["bytes"] for s in request["snapshot"]["splits"]),
                f"{name} split identity")
    if "pin" in request:
        snapshot = dict(version=1, entry=request["entry"], authority=b3(canonical(request["pin"])),
                        splits=request["pin"]["files"], replay="pinned")
        require(b3(canonical(snapshot)) == connector["snapshot"], f"{name} authority pin")


def read_sink(entries, key, receipt):
    raw = entries[f"sink/{key}.ade-ipc"]
    require(raw[:8] == b"ADEIPC21" and raw[8:72].decode() == receipt["plan_digest"], "sink plan envelope")
    data = raw[104:]
    require(b3(data) == receipt["payload_blake3"] and len(data) == receipt["payload_bytes"],
            "sink payload receipt")
    hasher = blake3.blake3(derive_key_context="ade-connectors local publication v1")
    hasher.update(raw[8:72] + bytes.fromhex(b3(data)) + key.encode())
    require(hasher.digest() == raw[72:104] and receipt["key"] == key, "sink envelope digest")
    require(sorted(pa.ipc.open_stream(data).read_all().column("value").to_pylist()) == [-4, 3, 3],
            "sink IPC row oracle")


def verify(entries, repo=None, binary=None):
    criteria_bytes = (HERE / "criteria.json").read_bytes()
    require(entries["criteria.json"] == criteria_bytes, "frozen criteria mismatch")
    criteria = parse(criteria_bytes)
    require(read(entries, "setup-failures.json") == {}, "fixture setup failure")
    env = read(entries, "environment.json")
    sources = read(entries, "source.json")

    def runtime_source(path):
        name = pathlib.PurePosixPath(path)
        return not name.is_absolute() and ".." not in name.parts and name.as_posix() == path and (
            path in ("Cargo.toml", "Cargo.lock", "scripts/phase15/Fixture.java")
            or (path.startswith("crates/") and (name.suffix == ".rs" or name.name == "Cargo.toml"))
            or path.startswith("scripts/phase21/"))

    require(all(runtime_source(p) and isinstance(h, str) and HEX.fullmatch(h) for p, h in sources.items())
            and HEX.fullmatch(env["executable_sha256"]), "source or executable digest grammar")
    require(sources.get("Cargo.lock") == env["cargo_lock_sha256"], "source lock identity")
    if repo:
        listed = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard"],
                                         cwd=repo, text=True).splitlines()
        expected = {p for p in listed if runtime_source(p)}
        require(expected == set(sources), "live source inventory coverage")
        require(all((repo / p).resolve().is_relative_to(repo.resolve()) and sha((repo / p).read_bytes()) == h
                    for p, h in sources.items()), "live source mismatch")
    if binary:
        require(sha(binary.read_bytes()) == env["executable_sha256"], "live executable mismatch")
    service = read(entries, "services.json")
    for certificate in ("ca.pem", "foreign.pem", "server.pem"):
        raw = entries["fixtures/tls/" + certificate]
        require(raw.startswith(b"-----BEGIN CERTIFICATE-----") and b"PRIVATE KEY" not in raw,
                "public TLS fixture evidence")
    require(b": OK" in entries["services/tls-independent-verify.stdout"], "independent certificate verification")
    require(set(service) == {"postgres", "flight", "kafka", "s3"}
            and len({x["pid"] for x in service.values()}) == 4, "live service inventory")
    case_specs = criteria["cases"] + [
        [f"timing_{i}_{mode}", mode, [3, -4, 3]]
        for i in range(criteria["timing"]["pairs"]) for mode in ("baseline", "local")] + [
        [f"timing_remote_{i}_{mode}", mode, [3, -4, 3]]
        for i in range(criteria["timing"]["remote_pairs"]) for mode in ("baseline", "s3")]
    exits = read(entries, "case-exits.json")
    names = {name for name, _, _ in case_specs}
    require(set(exits) == names and {p.split("/")[1] for p in entries if p.startswith("raw/")} == names,
            "ordered contract case inventory")
    ledger = [(int(p), int(o), parse(e)) for p, o, e in
              (line.split(b"\t", 2) for line in entries["services/kafka-ledger.stdout"].splitlines())]
    require(entries["services/kafka-ledger.stdout"] == entries["services/kafka-history-ledger.stdout"] +
            entries["services/kafka-live-ledger.stdout"], "Kafka producer boundary")
    require(entries["raw/kafka_history_live/stderr"] == b"HISTORY_RECEIPTS\n", "Kafka live append barrier")
    require([e["value_i64"] for _, _, e in ledger] == [3, -4, 3, 11, 8], "independent Kafka producer ledger")
    require(sorted(map(int, entries["services/pg-oracle.stdout"].splitlines())) == [-4, 3, 3], "Postgres oracle")
    outputs, sessions, pids = {}, {}, set()
    timings = {"baseline": [], "local": [], "remote_baseline": [], "s3": []}
    for name, mode, expected in case_specs:
        stem = "raw/" + name + "/"
        exit = read(entries, stem + "exit.json")
        req = read(entries, stem + "request.json")
        out, err = entries[stem + "stdout"], entries[stem + "stderr"]
        require(req["mode"] == mode and exit["code"] == exits[name] and exit["elapsed_ns"] > 0, name + " request/exit")
        require(isinstance(exit["pid"], int) and exit["pid"] > 0 and exit["pid"] not in pids, "isolated process identity")
        pids.add(exit["pid"])
        if expected == "SIGKILL":
            require(exit["code"] == -9 and err == b"REACHED\n" and not out and exit["live_stages"] == [],
                    name + " crash-cut witness")
            require(req["pause"] == name.removeprefix("sink_kill_")
                    and exit["selected"] == (req["pause"] == "committed")
                    and exit["pending"] and all(p.startswith(".ade-sink-") for p in exit["pending"]),
                    name + " reached cut")
            continue
        require(exit["live_stages"] == [], name + " staging cleanup")
        if isinstance(expected, str) and expected != "receipts":
            require(exit["code"] == 1 and expected.encode() in err and not out, name + " refusal witness")
            continue
        require(exit["code"] == 0, name + " successful exit")
        output = parse(out)
        outputs[name] = output
        if expected == "receipts":
            sessions[name] = check_kafka(output, req, ledger, name)
            continue
        if mode == "cli":
            require(read(entries, stem + "descriptor.json") == req["snapshot"], "CLI descriptor binding")
            read_sink(entries, "cli", output["receipt"])
            check_native(dict(output, rows=expected), req, name, "local", expected)
            continue
        check_native(output, req, name, mode, expected)
        if mode == "delta":
            check_delta(entries, req, expected)
            require(sorted(read(entries, f"fixtures/{name}.reference.json")) == sorted(expected), "Delta reference reader")
        if mode == "iceberg":
            check_iceberg(entries, req, expected)
            require(sorted(read(entries, f"fixtures/{name}.reference.json")) == sorted(expected), "Iceberg reference reader")
        if mode in ("local", "baseline", "s3"):
            split = req["snapshot"]["splits"][0]
            values = parquet(entries, "local", split)
            if name.startswith("timing_"):
                require(split["rows"] == criteria["timing"]["rows"] and values[:3] == expected, "timing fixture")
                digest = blake3.blake3()
                for value in values:
                    digest.update(value.to_bytes(8, "little", signed=True))
                require(digest.hexdigest() == output["row_digest"], "timing full row oracle")
                rss = re.search(rb"([0-9]+)\s+maximum resident set size", err)
                require(rss and int(rss[1]) > 0, "independent process RSS missing")
                real = re.search(rb"([0-9]+\.[0-9]+) real", err)
                require(real and -.015 <= exit["elapsed_ns"] / 1e9 - float(real[1]) <= .5,
                        "independent timing witness")
                require(0 < output["elapsed_ns"] <= exit["elapsed_ns"], "probe timing range")
                group = "remote_baseline" if mode == "baseline" and name.startswith("timing_remote_") else mode
                connector = output["diagnostics"].get("connector", {})
                require(connector.get("stage_ns", 0) + (connector.get("native_query_ns") or 0)
                        <= output["elapsed_ns"], "stage and query timing reconciliation")
                timings[group].append(dict(wall_ns=exit["elapsed_ns"], probe_ns=output["elapsed_ns"],
                    stage_ns=connector.get("stage_ns", 0), native_query_ns=connector.get("native_query_ns"),
                    wire_body_bytes=connector["staged_bytes"] if mode == "s3" else 0,
                    rss_bytes=int(rss[1]), stage_bytes=0 if mode == "baseline" else output["diagnostics"]["connector"]["staged_bytes"],
                    native_peak=output["diagnostics"]["runtime"]["metrics"]["managed"]["peak"]))
            else:
                require(sorted(values) == sorted(expected), f"{name} independent Parquet oracle")
    require(len(set(sessions.values())) == len(sessions), "Kafka restart session identity")
    require(outputs["flight"]["diagnostics"]["connector"]["snapshot"] !=
            outputs["flight_ticket_mutation"]["diagnostics"]["connector"]["snapshot"], "changed Flight ticket result identity")
    require(read(entries, "services/enospc.json") == {"errors": [28, 28, 28, 28], "free": 0, "visible": False},
            "actual private-volume ENOSPC proof")
    require(read(entries, "services/s3-faults.json") == {"requests": ["redirect", "truncated", "stall"], "redirected": 0},
            "S3 fault reachability")
    require(read(entries, "services/postgres-wire.json") == {"oversize_frame_reached": True}, "Postgres wire cap reachability")
    require(read(entries, "services/postgres-mid-export.json") == {"forwarded_data_rows": 260},
            "Postgres mid-export disconnect reachability")
    require(read(entries, "services/postgres-loss.json") == {"backend_terminated": True}, "Postgres loss reachability")
    require(outputs["local_abort"]["receipt"] == {"aborted": True}, "sink abort witness")
    require({p for p in entries if p.startswith("sink/") and p.endswith(".ade-ipc")} ==
            {"sink/once.ade-ipc", "sink/post.ade-ipc", "sink/race.ade-ipc", "sink/cli.ade-ipc"}, "sink visibility")
    for key, cases in [("once", ["local", "local_retry"]), ("post", ["sink_restart"]),
                       ("race", ["sink_concurrent_a", "sink_concurrent_b"])]:
        receipts = [outputs[name]["receipt"] for name in cases]
        require(all(r == receipts[0] for r in receipts), "same-key receipt drift")
        read_sink(entries, key, receipts[0])
    require(read(entries, "raw/delta_checkpoint/checkpoint.json") == {"version": 0, "size": 3}, "Delta checkpoint witness")
    for name in ("delta_protocol", "delta_deletion_vector", "delta_duplicate", "delta_traversal"):
        raw = entries[f"raw/{name}/changed.json"]
        actions = [parse(line) for line in raw.splitlines()]
        req = read(entries, f"raw/{name}/request.json")
        require(b3(raw) == req["pin"]["commits"][0], name + " changed pin")
        if name == "delta_protocol":
            valid = any(a.get("protocol", {}).get("minReaderVersion") == 3 for a in actions)
        elif name == "delta_deletion_vector":
            valid = any(a.get("add", {}).get("deletionVector") for a in actions)
        elif name == "delta_duplicate":
            valid = sum("protocol" in a for a in actions) == 2
        else:
            valid = any(a.get("add", {}).get("path", "").startswith("../") for a in actions)
        require(valid, name + " changed semantics")
    require(read(entries, "raw/iceberg_partition/changed.json")["default-spec-id"] == 1, "Iceberg partition mutation")
    deleted = entries["raw/iceberg_deleted_entry/changed.avro"]
    require(next(iter(fastavro.reader(io.BytesIO(deleted))))["status"] == 2, "Iceberg deleted-entry witness")
    return dict(cases=len(case_specs), timing=timings, median_ns={
        mode: int(statistics.median(t["wall_ns"] for t in samples)) for mode, samples in timings.items()})


def tamper(entries, archive):
    def replace(case, file, fn):
        def edit(data):
            key = f"raw/{case}/{file}"
            data[key] = canonical(fn(parse(data[key])))
        return edit

    attacks = {
        "source_path_escape": lambda e: e.__setitem__("source.json",
            canonical(dict(parse(e["source.json"]), **{"../Cargo.lock": "0" * 64}))),
        "different_rows": replace("iceberg_s1", "stdout", lambda v: dict(v, rows=[3, -4, 3, 11, 9])),
        "relabelled_request": replace("s3", "request.json", lambda v: dict(v, mode="local")),
        "missing_case": lambda e: e.pop("raw/kafka_idle/exit.json"),
        "forged_timing": replace("timing_0_local", "exit.json", lambda v: dict(v, elapsed_ns=v["elapsed_ns"] + 10**10)),
        "wrong_delta_cut": replace("delta_v1", "request.json", lambda v: dict(v, pin=dict(v["pin"], version=0))),
        "forged_sink": lambda e: e.__setitem__("sink/once.ade-ipc", e["sink/once.ade-ipc"][:-1] + b"X"),
        "forged_kafka_phase": replace("kafka_history_live", "stdout",
            lambda v: dict(v, trace=[dict(t, record=dict(t["record"], phase="live"))
                                     if t["type"] == "unified_confirmed" else t for t in v["trace"]])),
    }
    for name, edit in attacks.items():
        mutated = copy.copy(entries)
        edit(mutated)
        attack = archive.with_name(archive.stem + "-" + name + ".tar.gz")
        require(not attack.exists(), "refusing to replace tamper archive")
        pin = encode_archive(attack, mutated)
        try:
            verify(load_archive(attack, pin))
        except (ValueError, KeyError, IndexError):
            print(name, "REJECTED")
        else:
            raise ValueError(name + " rehashed attack passed")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=pathlib.Path)
    parser.add_argument("--pack", type=pathlib.Path)
    parser.add_argument("--pin")
    parser.add_argument("--repo", type=pathlib.Path)
    parser.add_argument("--binary", type=pathlib.Path)
    parser.add_argument("--tamper", action="store_true")
    args = parser.parse_args()
    if args.pack:
        require(not args.archive.exists(), "refusing to replace archive")
        args.pin = encode_archive(args.archive, pack(args.pack))
        print("archive_sha256", args.pin, flush=True)
    entries = load_archive(args.archive, args.pin)
    print(json.dumps(verify(entries, args.repo, args.binary), indent=2))
    if args.tamper:
        tamper(entries, args.archive)


if __name__ == "__main__":
    main()
