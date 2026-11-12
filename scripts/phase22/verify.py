#!/usr/bin/env python3
"""Detached, fail-closed verifier for a complete Phase 22 campaign directory."""
import argparse
import base64
import json
from pathlib import Path
import struct
import uuid

import pyarrow as pa

from cases import FAILURES, plan
from fixtures import SCHEMA, SQL, hash_bytes, oracle
from inventory import artifact_inventory, sha, source_inventory

HERE = Path(__file__).resolve().parent
CRITERIA = json.loads((HERE / "criteria.json").read_text())
LOCK = json.loads((HERE / "fixture-lock.json").read_text())
BUDGETS = CRITERIA["budgets"]
DIGEST = LOCK["function_blake3"]
EXPECTED = {case for group, cases in CRITERIA["cases"].items() if group != "evidence_tamper" for case in cases}
DATA_CASES = {"sql", "plan", "udf", "repeat", "empty", "single", "boundary"}
MAPPED = {"udf", "repeat", "empty", "single", "boundary"}


def need(condition, reason):
    if not condition:
        raise ValueError(reason)


def read(path):
    def unique(pairs):
        values = {}
        for key, value in pairs:
            need(key not in values, f"duplicate evidence field: {key}")
            values[key] = value
        return values
    return json.loads(path.read_text(), object_pairs_hook=unique, parse_constant=lambda _: None)


def arrow_result(path, expected):
    with pa.ipc.open_stream(path) as reader:
        table = reader.read_all()
    expected_types = {field[0]: field[2] for field in SCHEMA}
    schema = pa.schema([
        pa.field(field["name"], expected_types[field["name"]], field["nullable"])
        for field in expected["schema"]
    ])
    need(table.schema.equals(schema, check_metadata=True), "Arrow schema")
    need(table.num_rows == len(expected["rows"]), "Arrow row count")
    columns = []
    for field, data in zip(schema, table.columns):
        need(data.null_count == 0, "Arrow null")
        if pa.types.is_timestamp(field.type):
            data = data.cast(pa.int64())
        if pa.types.is_floating(field.type):
            columns.append([f"{struct.unpack('>Q', struct.pack('>d', v))[0]:x}" for v in data.to_pylist()])
        else:
            columns.append([str(v) for v in data.to_pylist()])
    need([list(row) for row in zip(*columns)] == expected["rows"], "Arrow rows")


def positive_int(value, limit=None):
    return type(value) is int and value > 0 and (limit is None or value <= limit)


def hex_id(value, length=64):
    return isinstance(value, str) and len(value) == length and all(c in "0123456789abcdef" for c in value)


def validate_receipt(receipt, name, source, *, flight=True):
    case = name if name in DATA_CASES else "sql"
    expected_rows = len(oracle(case)["rows"])
    count = {"empty": 0, "single": 1, "boundary": 256}.get(case, 521)
    identity = receipt["identity"]
    function = {"id": "map_secondary", "abi": 1, "digest": DIGEST} if case in MAPPED else None
    need(identity["function"] == function and identity["protocol"] == 1, "function identity")
    need(hex_id(identity["plan"]), "plan digest")
    expected_query = hash_bytes(json.dumps(identity, separators=(",", ":"), sort_keys=True).encode())
    need(receipt["query_hash"] == expected_query, "canonical query identity")
    need(str(uuid.UUID(receipt["attempt"], version=4)) == receipt["attempt"], "attempt UUID")
    need(str(uuid.UUID(receipt["request_id"], version=4)) == receipt["request_id"]
         and receipt["request_id"] != receipt["attempt"], "request UUID")
    need(receipt["source"] == source, "source manifest binding")
    need(receipt["replayable"] is (function is None), "replay classification")
    need(positive_int(receipt["native_wall_ns"], BUDGETS["query_ms"] * 1_000_000), "native timing bounds")
    need(type(receipt["native_peak_bytes"]) is int and
         0 <= receipt["native_peak_bytes"] <= BUDGETS["native_bytes"], "native peak bound")
    need(receipt["spill_bytes"] == 0, "spill bytes")
    ops = receipt["operators"]
    names = [operator["name"] for operator in ops]
    need(names[0] == "parquet-scan" and ops[0]["output_rows"] == count
         and ops[-1]["output_rows"] == expected_rows, "native operator cardinality")
    worker = receipt["worker"]
    if function:
        invokes = (count + BUDGETS["batch_rows"] - 1) // BUDGETS["batch_rows"]
        need(names[1] == "map-batches" and len(names) >= 3 and names[2] in {"filter", "schema-adapter"}
             and ops[1]["input_rows"] == ops[1]["output_rows"] == count, "downstream native operator")
        need(worker["invocations"] == ops[1]["output_batches"] == invokes, "UDF invocation count")
        if invokes:
            need(all(positive_int(worker[key], invokes * BUDGETS["worker_frame_bytes"])
                     for key in ("input_bytes", "output_bytes")), "UDF encoded budget")
            need(positive_int(worker["worker_reported_peak_rss_bytes"]), "UDF RSS measurement")
        else:
            need(all(value == 0 for value in worker.values()), "empty worker counters")
    else:
        need(worker is None and "map-batches" not in names, "unexpected UDF invocation")
    if flight:
        result = receipt["result"]
        need(result["rows"] == expected_rows and hex_id(result["flight_digest"])
             and positive_int(result["flight_bytes"], BUDGETS["result_bytes"]), "Flight result receipt")
        retain = receipt["retention"]
        need(positive_int(retain["ticket_count"], BUDGETS["ticket_slots"])
             and result["flight_bytes"] <= retain["ticket_wire_bytes"] <=
             retain["ticket_count"] * BUDGETS["result_bytes"] and retain["spool_bytes"] == 0, "ticket peak bound")


def validate_request(name, value, digest):
    raw = base64.b64decode(value, validate=True)
    if name == "malformed_json":
        need(raw == b"{", "request: malformed")
    elif name == "invalid_utf8":
        need(raw == b'{"table":"\xff"}', "request: UTF-8")
    elif name == "oversized":
        need(len(raw) == BUDGETS["request_bytes"] + 1 and json.loads(raw) == plan(name, digest), "request: size")
    elif name == "duplicate":
        need(raw.startswith(b'{"version":1,') and raw.count(b'"version"') == 2 and
             json.loads(raw) == plan(name, digest), "request: duplicate")
    else:
        expected = plan(name, digest)
        if name in {"plan", "udf"} and "function" not in expected:
            expected = expected | {"function": None}  # SDK explicitly serializes None.
        need(json.loads(raw) == expected, f"request: {name}")


def validate_command(command, expected):
    need(command["status"] == expected and positive_int(command["wall_ns"]), "command exit/timing")


def validate_benchmarks(root, source):
    runs = read(root / "benchmark.json")
    need(set(runs) == {"native_plan", "native_udf", "python_sql", "java_sql"}, "benchmark paths")
    for name, run in runs.items():
        samples = run["samples"]
        need(len(samples) == CRITERIA["iterations"], "benchmark sample count")
        if "command" in run:
            validate_command(run["command"], 0)
            need(json.loads(run["command"]["stdout"]) == samples, "benchmark command sample attribution")
            need(sum(s["wall_ns"] for s in samples) <= run["command"]["wall_ns"], "benchmark command timing")
            need(positive_int(run["user_cpu_ns"]) and run["system_cpu_ns"] >= 0, "benchmark CPU")
        for sample in samples:
            need(positive_int(sample["wall_ns"], 2 * BUDGETS["query_ms"] * 1_000_000), "benchmark wall timing")
            if name.startswith("native_"):
                case = name.removeprefix("native_")
                validate_receipt(sample, case, source, flight=False)
                need(sample["native_wall_ns"] <= sample["wall_ns"], "benchmark native timing")
            elif name == "python_sql":
                validate_receipt(sample["receipt"], "sql", source)
                need(positive_int(sample["client_cpu_ns"]), "benchmark client CPU")
            else:
                need({k: sample["observed"][k] for k in ("rows", "schema")} == oracle("sql"), "benchmark Java rows")
                validate_receipt(sample["observed"]["receipt"], "sql", source)
        if name.startswith("native_"):
            arrow_result(root / (name.replace("_", "-", 1).replace("native-", "benchmark-native-") + ".arrow"),
                         oracle(name.removeprefix("native_")))


def check_archive(root):
    manifest_hashes = {}
    for name in ("main", "empty", "single", "boundary", "overflow"):
        src = root / f"source-{name}"
        need({name: sha(src / name) for name in LOCK[name]} == LOCK[name], f"fixture bytes: {name}")
        manifest_hashes[name] = hash_bytes((src / "manifest.json").read_bytes())
    env = read(root / "environment.json")
    servers = [read(path) for path in sorted(root.glob("server-*.json"))]
    need(len(servers) >= 20 and all(
        server["gateway_sha256"] == env["binary_sha256"]
        and server["socket"].startswith("127.0.0.1:")
        and positive_int(server["process_usage"]["rss_peak_bytes"])
        and server["exit"] == -9
        for server in servers
    ), "service binary/usage attribution")
    cases = read(root / "cases.json")
    need(set(cases) == EXPECTED, "exact case set")
    for name in DATA_CASES:
        case = cases[name]
        expected = oracle(name)
        need(case["data"]["schema"] == expected["schema"] and case["schema"] == expected["schema"], "result schema")
        need(case["data"]["rows"] == expected["rows"], "result rows")
        need(case["arrow_sha256"] == sha(root / f"{name}.arrow"), "Arrow result digest")
        arrow_result(root / f"{name}.arrow", expected)
        source = manifest_hashes.get(name, manifest_hashes["main"])
        validate_receipt(case["receipt"], name, source)
        if name == "sql":
            need(case["request"] == SQL, "request: SQL")
        else:
            validate_request(name, case["request"], DIGEST)
        if name in ("sql", "plan", "udf", "boundary"):
            java = case["java"]
            validate_command(case["java_command"], 0)
            need(json.loads(case["java_command"]["stdout"]) == java, "Java command attribution")
            need({key: java[key] for key in ("schema", "rows")} == expected, "Java rows/schema")
            validate_receipt(java["receipt"], name, source)
            need(java["receipt"]["result"] == case["receipt"]["result"], "Flight cross-client digest")
    need(len({cases[n]["receipt"]["identity"]["plan"] for n in ("sql", "plan", "udf", "repeat")}) == 1,
         "shared PlanV1 digest")
    need(cases["repeat"]["repeat"] == {k: v for k, v in cases["repeat"].items() if k not in ("repeat", "request")},
         "repeat read identity")
    baseline_ticket = cases["repeat"]["ticket"]
    for name in ("cross_session", "stale", "restart"):
        need(cases[name]["ticket"] == baseline_ticket, "ticket ownership/restart/expiry witness")
    need(cases["cross_session"]["different_session"] is True, "ticket session witness")
    need(cases["stale"]["wait_ns"] >= BUDGETS["ticket_ttl_seconds"] * 10**9, "ticket expiry timing")
    original = base64.b64decode(baseline_ticket, validate=True)
    forged = base64.b64decode(cases["forged_ticket"]["ticket"], validate=True)
    need(cases["forged_ticket"]["original_ticket"] == baseline_ticket and
         forged == original[:-1] + bytes([original[-1] ^ 1]), "forged ticket")
    metadata = cases["metadata"]
    need(metadata["request"] == [0, 3, 10001] and metadata["data"] ==
         [[0, "ADE"], [3, True], [10001, "ade.gateway/QueryRequestV1;local;map_secondary/1"]], "Flight SQL metadata")
    validate_command(metadata["java_command"], 0)
    need(metadata["java"]["rows"] == [["0", "ADE"], ["3", "true"],
         ["10001", "ade.gateway/QueryRequestV1;local;map_secondary/1"]], "Java metadata")
    need(cases["schema"]["request"] == SQL and cases["schema"]["schema"] == oracle("sql")["schema"], "Flight SQL schema")
    validate_command(cases["schema"]["java_command"], 0)
    for name, (code, detail) in FAILURES.items():
        case = cases[name]
        need(case["code"] == code and case["returned_ticket"] is False and detail in case.get("message", ""), f"refusal status: {name}")
        need("PHASE22_REDACTION_SENTINEL" not in case.get("message", ""), "diagnostic redaction")
        if "java" in case:
            validate_command(case["java_command"], 2)
            need(case["java"] == {"code": code} == json.loads(case["java_command"]["stdout"]), f"Java status: {name}")
        if name in CRITERIA["cases"]["worker"] or name in ("disconnect", "concurrent"):
            digest = hash_bytes((root / f"worker-{name}.py").read_bytes())
            need(case["artifact"] == f"worker-{name}.py" and case["digest"] == digest, f"worker artifact: {name}")
            validate_request(name, case.get("held_request", case.get("request")), digest)
        elif name in CRITERIA["cases"]["admission"] and name not in (
            "unsupported_sql", "unsupported_write", "symlink_escape", "source_drift", "missing_auth"
        ):
            validate_request(name, case["request"], DIGEST)
        if name in ("hang", "cancel", "gateway_crash", "disconnect"):
            need(positive_int(case["pid"]) and case["exit_observed"] is True and
                 (not case["process_states"][-1] or case["process_states"][-1].startswith("Z")), f"worker exit: {name}")
    need(cases["unsupported_sql"]["request"] == "DROP TABLE input", "DDL witness")
    need(cases["unsupported_write"]["request"] == "CreatePreparedStatement", "action witness")
    need(cases["missing_auth"]["request"] == SQL, "auth witness")
    late = cases["wrong_rows"]["failed_after_first_batch"]
    need(late["worker"]["invocations"] == 2 and late["status"] == 9
         and late["reason"] == "UDF record shape/compression", "post-first-batch refusal")
    need(cases["disconnect"]["client_exit"] == -9 and not cases["disconnect"]["stdout"], "disconnect witness")
    need(read(root / "server-gateway_crash.json")["exit"] == -9, "crash witness")
    need(cases["artifact_drift"]["original_digest"] == DIGEST and
         hash_bytes((root / "changed.py").read_bytes()) == cases["artifact_drift"]["changed_digest"] != DIGEST,
         "artifact drift witness")
    outside = root / "outside.parquet"
    changed = root / "changed-source.parquet"
    baseline = (root / "source-main/fixture.parquet").read_bytes()
    expected = baseline[:16] + bytes([baseline[16] ^ 1]) + baseline[17:]
    need(changed.read_bytes() == expected and sha(changed) == cases["source_drift"]["changed_sha256"], "source drift witness")
    need(outside.read_bytes() == baseline and sha(outside) == cases["symlink_escape"]["outside_sha256"], "symlink witness")
    validate_command(cases["non_loopback"], 1)
    need("numeric loopback address required" in cases["non_loopback"]["stderr"]
         and not cases["non_loopback"]["stdout"], "non-loopback witness")
    receipts = cases["ticket_cap"]["receipts"]
    need([r["retention"]["ticket_count"] for r in receipts] == list(range(1, BUDGETS["ticket_slots"] + 1)),
         "ticket capacity reached")
    for receipt in receipts:
        validate_receipt(receipt, "sql", manifest_hashes["main"])
    need(cases["result_overflow"]["next_receipt"]["retention"]["ticket_count"] == 1, "failed publication left a ticket")
    full = cases["enospc"]
    need(full["attempts"][-1] == {"write_bytes": 512, "errno": 28}
         and all(a["errno"] == 28 for a in full["attempts"])
         and 0 <= full["available_blocks"] <= 1 and positive_int(full["fragment_bytes"])
         and full["data"] == oracle("sql") and "phase22-fill" in full["entries"], "ENOSPC witness")
    validate_receipt(full["receipt"], "sql", manifest_hashes["main"])
    validate_benchmarks(root, manifest_hashes["main"])


def verify(root, mode, pin, repo=None, binary=None, native=None):
    seal_path = root / "seal.json"
    need(artifact_inventory(root) == read(seal_path), "archive inventory")
    env = read(root / "environment.json")
    need(env["criteria_sha256"] == sha(HERE / "criteria.json") and
         env["fixture_lock_sha256"] == sha(HERE / "fixture-lock.json"), "criteria/fixture lock")
    if mode == "current-source":
        need(repo is not None and binary is not None and native is not None, "current-source needs repo and both executables")
        need(read(root / "inputs.json") == source_inventory(repo), "current-source runtime inventory")
        need(sha(binary) == env["binary_sha256"] and sha(native) == env["native_sha256"], "current-source executable digest")
        need(sha(repo / "clients/python/src/ade_client/__init__.py") == env["package_sha256"], "Python package digest")
    else:
        need(repo is None and binary is None and native is None, "historical mode does not take current source")
    check_archive(root)
    need(sha(seal_path) == pin, "independent receipt pin")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("current-source", "historical-archive"), required=True)
    parser.add_argument("--receipt-pin", required=True)
    parser.add_argument("--repo", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--native", type=Path)
    parser.add_argument("archive", type=Path)
    args = parser.parse_args()
    try:
        verify(args.archive, args.mode, args.receipt_pin, args.repo, args.binary, args.native)
    except (ValueError, KeyError, OSError, TypeError, IndexError) as exc:
        parser.exit(1, f"Phase 22 verification refused: {exc}\n")
    if args.mode == "current-source":
        print("Verified Phase 22 campaign semantics and matched current source/executables")
    else:
        print("Verified historical Phase 22 archive semantics; no current-source attribution")


if __name__ == "__main__":
    main()
