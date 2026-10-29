#!/usr/bin/env python3
"""Verify portable Phase14 source/log evidence without extracting archive paths."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import pathlib
import tarfile

try:
    from .evidence_contracts import (
        ADMITTED_FAILURES,
        PRE_ADMISSION_FAILURES,
        EvidenceContractError,
        is_historical_missing_partition_false_witness,
        parse_trace,
        validate_backpressure,
        validate_case_contract,
    )
except ImportError:  # direct `python3 scripts/phase14/verify_evidence.py`
    from evidence_contracts import (
        ADMITTED_FAILURES,
        PRE_ADMISSION_FAILURES,
        EvidenceContractError,
        is_historical_missing_partition_false_witness,
        parse_trace,
        validate_backpressure,
        validate_case_contract,
    )

if not __debug__:
    raise SystemExit("Evidence checks require Python assertions enabled; do not use -O/PYTHONOPTIMIZE.")


def _load_archive(path: pathlib.Path, manifest_path: pathlib.Path) -> tuple[dict, dict[str, bytes]]:
    expected = json.loads(manifest_path.read_text())
    data = path.read_bytes()
    assert hashlib.sha256(data).hexdigest() == expected["archive_sha256"], "archive identity"
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        actual: dict[str, bytes] = {}
        for member in archive.getmembers():
            member_path = pathlib.PurePosixPath(member.name)
            assert not member_path.is_absolute() and ".." not in member_path.parts, "unsafe archive path"
            assert member.isfile(), "only regular evidence files admitted"
            assert member.name not in actual and member.size <= 16 * 1024 * 1024, "duplicate/large file"
            stream = archive.extractfile(member)
            assert stream is not None
            actual[member.name] = stream.read()
    assert set(actual) == set(expected["files"]), "archive coverage"
    for name, digest in expected["files"].items():
        assert hashlib.sha256(actual[name]).hexdigest() == digest, name
    return expected, actual


def _verify_source_snapshot(source_archive: bytes, inventory_bytes: bytes) -> dict[str, str]:
    inventory = json.loads(inventory_bytes)
    with tarfile.open(fileobj=io.BytesIO(source_archive), mode="r:gz") as source:
        members = {}
        for member in source.getmembers():
            p = pathlib.PurePosixPath(member.name)
            assert member.isfile() and not p.is_absolute() and ".." not in p.parts
            assert member.name not in members, "duplicate source member"
            members[member.name] = member
        assert set(members) == set(inventory), "source inventory coverage"
        for name, digest in inventory.items():
            stream = source.extractfile(members[name])
            assert stream is not None and hashlib.sha256(stream.read()).hexdigest() == digest, name
    return inventory


def _trace_and_output(actual: dict[str, bytes], prefix: str, label: str) -> tuple[bytes | None, bytes | None, dict | None]:
    config_raw = actual.get(f"{prefix}/{label}.config.json")
    config = json.loads(config_raw) if config_raw is not None else None
    trace_raw = actual.get(f"{prefix}/{label}.trace.jsonl")
    if trace_raw is None and label in {"config-fifo", "nested-unknown"}:
        trace_raw = actual.get(f"{prefix}/{label}.stderr")
    output = actual.get(f"{prefix}/{label}.output.jsonl")
    if output is None and label in {"config-fifo", "nested-unknown"}:
        output = actual.get(f"{prefix}/{label}.stdout")
    return trace_raw, output, config


def _validate_negative_cases(
    actual: dict[str, bytes], prefix: str, cases: list[dict], mode: str
) -> bool:
    by_label = {case["case"]: case for case in cases}
    negative_labels = sorted(set(PRE_ADMISSION_FAILURES) | set(ADMITTED_FAILURES) | {"kill"})
    historical_limit = False
    for label in negative_labels:
        trace_raw, output, config = _trace_and_output(actual, prefix, label)
        try:
            trace = validate_case_contract(label, by_label[label], config, trace_raw, output)
        except EvidenceContractError:
            if (
                mode == "historical"
                and label == "missing-partition"
                and is_historical_missing_partition_false_witness(config, trace_raw, output)
            ):
                historical_limit = True
                continue
            raise
        if label == "missing-partition" and mode == "historical":
            # Historical mode never promotes a historical archive to a fresh
            # all-gates PASS, even if a later archive happens to repair it.
            historical_limit = historical_limit or not trace
    return historical_limit


def _validate_positive_case(
    actual: dict[str, bytes], prefix: str, label: str
) -> list[dict]:
    config = json.loads(actual[f"{prefix}/{label}.config.json"])
    topic = config["topic"]
    starts = {entry["partition"]: entry["offset"] for entry in config["starts"]}
    ledger: dict[tuple[int, int], dict] = {}
    for name, contents in actual.items():
        suffix = f"-produce-{topic}.stdout"
        if name.startswith(f"{prefix}/") and name.endswith(suffix):
            for line in contents.decode().splitlines():
                partition, offset, payload = line.split("\t", 2)
                partition, offset = int(partition), int(offset)
                if offset >= starts[partition]:
                    ledger[partition, offset] = json.loads(payload)

    output_bytes = actual[f"{prefix}/{label}.output.jsonl"]
    assert output_bytes.endswith(b"\n"), f"{label}: successful output must end on a complete frame"
    rows = [json.loads(line) for line in output_bytes.decode().splitlines()]
    assert len(rows) == len(ledger) and ledger, f"{label}: independent producer coverage"
    trace = parse_trace(actual.get(f"{prefix}/{label}.trace.jsonl"), label)
    forwarded = [event for event in trace if event.get("type") == "forwarded"]
    assert len(forwarded) == len(rows), f"{label}: forwarded trace count"

    last: dict[int, int] = {}
    maxima: dict[int, int] = {}
    known: dict[int, int] = {}
    unique: set[tuple[int, int]] = set()
    timer_ids = []
    published = None
    for event in trace:
        if "progress" in event:
            published = event["progress"].get("frontier")
        if event.get("type") == "timer":
            timer = event["timer"]
            timer_ids.append(timer["id"])
            if timer["kind"] == "processing":
                assert event["processing_ms"] >= timer["deadline_ms"]
            if timer["kind"] == "event":
                assert published is not None and published >= timer["deadline_ms"]
    assert len(timer_ids) == len(set(timer_ids)) and timer_ids[:2] == [0, 1]

    for index, (row, forwarded_event) in enumerate(zip(rows, forwarded)):
        key = (row["partition"], row["offset"])
        assert key in ledger and key not in unique, f"{label}: producer ledger identity"
        unique.add(key)
        partition, offset = key
        event = ledger[key]
        assert offset > last.get(partition, -1), f"{label}: per-partition order"
        assert row["event_time_ms"] == event["event_time_ms"]
        assert row["late"] == (partition in known and event["event_time_ms"] < known[partition])
        expected_projection = (
            {field: event[field] for field in ("value_i64", "key", "event_time_ms")}
            if event["key"] < config["key_less_than"]
            else None
        )
        assert row["projected"] == expected_projection, f"{label}: independent native filter/projection oracle"

        progress = forwarded_event["progress"]
        cursors = progress["partitions"]
        changed = [
            int(part)
            for part, cursor in cursors.items()
            if cursor["forwarded"] != last.get(int(part))
        ]
        assert changed == [partition], f"{label}: receipt does not match the acknowledged partition cursor"
        assert cursors[str(partition)]["forwarded"] == offset, f"{label}: receipt offset differs from forwarded cursor"
        last[partition] = offset
        maxima[partition] = max(maxima.get(partition, event["event_time_ms"]), event["event_time_ms"])
        known[partition] = maxima[partition] - config["allowance_ms"]
        assert cursors[str(partition)]["watermark"] == known[partition]
        assert progress["frontier"] == (min(known.values()) if len(known) == len(starts) else None)

    assert len(unique) == len(ledger), f"{label}: output conservation"
    terminals = [event for event in trace if event.get("type") == "terminal"]
    assert len(terminals) == 1 and terminals[0]["status"] == "STOPPED"
    terminal = terminals[0]
    assert terminal["progress"]["forwarded_records"] == len(rows)
    assert terminal["managed_in_flight_peak_bytes"] <= 16384
    return rows


def verify(path: pathlib.Path, manifest: pathlib.Path, mode: str = "current") -> dict:
    if mode not in {"current", "historical"}:
        raise ValueError("mode must be 'current' or 'historical'")
    expected, actual = _load_archive(path, manifest)
    result_path = expected["acceptance_result"]
    result = json.loads(actual[result_path])
    assert result["status"] == "PASS" and all(case["status"] == "PASS" for case in result["cases"])
    assert len({case["pid"] for case in result["cases"]}) == len(result["cases"]), "fresh reader processes"
    source_prefix = result_path.rsplit("/", 1)[0]
    inventory_bytes = actual[f"{source_prefix}/source-inventory.json"]
    source_archive = actual[f"{source_prefix}/source.tar.gz"]
    inventory = _verify_source_snapshot(source_archive, inventory_bytes)
    if "source_inventory_sha256" in result:
        assert hashlib.sha256(inventory_bytes).hexdigest() == result["source_inventory_sha256"], "source inventory result binding"
    if "source_archive_sha256" in result:
        assert hashlib.sha256(source_archive).hexdigest() == result["source_archive_sha256"], "source archive result binding"

    labels = {case["case"] for case in result["cases"]}
    required = {
        "native", "middle-replay", "legal-gap", "quiet", "sasl-ssl", "bad-credentials", "backpressure",
        "cancel-poll", "cancel-stall", "epipe", "kill", "kill-replay", "bad-schema", "oversize", "underflow",
        "out-of-range", "missing-topic", "missing-partition", "wrong-cluster", "remote-plaintext", "timer-cap",
        "tls-refusal", "compression-refusal", "retention-drift", "partition-drift", "compression-drift",
        "broker-outage", "broker-restart-explicit", "config-fifo", "nested-unknown",
    }
    assert required <= labels, "mandatory independent case coverage"
    positive_labels = (
        "native", "middle-replay", "sasl-ssl", "legal-gap", "quiet", "backpressure", "kill-replay",
        "broker-restart-explicit",
    )
    for case in result["cases"]:
        expected_exit = -9 if case["case"] == "kill" else (0 if case["case"] in positive_labels else 1)
        assert case["exit"] == expected_exit, "exact child exit"

    case_by_label = {case["case"]: case for case in result["cases"]}
    for label in positive_labels:
        rows = _validate_positive_case(actual, source_prefix, label)
        if label == "backpressure":
            trace = parse_trace(actual[f"{source_prefix}/{label}.trace.jsonl"], label)
            validate_backpressure(case_by_label[label], trace, actual[f"{source_prefix}/{label}.output.jsonl"])

    historical_limit = _validate_negative_cases(actual, source_prefix, result["cases"], mode)
    if mode == "current":
        assert not historical_limit
        status = "PASS"
        qualification = "strict current negative contracts and positive oracle validated"
    else:
        status = "PASS_HISTORICAL_WITH_F14_01_LIMIT"
        qualification = (
            "historical integrity and positive oracle replay only; the archived missing-partition case "
            "is not a source-partition witness"
        )
    return {
        "status": status,
        "qualification": qualification,
        "cases": len(result["cases"]),
        "files": len(actual),
        "source_files": len(inventory),
        "source_inventory_sha256": hashlib.sha256(inventory_bytes).hexdigest(),
        "source_archive_sha256": hashlib.sha256(source_archive).hexdigest(),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", type=pathlib.Path, default=pathlib.Path("docs/evidence/phase14/acceptance.tar.gz"))
    parser.add_argument("--manifest", type=pathlib.Path, default=pathlib.Path("docs/evidence/phase14/manifest.json"))
    parser.add_argument("--mode", choices=("current", "historical"), default="current")
    args = parser.parse_args()
    print(json.dumps(verify(args.archive, args.manifest, mode=args.mode), sort_keys=True))
