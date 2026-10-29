"""Independent semantic contracts for the Phase 14 acceptance evidence.

These checks consume captured configs, traces and output. They do not call ADE
runtime code or use its parser/transform as an expected-value oracle.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any


class EvidenceContractError(ValueError):
    """Captured evidence does not satisfy its declared case contract."""


def _require(condition: bool, label: str, detail: str) -> None:
    if not condition:
        raise EvidenceContractError(f"{label}: {detail}")


def build_descriptor(
    topic: str,
    bootstrap: str,
    cluster_id: str,
    *,
    offsets: Sequence[int] = (0, 0),
    explicit_starts: Sequence[Mapping[str, int]] | None = None,
    **changes: Any,
) -> dict[str, Any]:
    """Build a strict descriptor with scalar offsets or explicitly typed starts.

    `offsets` is only a positional vector. `explicit_starts` is only a sequence
    of `{partition, offset}` objects. Keeping these inputs separate prevents a
    descriptor object from accidentally being nested as an offset scalar.
    """
    if explicit_starts is not None and tuple(offsets) != (0, 0):
        raise ValueError("choose offsets or explicit_starts, not both")
    if explicit_starts is None:
        if not offsets or any(type(offset) is not int or offset < 0 for offset in offsets):
            raise ValueError("offsets must be a nonempty sequence of nonnegative integers")
        starts = [dict(partition=partition, offset=offset) for partition, offset in enumerate(offsets)]
    else:
        starts = []
        for start in explicit_starts:
            if set(start) != {"partition", "offset"}:
                raise ValueError("each explicit start must contain partition and offset")
            partition, offset = start["partition"], start["offset"]
            if type(partition) is not int or partition < 0 or type(offset) is not int or offset < 0:
                raise ValueError("explicit partition and offset values must be nonnegative integers")
            starts.append({"partition": partition, "offset": offset})

    descriptor: dict[str, Any] = dict(
        version=1,
        bootstrap=bootstrap,
        expected_cluster_id=cluster_id,
        topic=topic,
        starts=starts,
        security=dict(mode="loopback_dev"),
        key_less_than=10,
        allowance_ms=10,
        payload_bytes=4096,
        client_queue_kib=16,
        sink_stall_ms=90000,
        timers=[
            dict(kind="processing", deadline_ms=0),
            dict(kind="processing", deadline_ms=0),
            dict(kind="event", deadline_ms=0),
            dict(kind="processing", deadline_ms=1000),
        ],
    )
    descriptor.update(changes)
    return descriptor


PRE_ADMISSION_FAILURES: dict[str, tuple[int, str, str]] = {
    "bad-credentials": (1, "broker", "broker failure"),
    "out-of-range": (1, "source_identity_or_offset", "offset range"),
    "missing-topic": (1, "source_identity_or_offset", "topic requires immutable uncompressed"),
    "missing-partition": (1, "source_identity_or_offset", "assigned partition missing"),
    "wrong-cluster": (1, "source_identity_or_offset", "cluster ID changed"),
    "remote-plaintext": (1, "admission", "plaintext only explicit loopback development"),
    "timer-cap": (1, "timer_capacity_or_identity", "registration cap/deadline"),
    "tls-refusal": (1, "admission", "CA regular file"),
    "compression-refusal": (1, "source_identity_or_offset", "topic requires immutable uncompressed"),
    "config-fifo": (1, "admission", "config must be regular file"),
    "nested-unknown": (1, "admission", "strict config JSON"),
}

ADMITTED_FAILURES: dict[str, tuple[int, str, str]] = {
    "bad-schema": (1, "event_schema_or_size", "invalid event"),
    "oversize": (1, "event_schema_or_size", "invalid event"),
    "underflow": (1, "arithmetic_overflow", "watermark"),
    "cancel-poll": (1, "cancelled", "cancelled"),
    "cancel-stall": (1, "cancelled", "cancelled"),
    "epipe": (1, "sink_or_diagnostics", "sink failure"),
    "retention-drift": (1, "source_identity_or_offset", "offset range"),
    "partition-drift": (1, "source_identity_or_offset", "partition inventory"),
    "compression-drift": (1, "source_identity_or_offset", "topic requires immutable uncompressed"),
    "broker-outage": (1, "source_identity_or_offset", "unexpected main-queue data/error"),
}

POISON_FIRST_RECORD_CASES = {"bad-schema", "oversize", "underflow"}
BLOCKED_FAILURE_CASES = {"cancel-stall", "epipe", "retention-drift", "partition-drift", "compression-drift"}


def parse_jsonl(raw: bytes, label: str) -> tuple[list[dict[str, Any]], bytes]:
    """Parse complete JSONL frames, retaining any final partial frame bytes."""
    if not raw:
        return [], b""
    lines = raw.splitlines(keepends=True)
    rows: list[dict[str, Any]] = []
    trailing = b""
    for index, line in enumerate(lines):
        if not line.endswith(b"\n"):
            _require(index == len(lines) - 1, label, "non-final output frame lacks newline")
            trailing = line
            continue
        try:
            value = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EvidenceContractError(f"{label}: invalid complete JSONL output frame") from exc
        _require(isinstance(value, dict), label, "output frame is not an object")
        rows.append(value)
    return rows, trailing


def parse_trace(raw: bytes | None, label: str) -> list[dict[str, Any]]:
    if raw is None:
        raise EvidenceContractError(f"{label}: required trace is missing")
    try:
        events = [json.loads(line) for line in raw.splitlines() if line.strip()]
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceContractError(f"{label}: trace is not valid JSONL") from exc
    _require(all(isinstance(event, dict) for event in events), label, "trace event is not an object")
    return events


def _validate_case_identity(label: str, case: Mapping[str, Any], expected_exit: int) -> None:
    _require(case.get("case") == label, label, "result case label mismatch")
    _require(case.get("status") == "PASS", label, "live case did not satisfy its harness assertions")
    _require(case.get("exit") == expected_exit, label, "unexpected child exit code")
    _require(type(case.get("pid")) is int and case["pid"] > 0, label, "missing child process identity")


def _validate_no_output(label: str, output: bytes | None) -> None:
    _require(output == b"", label, "case must have an empty output file")


def _validate_failure_event(
    label: str,
    trace: Sequence[Mapping[str, Any]],
    expected_class: str,
    reason_fragment: str,
) -> Mapping[str, Any]:
    failures = [event for event in trace if event.get("type") == "failure"]
    _require(len(failures) == 1, label, "required failure trace is missing or duplicated")
    failure = failures[0]
    _require(failure.get("failure_class") == expected_class, label, "failure trace reached the wrong failure class")
    _require(reason_fragment in str(failure.get("reason", "")), label, "failure trace reached the wrong reason")
    return failure


def _validate_pre_admission(
    label: str,
    case: Mapping[str, Any],
    config: Mapping[str, Any] | None,
    trace: Sequence[Mapping[str, Any]],
    output: bytes | None,
) -> None:
    expected_exit, expected_class, reason_fragment = PRE_ADMISSION_FAILURES[label]
    _validate_case_identity(label, case, expected_exit)
    _require(not any(event.get("type") == "admitted" for event in trace), label, "failure occurred after admission")
    _require(not any(event.get("type") == "terminal" for event in trace), label, "pre-admission failure emitted a terminal session event")
    _require([event.get("type") for event in trace] == ["failure"], label, "unexpected pre-admission trace events")
    _validate_failure_event(label, trace, expected_class, reason_fragment)
    _validate_no_output(label, output)

    if label == "missing-partition":
        _require(config is not None, label, "descriptor is missing")
        starts = config.get("starts")
        _require(
            starts == [{"partition": 3, "offset": 0}]
            and type(starts[0]["partition"]) is int
            and type(starts[0]["offset"]) is int,
            label,
            "expected one correctly typed explicit start for absent partition 3",
        )


def _validate_admitted_failure(
    label: str,
    case: Mapping[str, Any],
    trace: Sequence[Mapping[str, Any]],
    output: bytes | None,
) -> None:
    expected_exit, expected_class, reason_fragment = ADMITTED_FAILURES[label]
    _validate_case_identity(label, case, expected_exit)
    admitted = [event for event in trace if event.get("type") == "admitted"]
    terminals = [event for event in trace if event.get("type") == "terminal"]
    _require(len(admitted) == 1, label, "expected one admitted session")
    _require(len(terminals) == 1, label, "admitted failure must end with one terminal event")
    terminal = terminals[0]
    _require(terminal.get("status") == "FAILED", label, "admitted negative case claimed a successful or stopped terminal status")
    _require(terminal.get("failure_class") == expected_class, label, "terminal reached the wrong failure class")
    _require(reason_fragment in str(terminal.get("reason", "")), label, "terminal reached the wrong failure reason")
    failure = _validate_failure_event(label, trace, expected_class, reason_fragment)
    _require(
        trace.index(admitted[0]) < trace.index(terminal) < trace.index(failure),
        label,
        "admitted failure trace order is invalid",
    )
    _require(not any(event.get("status") == "STOPPED" for event in trace), label, "failure also claims a successful stop")

    forwarded = [event for event in trace if event.get("type") == "forwarded"]
    progress = terminal.get("progress")
    _require(isinstance(progress, Mapping), label, "terminal progress is missing")
    _require(progress.get("forwarded_records") == len(forwarded), label, "terminal forwarded count disagrees with trace")

    if label in POISON_FIRST_RECORD_CASES:
        _validate_no_output(label, output)
        _require(not forwarded, label, "poison first record advanced the forwarded trace")
        _require(progress.get("forwarded_records") == 0, label, "poison first record advanced forwarded record count")
        cursor = progress.get("partitions", {}).get("0", {})
        start = cursor.get("start")
        _require(cursor.get("seen") == start, label, "poison first record advanced the seen cursor")
        _require(cursor.get("forwarded") is None, label, "poison first record advanced the forwarded cursor")
        _require(cursor.get("next_offset_suggestion") == start, label, "poison first record advanced its replay suggestion")
    elif label == "cancel-poll":
        _validate_no_output(label, output)
        _require(progress.get("forwarded_records") == 0, label, "idle cancellation advanced progress")
    elif label in BLOCKED_FAILURE_CASES:
        blocks = [event for event in trace if event.get("type") == "sink_blocked"]
        _require(bool(blocks), label, "blocked-sink failure has no blocked snapshot")
        _require(all(event.get("progress") == blocks[0].get("progress") for event in blocks), label, "forwarded cursors changed while sink demand was blocked")
        _require(terminal.get("discarded_in_flight") is True, label, "failed blocked write did not report discarded in-flight input")

    if output is not None:
        rows, partial = parse_jsonl(output, label)
        _require(len(rows) >= len(forwarded), label, "captured output omits already acknowledged receipts")
        _require(len(rows) - len(forwarded) <= 1, label, "captured output exceeds the single in-flight receipt envelope")
        _require(len(partial) <= 2048, label, "partial output exceeds the bounded sink frame")
    elif label in POISON_FIRST_RECORD_CASES or label == "cancel-poll":
        _validate_no_output(label, output)


def _validate_kill(
    case: Mapping[str, Any], trace: Sequence[Mapping[str, Any]], output: bytes | None
) -> None:
    label = "kill"
    _validate_case_identity(label, case, -9)
    _require(sum(event.get("type") == "admitted" for event in trace) == 1, label, "kill has no admitted session")
    _require(not any(event.get("type") in {"terminal", "failure"} for event in trace), label, "SIGKILL has a fabricated terminal trace")
    blocks = [event for event in trace if event.get("type") == "sink_blocked"]
    _require(bool(blocks), label, "SIGKILL case did not reach blocked sink")
    _require(all(event.get("progress") == blocks[0].get("progress") for event in blocks), label, "cursors advanced while killed reader was blocked")
    _require(output is not None, label, "partial pipe output is missing")
    rows, partial = parse_jsonl(output or b"", label)
    _require(bool(rows), label, "expected truthful output prefix before SIGKILL")
    _require(len(partial) <= 2048, label, "partial output exceeds the bounded sink frame")
    progress = blocks[-1].get("progress", {})
    _require(progress.get("forwarded_records", 0) <= len(rows), label, "captured prefix omits acknowledged receipts")
    _require(len(rows) - progress.get("forwarded_records", 0) <= 1, label, "captured output exceeds one pending receipt")


def validate_case_contract(
    label: str,
    case: Mapping[str, Any],
    config: Mapping[str, Any] | None,
    trace_raw: bytes | None,
    output: bytes | None,
) -> list[dict[str, Any]]:
    """Validate a named negative case and return its parsed trace events."""
    trace = parse_trace(trace_raw, label)
    if label in PRE_ADMISSION_FAILURES:
        _validate_pre_admission(label, case, config, trace, output)
    elif label in ADMITTED_FAILURES:
        _validate_admitted_failure(label, case, trace, output)
    elif label == "kill":
        _validate_kill(case, trace, output)
    else:
        raise EvidenceContractError(f"{label}: no declared negative evidence contract")
    return [dict(event) for event in trace]


def is_historical_missing_partition_false_witness(
    config: Mapping[str, Any] | None,
    trace_raw: bytes | None,
    output: bytes | None,
) -> bool:
    """Recognize only the exact old parser-refusal witness for qualified replay."""
    try:
        trace = parse_trace(trace_raw, "missing-partition")
    except EvidenceContractError:
        return False
    starts = config.get("starts") if config is not None else None
    return (
        starts == [{"partition": 0, "offset": {"partition": 3, "offset": 0}}]
        and [event.get("type") for event in trace] == ["failure"]
        and trace[0].get("failure_class") == "admission"
        and "strict config JSON" in str(trace[0].get("reason", ""))
        and output == b""
    )


def validate_backpressure(
    case: Mapping[str, Any],
    trace: Sequence[Mapping[str, Any]],
    output: bytes | None,
) -> list[dict[str, Any]]:
    """Validate raw blocked snapshots and the complete resumed output boundary."""
    label = "backpressure"
    _validate_case_identity(label, case, 0)
    blocks = [event for event in trace if event.get("type") == "sink_blocked"]
    _require(len(blocks) >= 25, label, "fewer than 25 blocked snapshots")
    frozen = blocks[0].get("progress")
    _require(isinstance(frozen, Mapping), label, "blocked snapshot progress is missing")
    _require(all(event.get("progress") == frozen for event in blocks), label, "forwarded frontier or cursor changed while blocked")

    first_ms, last_ms = blocks[0].get("elapsed_ms"), blocks[-1].get("elapsed_ms")
    _require(type(first_ms) is int and type(last_ms) is int and last_ms - first_ms >= 30000, label, "raw blocked interval is shorter than 30 seconds")
    _require(type(case.get("blocked_seconds")) in {int, float} and case["blocked_seconds"] >= 30, label, "reported blocked interval is shorter than 30 seconds")

    last_backlogs = [event.get("source", {}).get("backlog", {}) for event in blocks[-5:]]
    _require(all(int(backlog.get("0", 0)) > 500 and int(backlog.get("1", 0)) > 500 for backlog in last_backlogs), label, "broker backlog did not remain above 500 records per partition")
    peaks = [event.get("source", {}).get("queue_bytes_peak") for event in blocks[-10:]]
    _require(all(type(peak) is int for peak in peaks) and len(set(peaks)) == 1, label, "last ten client queue snapshots did not plateau")
    queue_peak = peaks[0]
    source = blocks[-1].get("source", {})
    cap = 4 * (int(source.get("queue_target_bytes", 0)) + int(source.get("response_cap_bytes", 0)))
    _require(cap > 0 and queue_peak <= cap, label, "client queue exceeded the documented soft bound")

    terminal_events = [event for event in trace if event.get("type") == "terminal"]
    _require(len(terminal_events) == 1 and terminal_events[0].get("status") == "STOPPED", label, "resumed process did not stop at its declared operator limit")
    terminal = terminal_events[0]
    rows, partial = parse_jsonl(output if output is not None else b"", label)
    _require(not partial, label, "successful resumed output ends in a partial frame")
    forwarded = [event for event in trace if event.get("type") == "forwarded"]
    _require(len(rows) == 1600, label, "resumed output did not conserve all 1600 expected receipts")
    _require(terminal.get("progress", {}).get("forwarded_records") == len(rows) == len(forwarded), label, "output, forwarded trace and terminal counts disagree")
    metrics = terminal.get("metrics", {})
    _require(metrics.get("in_flight_peak_records", 99) <= 1, label, "in-flight record limit exceeded")
    _require(metrics.get("sink_frame_peak_bytes", 2049) <= 2048, label, "sink frame limit exceeded")
    _require(terminal.get("managed_in_flight_peak_bytes", 16385) <= 16384, label, "managed in-flight byte budget exceeded")
    return rows
