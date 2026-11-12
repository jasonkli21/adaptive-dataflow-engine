"""Predeclared case details shared by the campaign and independently checked offline."""

PLAN = {
    "version": 1, "target": "local", "table": "input", "timeout_ms": 10_000,
    "operations": [
        {"op": "filter", "column": "secondary", "less_than": "3"},
        {"op": "project", "columns": ["row_id", "secondary", "value_i64", "payload", "event_time"]},
    ],
}
FAILURES = {
    "malformed_json": ("INVALID_ARGUMENT", "invalid query request"),
    "oversized": ("RESOURCE_EXHAUSTED", "query request cap"),
    "unknown_version": ("UNIMPLEMENTED", "query version or target"),
    "duplicate": ("INVALID_ARGUMENT", "invalid query request"),
    "unknown_field": ("INVALID_ARGUMENT", "invalid query request"),
    "unsupported_target": ("UNIMPLEMENTED", "query version or target"),
    "unknown_table": ("NOT_FOUND", "unknown table"),
    "invalid_utf8": ("INVALID_ARGUMENT", "invalid query request"),
    "overflow": ("INVALID_ARGUMENT", "invalid query request"),
    "unsupported_sql": ("INVALID_ARGUMENT", "SQL admission refused"),
    "unsupported_write": ("UNIMPLEMENTED", "Flight SQL operation unsupported"),
    "wrong_abi": ("UNIMPLEMENTED", "function ABI"),
    "wrong_digest": ("FAILED_PRECONDITION", "function identity mismatch"),
    "unknown_function": ("NOT_FOUND", "unknown function"),
    "forged_path": ("INVALID_ARGUMENT", "invalid query request"),
    "artifact_drift": ("FAILED_PRECONDITION", "function artifact drift"),
    "symlink_escape": ("FAILED_PRECONDITION", "source path drift"),
    "source_drift": ("FAILED_PRECONDITION", "source verification failed"),
    "missing_auth": ("UNAUTHENTICATED", "gateway credential required"),
    "wrong_schema": ("FAILED_PRECONDITION", "UDF output schema"),
    "nulls": ("FAILED_PRECONDITION", "UDF record shape/compression"),
    "wrong_rows": ("FAILED_PRECONDITION", "UDF record shape/compression"),
    "wrong_order": ("FAILED_PRECONDITION", "UDF output rows/order/values/cap"),
    "wrong_batch": ("FAILED_PRECONDITION", "UDF frame identity/length"),
    "trailing": ("FAILED_PRECONDITION", "UDF extra IPC data"),
    "malformed": ("FAILED_PRECONDITION", "UDF invalid IPC envelope"),
    "oversized_encoded": ("RESOURCE_EXHAUSTED", "UDF encoded cap"),
    "oversized_decoded": ("FAILED_PRECONDITION", "UDF invalid IPC envelope"),
    "dictionary": ("FAILED_PRECONDITION", "UDF invalid IPC envelope"),
    "compression": ("FAILED_PRECONDITION", "UDF record shape/compression"),
    "stderr": ("FAILED_PRECONDITION", "UDF unexpected stderr"),
    "exit": ("FAILED_PRECONDITION", "UDF worker failure"),
    "hang": ("DEADLINE_EXCEEDED", "query deadline"),
    "cancel": ("DEADLINE_EXCEEDED", ""),
    "gateway_crash": ("UNAVAILABLE", ""),
    "forged_ticket": ("NOT_FOUND", "ticket unknown or expired"),
    "cross_session": ("NOT_FOUND", "ticket unknown or expired"),
    "stale": ("NOT_FOUND", "ticket unknown or expired"),
    "restart": ("NOT_FOUND", "ticket unknown or expired"),
    "ticket_cap": ("RESOURCE_EXHAUSTED", "ticket capacity"),
    "concurrent": ("RESOURCE_EXHAUSTED", "query busy"),
    "result_overflow": ("RESOURCE_EXHAUSTED", "native result cap"),
    "disconnect": ("DISCONNECTED", ""),
}


def function(digest, **overrides):
    return {"id": "map_secondary", "abi": 1, "digest": digest} | overrides


def plan(case, digest):
    if case in ("udf", "repeat", "wrong_schema", "nulls", "wrong_rows", "wrong_order", "wrong_batch",
                "trailing", "malformed", "oversized_encoded", "oversized_decoded", "dictionary",
                "compression", "stderr", "exit", "artifact_drift", "cancel", "disconnect",
                "concurrent", "gateway_crash"):
        return PLAN | {"function": function(digest)}
    if case == "hang":
        return PLAN | {"function": function(digest), "timeout_ms": 1800}
    if case in ("empty", "single", "boundary"):
        return PLAN | {"function": function(digest), "operations": [
            {"op": "filter", "column": "key", "less_than": "18446744073709551615"},
        ]} if case == "empty" else PLAN | {"function": function(digest), "operations": [
            {"op": "project", "columns": ["row_id", "key", "secondary", "value_i64",
                                          "value_f64", "category", "payload", "event_time"]},
        ]}
    mutations = {
        "unknown_version": {"version": 2},
        "unknown_field": {"replayable": True},
        "unsupported_target": {"target": "distributed_aggregate"},
        "unknown_table": {"table": "../private"},
        "overflow": {"operations": [{"op": "filter", "column": "secondary", "less_than": "18446744073709551616"}]},
        "wrong_abi": {"function": function(digest, abi=2)},
        "wrong_digest": {"function": function("0" * 64)},
        "unknown_function": {"function": function(digest, id="unknown")},
        "forged_path": {"function": function(digest, path="/bin/sh")},
    }
    return PLAN | mutations.get(case, {})
