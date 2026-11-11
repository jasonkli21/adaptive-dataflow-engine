"""Frozen Phase 20 scenario expectations, independent of archived process claims."""
import hashlib
import json
import math


class ContractError(ValueError):
    pass


def standard_deviation_matches(claimed, expected):
    """Allow cross-platform float serialization noise without softening guards."""
    return math.isclose(claimed, expected, rel_tol=1e-12, abs_tol=1e-9)


def request_identity(data):
    """Hash request semantics while allowing fresh campaign parent directories."""
    if data is None:
        return None
    try:
        value = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return hashlib.sha256(data).hexdigest()
    if isinstance(value, dict):
        for field in ("source_dir", "root"):
            path = value.get(field)
            if isinstance(path, str):
                value[field] = "@" + field.upper() + "/" + path.rstrip("/").rsplit("/", 1)[-1]
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(canonical).hexdigest()


def _require(condition, name, detail):
    if not condition:
        raise ContractError(f"{name}: {detail}")


def validate_case(name, case, request_bytes, spec, terminal_bytes=None):
    """Check immutable scenario identity and expected disposition before semantics."""
    _require(name in spec["cases"], name, "unexpected case")
    expected = spec["cases"][name]
    _require(case.get("name") == name, name, "record name differs")
    _require(case.get("argv", [None, None])[1] == expected["operation"], name,
             "operation differs from frozen scenario")
    argv = case.get("argv", [])
    _require(len(argv) == 3 and argv[2].rsplit("/", 1)[-1] == name + ".json", name,
             "request command path differs from frozen case name")
    digest = hashlib.sha256(request_bytes).hexdigest() if request_bytes is not None else None
    identity = request_identity(request_bytes)
    _require(digest == expected["request_sha256"] or
             identity == expected["request_identity_sha256"], name,
             "request identity differs from frozen scenario")
    _require(case.get("exit") == expected["exit"], name,
             f"exit disposition differs: expected {expected['exit']}, got {case.get('exit')}")
    stderr = case.get("stderr", "")
    marker = expected["stderr_contains"]
    _require(marker is None or marker in stderr, name,
             f"required diagnostic missing: {marker!r}")
    output = case.get("stdout", "").encode()
    policy = expected["stdout_policy"]
    if policy == "empty":
        _require(not output, name, "unexpected stdout on refusal/interruption")
    elif policy == "terminal_prefix":
        _require(terminal_bytes is not None and terminal_bytes.startswith(output), name,
                 "partial output is not a prefix of independently derived terminal stream")
        _require(b'"kind":"complete"' not in output, name,
                 "incomplete command emitted a complete footer")
    elif policy != "semantic":
        raise ContractError(f"{name}: unknown frozen stdout policy {policy!r}")


def validate_inventory(cases, spec):
    actual = set(cases)
    expected = set(spec["cases"])
    missing, extra = sorted(expected - actual), sorted(actual - expected)
    _require(not missing and not extra, "case inventory",
             f"missing={missing[:8]} extra={extra[:8]}")


def validate_category_totals(cases, spec):
    counts = {}
    for name, case in cases.items():
        key = f"{spec['cases'][name]['operation']}:{case.get('exit')}"
        counts[key] = counts.get(key, 0) + 1
    _require(counts == spec["operation_exit_counts"], "case category totals",
             f"expected={spec['operation_exit_counts']} observed={counts}")


def validate_root_inventory(root_names, spec):
    _require(set(root_names) == set(spec["roots"]), "root inventory",
             f"expected={len(spec['roots'])} observed={len(set(root_names))}")


def request_bytes(path):
    if not path.exists():
        return None
    return path.read_bytes()


def decode_request(data):
    if data is None:
        return None
    try:
        return json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
