"""Validate the sealed process records against trusted inputs and original Parquet."""

import argparse
import datetime
import hashlib
from pathlib import Path
import platform
import statistics
import subprocess

from campaign import FAILURES, PROFILE_CASES
from common import (
    ARMS, CRITERIA, ROOT, SPEC, audit, child_fields, decode, encode, read,
    sampled_runs, schedule, sha, source_inventory, spec_text,
)
from oracle import blake_file, verify_fixtures


def require(condition, message):
    if not condition:
        raise ValueError(message)


def positive(value):
    return type(value) is int and 0 < value < 10**12


def check_seal(bundle, pin):
    require(sha(bundle / "seal.json") == pin, "external seal mismatch")
    expected = {p.name: sha(p) for p in bundle.iterdir() if p.name != "seal.json"}
    require(read(bundle / "seal.json") == expected, "archive inventory mismatch")


def verify_profile(bundle, provenance, rows, criteria, fixtures, bench):
    profile = read(bundle / "profile-current.json")
    shape = criteria["calibration_shape"]
    selected = {
        name: sampled_runs(rows, shape, arm)
        for name, arm in [("scalar", "scalar"), ("arrow", "arrow"), ("neon", "force-neon")]
    }
    require(profile["sample_ids"] == [1, 2, 3, 4, 5, 6, 7], "profile sample coverage")
    require(profile["calibration_sha256"] == hashlib.sha256(encode(selected).encode()).hexdigest(),
            "profile calibration source")
    for name, samples in selected.items():
        for field in ("filter_ns", "query_ns", "process_ns", "rss_bytes"):
            require(profile[name][field] == [child_fields(run)[field] for run in samples],
                    "profile costs disagree with raw observations")
    distribution, size, threshold = criteria["macro_shapes"][shape]
    require(profile["shape"] == {"rows": size, "nulls": 0, "threshold": threshold}
            and profile["full_batches"] == criteria["dataset_rows"] // size, "profile shape")
    config = read(fixtures / distribution / "manifest.json")
    from blake3 import blake3
    workload = [1, "scan-filter-project", 1, {"required_schema": config["required_schema"]},
                {"kind": "scan_filter_project", "threshold": threshold},
                {"kind": "canonical_integer_rows_v1"}]
    require(profile["binding"] == {
        "executable_blake3": blake_file(bench),
        "cpu_class": provenance["host"]["cpu_class"],
        "os_release": provenance["host"]["os_release"],
        "target": provenance["build"]["build"]["target"],
        "dataset": blake3(encode(config).encode()).hexdigest(),
        "workload": blake3(encode(workload).encode()).hexdigest(),
        "batch_size": size, "memory_limit_bytes": 256 * 1024 * 1024,
        "output_batch_rows": 1024, "spill_enabled": True, "prefetch_batches": 0,
    }, "profile executable, CPU, workload or fixture binding")
    require(profile["version"] == 1 and profile["mechanism"] == "arrow-59.3.1-u64-lt-neon-v1",
            "profile version")
    return profile


def check_profile_variant(path, variant, profile):
    current = decode(encode(profile))
    if variant == "duplicate":
        require(path.read_text() == encode(current).replace(
            '{"version":1,', '{"version":1,"version":1,', 1), "missing duplicate profile key")
        return
    if variant == "binary":
        current["binding"]["executable_blake3"] = "a" * 64
    elif variant == "version":
        current["version"] = 2
    elif variant == "expired":
        current["created_unix_s"] = 1
    elif variant == "shape":
        current["shape"]["threshold"] += 1
    elif variant == "cost":
        current["neon"]["query_ns"][0] = 2**64 - 1
    require(read(path) == current, "incorrect negative profile input")


def profile_expectation(profile, prediction, setup_ns):
    scalar, arrow, neon = [profile[arm] for arm in ("scalar", "arrow", "neon")]
    pairs = [(ctrl, i) for ctrl in (scalar, arrow) for i in range(7)]
    gains = [(ctrl["query_ns"][i] - neon["query_ns"][i]) * 10000 / ctrl["query_ns"][i]
             for ctrl, i in pairs]
    process_headroom = min(max(0, ctrl["process_ns"][i] * 105 // 100 - neon["process_ns"][i])
                           for ctrl, i in pairs)
    require(prediction == {
        **{f"{arm}_filter_ns": int(statistics.median(profile[arm]["filter_ns"]))
           // profile["full_batches"] for arm in ("scalar", "arrow", "neon")},
        "shape": profile["shape"], "pairs": 7,
        "min_paired_query_gain_basis_points": int(min(gains)),
        "process_headroom_ns": process_headroom,
        "admission_ns": prediction["admission_ns"],
    } and positive(prediction["admission_ns"]) and prediction["admission_ns"] <= setup_ns,
            "prediction or confidence disagrees with raw observations")
    gained = min(gains) >= 500 and prediction["admission_ns"] <= process_headroom and all(
        neon["filter_ns"][i] * 100 <= ctrl["filter_ns"][i] * 95
        and neon["process_ns"][i] * 100 <= ctrl["process_ns"][i] * 105
        and neon["rss_bytes"][i] * 100 <= ctrl["rss_bytes"][i] * 105 for ctrl, i in pairs)
    return ("neon", "measured-gain") if gained else ("scalar", "insufficient-gain")


def check_success(run, row_oracle, size, rows, neon, requested, expected_reason,
                  expected_kernel, profile=None):
    require(run["exit"] == 0, "unexpected child refusal")
    result = decode(run["stdout"])
    require(set(result) == {
        "schema_version", "validated", "engine", "setup_time_ns", "query_time_ns",
        "rows_output", "result_fingerprint",
    } and result["schema_version"] == 1 and result["validated"] is True, "legacy response schema")
    require((result["rows_output"], result["result_fingerprint"]) == row_oracle,
            "result differs from direct Parquet oracle")
    require(positive(result["setup_time_ns"]) and positive(result["query_time_ns"])
            and result["setup_time_ns"] + result["query_time_ns"] <= run["process_ns"],
            "inconsistent query/process timings")
    engine = result["engine"]
    config = {
        "batch_size": str(size), "memory_limit_bytes": str(256 * 1024 * 1024),
        "spill_enabled": "true", "prefetch_batches": "0",
    }
    if requested != "legacy":
        config["kernel_policy"] = requested
    if run["profile"]:
        config["kernel_profile_blake3"] = run["profile"]
    require(engine == {"name": "ade-local", "version": "0.1.0", "configuration": config},
            "child requested kernel provenance")
    if requested == "legacy":
        require("phase25-filter-v1 " not in run["stderr"], "legacy dispatch unexpectedly enabled")
        return
    report = audit(run)
    details = report["audit"]
    require(details["requested"] == requested and details["neon_available"] is neon,
            "inconsistent runtime hardware capability or requested mode")
    if profile:
        require(details["prediction"] is not None, "compatible profile was not evaluated")
        expected_kernel, expected_reason = profile_expectation(
            profile, details["prediction"], result["setup_time_ns"])
    else:
        require(details["prediction"] is None, "unexpected calibration prediction")
    require(report["profile_blake3"] == run["profile"], "profile digest trace")
    observed = details["observations"]
    require(list(observed) == [expected_reason], "incorrect kernel decision or fallback")
    counts = observed[expected_reason]
    require(counts["kernel"] == expected_kernel, "incorrect effective kernel")
    require(counts["simd_vectors"] == (rows // 2 if expected_kernel == "neon" else 0),
            "incorrect executed vector count")
    require(counts["input_rows"] == rows and counts["batches"] == rows // size
            and counts["output_rows"] == row_oracle[0] and counts["null_rows"] == 0
            and report["output_rows"] == row_oracle[0], "native row counts")
    scratch = size // 8 if expected_kernel == "neon" else 2 * ((size + 511) // 512 * 64) + 64
    require(counts["scratch_peak_bytes"] == scratch
            and scratch <= report["managed_peak_bytes"] <= report["managed_limit_bytes"]
            and report["managed_limit_bytes"] == 256 * 1024 * 1024
            and 0 < report["managed_retained_bytes"] <= report["managed_peak_bytes"],
            "managed scratch/retained accounting")
    require(positive(counts["mask_ns"]) and positive(counts["gather_ns"])
            and counts["mask_ns"] + counts["gather_ns"] <= result["query_time_ns"],
            "filter elapsed times exceed query")


def stats(rows, criteria, neon):
    result = []
    for index, shape in enumerate(criteria["macro_shapes"]):
        by_arm = {}
        for arm in ARMS:
            if arm == "force-neon" and not neon:
                continue
            observations = list(map(child_fields, sampled_runs(rows, index, arm)))
            by_arm[arm] = {}
            for metric in observations[0]:
                times = [o[metric] for o in observations]
                by_arm[arm][metric] = {
                    "n": len(times), "p50": statistics.median(times),
                    "p95": statistics.quantiles(times, n=20, method="inclusive")[18],
                    "min": min(times), "max": max(times),
                }
        row = {"shape": shape, "arms": by_arm}
        if neon:
            candidate = sampled_runs(rows, index, "force-neon")
            row["paired_query_gain_pct"] = {}
            for name in ("scalar", "arrow"):
                control = sampled_runs(rows, index, name)
                row["paired_query_gain_pct"][name] = [
                    round(100 * (1 - child_fields(a)["query_ns"] / child_fields(b)["query_ns"]), 3)
                    for a, b in zip(candidate, control)
                ]
        result.append(row)
    return result


def verify(bundle, seal_pin, fixture, bench, *, verify_executable=True):
    check_seal(bundle, seal_pin)
    provenance = read(bundle / "provenance.json")
    require(provenance["version"] == 1 and provenance["criteria_sha256"] == sha(CRITERIA)
            and provenance["source"] == source_inventory(ROOT)
            and provenance["spec_sha256"] == sha(SPEC), "pinned runtime source or criteria changed")
    require(provenance["binary"] == sha(bench), "pinned original executable mismatch")
    evidence_env = decode(provenance["environment_process"]["stdout"])
    require(evidence_env == provenance["build"] and provenance["environment_process"]["exit"] == 0
            and evidence_env["repository"]["git_commit"] == provenance["baseline"],
            "runner build metadata or stale source")
    if verify_executable:
        require(decode(subprocess.check_output([str(bench), "env", "--format", "json"]))
                == evidence_env, "executing binary build metadata differs")
    criteria = read(CRITERIA)
    answers, _ = verify_fixtures(fixture, criteria, provenance["fixtures"])
    rows = [decode(line) for line in (bundle / "runs.jsonl").read_text().splitlines()]
    neon = provenance["neon_available"]
    require(type(neon) is bool and (not neon or provenance["host"]["machine"] in ("arm64", "aarch64"))
            and provenance["host"]["os_release"] == evidence_env["environment"]["kernel"]
            and (not verify_executable or provenance["host"]["machine"] == platform.machine()),
            "impossible host ISA")
    require(("aarch64" in evidence_env["build"]["target"] or not neon),
            "compiled target cannot have NEON")
    for shape_id, shape in enumerate(criteria["macro_shapes"]):
        require((bundle / f"s{shape_id}.toml").read_text() == spec_text(shape[2]),
                "threshold/spec differs from frozen criteria")
    for distribution in {shape[0] for shape in criteria["macro_shapes"]}:
        gen = read(bundle / f"generate-{distribution}.json")
        require(gen["exit"] == 0 and not gen["timeout"] and not any(gen["truncated"])
                and decode(gen["stdout"]) == read(fixture / distribution / "manifest.json"),
                "fixture generation observation")
    index = criteria["calibration_shape"]
    matrix = [("probe-neon", index, "force-neon", -1), *schedule(criteria, neon),
              *((name, index, entry[0], -1) for name, entry in FAILURES.items()),
              ("retry", index, "scalar", -1), ("legacy", index, "legacy", -1)]
    profiled = neon and provenance["host"].get("cpu_class") and evidence_env["build"]["profile"] == "release"
    if profiled:
        matrix.extend((name, index, "measured-auto", -1) for name in PROFILE_CASES)
    require(len(rows) == len(matrix)
            and [tuple(row[key] for key in ("id", "shape", "arm", "trial")) for row in rows] == matrix,
            "missing, duplicate or relabelled process case")
    profile = verify_profile(bundle, provenance, rows, criteria, fixture, bench) if profiled else None
    ordered = None
    for run in rows:
        sid, arm = run["id"], run["arm"]
        current = datetime.datetime.fromisoformat(run["start_utc"])
        require(ordered is None or current >= ordered, "timestamps out of order")
        ordered = current
        require(not run["timeout"] and run["truncated"] == [False, False]
                and positive(run["process_ns"]) and positive(run["cpu_ns"])
                and positive(run["rss_bytes"])
                and run["warmup"] == (0 <= run["trial"] < criteria["warmups"]),
                "invalid process resource/iteration observation")
        failed = FAILURES.get(sid)
        if not neon and sid == "probe-neon":
            failed = ("force-neon", (), "unsupported native filter ISA: neon")
        if failed:
            require(run["exit"] == 1 and run["stdout"] == "" and failed[2] in run["stderr"]
                    and run["options"] == list(failed[1]) and run["profile"] is None
                    and "phase25-filter-v1 " not in run["stderr"], "negative stage did not refuse")
            continue
        predicted = None
        if sid in PROFILE_CASES:
            variant = PROFILE_CASES[sid]
            path = bundle / (sid + ".json")
            check_profile_variant(path, variant, profile)
            require(run["profile"] == ("a" * 64 if variant == "pin" else blake_file(path))
                    and run["options"] is None, "profile pin observation")
            if variant == "current":
                predicted = profile
                reason, kernel = None, None
            elif variant in ("binary", "version", "expired", "shape"):
                reason, kernel = "incompatible-profile", "scalar"
            else:
                reason, kernel = "invalid-profile", "scalar"
        else:
            require(run["options"] == [] and run["profile"] is None, "unexpected extra child options")
            reason, kernel = {
                "scalar": ("explicit-scalar", "scalar"),
                "arrow": ("arrow-control", "arrow"),
                "force-neon": ("forced-neon", "neon"),
                "measured-auto": ("missing-profile" if neon else "unsupported-isa", "scalar"),
                "legacy": (None, None),
            }[arm]
        distribution, size, threshold = criteria["macro_shapes"][run["shape"]]
        check_success(run, answers[distribution, threshold], size, criteria["dataset_rows"],
                      neon, arm, reason, kernel, predicted)
    return {"children": len(rows), "neon": neon,
            "profile": audit(next(r for r in rows if r["id"] == "profile-current")) if profiled else None,
            "statistics": stats(rows, criteria, neon)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--fixtures", required=True, type=Path)
    parser.add_argument("--bench", required=True, type=Path)
    parser.add_argument("--seal-pin", required=True)
    args = parser.parse_args()
    print(encode(verify(args.bundle.resolve(), args.seal_pin, args.fixtures.resolve(), args.bench.resolve())))


if __name__ == "__main__":
    main()
