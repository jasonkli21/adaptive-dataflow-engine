"""Run source-bound ADE-local processes and retain their raw observations."""

import argparse
import datetime
import hashlib
import os
from pathlib import Path
import platform
import re
import subprocess
import tempfile
import time

import blake3

from common import (
    CRITERIA, ROOT, SPEC, child_fields, decode, encode, read, sampled_runs, schedule,
    sha, source_inventory, spec_text, write,
)
from oracle import blake_file, verify_fixtures

FAILURES = {
    "forced-unavailable": ("force-avx2", (), "unsupported native filter ISA: avx2"),
    "duplicate": ("scalar", ("kernel_policy=arrow",), "duplicate ade-local option: kernel_policy"),
    "unknown": ("scalar", ("unknown_phase25_option=neon",), "unsupported ade-local option"),
    "unknown-policy": ("simd", (), "unknown kernel_policy"),
    "scratch-denied": ("scalar", ("memory_limit_bytes=1",), "managed memory limit"),
    "profile-missing-pin": ("measured-auto", ("kernel_profile=missing",), "supplied together"),
}

PROFILE_CASES = {
    "profile-current": "current",
    "profile-binary": "binary",
    "profile-version": "version",
    "profile-expired": "expired",
    "profile-shape": "shape",
    "profile-cost": "cost",
    "profile-pin": "pin",
    "profile-duplicate": "duplicate",
}


def run(command, limit, timeout=90):
    """Capture separate streams and the actual waited child's OS resource figures."""
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        started = datetime.datetime.now(datetime.timezone.utc).isoformat()
        begin = time.monotonic_ns()
        with subprocess.Popen(
            command, stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
            env={**os.environ, "ADE_LOG": "warn"},
        ) as child:
            timed_out = False
            while True:
                pid, status, usage = os.wait4(child.pid, os.WNOHANG)
                if pid:
                    break
                if time.monotonic_ns() - begin > timeout * 10**9:
                    child.kill()
                    pid, status, usage = os.wait4(child.pid, 0)
                    timed_out = True
                    break
                time.sleep(0.0005)
            elapsed = time.monotonic_ns() - begin
            child.returncode = os.waitstatus_to_exitcode(status)
        stdout.seek(0)
        stderr.seek(0)
        streams = [stream.read(limit + 1) for stream in (stdout, stderr)]
        flags = [len(data) > limit for data in streams]
        return {
            "start_utc": started,
            "exit": child.returncode,
            "timeout": timed_out,
            "process_ns": elapsed,
            "cpu_ns": round((usage.ru_utime + usage.ru_stime) * 10**9),
            "rss_bytes": usage.ru_maxrss * (1 if platform.system() == "Darwin" else 1024),
            "truncated": flags,
            "stdout": streams[0][:limit].decode("utf-8", errors="replace"),
            "stderr": streams[1][:limit].decode("utf-8", errors="replace"),
        }


def child_args(shape, arm, profile_options=()):
    config = [f"batch_size={shape[1]}"]
    if arm != "legacy":
        config.append(f"kernel_policy={arm}")
    config.extend(profile_options)
    return ["__child-run", "--engine", "ade-local", *sum(
        (["--engine-option", value] for value in config), [])]


def hardware():
    result = dict(machine=platform.machine(), system=platform.system(),
                  os_release=platform.release(), logical_cpus=os.cpu_count())
    if platform.system() == "Darwin":
        raw = subprocess.check_output(
            ["/usr/sbin/ioreg", "-rd1", "-c", "IOPlatformExpertDevice"], text=True)
        model = re.search(r'^\s*"model" = <"([A-Za-z0-9,]+)">$' , raw, flags=re.M)
        raw = subprocess.check_output(
            ["/usr/sbin/system_profiler", "SPHardwareDataType", "-detailLevel", "mini", "-json"],
            text=True)
        fields = decode(raw)["SPHardwareDataType"][0]
        result.update(cpu_class=model.group(1), chip=fields.get("chip_type"),
                      topology=fields.get("number_processors"))
    return result


def create_profile(criteria, rows, fixtures, source_hardware, bench, target, created):
    index = criteria["calibration_shape"]
    distribution, batch_size, threshold = criteria["macro_shapes"][index]
    config = read(fixtures / distribution / "manifest.json")
    semantics = [1, "scan-filter-project", 1, {"required_schema": "synthetic-events-v1"},
                 {"kind": "scan_filter_project", "threshold": threshold},
                 {"kind": "canonical_integer_rows_v1"}]
    calibration = {}
    for name, arm in (("scalar", "scalar"), ("arrow", "arrow"), ("neon", "force-neon")):
        calibration[name] = sampled_runs(rows, index, arm)
    profile = {
        "version": 1,
        "mechanism": "arrow-59.3.1-u64-lt-neon-v1",
        "created_unix_s": created,
        "binding": {
            "executable_blake3": blake_file(bench),
            "cpu_class": source_hardware["cpu_class"],
            "os_release": source_hardware["os_release"],
            "target": target,
            "dataset": blake3.blake3(encode(config).encode()).hexdigest(),
            "workload": blake3.blake3(encode(semantics).encode()).hexdigest(),
            "batch_size": batch_size,
            "memory_limit_bytes": 256 * 1024 * 1024,
            "output_batch_rows": 1024,
            "spill_enabled": True,
            "prefetch_batches": 0,
        },
        "shape": {"rows": batch_size, "nulls": 0, "threshold": threshold},
        "full_batches": criteria["dataset_rows"] // batch_size,
        "sample_ids": [r["trial"] for r in calibration["scalar"]],
        "calibration_sha256": hashlib.sha256(encode(calibration).encode()).hexdigest(),
    }
    for name, values in calibration.items():
        stats = [child_fields(r) for r in values]
        profile[name] = {key: [stat[key] for stat in stats]
                         for key in ("filter_ns", "query_ns", "process_ns", "rss_bytes")}
    return profile


def profiles(directory, profile):
    for name, variant in PROFILE_CASES.items():
        entry = decode(encode(profile))
        if variant == "binary":
            entry["binding"]["executable_blake3"] = "a" * 64
        elif variant == "version":
            entry["version"] = 2
        elif variant == "expired":
            entry["created_unix_s"] = 1
        elif variant == "shape":
            entry["shape"]["threshold"] += 1
        elif variant == "cost":
            entry["neon"]["query_ns"][0] = 2**64 - 1
        path = directory / (name + ".json")
        if variant == "duplicate":
            path.write_text(encode(entry).replace('{"version":1,', '{"version":1,"version":1,', 1))
        else:
            write(path, entry)
        yield name, path, "a" * 64 if variant == "pin" else blake_file(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bench", required=True, type=Path)
    parser.add_argument("--fixtures", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    bench, fixture, output = [getattr(args, name).resolve()
                              for name in ("bench", "fixtures", "output")]
    output.mkdir()
    fixture.mkdir()
    criteria = read(CRITERIA)
    limit = criteria["maximum_raw_stream_bytes"]
    begin = source_inventory(ROOT)
    env = run([str(bench), "env", "--format", "json"], limit)
    if env["exit"] != 0:
        raise ValueError("benchmark environment probe failed")
    build = decode(env["stdout"])
    host = hardware()
    generation = {}
    for distribution in dict.fromkeys(case[0] for case in criteria["macro_shapes"]):
        command = [str(bench), "data", "generate", "synthetic",
                   "--dataset-id", "phase25-" + distribution,
                   "--rows", str(criteria["dataset_rows"]),
                   "--files", str(criteria["dataset_files"]),
                   "--row-group-rows", str(criteria["row_group_rows"]),
                   "--key-cardinality", str(criteria["key_cardinality"]),
                   "--seed", str(criteria["seed"]), "--distribution", distribution,
                   "--output", str(fixture / distribution)]
        generation[distribution] = run(command, limit)
        write(output / f"generate-{distribution}.json", generation[distribution])
        if generation[distribution]["exit"] != 0:
            raise ValueError(f"generation failed for {distribution}")
    _, pins = verify_fixtures(fixture, criteria, None, collect_pins=True)
    shape = criteria["macro_shapes"][criteria["calibration_shape"]]
    index = criteria["calibration_shape"]

    def invoke(sid, shape_idx, arm, trial, options=(), profile=None):
        spec = output / f"s{shape_idx}.toml"
        data_shape = criteria["macro_shapes"][shape_idx]
        if not spec.exists():
            spec.write_text(spec_text(data_shape[2]))
        command = child_args(data_shape, arm, options)
        result = run([str(bench), *command, "--spec", str(spec), "--dataset",
                      str(fixture / data_shape[0])], limit)
        record = dict(id=sid, shape=shape_idx, arm=arm, trial=trial,
                      warmup=0 <= trial < criteria["warmups"], profile=profile,
                      options=list(options) if profile is None else None, **result)
        with (output / "runs.jsonl").open("a") as stream:
            stream.write(encode(record) + "\n")
        if result["timeout"] or any(result["truncated"]):
            raise ValueError(f"process capture failed for {sid}")
        return record

    capability = invoke("probe-neon", index, "force-neon", -1)
    neon = capability["exit"] == 0
    if not neon and "unsupported native filter ISA: neon" not in capability["stderr"]:
        raise ValueError("native capability test failed unexpectedly")
    rows = []
    for sid, s, arm, trial in schedule(criteria, neon):
        record = invoke(sid, s, arm, trial)
        rows.append(record)
        if record["exit"]:
            raise ValueError(f"measured child failed: {sid}")
    for name, (arm, options, _) in FAILURES.items():
        invoke(name, index, arm, -1, options)
    invoke("retry", index, "scalar", -1)
    invoke("legacy", index, "legacy", -1)
    if neon and host.get("cpu_class") and build["build"]["profile"] == "release":
        profile = create_profile(criteria, rows, fixture, host, bench, build["build"]["target"],
                                 int(time.time()))
        for name, path, pin in profiles(output, profile):
            invoke(name, index, "measured-auto", -1,
                   (f"kernel_profile={path}", f"kernel_profile_blake3={pin}"), pin)
    if source_inventory(ROOT) != begin:
        raise ValueError("runtime inputs changed during campaign")
    write(output / "provenance.json", {
        "version": 1, "source": begin, "binary": sha(bench),
        "build": build, "environment_process": env, "host": host,
        "neon_available": neon, "fixtures": pins, "criteria_sha256": sha(CRITERIA),
        "spec_sha256": sha(SPEC), "baseline": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
    })
    write(output / "seal.json", {p.name: sha(p) for p in sorted(output.iterdir())})
    print(sha(output / "seal.json"))


if __name__ == "__main__":
    main()
