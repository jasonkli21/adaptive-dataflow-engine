"""Stable experiment inputs and byte identities."""

import hashlib
import json
from pathlib import Path
import random
import subprocess


ROOT = Path(__file__).resolve().parents[2]
SPEC = ROOT / "benchmarks/specs/scan-filter-project.toml"
CRITERIA = ROOT / "scripts/phase25/criteria.json"
CONTROLS = ("scalar", "arrow")
ARMS = CONTROLS + ("force-neon", "measured-auto")


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def unique(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError(f"duplicate JSON key {name}")
        result[name] = value
    return result


def decode(data):
    return json.loads(data, object_pairs_hook=unique, parse_constant=bad_constant)


def bad_constant(value):
    raise ValueError(f"invalid numeric constant {value}")


def read(path):
    return decode(Path(path).read_text())


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def write(path, value):
    with Path(path).open("x") as stream:
        stream.write(encode(value) + "\n")


def source_inventory(root):
    files = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=root
    ).decode().split("\0")
    return {
        name: sha(root / name)
        for name in sorted(set(files))
        if name in ("Cargo.lock", "Cargo.toml", "rust-toolchain.toml", ".github/workflows/ci.yml")
        or (name.startswith("crates/") and (name.endswith(".rs") or name.endswith("/Cargo.toml")))
        or (name.startswith("scripts/phase25/") and name.endswith((".py", ".json", ".txt")))
        or name == "scripts/tests/test_phase25_contract.py"
        or name == "benchmarks/specs/scan-filter-project.toml"
    }


def spec_text(threshold):
    text = SPEC.read_text()
    if text.count("threshold = 250\n") != 1:
        raise ValueError("base spec changed")
    return text.replace("threshold = 250\n", f"threshold = {threshold}\n")


def schedule(criteria, neon):
    randomizer = random.Random(criteria["seed"])
    for trial in range(criteria["warmups"] + criteria["trials"]):
        combinations = [
            (shape, arm) for shape in range(len(criteria["macro_shapes"]))
            for arm in ARMS if neon or arm != "force-neon"
        ]
        randomizer.shuffle(combinations)
        for shape, arm in combinations:
            yield (f"s{shape}-{arm}-{trial}", shape, arm, trial)


def sampled_runs(rows, shape, arm):
    return sorted(
        [r for r in rows
         if r["shape"] == shape and r["arm"] == arm and r["trial"] > 0 and not r["warmup"]],
        key=lambda r: r["trial"],
    )


def audit(run):
    tagged = [line.removeprefix("phase25-filter-v1 ") for line in run["stderr"].splitlines()
              if line.startswith("phase25-filter-v1 ")]
    if len(tagged) != 1:
        raise ValueError("missing or duplicate in-runtime filter audit")
    return decode(tagged[0])


def child_fields(run):
    report = audit(run)
    observations = list(report["audit"]["observations"].values())
    return {
        "mask_ns": sum(obs["mask_ns"] for obs in observations),
        "filter_ns": sum(obs["mask_ns"] + obs["gather_ns"] for obs in observations),
        "query_ns": decode(run["stdout"])["query_time_ns"],
        "process_ns": run["process_ns"],
        "rss_bytes": run["rss_bytes"],
    }
