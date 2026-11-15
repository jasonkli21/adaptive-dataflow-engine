"""Compare release implementations in randomized interleaved fresh processes."""

import argparse
import hashlib
import json
from pathlib import Path
import random
import subprocess
import time


ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--compare", nargs="+")
    args = parser.parse_args()
    criteria = json.loads((ROOT / "scripts/phase25/criteria.json").read_text())
    arms = args.compare or criteria["study_arms"]
    binary = args.binary.resolve()
    runs = []
    randomizer = random.Random(criteria["seed"])
    for trial in range(criteria["warmups"] + criteria["trials"]):
        cases = [(shape, arm) for arm in arms for shape in criteria["micro_shapes"]]
        randomizer.shuffle(cases)
        for shape, arm in cases:
            command = [str(binary), arm, *map(str, shape)]
            start = time.monotonic_ns()
            result = subprocess.run(command, capture_output=True, text=True, timeout=30)
            runs.append(dict(
                trial=trial, warmup=trial < criteria["warmups"], arm=arm, shape=shape,
                start_ns=start, process_ns=time.monotonic_ns() - start,
                exit=result.returncode, stdout=result.stdout, stderr=result.stderr,
            ))
    evidence = dict(
        compiler=subprocess.check_output(["rustc", "-vV"], text=True),
        binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
        study_sha256=hashlib.sha256(
            (ROOT / "crates/ade-exec/examples/phase25-study.rs").read_bytes()).hexdigest(),
        criteria=criteria, runs=runs,
    )
    with args.output.open("x") as output:
        json.dump(evidence, output, separators=(",", ":"))
        output.write("\n")
    if any(run["exit"] != 0 for run in runs):
        raise SystemExit("study trial failed")


if __name__ == "__main__":
    main()
