"""Re-seal mutated evidence and assert substantive, detached rejection reasons."""

import argparse
from pathlib import Path
import shutil
import tempfile

from common import decode, encode, read, sha, write
from verify import verify


def json_edit(path, change):
    value = read(path)
    change(value)
    path.write_text(encode(value) + "\n")


def run_edit(directory, selector, change):
    path = directory / "runs.jsonl"
    runs = [decode(line) for line in path.read_text().splitlines()]
    change(runs, next(i for i, run in enumerate(runs) if selector(run)))
    path.write_text("".join(encode(run) + "\n" for run in runs))


def change_run(directory, case, field, value):
    run_edit(directory, lambda r: r["id"] == case,
             lambda runs, i: runs[i].update({field: value}))


def re_seal(bundle):
    (bundle / "seal.json").unlink()
    write(bundle / "seal.json", {p.name: sha(p) for p in sorted(bundle.iterdir())})
    return sha(bundle / "seal.json")


def mutations(neon, profiled):
    yield "binary", "original executable", lambda d: json_edit(
        d / "provenance.json", lambda data: data.update(binary="a" * 64))
    yield "source", "pinned runtime source", lambda d: json_edit(
        d / "provenance.json", lambda data: data["source"].update({"Cargo.lock": "b" * 64}))
    yield "dataset", "fixture external", lambda d: json_edit(
        d / "provenance.json", lambda data: data["fixtures"].update({"uniform/manifest.json": "c" * 64}))
    yield "stale", "metadata or stale source", lambda d: json_edit(
        d / "provenance.json", lambda data: data.update(baseline="d" * 40))
    yield "threshold", "threshold/spec", lambda d: (d / "s2.toml").write_text(
        (d / "s2.toml").read_text().replace("threshold = 250", "threshold = 251"))
    yield "case-omitted", "missing, duplicate or relabelled", lambda d: run_edit(
        d, lambda r: r["id"] == "retry", lambda runs, i: runs.pop(i))
    yield "case-duplicated", "missing, duplicate or relabelled", lambda d: run_edit(
        d, lambda r: r["id"] == "retry", lambda runs, i: runs.insert(i, runs[i]))
    yield "false-forced-isa", "negative stage did not refuse", lambda d: change_run(
        d, "forced-unavailable", "exit", 0)
    yield "forged-output", "direct Parquet oracle", lambda d: edit_stdout(
        d, "retry", lambda outcome: outcome.update(result_fingerprint="e" * 64))
    yield "forged-request", "child requested kernel provenance", lambda d: edit_stdout(
        d, "retry", lambda outcome: outcome["engine"]["configuration"].update(kernel_policy="arrow"))
    yield "forged-query-time", "inconsistent query/process", lambda d: edit_stdout(
        d, "retry", lambda outcome: outcome.update(query_time_ns=10**11))
    yield "forged-kernel", "incorrect effective kernel", lambda d: edit_audit(
        d, "retry", lambda tail: tail["audit"]["observations"]["explicit-scalar"].update(kernel="neon"))
    reason = "missing-profile" if neon else "unsupported-isa"
    yield "removed-fallback", "incorrect kernel decision or fallback", lambda d: edit_audit(
        d, "s2-measured-auto-0", lambda tail: tail["audit"]["observations"].update(
            {"measured-gain": tail["audit"]["observations"].pop(reason)}))
    if profiled:
        yield "sample", "profile calibration source", lambda d: json_edit(
            d / "profile-current.json", lambda value: value.update(calibration_sha256="f" * 64))
        yield "cost", "profile costs disagree", lambda d: json_edit(
            d / "profile-current.json", lambda value: value["neon"]["query_ns"].__setitem__(0, 1))
        yield "confidence", "prediction or confidence", lambda d: edit_audit(
            d, "profile-current", lambda tail: tail["audit"]["prediction"].update(
                min_paired_query_gain_basis_points=9999))


def edit_stdout(directory, case, change):
    def patch(rows, i):
        value = decode(rows[i]["stdout"])
        change(value)
        rows[i]["stdout"] = encode(value)
    run_edit(directory, lambda r: r["id"] == case, patch)


def edit_audit(directory, case, change):
    def patch(rows, i):
        marker = "phase25-filter-v1 "
        lead, tail = rows[i]["stderr"].split(marker)
        data = decode(tail)
        change(data)
        rows[i]["stderr"] = lead + marker + encode(data) + "\n"
    run_edit(directory, lambda r: r["id"] == case, patch)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--fixtures", required=True, type=Path)
    parser.add_argument("--bench", required=True, type=Path)
    parser.add_argument("--seal-pin", required=True)
    args = parser.parse_args()
    accepted = verify(args.bundle, args.seal_pin, args.fixtures, args.bench.resolve())
    for name, expected, mutation in mutations(accepted["neon"], accepted["profile"] is not None):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp) / "candidate"
            shutil.copytree(args.bundle, directory)
            mutation(directory)
            pin = re_seal(directory)
            try:
                verify(directory, pin, args.fixtures, args.bench, verify_executable=False)
            except ValueError as error:
                if expected not in str(error):
                    raise ValueError(f"{name} refused for {error}, expected {expected}") from error
                print(f"{name}: {error}", flush=True)
            else:
                raise ValueError(f"accepted fully re-sealed mutation: {name}")


if __name__ == "__main__":
    main()
