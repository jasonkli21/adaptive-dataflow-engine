"""Challenge semantic verification after regenerating every enclosing member hash."""
import argparse
from pathlib import Path
import shutil
import tempfile

from provenance import dump, load, members, sha
from verify import historical


def modify(path, edit):
    data = load(path)
    edit(data)
    dump(path, data)


def tamper(root):
    def result(case, edit):
        return lambda archive: modify(archive / "cases" / case / "stdout", edit)

    def process(case, edit):
        return lambda archive: modify(archive / "cases" / case / "process.json", edit)

    def substitute_model(archive):
        path = archive / "fixtures" / "model.json"
        modify(path, lambda model: model["weights"].__setitem__(0, 900))

    def swap_sources(archive):
        first = archive / "fixtures" / "combined.parquet"
        second = archive / "fixtures" / "score.parquet"
        left, right = first.read_bytes(), second.read_bytes()
        first.write_bytes(right)
        second.write_bytes(left)

    def terminal(archive):
        path = archive / "cases" / "graph-mixed-terminal" / "stdout"
        lines = path.read_bytes().splitlines(keepends=True)
        path.write_bytes(b"".join(lines[:-1]) + lines[-1].replace(b'"rounds":6', b'"rounds":8'))

    def forge_trace(archive):
        path = archive / "cases" / "graph-mixed-run" / "stderr"
        data = path.read_bytes()
        changed = data.replace(b'"feedback_round":2', b'"feedback_round":3', 1)
        assert changed != data
        path.write_bytes(changed)

    cases = {
        "omitted-success": ("required case inventory", lambda a: shutil.rmtree(a / "cases" / "single")),
        "success-to-failure": ("required exit disposition", process("single", lambda o: o.update(exit=1))),
        "model-substitution": ("frozen fixtures or model changed", substitute_model),
        "source-swap": ("frozen fixtures or model changed", swap_sources),
        "fabricated-terminal": ("fabricated or invalid graph terminal footer", terminal),
        "duplicate-result-row": ("graph union-find oracle", result("graph-mixed", lambda o: o["components"].append(o["components"][0]))),
        "numerical-drift": ("numerical oracle bound", result("combined", lambda o: o["values"].__setitem__(0, o["values"][0] + 2e-10))),
        "forged-route": ("vector native route", result("combined", lambda o: o["metrics"][1].__setitem__(0, "host-only"))),
        "forged-stage-ownership": ("forged stage ownership", process("graph-mixed", lambda o: o["reached"].update(stage_file="/foreign/labels.parquet"))),
        "case-relabel": ("case label or execution route", process("single", lambda o: o.update(name="combined"))),
        "semantically-false-trace": ("native graph feedback sequence", forge_trace),
    }
    if (root / "measurements.json").is_file():
        cases["sample-change"] = ("altered measurement samples or summary",
                                  lambda a: modify(a / "measurements.json",
                                                   lambda o: o["vector-combined"]["samples_ns"].__setitem__(0, 123)))
    with tempfile.TemporaryDirectory(prefix="phase24-mutations-") as directory:
        out = Path(directory)
        for name, (reason, mutation) in cases.items():
            archive = out / name
            shutil.copytree(root, archive)
            mutation(archive)
            dump(archive / "seal.json", members(archive))
            try:
                historical(archive, sha(archive / "seal.json"))
            except ValueError as error:
                if reason not in str(error):
                    raise ValueError(f"{name}: unexpected rejection {error}") from error
                print(f"{name}: {error}")
            else:
                raise ValueError(f"{name}: forged archive passed")
            shutil.rmtree(archive)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--seal-pin", required=True)
    args = parser.parse_args()
    historical(args.archive, args.seal_pin)
    tamper(args.archive)
