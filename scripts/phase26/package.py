"""Archive the finite evidence set and preserve all preflight receipts."""
import argparse
from pathlib import Path
import shutil
import tarfile

from source_inventory import snapshot
from verify import seal


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("work", type=Path)
    cli.add_argument("candidate")
    cli.add_argument("repo", type=Path)
    cli.add_argument("evidence", type=Path)
    args = cli.parse_args()
    stage = args.work / "bundle"
    shutil.copytree(args.work / args.candidate, stage, ignore=shutil.ignore_patterns("roots"))
    shutil.copytree(args.work / "enospc-matched", stage / "enospc",
                    ignore=shutil.ignore_patterns("roots", "resume-disk"))
    shutil.copy2(args.work / "linux-cross-uid-final.log", stage / "linux-cross-uid.log")
    shutil.copy2(args.work / "ade-cache-linux-stripped", stage / "ade-cache-linux")
    supplement = stage / "commands"
    supplement.mkdir()
    for p in args.work.iterdir():
        if p.is_file() and p.suffix in {".command", ".exit", ".log"}:
            shutil.copy2(p, supplement / p.name)
    for name in ("preflight1", "preflight2", "preflight3", "candidate4", "candidate5", "candidate6"):
        predecessor = stage / "predecessors" / name
        predecessor.mkdir(parents=True)
        for filename in ("events.jsonl", "host.json", "sequences.json"):
            src = args.work / name / filename
            if src.is_file():
                shutil.copy2(src, predecessor / filename)
    snapshot(args.repo, stage / "source")
    args.evidence.mkdir(parents=True, exist_ok=True)
    pin = seal(stage)
    (args.evidence / "candidate-final.seal-sha256").write_text(pin + "\n")
    with tarfile.open(args.evidence / "candidate-final.tar.gz", "w:gz", compresslevel=6) as tar:
        for file in sorted(stage.rglob("*")):
            if file.is_file():
                tar.add(file, arcname=str(file.relative_to(stage)), recursive=False)
    print(pin)


if __name__ == "__main__":
    main()
