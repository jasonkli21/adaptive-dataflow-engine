"""Write the Phase 19 evidence archive and exact outer member inventory."""
import argparse
import gzip
import io
import tarfile
from pathlib import Path

from oracle import compact
from verify_evidence import sha


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    here = Path(__file__).resolve().parent
    root = here.parents[1]
    entries = {str(p.relative_to(args.run)): p.read_bytes() for p in args.run.rglob("*")
               if p.is_file() and p.relative_to(args.run).parts[0] != "work"}
    for p in list(here.glob("*.py")) + [root / "scripts/phase19/criteria.json",
                                                        root / "scripts/phase17/digest.py"]:
        entries[str(p.relative_to(root))] = p.read_bytes()
    args.output.mkdir(parents=True, exist_ok=True)
    archive = args.output / "acceptance.tar.gz"
    with archive.open("wb") as file, gzip.GzipFile(filename="", fileobj=file, mode="wb", mtime=0) as encoded:
        with tarfile.open(fileobj=encoded, mode="w") as tar:
            for name, data in sorted(entries.items()):
                entry = tarfile.TarInfo(name)
                entry.size = len(data)
                entry.mode = 0o600
                tar.addfile(entry, io.BytesIO(data))
    manifest = {"version": 1, "archive_sha256": sha(archive.read_bytes()),
                "members": {n: sha(b) for n, b in sorted(entries.items())}}
    (args.output / "manifest.json").write_bytes(compact(manifest) + b"\n")


if __name__ == "__main__":
    main()
