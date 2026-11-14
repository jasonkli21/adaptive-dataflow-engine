#!/usr/bin/env python3
"""Assemble and seal a matched live campaign and its disposable-volume supplement."""
import argparse
import gzip
import io
from pathlib import Path
import shutil
import tarfile

from verify import RECEIPTS, digest_file, load, require
from campaign import json_file


def seal(root):
    paths = sorted(p for p in root.rglob("*") if p.is_file() and p != root / "seal.json")
    json_file(root / "seal.json", {str(p.relative_to(root)): digest_file(p) for p in paths})
    return digest_file(root / "seal.json")


def archive(root, target):
    with target.open("wb") as output, gzip.GzipFile(fileobj=output, mode="wb", filename="", mtime=0) as stream:
        with tarfile.open(fileobj=stream, mode="w|") as archive:
            for path in sorted(p for p in root.rglob("*") if p.is_file()):
                data = path.read_bytes()
                info = tarfile.TarInfo(str(path.relative_to(root)))
                info.size, info.mode = len(data), 0o644
                archive.addfile(info, io.BytesIO(data))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--live", required=True, type=Path)
    p.add_argument("--enospc", required=True, type=Path)
    p.add_argument("--support", required=True, type=Path)
    p.add_argument("--staging", required=True, type=Path)
    p.add_argument("--archive", required=True, type=Path)
    p.add_argument("--pin", required=True, type=Path)
    args = p.parse_args()
    identity = load(args.enospc / "identity.json")
    require(identity["inventory"] == load(args.live / "source-inventory.json")
            and identity["executables"] == load(args.live / "executables.json"),
            "ENOSPC executable or source mismatch")
    root = args.staging
    root.mkdir(exist_ok=False)
    for file in RECEIPTS - {"support.json", "enospc.json"}:
        shutil.copyfile(args.live / file, root / file)
    shutil.copyfile(args.support, root / "support.json")
    shutil.copyfile(args.enospc / "cases.json", root / "enospc.json")
    (root / "ipc").mkdir()
    for source in (args.live / "ipc", args.enospc / "ipc"):
        for asset in source.glob("*.arrow"):
            shutil.copyfile(asset, root / "ipc" / asset.name)
    pin = seal(root)
    archive(root, args.archive)
    args.pin.write_text(pin + "\n")


if __name__ == "__main__":
    main()
