#!/usr/bin/env python3
"""Package one raw Phase14 campaign as an authenticated portable evidence set."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import tarfile


def package(campaign: pathlib.Path, destination: pathlib.Path, prefix: str = "campaign") -> dict:
    campaign = campaign.resolve()
    destination = destination.resolve()
    if not campaign.is_dir():
        raise ValueError(f"campaign directory does not exist: {campaign}")
    if destination.exists():
        raise FileExistsError(f"evidence destination already exists: {destination}")
    if not prefix or pathlib.PurePosixPath(prefix).is_absolute() or ".." in pathlib.PurePosixPath(prefix).parts:
        raise ValueError("prefix must be a safe relative archive path")

    destination.mkdir(parents=True)
    archive_path = destination / "acceptance.tar.gz"
    campaign_files = sorted(
        path for path in campaign.rglob("*")
        if path.is_file() and "broker-data" not in path.relative_to(campaign).parts
    )
    if not campaign_files:
        raise ValueError("campaign contains no portable evidence files")

    members = {
        f"{prefix}/{path.relative_to(campaign).as_posix()}": path
        for path in campaign_files
    }
    with tarfile.open(archive_path, "w:gz") as archive:
        for name, path in members.items():
            archive.add(path, arcname=name, recursive=False)

    manifest = {
        "version": 1,
        "acceptance_result": f"{prefix}/result.json",
        "archive_sha256": hashlib.sha256(archive_path.read_bytes()).hexdigest(),
        "files": {
            name: hashlib.sha256(path.read_bytes()).hexdigest()
            for name, path in members.items()
        },
    }
    if f"{prefix}/result.json" not in members:
        archive_path.unlink()
        raise ValueError("campaign result.json is required")
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign", type=pathlib.Path, required=True)
    parser.add_argument("--destination", type=pathlib.Path, required=True)
    parser.add_argument("--prefix", default="campaign")
    args = parser.parse_args()
    manifest = package(args.campaign, args.destination, args.prefix)
    print(json.dumps({"archive": str(args.destination / "acceptance.tar.gz"), "files": len(manifest["files"]), "sha256": manifest["archive_sha256"]}, sort_keys=True))
