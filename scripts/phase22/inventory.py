"""SHA-256 inventories for Phase 22 inputs and archived observations."""

import hashlib
import json
from pathlib import Path
import subprocess

RUNTIME_ROOTS = ("crates/", "clients/", "scripts/phase22/")
TOP_LEVEL = {"Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "rustfmt.toml", ".github/workflows/ci.yml"}


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def source_inventory(root):
    command = ["git", "-C", str(root), "ls-files", "-z", "--cached", "--others", "--exclude-standard"]
    paths = set(subprocess.check_output(command).decode().split("\0"))
    return {
        name: sha(root / name)
        for name in sorted(paths)
        if name in TOP_LEVEL or name.startswith(RUNTIME_ROOTS)
        if "__pycache__" not in name and not name.endswith(".pyc")
    }


def artifact_inventory(root):
    files = sorted(root.rglob("*"))
    if any(path.is_symlink() for path in files):
        raise ValueError("archive symlink")
    return {
        path.relative_to(root).as_posix(): sha(path)
        for path in files
        if path.is_file() and path != root / "seal.json"
    }


def seal(root):
    path = root / "seal.json"
    with path.open("x", encoding="utf-8") as handle:
        json.dump(artifact_inventory(root), handle, sort_keys=True, indent=2)
        handle.write("\n")
    return sha(path)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    print(seal(parser.parse_args().archive))
