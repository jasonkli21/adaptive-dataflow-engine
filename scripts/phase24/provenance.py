"""Hashes for runtime and research inputs, independent of report generations."""
import hashlib
import json
import subprocess
from pathlib import Path


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as data:
        for block in iter(lambda: data.read(64 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def dump(path, data):
    path.write_text(json.dumps(data, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def load(path):
    return json.loads(path.read_bytes())


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args]).decode().rstrip("\n")


def source_inventory(root):
    paths = git(root, "ls-files", "-c", "-o", "--exclude-standard", "--",
                "Cargo.toml", "Cargo.lock", "rust-toolchain.toml", ".github", "crates",
                "examples/phase24-external-consumer", "scripts/phase24").splitlines()
    return {name: sha(root / name) for name in sorted(set(paths))}


def members(root):
    return {path.relative_to(root).as_posix(): sha(path) for path in sorted(root.rglob("*"))
            if path.is_file() and path.name != "seal.json"}
