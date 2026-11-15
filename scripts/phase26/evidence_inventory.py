"""Safe inventory for the sealed Phase 26 evidence tree."""
import hashlib
from pathlib import Path


def sha(path):
    hasher = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1 << 20), b""):
            hasher.update(block)
    return hasher.hexdigest()


def file_inventory(root):
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError("artifact symlink")
        if path.is_file() and path != root / "seal.json":
            result[str(path.relative_to(root))] = sha(path)
    return result
