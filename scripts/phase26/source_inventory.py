"""Archive and compare the build-source closure for the measured Phase 26 runtime."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

from fixtures import canonical, write
from evidence_inventory import sha

ROOT_INPUTS = ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml", ".github/workflows/ci.yml",
               "scripts/phase23/verify.py")
SOURCE_PATHS = (*ROOT_INPUTS, "crates", "scripts/phase26")


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def source_paths(raw):
    return {p for p in raw.decode().split("\0")
            if p and "__pycache__" not in Path(p).parts and not p.endswith(".pyc")}


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args])


def snapshot(repo, out):
    paths = sorted(source_paths(git(repo, "ls-files", "-z", "--cached", "--others",
                                    "--exclude-standard", "--", *SOURCE_PATHS)))
    require(len(paths) > 20, "incomplete workspace sources")
    out.mkdir(mode=0o700)
    files = {}
    for path in paths:
        src = repo / path
        dst = out / "files" / path
        require(src.is_file() and not src.is_symlink(), f"missing source {path}")
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)
        files[path] = sha(dst)
    manifest = dict(
        version=1,
        baseline_sha=git(repo, "rev-parse", "HEAD").decode().strip(),
        baseline_tree=git(repo, "rev-parse", "HEAD^{tree}").decode().strip(),
        build_inputs_sha256=hashlib.sha256(canonical(files)).hexdigest(),
        files=files,
    )
    write(out / "inputs.json", manifest)
    print(manifest["build_inputs_sha256"])


def verify_source(repo, archive, revision):
    locked = json.loads((archive / "inputs.json").read_bytes())
    files = locked["files"]
    require(locked["build_inputs_sha256"] == hashlib.sha256(canonical(files)).hexdigest(),
            "source manifest identity")
    if revision:
        paths = source_paths(git(repo, "ls-tree", "-rz", "--name-only", revision, "--", *SOURCE_PATHS))
        require(set(files) == paths, "incomplete or extra revision source inputs")
    for path, expected in files.items():
        relative = Path(path)
        require(not relative.is_absolute() and ".." not in relative.parts, "source path")
        require(sha(archive / "files" / path) == expected, f"archived source drift {path}")
        actual = git(repo, "show", f"{revision}:{path}") if revision else (repo / path).read_bytes()
        require(hashlib.sha256(actual).hexdigest() == expected, f"checkout drift {path}")
    if revision:
        baseline = locked["baseline_sha"]
        require(baseline in (revision, git(repo, "rev-parse", f"{revision}^").decode().strip())
                and git(repo, "rev-parse", f"{baseline}^{{tree}}").decode().strip()
                == locked["baseline_tree"], "commit or parent does not match baseline")
    return dict(
        verified_source_inputs=len(files), build_inputs_sha256=locked["build_inputs_sha256"],
        commit=git(repo, "rev-parse", revision).decode().strip() if revision else None,
        tree=git(repo, "rev-parse", f"{revision}^{{tree}}").decode().strip() if revision else None,
    )


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("repo", type=Path)
    cli.add_argument("archive", type=Path)
    cli.add_argument("--compare", action="store_true")
    cli.add_argument("--revision")
    args = cli.parse_args()
    if args.compare:
        revision = git(args.repo, "rev-parse", args.revision).decode().strip() if args.revision else None
        print(json.dumps(verify_source(args.repo, args.archive, revision), indent=2))
    else:
        snapshot(args.repo, args.archive)


if __name__ == "__main__":
    main()
