#!/usr/bin/env python3
"""Exercise the native publisher and manager snapshot on a disposable macOS volume."""
import argparse
import errno
import os
from pathlib import Path
import subprocess
import tempfile

from campaign import Case, inventory, json_file, sha


def fill(mount):
    path = mount / "fill"
    errors = []
    with path.open("wb", buffering=0) as output:
        for size in (64 * 1024, 4096, 512, 1):
            block = bytes(size)
            while True:
                try:
                    output.write(block)
                except OSError as error:
                    assert error.errno == errno.ENOSPC
                    errors.append(dict(errno=error.errno, bytes=size))
                    break
        os.fsync(output.fileno())
    stat = os.statvfs(mount)
    assert stat.f_bavail == 0
    return path, errors


def run(case, mount, kind):
    if kind == "manager":
        case.state = mount / "manager"
        case.socket = case.state / "control.sock"
    else:
        case.ops_base = mount
    if kind == "native":
        # The worker owns a separate scratch root on the normal temp filesystem.
        env = {"ADE_OPS_FAULT": "manifest_synced", "ADE_OPS_FAULT_PAUSE_MS": "1400"}
    else:
        env = None
    case.setup(env=env)
    filler = None
    try:
        if kind == "manager":
            filler, errors = fill(mount)
            try:
                response = case.ask(case.token(0), "submit", key="enospc", spec=case.specs[0, "aggregate"])
            except (EOFError, OSError):
                response = None
            if response:
                assert response == {"ok": False, "code": "LEDGER_IO"}, response
            case.await_(lambda: "ADE_RESOURCE_LEDGER_ERRNO Some(28)" in case.manager_log.read_text())
            case.manager.wait(timeout=10)
            assert case.manager.returncode == 1
            filler.unlink()
            filler = None
            case.start()
            assert case.ok(case.token(0), "list") == []
            job = case.submit(0, "enospc")
            case.check_result(0, case.wait(0, job["id"]), "aggregate")
            reached = "ADE_RESOURCE_LEDGER_ERRNO Some(28)"
        else:
            job = case.submit(0, "enospc")
            reached = "ADE_OPS_REACHED manifest_synced"
            case.await_(lambda: reached in case.manager_log.read_text())
            filler, errors = fill(mount)
            # The fill is an OS-backed refusal; a no-clobber link sometimes
            # consumes preallocated directory structures on HFS+. Preserve
            # the actual outcome independently of the expected gate.
            terminal = case.wait(0, job["id"])
            assert terminal["state"] == "quarantined", terminal
            assert "ADE_RESOURCE_NATIVE_ERRNO Some(28)" in case.manager_log.read_text()
            filler.unlink()
            filler = None
            native = terminal["attempts"][0]
            if native["native_id"] is not None:
                selected = Path(case.cohorts[native["cohort"]][0]["root"])
                assert not (selected / "jobs" / native["native_id"] / "COMMITTED.json").exists()
            case.events.append(dict(check="native_full_terminal", status=terminal,
                                    errno_witness="ADE_RESOURCE_NATIVE_ERRNO Some(28)"))
        case.events.append(dict(check="enospc", kind=kind, reached=reached, errno=errno.ENOSPC,
                                independent_device=mount.stat().st_dev != case.root.stat().st_dev,
                                fill_errors=errors, available_bytes=0))
    finally:
        if filler is not None and filler.exists():
            filler.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.bin_dir = args.bin_dir.resolve()
    args.manager, args.batch, args.worker = (args.bin_dir / name for name in
                                            ("ade-resource", "ade-batch", "ade-worker"))
    args.fixture = args.bin_dir / "examples/phase23-fixture"
    args.output.mkdir(exist_ok=False)
    receipts = []
    json_file(args.output / "identity.json", dict(inventory=inventory(),
              executables={name: sha(getattr(args, name)) for name in ("manager", "batch", "worker", "fixture")}))
    with tempfile.TemporaryDirectory(prefix="a23disk-", dir="/private/tmp") as raw:
        root = Path(raw).resolve()
        for mode in ("manager", "native"):
            image = root / f"{mode}.dmg"
            mount = root / f"v-{mode}"
            mount.mkdir()
            def hdiutil(*argv):
                return subprocess.run(["hdiutil", *argv], check=True, timeout=40)
            hdiutil("create", "-size", "32m", "-fs", "HFS+", "-volname", "ADE23", str(image))
            hdiutil("attach", "-nobrowse", "-mountpoint", str(mount), str(image))
            try:
                assert os.path.ismount(mount) and mount.stat().st_dev != root.stat().st_dev
                case = Case("enospc_" + mode, args, root / mode)
                try:
                    run(case, mount, mode)
                    receipts.append(case.receipt())
                    json_file(args.output / "cases.json", receipts)
                except Exception:
                    print(case.manager_log.read_text() if case.manager_log.exists() else "", flush=True)
                    raise
                finally:
                    case.close()
            finally:
                hdiutil("detach", str(mount))


if __name__ == "__main__":
    main()
