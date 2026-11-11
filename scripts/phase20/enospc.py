"""Prove real kernel ENOSPC on a private temporary HFS+ image (macOS)."""
import argparse
import errno
import os
import shutil
import subprocess
from pathlib import Path

from campaign import BFS, Campaign, pin, root_layout, save, sha


def fill(path):
    failures = []
    with path.open("wb", buffering=0) as file:
        for size in (64 * 1024, 4096, 512, 1):
            block = bytes(size)
            while True:
                try:
                    file.write(block)
                except OSError as error:
                    if error.errno != errno.ENOSPC:
                        raise
                    failures.append({"block": size, "errno": error.errno, "message": str(error)})
                    break
        os.fsync(file.fileno())
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("work", type=Path)
    args = parser.parse_args()
    work = args.work.resolve()
    assert str(work).startswith("/private/tmp/"), "temporary filesystem fixture only"
    work.mkdir()
    image = work / "phase20-owned.dmg"
    mount = work / "mnt"
    mount.mkdir()
    driver = Campaign(args.binary.resolve(), work)
    record = {"binary_sha256": sha(driver.binary), "filesystem": "private 32 MiB HFS+ disk image"}
    logfile = work / "enospc.json"

    def command(*argv):
        process = subprocess.run(argv, capture_output=True, text=True, timeout=45)
        record.setdefault("mount_commands", []).append(
            {"argv": argv, "exit": process.returncode, "stdout": process.stdout, "stderr": process.stderr})
        save(logfile, record)
        process.check_returncode()

    command("hdiutil", "create", "-size", "32m", "-fs", "HFS+", "-volname", "ADE20", str(image))
    command("hdiutil", "attach", "-nobrowse", "-mountpoint", str(mount), str(image))
    try:
        assert os.path.ismount(mount), "refusing to write outside the private image"
        assert mount.stat().st_dev != work.stat().st_dev, "expected an independent volume"
        driver.roots = mount
        req = driver.request("job", "mixed", BFS, checkpoint_every=1)
        sentinel = work / "continue"
        job = driver.launch("enospc-run", "run", req, ("enospc_continue", 2, sentinel))
        observed = driver.reached(job)
        filler = mount / "cycle20-filler"
        record["fill_errors"] = fill(filler)
        stat = os.statvfs(mount)
        record["available_bytes"] = stat.f_bavail * stat.f_frsize
        assert record["available_bytes"] == 0
        sentinel.touch()
        failure = driver.finish(job, observed)
        record["failed_case"] = failure["name"]
        assert failure["exit"] == 1 and "No space left on device (os error 28)" in failure["stderr"]
        assert not failure["stdout"] and pin(failure)["round"] == 1
        req["expected"] = pin(failure)
        old = driver.run("enospc-previous-cut", "verify", req, "provisional")
        record["previous_case"] = old["name"]
        filler.unlink()
        resumed = driver.run("enospc-resume", "resume", req)
        req["expected"] = pin(resumed)
        terminal = driver.terminal("enospc-recovered", req, driver.expected("mixed", req))
        record["resume_case"] = resumed["name"]
        record["terminal_case"] = terminal["name"]
        save(work / "root-layout.json", {"job": root_layout(Path(req["root"]))})
        shutil.copytree(req["root"], work / "selected-root")
        save(logfile, record)
    finally:
        command("hdiutil", "detach", str(mount))
        image.unlink()


if __name__ == "__main__":
    main()
