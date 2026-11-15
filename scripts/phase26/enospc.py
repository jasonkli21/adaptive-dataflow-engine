"""Witness real ENOSPC on an already mounted, empty disposable macOS disk image."""
import argparse
import errno
import os
from pathlib import Path

from campaign import Campaign
from fixtures import make_fixture, write


def fill(path):
    with path.open("xb", buffering=0) as file:
        for size in (1 << 20, 4096, 1):
            while True:
                try:
                    file.write(b"\0" * size)
                except OSError as exc:
                    if exc.errno != errno.ENOSPC:
                        raise
                    break
        os.fsync(file.fileno())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("work", type=Path)
    parser.add_argument("volume", type=Path)
    args = parser.parse_args()
    volume = args.volume.resolve()
    args.work.mkdir(mode=0o700)
    c = Campaign(args.binary, args.work)
    assert volume.is_mount() and volume.stat().st_dev != args.work.stat().st_dev
    assert volume.is_relative_to("/private/tmp") and os.statvfs(volume).f_blocks * os.statvfs(volume).f_frsize < 64 << 20
    source = make_fixture(c.fixtures / "disk", 20_000, 4, 1)
    a = c.native("disk-a", source, 1024)
    b = c.native("disk-b", source, 8000)
    root = c.cache("cache-enospc", parent=volume)
    before = c.call("enospc-old-fill", c.args(a, source, root))
    resume = c.out / "resume-disk"
    pending = c.pause(c.args(b, source, root), "before_data_write", release=resume)
    filler = volume / "phase26-disposable-filler"
    try:
        fill(filler)
        stat = os.statvfs(volume)
        assert stat.f_bavail == 0
        write(c.out / "enospc.json", dict(available_bytes=stat.f_bavail * stat.f_frsize,
                                        errno=errno.ENOSPC, old_key=before["audit"]["prefix_key"]))
        resume.touch()
        c.finish("kernel-enospc", pending, 1, error=f"local I/O error (Some({errno.ENOSPC}))")
    finally:
        if filler.exists():
            filler.unlink()
    c.call("enospc-old-hit", c.args(a, source, root))
    c.call("enospc-new-fill", c.args(b, source, root))
    c.call("enospc-new-hit", c.args(b, source, root))


if __name__ == "__main__":
    main()
