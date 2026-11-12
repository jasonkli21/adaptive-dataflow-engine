# The gateway executes this bootstrap with isolated Python and a scrubbed environment.
import os
import resource
import signal
import struct
import sys
import threading
import time

parent = int(sys.argv[2])


def watch_parent():
    while os.getppid() == parent:
        time.sleep(0.025)
    os.killpg(os.getpgrp(), signal.SIGKILL)


threading.Thread(target=watch_parent, daemon=True).start()
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
resource.setrlimit(resource.RLIMIT_CPU, (10, 10))
if sys.platform.startswith("linux"):
    resource.setrlimit(resource.RLIMIT_AS, (768 * 1024 * 1024,) * 2)

import pyarrow as pa

if pa.__version__ != "24.0.0":
    raise RuntimeError("unexpected PyArrow runtime")


def read_exact(length):
    value = sys.stdin.buffer.read(length)
    if len(value) != length:
        raise EOFError("short frame")
    return value


header = read_exact(88)
magic, sequence, rows, identity, size = struct.unpack("<8sQI64sI", header)
if magic != b"ADEUDF01" or not (0 < size <= 1024 * 1024 - 88):
    raise ValueError("frame header")
with pa.ipc.open_stream(read_exact(size)) as reader:
    batch = reader.read_next_batch()
    if batch.num_rows != rows or next(reader, None) is not None:
        raise ValueError("frame shape")

namespace = {"__name__": "ade_registered_function"}
exec(compile(sys.argv[1], "<registered>", "exec"), namespace)
result = namespace["transform"](batch)
out = pa.BufferOutputStream()
with pa.ipc.new_stream(out, result.schema, options=pa.ipc.IpcWriteOptions(compression=None)) as writer:
    writer.write_batch(result)
payload = out.getvalue()
rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
if sys.platform != "darwin":
    rss *= 1024
sys.stdout.buffer.write(header[:84] + struct.pack("<IQ", len(payload), rss))
sys.stdout.buffer.write(payload)
