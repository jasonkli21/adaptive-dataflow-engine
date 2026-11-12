"""Trusted operator fixture artifacts used to trigger genuine worker faults."""

# The wrapper changes the actual child response while retaining a valid ABI
# header where the fault is intentionally in Arrow metadata.
WRAPPER = '''
import struct
import sys
import pyarrow as pa


class MutatingStdout:
    def __init__(self):
        self.original = sys.stdout.buffer
        self.buffer = self
        self.header = None

    def flush(self):
        self.original.flush()

    def write(self, value):
        if self.header is None:
            self.header = bytearray(value)
            return len(value)
        payload = bytearray(self.compressed if MODE == "compression" else value)
        if MODE == "wrong_batch":
            self.header[8] ^= 1
        elif MODE == "trailing":
            payload += b"trailing"
        elif MODE == "malformed":
            payload[0] = 0
        elif MODE == "oversized_decoded":
            schema_length = struct.unpack_from("<I", payload, 4)[0]
            metadata = 8 + schema_length + 8
            table = metadata + struct.unpack_from("<I", payload, metadata)[0]
            vtable = table - struct.unpack_from("<i", payload, table)[0]
            offset = struct.unpack_from("<H", payload, vtable + 10)[0]
            assert offset != 0
            struct.pack_into("<q", payload, table + offset, 1 << 40)
        struct.pack_into("<I", self.header, 84, len(payload))
        self.original.write(self.header)
        self.original.write(payload)
        return len(value)


def transform(batch):
    if MODE == "compression":
        output = pa.BufferOutputStream()
        with pa.ipc.new_stream(output, batch.schema, options=pa.ipc.IpcWriteOptions(compression="zstd")) as writer:
            writer.write_batch(batch)
        sink = MutatingStdout()
        sink.compressed = output.getvalue()
        sys.stdout = sink
    else:
        sys.stdout = MutatingStdout()
    return batch
'''


def script(case, marker):
    if case in {"wrong_batch", "trailing", "malformed", "oversized_decoded", "compression"}:
        return f"MODE = {case!r}\n" + WRAPPER
    if case in {"hang", "cancel", "disconnect", "concurrent", "gateway_crash"}:
        return f"""
import os
import time
from pathlib import Path

def transform(batch):
    Path({str(marker)!r}).write_text(str(os.getpid()))
    time.sleep(60)
    return batch
"""
    lines = {
        "wrong_schema": "return batch.rename_columns(['changed', *batch.schema.names[1:]])",
        "nulls": "return batch.set_column(2, batch.schema.field(2), pa.nulls(batch.num_rows, type=pa.uint64()))",
        "wrong_rows": "return batch if batch.column(0)[0].as_py() == 2**63 else batch.slice(1)",
        "wrong_order": "return batch.take(list(reversed(range(batch.num_rows))))",
        "dictionary": "return batch.set_column(6, 'payload', pc.dictionary_encode(batch.column(6)))",
        "oversized_encoded": "sys.stdout.buffer.write(b'X' * (1024 * 1024 + 1)); return batch",
        "stderr": "sys.stderr.write('PHASE22_REDACTION_SENTINEL'); sys.stderr.flush(); return batch",
        "exit": "os._exit(27)",
    }
    return "import os, sys\nimport pyarrow as pa\nimport pyarrow.compute as pc\n\ndef transform(batch):\n    " + lines[case] + "\n"
