"""Independent PyArrow Flight server for the disposable Phase 21 campaign."""
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.flight as flight


SCHEMA = pa.schema([pa.field("value", pa.int64(), nullable=False)])


def table(values, schema=SCHEMA):
    return pa.Table.from_arrays([pa.array(values, schema[0].type)], schema=schema)


class Server(flight.FlightServerBase):
    def __init__(self, marker, tls):
        if tls:
            cert, key = (Path(path).read_bytes() for path in tls)
            super().__init__("grpc+tls://127.0.0.1:0", tls_certificates=[(cert, key)])
        else:
            super().__init__("grpc://127.0.0.1:0")
        self.marker = marker

    def do_get(self, context, ticket):
        kind = ticket.ticket
        if kind == b"stall":
            limit = time.monotonic() + 5
            while time.monotonic() < limit and not context.is_cancelled():
                time.sleep(0.025)
        if kind == b"drift":
            wrong = pa.record_batch([pa.array(["bad"])], names=["value"])
            batches = iter([table([3]).to_batches()[0], wrong])
            return flight.GeneratorStream(SCHEMA, batches)
        if kind == b"wrong":
            data = table(["wrong"], pa.schema([pa.field("value", pa.string(), nullable=False)]))
        elif kind == b"large":
            data = table(list(range(150000)))
        else:
            data = table([11, 8] if self.marker.exists() else [3, -4, 3])
        return flight.RecordBatchStream(data)


if __name__ == "__main__":
    server = Server(Path(sys.argv[1]), sys.argv[2:])
    print(server.port, flush=True)
    server.serve()
