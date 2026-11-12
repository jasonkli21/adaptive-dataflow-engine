"""Example operator-installed ABI-1 function."""
import pyarrow as pa


def transform(batch):
    index = batch.schema.get_field_index("secondary")
    values = pa.array([value % 7 for value in batch.column(index).to_pylist()], type=pa.uint64())
    return batch.set_column(index, batch.schema.field(index), values)
