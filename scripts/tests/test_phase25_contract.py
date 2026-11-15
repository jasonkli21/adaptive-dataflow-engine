"""Check the small independent parsing and schedule contract without PyArrow."""
import importlib.util
from pathlib import Path
import unittest


SPEC = importlib.util.spec_from_file_location(
    "phase25_common", Path(__file__).resolve().parents[1] / "phase25/common.py"
)
COMMON = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMMON)


class Phase25ContractTest(unittest.TestCase):
    def test_duplicate_and_nonfinite_values_refuse(self):
        for raw in ('{"a":1,"a":2}', '{"value":NaN}', '{"value":Infinity}'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                COMMON.decode(raw)

    def test_matrix_preserves_identity_and_counts_on_portable_host(self):
        criteria = COMMON.read(COMMON.CRITERIA)
        for neon in (False, True):
            matrix = list(COMMON.schedule(criteria, neon))
            count = (criteria["warmups"] + criteria["trials"]) * len(criteria["macro_shapes"])
            self.assertEqual(len(matrix), count * (4 if neon else 3))
            self.assertEqual(len({r[0] for r in matrix}), len(matrix))
            self.assertEqual(any(r[2] == "force-neon" for r in matrix), neon)

    def test_calibration_excludes_probe_retry_and_warmup(self):
        rows = [
            {"shape": 2, "arm": "scalar", "trial": trial, "warmup": trial == 0}
            for trial in (-1, 0, 1, 2)
        ]
        self.assertEqual([r["trial"] for r in COMMON.sampled_runs(rows, 2, "scalar")], [1, 2])
