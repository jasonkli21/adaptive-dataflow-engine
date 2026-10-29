"""Evidence cannot silently select the best sample or relabel timing as valid."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('phase13_verify', Path(__file__).parents[1] / 'phase13-verify.py')
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)

class EvidenceTests(unittest.TestCase):
    def test_incomplete_and_best_only_sets_refuse(self):
        for count in [0, 1, 24, 31, 33]:
            with self.subTest(count=count), self.assertRaises(ValueError):
                verifier.verify([{}] * count, [])

    def test_duplicate_or_foreign_sample_refuses_before_claiming_oracle(self):
        row = {'coordinator_pid': 1, 'distribution': 'foreign', 'workers': 2, 'mode': 'remote_object', 'repetition': 0}
        with self.assertRaisesRegex(ValueError, 'duplicate/foreign'):
            verifier.verify([dict(row, coordinator_pid=i+1) for i in range(32)], [])

if __name__ == '__main__':
    unittest.main()
