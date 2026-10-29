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

    def test_v2_execution_and_post_cleanup_lifecycle_costs_are_separate(self):
        counters = dict.fromkeys(['put', 'head', 'get', 'delete', 'write_bytes',
                                  'read_bytes', 'retries', 'elapsed_ns',
                                  'http_requests', 'http_request_body_bytes',
                                  'http_response_body_bytes'], 0)
        counters.update({'put': 3, 'head': 1, 'get': 1, 'delete': 1,
                         'write_bytes': 100, 'read_bytes': 4})
        rates = {'put_nano_usd': 5000, 'head_nano_usd': 400,
                 'get_nano_usd': 400, 'delete_nano_usd': 3,
                 'egress_byte_nano_usd': 1, 'storage_byte_ms_nano_usd': 2,
                 'retention_ms': 60000, 'write_byte_nano_usd': 0,
                 'read_byte_nano_usd': 0}
        execution_cost = 3 * 5000 + 400 + 400 + 3 + 4
        receipt = {'effective': {'rates': rates}, 'counters': counters,
                   'estimated_nano_usd': execution_cost}
        cleanup_counters = dict.fromkeys(counters, 0)
        cleanup_counters['delete'] = 2
        cleanup = {'counters': cleanup_counters, 'verified_retained_bytes': 10,
                   'assumed_retention_ms': 60000,
                   'post_cleanup_lifecycle_proxy_nano_usd': execution_cost + 2 * 3 + 10 * 60000 * 2}
        verifier.verify_remote_cost(receipt, cleanup, 2)
        cleanup['post_cleanup_lifecycle_proxy_nano_usd'] -= 1
        with self.assertRaisesRegex(ValueError, 'post-cleanup lifecycle'):
            verifier.verify_remote_cost(receipt, cleanup, 2)

if __name__ == '__main__':
    unittest.main()
