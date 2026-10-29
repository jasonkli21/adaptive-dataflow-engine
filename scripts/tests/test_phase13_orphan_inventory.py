"""Orphan inventories stay exact-prefix, bounded, read-only diagnostics."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    'phase13_orphan_inventory', Path(__file__).parents[1] / 'phase13-orphan-inventory.py')
inventory_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(inventory_module)


class OrphanInventoryTests(unittest.TestCase):
    def setUp(self):
        self.execution = '12345678-1234-4234-8234-123456789abc'
        self.prefix = 'private-run'
        self.namespace = f'{self.prefix}/remote-v1/{self.execution}/attempt-0/'

    def test_classifies_only_keys_in_one_bounded_attempt(self):
        response = {'IsTruncated': False, 'Contents': [
            {'Key': self.namespace + 'CAPABILITY', 'Size': 14},
            {'Key': self.namespace + 'p-0/from-0/TERMINAL.json', 'Size': 128},
            {'Key': self.namespace + 'p-0/from-0/segment-0.arrow', 'Size': 512},
        ]}
        result = inventory_module.inventory(response, self.prefix, self.execution, 2)
        self.assertEqual(result['object_count'], 3)
        self.assertTrue(result['global_manifest_present'] is False)
        self.assertEqual(result['automatic_delete'], 'NO')
        self.assertEqual(result['cleanup_eligibility'], 'NOT_ESTABLISHED_BY_INVENTORY')

    def test_rejects_truncation_foreign_keys_unknown_keys_and_overflow(self):
        base = {'IsTruncated': False, 'Contents': []}
        attacks = [
            {'IsTruncated': True, 'Contents': []},
            {'IsTruncated': False, 'NextToken': 'more', 'Contents': []},
            {'IsTruncated': False, 'Contents': [{'Key': 'other/remote-v1/x', 'Size': 1}]},
            {'IsTruncated': False, 'Contents': [{'Key': self.namespace + 'unknown', 'Size': 1}]},
            {'IsTruncated': False, 'Contents': [{'Key': self.namespace + 'p-0/from-0/segment-0.arrow', 'Size': 1024 * 1024 + 1}]},
        ]
        for response in attacks:
            with self.subTest(response=response), self.assertRaises(ValueError):
                inventory_module.inventory(response, self.prefix, self.execution, 2)

    def test_rejects_noncanonical_attempt_and_duplicate_keys(self):
        with self.assertRaisesRegex(ValueError, 'canonical non-nil UUID'):
            inventory_module.inventory({'Contents': []}, self.prefix, 'not-a-uuid', 2)
        key = self.namespace + 'MANIFEST.json'
        response = {'Contents': [{'Key': key, 'Size': 16}, {'Key': key, 'Size': 16}]}
        with self.assertRaisesRegex(ValueError, 'duplicate current object key'):
            inventory_module.inventory(response, self.prefix, self.execution, 2)


if __name__ == '__main__':
    unittest.main()
