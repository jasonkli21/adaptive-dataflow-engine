"""Focused verifier regressions in addition to the full rehashed mutation campaign."""
from pathlib import Path
import tempfile
import unittest

from verify import positive_int, read, validate_request


class StrictEvidence(unittest.TestCase):
    def test_duplicate_fields_fail_before_status_attribution(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "case.json"
            path.write_text('{"case":{"code":"UNIMPLEMENTED","code":"OK"}}')
            with self.assertRaisesRegex(ValueError, "duplicate evidence field: code"):
                read(path)

    def test_boolean_or_fractional_metrics_do_not_count_as_integers(self):
        for invalid in (True, False, 0, -1, 1.25, "1"):
            with self.subTest(invalid=invalid):
                self.assertFalse(positive_int(invalid, 2))
        self.assertTrue(positive_int(2, 2))
        self.assertFalse(positive_int(3, 2))

    def test_request_labels_do_not_substitute_for_wire_bytes(self):
        with self.assertRaisesRegex(ValueError, "request: UTF-8"):
            validate_request("invalid_utf8", "e30=", "0" * 64)


if __name__ == "__main__":
    unittest.main()
