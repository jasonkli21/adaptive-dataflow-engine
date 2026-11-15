import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "phase26"))
from evidence_inventory import file_inventory


class Phase26EvidenceInventoryTests(unittest.TestCase):
    def test_unexpected_evidence_symlink_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "receipt.json").write_text("{}")
            (root / "hostile").symlink_to(root / "receipt.json")
            with self.assertRaisesRegex(ValueError, "artifact symlink"):
                file_inventory(root)


if __name__ == "__main__":
    unittest.main()
