import pathlib
import sys
import unittest

PHASE17 = pathlib.Path(__file__).resolve().parents[1] / "phase17"
sys.path.insert(0, str(PHASE17))
from remediation import check_refusal


class Phase17Remediation(unittest.TestCase):
    def test_selector_loss_refusal_requires_failure_before_assignment(self):
        check_refusal(
            "missing-selector-run",
            {"exit": 1},
            [{"type": "failure", "status": "REFUSED"}],
            b"",
        )

    def test_refusal_rejects_kafka_assignment(self):
        with self.assertRaisesRegex(ValueError, "Kafka assignment"):
            check_refusal(
                "missing-selector-resume",
                {"exit": 1},
                [
                    {"type": "txn_step", "point": "source_assigned"},
                    {"type": "failure", "status": "REFUSED"},
                ],
                b"",
            )

    def test_refusal_rejects_output_and_successful_exit(self):
        with self.assertRaisesRegex(ValueError, "business output"):
            check_refusal(
                "missing-selector-read",
                {"exit": 1},
                [{"type": "failure"}],
                b"{}\n",
            )
        with self.assertRaisesRegex(ValueError, "missing refusal"):
            check_refusal("missing-selector-read", {"exit": 0}, [{"type": "failure"}], b"")


if __name__ == "__main__":
    unittest.main()
