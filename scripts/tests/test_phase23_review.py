import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "phase23"))
from verify import phase_status, verify_ci_receipt  # noqa: E402


SHA = "a" * 40


def receipt():
    return {
        "version": 1,
        "repository": "jasonkli21/adaptive-dataflow-engine",
        "commit_sha": SHA,
        "run_id": 12345,
        "workflow": "ci",
        "workflow_path": ".github/workflows/ci.yml",
        "event": "push",
        "ref": "refs/heads/main",
        "run_url": f"https://github.com/jasonkli21/adaptive-dataflow-engine/actions/runs/12345",
        "created_at": "2026-11-14T10:00:00Z",
        "completed_at": "2026-11-14T10:30:00Z",
        "run_status": "completed",
        "run_conclusion": "success",
        "checks": [
            {"name": name, "status": "completed", "conclusion": "success", "head_sha": SHA}
            for name in ("rust", "msrv", "phase22-interop", "phase23-local")
        ],
    }


def completed_support():
    return {
        "gates": {f"G{i}": {"status": "PASS"} for i in range(18)},
        "status": "COMPLETE_BOUNDED_LOCAL_MULTITENANT_MANAGER_V1",
    }


class Phase23ReviewTests(unittest.TestCase):
    def test_current_source_rejects_coordinated_forged_ci_claim(self):
        support = completed_support()
        support["ci_receipt_sha256"] = "f" * 64
        with self.assertRaisesRegex(ValueError, "receipt"):
            phase_status(support, "current-source", final_sha=SHA)

    def test_receipt_binds_exact_sha_and_required_jobs(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ci.json"
            value = receipt()
            path.write_text(json.dumps(value))
            pin = hashlib.sha256(path.read_bytes()).hexdigest()
            self.assertEqual(verify_ci_receipt(path, pin, SHA)["commit_sha"], SHA)
            value["checks"] = value["checks"][:-1]
            path.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError, "pin"):
                verify_ci_receipt(path, pin, SHA)
            pin = hashlib.sha256(path.read_bytes()).hexdigest()
            with self.assertRaisesRegex(ValueError, "check"):
                verify_ci_receipt(path, pin, SHA)

    def test_old_commit_or_failed_run_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ci.json"
            value = receipt()
            value["commit_sha"] = "b" * 40
            path.write_text(json.dumps(value))
            pin = hashlib.sha256(path.read_bytes()).hexdigest()
            with self.assertRaisesRegex(ValueError, "provenance or SHA"):
                verify_ci_receipt(path, pin, SHA)

    def test_stale_or_invalid_ci_chronology_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ci.json"
            value = receipt()
            value["completed_at"] = "2026-11-13T10:00:00Z"
            path.write_text(json.dumps(value))
            pin = hashlib.sha256(path.read_bytes()).hexdigest()
            with self.assertRaisesRegex(ValueError, "chronology"):
                verify_ci_receipt(path, pin, SHA)


if __name__ == "__main__":
    unittest.main()
