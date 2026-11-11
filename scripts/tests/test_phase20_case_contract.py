import copy
import hashlib
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "phase20"))
from case_contract import (ContractError, request_identity, standard_deviation_matches, validate_case,
                           validate_inventory, validate_root_inventory)


SPEC = json.loads((Path(__file__).resolve().parents[1] / "phase20" / "case-contract.json").read_text())


def case(name, operation, request, exit_code=0, stderr="", stdout=""):
    return {"name": name, "argv": ["ade-iterate", operation, f"/tmp/{name}.json"],
            "exit": exit_code, "stderr": stderr, "stdout": stdout}


class Phase20CaseContractTests(unittest.TestCase):
    def test_standard_deviation_tolerance_is_ulp_only_and_rejects_forgery(self):
        import math
        expected = 2278.2889544568307
        self.assertTrue(standard_deviation_matches(math.nextafter(expected, math.inf), expected))
        self.assertFalse(standard_deviation_matches(expected + 1.0, expected))

    def test_success_cannot_be_reclassified_as_failure(self):
        name = "0035-mixed-bfs-0"
        spec = copy.deepcopy(SPEC)
        expected = spec["cases"][name]
        request = b"frozen request"
        # The archive's digest is authoritative; the record's mutable exit is not.
        expected["request_sha256"] = hashlib.sha256(request).hexdigest()
        changed = case(name, expected["operation"], request, 1, "INCOMPLETE_LIMIT")
        with self.assertRaisesRegex(ContractError, "exit disposition differs"):
            validate_case(name, changed, request, spec)

    def test_refusal_requires_its_specific_reason_and_empty_stdout(self):
        name = "0096-duplicate_vertex"
        request = b"frozen refusal request"
        spec = copy.deepcopy(SPEC)
        spec["cases"][name].update(request_sha256=hashlib.sha256(request).hexdigest())
        malformed = case(name, "run", request, 1, "some unrelated failure", '{"vertex":999}\n')
        with self.assertRaisesRegex(ContractError, "required diagnostic missing"):
            validate_case(name, malformed, request, spec)
        malformed["stderr"] = spec["cases"][name]["stderr_contains"]
        with self.assertRaisesRegex(ContractError, "unexpected stdout"):
            validate_case(name, malformed, request, spec)

    def test_changed_request_is_rejected_even_if_exit_matches(self):
        name = "0096-duplicate_vertex"
        spec = copy.deepcopy(SPEC)
        expected = spec["cases"][name]
        req = b"frozen request"
        expected["request_sha256"] = hashlib.sha256(req).hexdigest()
        changed_request = b"valid graph request"
        record = case(name, "run", changed_request, 1, expected["stderr_contains"])
        with self.assertRaisesRegex(ContractError, "request identity differs"):
            validate_case(name, record, changed_request, spec)

    def test_fresh_temporary_roots_preserve_request_identity(self):
        name = "case"
        original = json.dumps({"version": 1, "source_dir": "/tmp/old/fixtures/mixed",
                               "root": "/tmp/old/roots/job", "limit": 9}).encode()
        relocated = json.dumps({"version": 1, "source_dir": "/private/tmp/new/fixtures/mixed",
                                "root": "/private/tmp/new/roots/job", "limit": 9}).encode()
        spec = {"cases": {name: {"operation": "run", "request_sha256": "different",
                                  "request_identity_sha256": request_identity(original),
                                  "exit": 0, "stderr_contains": None,
                                  "stdout_policy": "semantic"}}}
        record = case(name, "run", relocated)
        validate_case(name, record, relocated, spec)

    def test_case_and_root_omissions_are_rejected(self):
        with self.assertRaisesRegex(ContractError, "missing="):
            validate_inventory({"one": {}}, {"cases": {"one": {}, "two": {}}})
        with self.assertRaisesRegex(ContractError, "root inventory"):
            validate_root_inventory(["known", "unexpected"], {"roots": ["known"]})

    def test_changed_error_stage_is_rejected(self):
        name = "0111-zero_delay"
        spec = copy.deepcopy(SPEC)
        request = b"frozen request"
        spec["cases"][name]["request_sha256"] = hashlib.sha256(request).hexdigest()
        record = case(name, "run", request, 1, "refused: graph row cap")
        with self.assertRaisesRegex(ContractError, "required diagnostic missing"):
            validate_case(name, record, request, spec)

    def test_stalled_output_must_be_an_incomplete_terminal_prefix(self):
        name = "0251-stalled-stdout"
        spec = copy.deepcopy(SPEC)
        request = b"frozen request"
        spec["cases"][name]["request_sha256"] = hashlib.sha256(request).hexdigest()
        spec["cases"][name]["stderr_contains"] = "deadline"
        record = case(name, "read", request, 1, "deadline", "not terminal")
        with self.assertRaisesRegex(ContractError, "not a prefix"):
            validate_case(name, record, request, spec, b"expected terminal stream")


if __name__ == "__main__":
    unittest.main()
