import copy
import hashlib
import io
import json
import pathlib
import sys
import tarfile
import tempfile
import unittest

REPO = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))

from phase14.evidence_contracts import (  # noqa: E402
    EvidenceContractError,
    build_descriptor,
    validate_backpressure,
    validate_case_contract,
)
from phase14.verify_evidence import verify  # noqa: E402


def terminal_progress(forwarded=0, poison_cursor=None):
    return {
        "forwarded_records": forwarded,
        "frontier": None,
        "partitions": {
            "0": {
                "start": 0,
                "seen": 0,
                "forwarded": poison_cursor,
                "next_offset_suggestion": 0,
                "watermark": None,
            }
        },
    }


def poison_trace(status="FAILED", forwarded=0, poison_cursor=None, include_failure=True):
    trace = [
        {"type": "admitted", "progress": terminal_progress()},
        {
            "type": "terminal",
            "status": status,
            "failure_class": "event_schema_or_size",
            "reason": "invalid event (payload redacted)",
            "progress": terminal_progress(forwarded, poison_cursor),
        },
    ]
    if include_failure:
        trace.append(
            {
                "type": "failure",
                "failure_class": "event_schema_or_size",
                "reason": "invalid event (payload redacted)",
            }
        )
    return trace


def encode_trace(trace):
    return b"".join((json.dumps(event, separators=(",", ":")) + "\n").encode() for event in trace)


class Phase14EvidenceContractsTest(unittest.TestCase):
    def setUp(self):
        self.case = {"case": "bad-schema", "status": "PASS", "pid": 17, "exit": 1}
        self.config = {"starts": [{"partition": 0, "offset": 0}]}

    def test_descriptor_has_unambiguous_explicit_starts(self):
        descriptor = build_descriptor(
            "topic",
            "127.0.0.1:9092",
            "cluster",
            explicit_starts=[{"partition": 3, "offset": 0}],
        )
        self.assertEqual(descriptor["starts"], [{"partition": 3, "offset": 0}])
        self.assertTrue(all(type(entry[key]) is int for entry in descriptor["starts"] for key in ("partition", "offset")))
        with self.assertRaisesRegex(ValueError, "nonnegative integers"):
            build_descriptor("topic", "127.0.0.1:9092", "cluster", offsets=({"partition": 3},))

    def test_correct_missing_partition_reaches_source_identity_refusal(self):
        config = {"starts": [{"partition": 3, "offset": 0}]}
        trace = encode_trace(
            [{"type": "failure", "failure_class": "source_identity_or_offset", "reason": "source identity/offset: assigned partition missing"}]
        )
        validate_case_contract("missing-partition", {"case": "missing-partition", "status": "PASS", "pid": 18, "exit": 1}, config, trace, b"")

    def test_malformed_start_cannot_pass_as_missing_partition(self):
        config = {"starts": [{"partition": 0, "offset": {"partition": 3, "offset": 0}}]}
        trace = encode_trace([{"type": "failure", "failure_class": "admission", "reason": "stream admission: strict config JSON"}])
        with self.assertRaisesRegex(EvidenceContractError, "wrong failure class"):
            validate_case_contract("missing-partition", {"case": "missing-partition", "status": "PASS", "pid": 19, "exit": 1}, config, trace, b"")

    def test_poison_output_and_forwarded_cursor_mutations_are_rejected(self):
        trace = encode_trace(poison_trace())
        validate_case_contract("bad-schema", self.case, self.config, trace, b"")
        injected = b'{"partition":0,"offset":0}\n'
        with self.assertRaisesRegex(EvidenceContractError, "empty output"):
            validate_case_contract("bad-schema", self.case, self.config, trace, injected)

        advanced = encode_trace(poison_trace(forwarded=0, poison_cursor=0))
        with self.assertRaisesRegex(EvidenceContractError, "forwarded cursor"):
            validate_case_contract("bad-schema", self.case, self.config, advanced, b"")

    def test_failed_session_cannot_be_relabelled_stopped_or_lose_failure_event(self):
        stopped = encode_trace(poison_trace(status="STOPPED"))
        with self.assertRaisesRegex(EvidenceContractError, "successful or stopped terminal"):
            validate_case_contract("bad-schema", self.case, self.config, stopped, b"")
        missing_failure = encode_trace(poison_trace(include_failure=False))
        with self.assertRaisesRegex(EvidenceContractError, "failure trace is missing"):
            validate_case_contract("bad-schema", self.case, self.config, missing_failure, b"")

    def test_truthful_partial_output_is_allowed_for_cancelled_blocked_sink(self):
        case = {"case": "cancel-stall", "status": "PASS", "pid": 20, "exit": 1}
        progress = terminal_progress(forwarded=1, poison_cursor=0)
        trace = [
            {"type": "admitted", "progress": terminal_progress()},
            {"type": "forwarded", "progress": progress},
            {"type": "sink_blocked", "progress": progress},
            {
                "type": "terminal",
                "status": "FAILED",
                "failure_class": "cancelled",
                "reason": "cancelled",
                "discarded_in_flight": True,
                "progress": progress,
            },
            {"type": "failure", "failure_class": "cancelled", "reason": "cancelled"},
        ]
        # One acknowledged receipt plus a bounded, unterminated frame is an
        # honest sink boundary after cancellation; it is not a success claim.
        output = b'{"partition":0,"offset":0}\n{'
        validate_case_contract("cancel-stall", case, {"starts": [{"partition": 0, "offset": 0}]}, encode_trace(trace), output)

    def test_backpressure_checks_frozen_cursors_backlog_plateau_and_resume(self):
        frozen = {"forwarded_records": 37, "frontier": 4, "partitions": {"0": {"forwarded": 18}, "1": {"forwarded": 18}}}
        source = {"backlog": {"0": 600, "1": 601}, "queue_bytes_peak": 7937, "queue_target_bytes": 16384, "response_cap_bytes": 65536}
        trace = [
            {"type": "sink_blocked", "elapsed_ms": 1000 + index * 1250, "progress": copy.deepcopy(frozen), "source": copy.deepcopy(source)}
            for index in range(25)
        ]
        trace.extend({"type": "forwarded", "progress": {"forwarded_records": index + 1}} for index in range(1600))
        trace.append({"type": "terminal", "status": "STOPPED", "metrics": {"in_flight_peak_records": 1, "sink_frame_peak_bytes": 182}, "managed_in_flight_peak_bytes": 3202, "progress": {"forwarded_records": 1600}})
        output = b"".join((json.dumps({"receipt": index}) + "\n").encode() for index in range(1600))
        case = {"case": "backpressure", "status": "PASS", "pid": 21, "exit": 0, "blocked_seconds": 35}
        validate_backpressure(case, trace, output)

        changed_frontier = copy.deepcopy(trace)
        changed_frontier[12]["progress"]["frontier"] = 5
        with self.assertRaisesRegex(EvidenceContractError, "frontier or cursor changed"):
            validate_backpressure(case, changed_frontier, output)

        changed_cursor = copy.deepcopy(trace)
        changed_cursor[12]["progress"]["partitions"]["0"]["forwarded"] = 19
        with self.assertRaisesRegex(EvidenceContractError, "frontier or cursor changed"):
            validate_backpressure(case, changed_cursor, output)

    def test_original_archive_is_only_qualified_historical_evidence(self):
        archive = REPO / "docs/evidence/phase14/acceptance.tar.gz"
        manifest = REPO / "docs/evidence/phase14/manifest.json"
        with self.assertRaisesRegex(EvidenceContractError, "wrong failure class"):
            verify(archive, manifest)
        result = verify(archive, manifest, mode="historical")
        self.assertEqual(result["status"], "PASS_HISTORICAL_WITH_F14_01_LIMIT")
        self.assertIn("not a source-partition witness", result["qualification"])

    def test_checksum_consistent_semantic_forgery_is_rejected_offline(self):
        archive_path = REPO / "docs/evidence/phase14/acceptance.tar.gz"
        manifest_data = json.loads((REPO / "docs/evidence/phase14/manifest.json").read_text())
        members = {}
        with tarfile.open(archive_path, "r:gz") as source:
            for member in source.getmembers():
                members[member.name] = source.extractfile(member).read()

        missing_config = "candidate-7/missing-partition.config.json"
        config = json.loads(members[missing_config])
        config["starts"] = [{"partition": 3, "offset": 0}]
        members[missing_config] = json.dumps(config, sort_keys=True).encode()
        members["candidate-7/missing-partition.trace.jsonl"] = (
            b'{"failure_class":"source_identity_or_offset","reason":"source identity/offset: assigned partition missing","type":"failure"}\n'
        )
        poison_output = "candidate-7/oversize.output.jsonl"
        members[poison_output] = b'{"partition":0,"offset":0,"event_time_ms":0,"projected":null,"late":false}\n'

        with tempfile.TemporaryDirectory(prefix="ade14-semantic-forgery-") as temporary:
            temporary = pathlib.Path(temporary)
            copied_archive = temporary / "forged.tar.gz"
            with tarfile.open(copied_archive, "w:gz") as output:
                for name, body in sorted(members.items()):
                    info = tarfile.TarInfo(name)
                    info.size = len(body)
                    info.mode = 0o644
                    output.addfile(info, io.BytesIO(body))
            for name in (missing_config, "candidate-7/missing-partition.trace.jsonl", poison_output):
                manifest_data["files"][name] = hashlib.sha256(members[name]).hexdigest()
            manifest_data["archive_sha256"] = hashlib.sha256(copied_archive.read_bytes()).hexdigest()
            copied_manifest = temporary / "manifest.json"
            copied_manifest.write_text(json.dumps(manifest_data))

            with self.assertRaisesRegex(EvidenceContractError, "oversize: case must have an empty output file"):
                verify(copied_archive, copied_manifest)


if __name__ == "__main__":
    unittest.main()
