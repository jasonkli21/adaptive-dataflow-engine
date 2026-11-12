#!/usr/bin/env python3
"""Mutate and fully rehash real evidence; require case-specific refusal."""
import argparse
import json
from pathlib import Path
import shutil
import tempfile

from inventory import seal
from verify import verify


def json_edit(path, edit):
    document = json.loads(path.read_text())
    edit(document)
    path.write_text(json.dumps(document, indent=2))


def mutations(root):
    yield "missing_case", "exact case set", lambda p: json_edit(p / "cases.json", lambda d: d.pop("udf"))
    yield "request", "request: udf", lambda p: json_edit(
        p / "cases.json", lambda d: d["udf"].update(request=d["plan"]["request"]))
    yield "status", "refusal status: wrong_rows", lambda p: json_edit(
        p / "cases.json", lambda d: d["wrong_rows"].update(code="OK", returned_ticket=True))
    yield "rows", "result rows", lambda p: json_edit(
        p / "cases.json", lambda d: d["sql"]["data"]["rows"].reverse())
    yield "schema", "result schema", lambda p: json_edit(
        p / "cases.json", lambda d: d["plan"]["data"]["schema"][0].update(nullable=True))
    yield "function", "function identity", lambda p: json_edit(
        p / "cases.json", lambda d: d["udf"]["receipt"]["identity"]["function"].update(digest="b" * 64))
    yield "invocations", "UDF invocation count", lambda p: json_edit(
        p / "cases.json", lambda d: d["udf"]["receipt"]["worker"].update(invocations=0))
    yield "ticket", "ticket ownership/restart/expiry witness", lambda p: json_edit(
        p / "cases.json", lambda d: d["cross_session"].update(ticket=d["forged_ticket"]["ticket"]))
    yield "time", "benchmark wall timing", lambda p: json_edit(
        p / "benchmark.json", lambda d: d["python_sql"]["samples"][0].update(wall_ns=0))
    yield "peak", "native peak bound", lambda p: json_edit(
        p / "cases.json", lambda d: d["udf"]["receipt"].update(native_peak_bytes=2**48))
    # Two additional attacks target the independent pin and binary Arrow witness.
    yield "relabel_archive", "independent receipt pin", lambda p: (p / "added.log").write_text("altered")
    def change_arrow(path):
        arrow = path / "sql.arrow"
        arrow.write_bytes(arrow.read_bytes() + b"extra")
    yield "arrow_bytes", "Arrow result digest", change_arrow


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    parser.add_argument("--receipt-pin", required=True)
    args = parser.parse_args()
    verify(args.archive, "historical-archive", args.receipt_pin)
    for name, reason, mutate in mutations(args.archive):
        with tempfile.TemporaryDirectory(prefix=f"phase22-{name}-") as temp:
            alternate = Path(temp) / "archive"
            shutil.copytree(args.archive, alternate)
            (alternate / "seal.json").unlink()
            mutate(alternate)
            counterfeit = seal(alternate)
            assert counterfeit != args.receipt_pin
            try:
                verify(alternate, "historical-archive", args.receipt_pin)
            except ValueError as exc:
                if str(exc) != reason:
                    raise AssertionError(f"{name} refused for {exc}, expected {reason}") from exc
                print(json.dumps({"mutation": name, "fully_rehashed": counterfeit, "refused": str(exc)}))
            else:
                raise AssertionError(f"accepted tamper {name}")


if __name__ == "__main__":
    main()
