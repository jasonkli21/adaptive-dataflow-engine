"""Recompute the outer seal after each hostile evidence edit."""
import argparse
import json
from pathlib import Path
import shutil
import tempfile

from fixtures import write
from verify import (
    receipt_index, scenarios, seal, verify_artifact, verify_jobs, verify_seal,
)


def edit_receipts(root, label, fn):
    path = root / "events.jsonl"
    events = [json.loads(row) for row in path.read_text().splitlines()]
    event = next(e for e in events if e["label"] == label)
    value = json.loads(event["stdout"])
    fn(event, value)
    event["stdout"] = json.dumps(value)
    path.write_text("\n".join(map(json.dumps, events)) + "\n")


def mutate_payload(root):
    path = root / "artifacts/prefix/payload.parquet"
    data = bytearray(path.read_bytes())
    data[30] ^= 1
    path.write_bytes(data)


def mutate_catalog(root):
    path = root / "fixtures/multi/catalog.json"
    catalog = json.loads(path.read_bytes())
    catalog["files"].reverse()
    write(path, catalog)


def run(root):
    variants = {
        "forged_result": (lambda r: edit_receipts(r, "hit-beta", lambda e, v: v["rows"][0].__setitem__(0, 999)), "original-source result oracle"),
        "wrong_prefix_key": (lambda r: edit_receipts(r, "hit-beta", lambda e, v: v["audit"].__setitem__("prefix_key", "2" * 64)), "derived prefix key"),
        "wrong_rewrite": (lambda r: edit_receipts(r, "hit-beta", lambda e, v: v["audit"].__setitem__("rewritten_plan", "2" * 64)), "derived native suffix identity"),
        "missing_prefix_work": (lambda r: edit_receipts(r, "fill-alpha", lambda e, v: v["audit"].__setitem__("prefix", [])), "genuine prefix scan/filter audit"),
        "payload_mutated": (mutate_payload, "artifact digest or rows"),
        "catalog_reordered": (mutate_catalog, "plan catalog identity"),
        "fake_victim": (lambda r: edit_receipts(r, "evict-medium-removes-low",
                        lambda e, v: v["audit"]["decision"].__setitem__("victims", [])), "policy selection"),
        "same_process_hit": (lambda r: edit_receipts(r, "hit-beta", lambda e, v: same_pid(r, e, v)), "independent cross-job hit"),
    }
    for label, (edit, reason) in variants.items():
        with tempfile.TemporaryDirectory(prefix="ade26-tamper-") as temp:
            copy = Path(temp) / "evidence"
            shutil.copytree(root, copy)
            edit(copy)
            pin = seal(copy)
            verify_seal(copy, pin)
            events, indexed = receipt_index(copy)
            try:
                jobs = verify_jobs(copy, events)
                verify_artifact(copy, indexed, jobs)
                scenarios(copy, indexed, jobs)
            except ValueError as error:
                if reason not in str(error):
                    raise
                print(f"{label}: {error}")
            else:
                raise ValueError(f"resealed mutation was accepted: {label}")


def same_pid(root, event, value):
    _, events = receipt_index(root)
    event["pid"] = events["fill-alpha"]["pid"]
    value["audit"]["pid"] = event["pid"]


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("archive", type=Path)
    args = cli.parse_args()
    run(args.archive.resolve())
