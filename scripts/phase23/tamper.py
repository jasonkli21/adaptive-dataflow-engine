#!/usr/bin/env python3
"""Reseal each mutation, then require independent semantic rejection."""
import argparse
from pathlib import Path
import shutil
import tempfile

from campaign import json_file
from package import seal
from verify import load, require, verify


def nested(cases, name, check):
    case = next(c for c in cases if c["name"] == name)
    return next(e for e in case["events"] if e["check"] == check)


def mutations():
    def duplicate(cases):
        case = next(c for c in cases if c["name"] == "auth")
        case["events"].append(nested(cases, "auth", "result"))

    def swapped_tenant(cases):
        nested(cases, "auth", "result")["tenant"] ^= 1

    def missing_fault(cases):
        cases.remove(next(c for c in cases if c["name"] == "crash_join_gather"))

    def forged_cost(cases):
        nested(cases, "auth", "result")["status"]["attempts"][0]["usage"]["charge"] += 1

    def forged_share(cases):
        nested(cases, "fairness_2", "fairness")["weights"] = [1, 1]

    def swapped_boot(cases):
        cohort = next(c for c in cases if c["name"] == "cohort_identity")["cohorts"][0]
        cohort["workers"][0]["instance"] = "00000000-0000-0000-0000-000000000002"

    def stale_sha(host):
        host["baseline"] = "e" * 40

    def unsupported_label(support):
        support["status"] = "COMPLETE_BOUNDED_LOCAL_MULTITENANT_MANAGER_V1"

    return {
        "duplicate_job": ("cases.json", duplicate), "swapped_tenant": ("cases.json", swapped_tenant),
        "missing_failure": ("cases.json", missing_fault), "forged_cost": ("cases.json", forged_cost),
        "forged_share": ("cases.json", forged_share), "altered_worker_boot": ("cases.json", swapped_boot),
        "stale_sha": ("host.json", stale_sha), "relabeled_support": ("support.json", unsupported_label),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("root", type=Path)
    p.add_argument("--receipt-pin", required=True)
    p.add_argument("--source", required=True, type=Path)
    p.add_argument("--bin-dir", required=True, type=Path)
    p.add_argument("--output", type=Path)
    args = p.parse_args()
    verify(args.root, args.receipt_pin, "current-source", args.source, args.bin_dir)
    outcomes = []
    with tempfile.TemporaryDirectory(prefix="ade23-tamper-") as tmp:
        for name, (file, mutate) in mutations().items():
            work = Path(tmp) / name
            shutil.copytree(args.root, work)
            data = load(work / file)
            mutate(data)
            json_file(work / file, data)
            pin = seal(work)
            require(pin != args.receipt_pin, "mutation must reseal")
            try:
                verify(work, pin, "current-source", args.source, args.bin_dir)
            except ValueError as error:
                outcomes.append({"case": name, "resealed_pin": pin, "refusal": str(error)})
            else:
                raise ValueError("accepted rehashed mutation: " + name)
    if args.output:
        json_file(args.output, outcomes)
    print(f"refused {len(outcomes)} fully rehashed semantic mutations")


if __name__ == "__main__":
    main()
