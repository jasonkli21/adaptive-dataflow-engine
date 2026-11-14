#!/usr/bin/env python3
"""Detached receipt and semantic verification for the bounded Phase 23 campaign."""
import argparse
from collections import Counter
from datetime import datetime
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[2]
RECEIPTS = {"criteria.json", "fixture.json", "frozen.sha256", "host.json", "cases.json",
            "source-inventory.json", "executables.json", "enospc.json", "support.json"}
DIGEST = re.compile(r"[0-9a-f]{64}")
CASES = {
    "auth", "preemption", "overlap", "commit_race", "quarantine", "quotas", "drift",
    "fairness_1", "fairness_2", "enospc_manager", "enospc_native",
    *(f"measure_{mode}_{i}" for mode in ("fifo", "adaptive") for i in range(3)),
    *("crash_" + stage for stage in (
        "reserved_before_registration", "native_registered_before_mapping", "mapped_before_start",
        "native_terminal_before_meter", "meter_persisted", "submitted_before_reply",
        "preempt_persisted_before_cancel", "drained_before_settlement", "snapshot_before_rename")),
    *(f"crash_{shape}_{stage}" for shape in ("aggregate", "join")
      for stage in ("claimed_native", "gather", "prepublish", "marker_published")),
    "history", "corruption", "cohort_identity",
}
AUTH = {
    "foreign_status": ("NOT_FOUND", 2), "foreign_cancel": ("NOT_FOUND", 2),
    "foreign_result": ("NOT_FOUND", 2), "changed_key": ("KEY_CONFLICT", 1),
    "forgery": ("UNAUTHORIZED", 1), "tenant_admin": ("UNAUTHORIZED", 1),
    "foreign_source": ("SOURCE_SCOPE", 1), "symlink": ("UNSAFE_PATH", 1),
    "oversized": ("FRAME_TOO_LARGE", 1), "malformed": ("ENCODING", 1),
    "truncated": ("TRUNCATED_FRAME", 1), "slow_peer": ("PEER_READ", 1),
    "rotated": ("UNAUTHORIZED", 1), "revoked": ("UNAUTHORIZED", 1),
}


def require(condition, label):
    if not condition:
        raise ValueError(label)


def digest_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load(path):
    def unique(pairs):
        obj = {}
        for key, value in pairs:
            require(key not in obj, "duplicate JSON member")
            obj[key] = value
        return obj
    require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 16 * 1024**2,
            "unsafe evidence file")
    return json.loads(path.read_bytes(), object_pairs_hook=unique,
                      parse_constant=lambda token: (_ for _ in ()).throw(ValueError(token)))


def one(case, event):
    items = [e for e in case["events"] if e["check"] == event]
    require(len(items) == 1, f'{case["name"]}: missing or duplicate {event}')
    return items[0]


def verify_attempt(job, attempt, cohort, price):
    require(0 <= attempt["cohort"] <= 1, "cohort range")
    grant, usage = attempt["grant"], attempt["usage"]
    slots = len(cohort["workers"])
    remaining = job["expires_ms"] - attempt["admitted_ms"]
    require(remaining > 0 and grant["slots"] == slots, "grant slots and deadline")
    require(grant["managed_bytes"] == (slots + 1) * (64 + 8 * slots) * 1024**2,
            "managed allowance")
    require(attempt["price"] == price, "price provenance")
    rate = price["microunits_per_slot_ms"]
    require(grant["cost"] == remaining * slots * rate, "cost ceiling")
    running = attempt["running_ms"]
    require(running is None or running >= attempt["admitted_ms"], "observed start")
    if usage is None:
        return
    elapsed = usage["end_ms"] - attempt["admitted_ms"]
    require(elapsed >= 0 and (running is None or running <= usage["end_ms"]), "usage time")
    require(usage["slot_ms"] == elapsed * slots, "slot time")
    require(usage["charge"] == min(elapsed, remaining) * slots * rate, "charge")
    require(usage["unpriced_slot_ms"] == max(elapsed - remaining, 0) * slots, "unpriced time")
    written = usage["written_bytes"]
    require(written is None or 0 <= written <= grant["write_bytes"], "write bytes")


def verify_outputs(case, fixture, root, decoder):
    keys, native = set(), set()
    results = []
    for event in case["events"]:
        if event["check"] != "result":
            continue
        job, kind = event["status"], event["kind"]
        require(event["tenant"] in (0, 1) and job["state"] == "succeeded", "result attribution")
        require(job["id"] not in keys and 1 <= len(job["attempts"]) <= 2, "duplicate logical result")
        keys.add(job["id"])
        for ordinal, attempt in enumerate(job["attempts"], 1):
            cohort = case["cohorts"][attempt["cohort"]]
            verify_attempt(job, attempt, cohort, case["config"]["price"])
            require(attempt["native_key"] == f'phase23-{job["id"]}-{ordinal}', "native key")
            key = (cohort["store"], attempt["native_id"])
            require(key not in native, "duplicate native registration")
            native.add(key)
        head = job["attempts"][-1]
        marker, decoded = event["marker"], event["decoded"]
        raw = bytes.fromhex(event["marker_hex"])
        require(json.loads(raw) == marker, "marker bytes")
        identity = marker["identity"]
        cohort = case["cohorts"][head["cohort"]]
        require(identity["store"] == cohort["store"] and identity["job"] == head["native_id"]
                and identity["instance"] == head["instance"] and identity["owner"] == head["owner"]
                and identity["engine_job"] == head["engine_job"], "marker/native fence")
        expected = [[5, 13, -7]] * 1025 if kind == "boundary" else fixture[kind]
        require(decoded["rows"] == expected, "literal arithmetic and multiplicity")
        columns = (["key", "sum", "count"] if kind in ("aggregate", "empty")
                   else ["key", "left_value", "right_value"])
        expected_schema = [dict(name=name, type="UInt64" if i == 0 else "Int64",
                                nullable=name == "sum") for i, name in enumerate(columns)]
        require(decoded["schema"] == expected_schema, "native output schema")
        asset_hash = event["ipc_sha256"]
        require(DIGEST.fullmatch(asset_hash), "IPC content address")
        asset = root / "ipc" / (asset_hash + ".arrow")
        require(digest_file(asset) == asset_hash and asset.stat().st_size == marker["bytes"], "IPC raw bytes")
        if decoder:
            import subprocess
            actual = subprocess.check_output([str(decoder), "read", str(asset)], timeout=15)
            require(json.loads(actual) == decoded, "redecoded IPC differs")
        usage = head["usage"]
        require(usage["outcome"] == "succeeded" and usage["ipc_hash"] == decoded["blake3"]
                == marker["blake3"] and DIGEST.fullmatch(marker["blake3"]), "IPC digest")
        require(marker["bytes"] == usage["ipc_bytes"]
                and usage["committed_bytes"] == usage["ipc_bytes"] + len(raw)
                <= head["grant"]["staging_bytes"], "publication byte accounting")
        terminals = marker["completion"]["terminals"]
        require(sorted(t["partition"] for t in terminals) == list(range(len(cohort["workers"])))
                and sum(t["rows"] for t in terminals) == marker["rows"] == len(expected),
                "native terminal coverage")
        results.append(event)
    return results


def verify_processes(case):
    cohorts = case["cohorts"]
    require(len(cohorts) == 2 and len({c["store"] for c in cohorts}) == 2, "separate stores")
    workers = [w for cohort in cohorts for w in cohort["workers"]]
    require(len(workers) == 4 and len({w["instance"] for w in workers}) == 4, "unique worker boots")
    require(len({w[k].rsplit(":", 1)[1] for w in workers for k in ("control", "exchange")}) == 8,
            "unique endpoints")
    require(len({pid for cohort in cohorts for pid in cohort["pids"]}) == 4, "worker processes")


def verify_fairness(case):
    event = one(case, "fairness")
    jobs = event["starts"]
    weights = dict(zip(case["tenant_ids"], event["weights"]))
    require(len(jobs) == 11 and len({j["id"] for j in jobs}) == 11, "fairness chronology")
    order = sorted(jobs, key=lambda job: (job["attempts"][0]["admitted_ms"], job["sequence"]))
    require(order[0]["id"] == event["blocker"], "fairness blocker")
    granted = Counter({order[0]["tenant"]: 1})
    waiting = order[1:]
    for index, job in enumerate(waiting):
        attempt = job["attempts"][0]
        at = attempt["admitted_ms"]
        candidates = [j for j in waiting[index:] if j["queued_ms"] <= at < j["expires_ms"]]
        require(len(candidates) > 0, "backlogged contenders")
        def score(j):
            tier = 2 if at - j["queued_ms"] >= case["config"]["aging_ms"] else j["priority"]
            return -tier, Fraction(granted[j["tenant"]], weights[j["tenant"]]), j["sequence"]
        chosen = min(candidates, key=score)
        require(chosen["id"] == job["id"]
                and attempt["tenant_work_before"] == granted[job["tenant"]]
                and attempt["priority"] == -score(job)[0], "weighted fairness and age")
        granted[job["tenant"]] += job["work"]
    require(next(i for i, job in enumerate(waiting) if job["tenant"] == case["tenant_ids"][0]) <= 1,
            "aged low-priority tenant progress")


REQUIRED_CI = {"rust", "msrv", "phase22-interop", "phase23-local"}


def verify_ci_receipt(path, pin, final_sha):
    require(path is not None and DIGEST.fullmatch(pin or "") and final_sha is not None,
            "external exact-SHA CI receipt and detached pin required")
    require(re.fullmatch(r"[0-9a-f]{40}", final_sha), "invalid final SHA")
    receipt = load(path)
    require(digest_file(path) == pin, "CI receipt detached pin mismatch")
    require(receipt.get("version") == 1 and receipt.get("repository")
            == "jasonkli21/adaptive-dataflow-engine"
            and receipt.get("commit_sha") == final_sha
            and isinstance(receipt.get("run_id"), int) and receipt["run_id"] > 0
            and receipt.get("workflow") == "ci"
            and receipt.get("workflow_path") == ".github/workflows/ci.yml"
            and receipt.get("event") == "push" and receipt.get("ref") == "refs/heads/main"
            and receipt.get("run_url")
            == f'https://github.com/{receipt["repository"]}/actions/runs/{receipt["run_id"]}'
            and isinstance(receipt.get("created_at"), str)
            and isinstance(receipt.get("completed_at"), str), "CI receipt provenance or SHA")
    try:
        created = datetime.fromisoformat(receipt["created_at"].replace("Z", "+00:00"))
        completed = datetime.fromisoformat(receipt["completed_at"].replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("CI receipt timestamps") from error
    require(created.tzinfo is not None and completed.tzinfo is not None and completed >= created,
            "CI receipt chronology")
    checks = receipt.get("checks")
    require(isinstance(checks, list) and len(checks) == len(REQUIRED_CI), "CI check inventory")
    rows = {check.get("name"): check for check in checks if isinstance(check, dict)}
    require(set(rows) == REQUIRED_CI and all(row.get("conclusion") == "success"
            and row.get("status") == "completed" and row.get("head_sha") == final_sha
            for row in rows.values()), "required CI checks")
    require(receipt.get("run_status") == "completed" and receipt.get("run_conclusion") == "success",
            "CI run incomplete or unsuccessful")
    return receipt


def phase_status(support, mode="historical-archive", ci_receipt=None, ci_pin=None, final_sha=None):
    require(list(sorted(support["gates"])) == sorted(f"G{i}" for i in range(18)), "gate coverage")
    statuses = {g["status"] for g in support["gates"].values()}
    require(statuses <= {"PASS", "FAIL", "PARTIAL", "NOT_RUN"}, "unknown gate status")
    final = support["status"]
    require((final == "COMPLETE_BOUNDED_LOCAL_MULTITENANT_MANAGER_V1") == (statuses == {"PASS"}),
            "support status conflicts with gates")
    if support["gates"]["G15"]["status"] == "PASS":
        if mode == "current-source" or final_sha is not None:
            receipt = verify_ci_receipt(ci_receipt, ci_pin, final_sha)
            require(support.get("ci_receipt_sha256") == digest_file(ci_receipt),
                    "support CI receipt digest mismatch")
            require(receipt["commit_sha"] == final_sha, "support final SHA mismatch")


def verify_semantics(root, decoder=None, mode="historical-archive", ci_receipt=None, ci_pin=None,
                    final_sha=None):
    fixture, support = load(root / "fixture.json"), load(root / "support.json")
    phase_status(support, mode, ci_receipt, ci_pin, final_sha)
    cases = load(root / "cases.json") + load(root / "enospc.json")
    names = {c["name"] for c in cases}
    require(len(names) == len(cases), "duplicate cases")
    require(names <= CASES, "unknown cases")
    claimed = {name for data in support["gates"].values() if data["status"] == "PASS"
               for name in data.get("cases", [])}
    require(claimed <= names, "required live cases missing")
    if support["status"].startswith("COMPLETE") or support.get("full_local_campaign") is True:
        require(names == CASES, "required live cases missing")
    for case in cases:
        name = case["name"]
        verify_processes(case)
        results = verify_outputs(case, fixture, root, decoder)
        # Recompute every attempt visible in terminal/nonterminal receipts,
        # including cancellation and quarantine, rather than trusting only
        # successful result events.
        seen_job_receipts = {}
        for event in case["events"]:
            witnesses = []
            if isinstance(event.get("status"), dict) and "attempts" in event["status"]:
                witnesses.append(event["status"])
            if isinstance(event.get("cancelled"), dict) and "attempts" in event["cancelled"]:
                witnesses.append(event["cancelled"])
            for job in witnesses:
                canonical = json.dumps(job, sort_keys=True, separators=(",", ":"))
                prior = seen_job_receipts.setdefault(job["id"], canonical)
                require(prior == canonical, "conflicting logical job receipts")
                require(1 <= len(job["attempts"]) <= 2 or job["state"] in
                        ("queued", "cancelled", "failed", "expired"), "attempt count")
                for attempt in job["attempts"]:
                    verify_attempt(job, attempt, case["cohorts"][attempt["cohort"]],
                                   case["config"]["price"])
        if name == "auth":
            findings = Counter((e["check"], e.get("code")) for e in case["events"] if "code" in e)
            require(all(findings[key, code] == n for key, (code, n) in AUTH.items()), "auth probes")
            idem = one(case, "idempotency")
            require(len(idem["same"]) == 4 and len(set(idem["same"])) == 1
                    and idem["foreign"] != idem["same"][0], "tenant-scoped idempotency")
            require({r["kind"] for r in results} == {"aggregate", "join", "empty", "boundary"},
                    "shape coverage")
            require(any(r["tenant"] == 0 and r["status"]["id"] == idem["same"][0] for r in results)
                    and any(r["tenant"] == 1 and r["status"]["id"] == idem["foreign"] for r in results),
                    "tenant/result binding")
        if name == "overlap":
            event = one(case, "overlap")
            jobs = event["observation"]["ledger"]["jobs"]
            require(len(jobs) == 2 and all(j["state"] == "running" for j in jobs)
                    and {j["attempts"][0]["cohort"] for j in jobs} == {0, 1}
                    and {w for c in case["cohorts"] for w in c["pids"]}
                    <= {p["pid"] for p in event["sample"]["processes"]}, "actual overlap")
            require([on for _, on in one(case, "standby")["transitions"]] == [True, False]
                    and len(results) == 2, "standby transitions")
        if name == "preemption":
            event = one(case, "preemption")
            a = next(r["status"] for r in results if r["tenant"] == 0)
            before, after = a["attempts"]
            require(before["usage"]["outcome"] == "preempted"
                    and before["native_id"] == event["old_native"]
                    and after["native_id"] == event["new_native"]
                    and before["native_id"] != after["native_id"]
                    and before["instance"] != after["instance"] and event["worker_stage"] == "producer",
                    "native running preemption")
        if name == "commit_race":
            event = one(case, "commit_race")
            require(event["reached"] == "ADE_OPS_REACHED marker_published"
                    and event["cancelled"]["intent"] == "cancel" and len(results) == 1
                    and results[0]["status"] == event["terminal"]
                    and len(event["terminal"]["attempts"]) == 1, "commit/cancel winner")
        if name == "quarantine":
            event = one(case, "quarantine")
            require(event["after"]["state"] == "quarantined" and event["reservation"]["running"] == 1
                    and event["after"]["attempts"][0]["usage"] is None and len(results) == 1,
                    "quarantine holds capacity")
        if name.startswith("crash_"):
            event = one(case, "crash")
            label = "ADE_OPS_REACHED " if name.startswith(("crash_aggregate_", "crash_join_")) else "ADE_RESOURCE_REACHED "
            require(event["reached"] == label + event["stage"] and event["killed_exit"] == -9
                    and case["launches"] == (3 if event["stage"] == "snapshot_before_rename" else 2)
                    and name.endswith(event["stage"]),
                    "reached crash and restart")
            require(event.get("job_id") == event["status"].get("id"), "crash logical identity")
            require(event["status"]["state"] in ("succeeded", "failed"),
                    "crash terminal state is not justified")
            require(len(results) >= 1, "recovery oracle")
        if name == "quotas":
            q = one(case, "quota_release")
            require(q["totals"]["running"] == q["totals"]["slots"] == 0
                    and q["cancelled"]["state"] == "cancelled", "quota release")
            refusal = {event["check"]: event["code"] for event in case["events"] if "code" in event}
            for dimension in ("managed_bytes", "staging_bytes", "slots", "retained_bytes",
                              "cumulative_write_bytes", "cost_microunits"):
                require(refusal["tighten_" + dimension] == "QUOTA_IN_USE", "quota tightening")
            require(refusal["queue_full"] == "QUEUE_FULL" and refusal["stale_price"] == "PRICE_PINNED",
                    "bounded queue and price")
        if name == "history":
            h = one(case, "history")
            require(h["native_count"] == 32
                    and h["native_history_refusal"]["code"] == "NATIVE_HISTORY_FULL",
                    "native history bounds")
        if name == "corruption":
            require(Counter(e["code"] for e in case["events"] if e["check"] == "reopen_refusal")
                    == {"CLOCK_ROLLBACK":1, "CORRUPT_LEDGER":1, "PRIVATE_FILE":3}, "reopen refusals")
        if name == "cohort_identity":
            require(one(case, "cohort_lease")["sibling_refusal"].endswith("COHORT_LEASE_BUSY"),
                    "sibling exclusion")
            boot = one(case, "boot_replacement")
            require(boot["old"] == case["cohorts"][0]["workers"][0]["instance"]
                    and boot["old"] != boot["new"] and boot["status"]["state"] == "quarantined"
                    and boot["status"]["reason"] == "WORKER_IDENTITY", "boot replacement")
        if name.startswith("fairness_"):
            verify_fairness(case)
        if name.startswith("measure_"):
            measure = one(case, "measure")
            require(len(results) == 10 and len(measure["samples"]) == 20
                    and measure["elapsed_ns"] > 0, "raw measurement")
            if measure["mode"] == "fifo":
                require(all(r["status"]["attempts"][0]["cohort"] == 0 for r in results), "FIFO control")
        if name.startswith("enospc_"):
            event = one(case, "enospc")
            require(event["available_bytes"] == 0 and event["independent_device"]
                    and event["errno"] == 28 and [e["errno"] for e in event["fill_errors"]] == [28] * 4,
                    "real filesystem exhaustion")
    return len(cases)


def verify(root, pin, mode, source=None, bin_dir=None, ci_receipt=None, ci_pin=None, final_sha=None):
    require(DIGEST.fullmatch(pin) and digest_file(root / "seal.json") == pin, "detached receipt pin")
    archive = load(root / "seal.json")
    require(RECEIPTS <= set(archive), "sealed artifact inventory")
    for name, digest in archive.items():
        require(name in RECEIPTS or re.fullmatch(r"ipc/[0-9a-f]{64}\.arrow", name),
                "unknown archive path")
        path = root / name
        require(not path.is_symlink() and path.is_file() and digest_file(path) == digest,
                "receipt digest " + name)
    references = {f'ipc/{e["ipc_sha256"]}.arrow' for c in load(root / "cases.json")
                  + load(root / "enospc.json") for e in c["events"] if e["check"] == "result"}
    require(set(archive) == RECEIPTS | references, "raw IPC inventory")
    frozen = dict(line.split()[::-1] for line in (root / "frozen.sha256").read_text().splitlines())
    require(set(frozen) == {"criteria.json", "fixture.json"}, "frozen fixture lock")
    for name, digest in frozen.items():
        require(digest == digest_file(root / name) == digest_file(ROOT / "docs/evidence/phase23" / name),
                "predeclared " + name)
    host = load(root / "host.json")
    require(host["baseline"] == load(ROOT / "docs/evidence/phase23/baseline.json")["source"],
            "baseline attribution")
    source_hashes, executables = load(root / "source-inventory.json"), load(root / "executables.json")
    require(len(source_hashes) > 100 and all(DIGEST.fullmatch(d) for d in source_hashes.values()),
            "source inventory")
    require(set(executables) == {"manager", "batch", "worker", "fixture"}
            and all(DIGEST.fullmatch(d) for d in executables.values()), "executable inventory")
    if mode == "current-source":
        from campaign import inventory
        require(source is not None and source.resolve() == ROOT and bin_dir is not None, "current-source inputs")
        require(source_hashes == inventory(), "current-source mismatch")
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()
        tree = subprocess.check_output(["git", "rev-parse", "HEAD^{tree}"], cwd=source, text=True).strip()
        dirty = subprocess.check_output(["git", "status", "--porcelain"], cwd=source, text=True).strip()
        require(final_sha is not None and host.get("candidate_sha") == final_sha == head
                and host.get("tree_sha") == tree and host.get("clean_worktree") is True
                and not dirty, "candidate/final source identity or clean tree")
        names = {"manager": "ade-resource", "worker": "ade-worker", "batch": "ade-batch",
                 "fixture": "examples/phase23-fixture"}
        require(all(digest_file(bin_dir / path) == executables[label] for label, path in names.items()),
                "binary mismatch")
    return verify_semantics(root, bin_dir / "examples/phase23-fixture" if mode == "current-source" else None,
                            mode, ci_receipt, ci_pin, final_sha)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--mode", choices=("historical-archive", "current-source"), required=True)
    parser.add_argument("--receipt-pin", required=True)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--bin-dir", type=Path)
    parser.add_argument("--ci-receipt", type=Path)
    parser.add_argument("--ci-receipt-pin")
    parser.add_argument("--final-sha")
    args = parser.parse_args()
    try:
        count = verify(args.root, args.receipt_pin, args.mode, args.source, args.bin_dir,
                       args.ci_receipt, args.ci_receipt_pin, args.final_sha)
    except (ValueError, KeyError, IndexError, TypeError, OSError, StopIteration) as error:
        parser.exit(1, f"verification refused: {error}\n")
    print(f"{args.mode}: authenticated and semantically verified {count} recorded cases")


if __name__ == "__main__":
    main()
