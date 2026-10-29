#!/usr/bin/env python3
"""Validate a read-only S3 ListObjectsV2 snapshot for one Phase 13 attempt.

This tool never connects to a provider and never deletes anything. Its input is
the JSON response from an operator-authorized, exact-prefix list operation.
"""
import argparse
import json
import re
import uuid
from pathlib import Path


MAX_SEGMENTS_PER_PRODUCER = 256
MAX_PRODUCER_BYTES = 16 * 1024 * 1024
MAX_TERMINAL_BYTES = 64 * 1024
MAX_MANIFEST_BYTES = 1024 * 1024
MAX_SEGMENT_BYTES = 1024 * 1024


def require(ok, message):
    if not ok:
        raise ValueError(message)


def inventory(document, prefix, execution, workers):
    require(re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", prefix) is not None,
            "prefix must be one canonical component")
    try:
        parsed = uuid.UUID(execution)
    except (ValueError, AttributeError) as error:
        raise ValueError("execution must be a canonical non-nil UUID") from error
    require(str(parsed) == execution and parsed.int != 0,
            "execution must be a canonical non-nil UUID")
    require(workers in (2, 3, 4), "workers must be 2, 3, or 4")
    require(isinstance(document, dict), "ListObjectsV2 response must be an object")
    require(not document.get("IsTruncated", False) and not document.get("NextToken"),
            "listing is truncated; request a fresh bounded inventory")
    contents = document.get("Contents", [])
    require(isinstance(contents, list), "Contents must be a list")

    namespace = f"{prefix}/remote-v1/{execution}/attempt-0/"
    max_objects = 2 + workers * workers + workers * MAX_SEGMENTS_PER_PRODUCER
    require(len(contents) <= max_objects, "namespace object count exceeds bounded v1 cap")

    seen = set()
    classes = {"capability": [], "manifest": [], "terminal": [], "segment": []}
    per_producer_segments = [0] * workers
    per_producer_bytes = [0] * workers
    total_bytes = 0
    terminal_pairs = set()
    segment_ids = set()
    for row in contents:
        require(isinstance(row, dict), "object row must be an object")
        key, size = row.get("Key"), row.get("Size")
        require(isinstance(key, str) and key.startswith(namespace),
                "listed key escaped the exact attempt namespace")
        require(key not in seen, "duplicate current object key")
        seen.add(key)
        require(isinstance(size, int) and not isinstance(size, bool) and size > 0,
                "object size must be a positive integer")
        relative = key[len(namespace):]
        total_bytes += size

        if relative == "CAPABILITY":
            require(size == len(b"create-only-v1"), "unexpected capability probe size")
            classes["capability"].append(key)
            continue
        if relative == "MANIFEST.json":
            require(size <= MAX_MANIFEST_BYTES, "manifest exceeds bounded v1 cap")
            classes["manifest"].append(key)
            continue

        terminal = re.fullmatch(r"p-(0|[1-9][0-9]*)/from-(0|[1-9][0-9]*)/TERMINAL\.json", relative)
        segment = re.fullmatch(
            r"p-(0|[1-9][0-9]*)/from-(0|[1-9][0-9]*)/segment-(0|[1-9][0-9]*)\.arrow",
            relative,
        )
        if terminal:
            partition, producer = map(int, terminal.groups())
            require(partition < workers and producer < workers,
                    "terminal identity is outside the fixed topology")
            require(size <= MAX_TERMINAL_BYTES, "terminal exceeds bounded v1 cap")
            terminal_pairs.add((partition, producer))
            per_producer_bytes[producer] += size
            classes["terminal"].append(key)
            continue
        if segment:
            partition, producer, sequence = map(int, segment.groups())
            require(partition < workers and producer < workers,
                    "segment identity is outside the fixed topology")
            require(size <= MAX_SEGMENT_BYTES, "segment exceeds bounded v1 cap")
            identity = (partition, producer, sequence)
            require(identity not in segment_ids, "duplicate segment identity")
            segment_ids.add(identity)
            per_producer_segments[producer] += 1
            per_producer_bytes[producer] += size
            classes["segment"].append(key)
            continue
        raise ValueError("unknown key in exact attempt namespace; leave quarantined")

    require(len(classes["manifest"]) <= 1 and len(classes["capability"]) <= 1,
            "duplicate singleton object")
    require(all(count <= MAX_SEGMENTS_PER_PRODUCER for count in per_producer_segments),
            "producer segment count exceeds bounded v1 cap")
    require(all(size <= MAX_PRODUCER_BYTES for size in per_producer_bytes),
            "producer object bytes exceed bounded v1 cap")
    require(total_bytes <= workers * MAX_PRODUCER_BYTES + MAX_MANIFEST_BYTES + 14,
            "attempt total bytes exceed bounded v1 cap")

    return {
        "schema_version": 1,
        "status": "READ_ONLY_INVENTORY",
        "namespace": namespace,
        "execution": execution,
        "workers": workers,
        "object_count": len(seen),
        "total_current_object_bytes": total_bytes,
        "per_producer_segment_counts": per_producer_segments,
        "per_producer_current_object_bytes": per_producer_bytes,
        "classified_keys": {name: sorted(keys) for name, keys in classes.items()},
        "terminal_pairs_present": len(terminal_pairs),
        "global_manifest_present": bool(classes["manifest"]),
        "automatic_delete": "NO",
        "cleanup_eligibility": "NOT_ESTABLISHED_BY_INVENTORY",
        "operator_disposition": "QUARANTINED_UNTIL_ACTORS_FENCED_AND_PROVIDER_IO_QUIESCED",
        "versioning_and_multipart": "NOT_INVENTORIED",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("response", type=Path, help="saved S3 ListObjectsV2 JSON response")
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--execution", required=True, help="exact UUID from ADE_REMOTE_ATTEMPT")
    parser.add_argument("--workers", type=int, required=True)
    args = parser.parse_args()
    result = inventory(json.loads(args.response.read_text()), args.prefix, args.execution, args.workers)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
