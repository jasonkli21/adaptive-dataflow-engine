"""Verify original Parquet bytes and compute the canonical projected bag directly."""

from pathlib import Path

import blake3
import pyarrow.parquet as pq

from common import encode, read, sha


def blake_file(path):
    digest = blake3.blake3()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def fingerprint(rows):
    rows.sort()
    return blake3.blake3(encode({"Integer": rows}).encode()).hexdigest()


def verify_fixtures(root, criteria, pins, *, collect_pins=False):
    expected = {}
    actual_pins = {}
    cases = criteria["macro_shapes"]
    for distribution in dict.fromkeys(case[0] for case in cases):
        prefix = Path(distribution)
        manifest = read(root / prefix / "manifest.json")
        config = manifest["synthetic"]
        if (manifest["dataset_id"] != "phase25-" + distribution
            or manifest["row_count"] != criteria["dataset_rows"]
            or config["rows"] != criteria["dataset_rows"]
            or config["seed"] != criteria["seed"]
            or config["key_cardinality"] != criteria["key_cardinality"]
            or config["distribution"] != distribution
            or config["row_group_rows"] != criteria["row_group_rows"]
            or len(manifest["files"]) != criteria["dataset_files"]):
            raise ValueError("fixture configuration")
        manifest_path = prefix / "manifest.json"
        actual_pins[str(manifest_path)] = sha(root / manifest_path)
        thresholds = sorted({case[2] for case in cases if case[0] == distribution})
        answers = {threshold: [] for threshold in thresholds}
        next_id = 0
        for index, file in enumerate(manifest["files"]):
            relative = f"part-{index:05}.parquet"
            path = root / prefix / relative
            if (file["relative_path"] != relative or path.is_symlink()
                    or path.stat().st_size != file["bytes"]
                    or blake_file(path) != file["fingerprint"]):
                raise ValueError("original Parquet byte binding")
            actual_pins[str(prefix / relative)] = sha(path)
            for batch in pq.ParquetFile(path).iter_batches(
                    columns=["row_id", "secondary", "key", "value_i64"]):
                for row, value, key, payload in zip(*(arr.to_pylist() for arr in batch.columns)):
                    if row != next_id or value is None or not 0 <= value < 1000:
                        raise ValueError("fixture row identity or predicate domain")
                    next_id += 1
                    for threshold in thresholds:
                        if value < threshold:
                            answers[threshold].append((key, payload))
        if next_id != criteria["dataset_rows"]:
            raise ValueError("fixture row count")
        for threshold, rows in answers.items():
            expected[distribution, threshold] = (len(rows), fingerprint(rows))
    if not collect_pins and pins != actual_pins:
        raise ValueError("fixture external SHA-256 inventory")
    return expected, actual_pins
