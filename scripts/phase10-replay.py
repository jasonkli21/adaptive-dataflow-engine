#!/usr/bin/env python3
"""Replay the historical remediation bundle read-only with its OWN verifiers.

Requires trusted source/binaries on their original platform. SHA256 proves byte
integrity, not signed host attestation. Output is a new directory; artifacts in
the archive are never rewritten, including verification.json.
"""
import argparse
import hashlib
import json
import os
import pathlib
import subprocess
import tarfile
import tempfile


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def extract(archive, destination):
    with tarfile.open(archive) as source:
        for entry in source.getmembers():
            path = pathlib.PurePosixPath(entry.name)
            if path.is_absolute() or '..' in path.parts or not (entry.isfile() or entry.isdir()):
                raise ValueError('unsafe archive member: ' + entry.name)
        source.extractall(destination)


def replay(archive, expected_sha256, output):
    if sha(archive) != expected_sha256:
        raise ValueError('outer archive checksum mismatch')
    output.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix='ade-phase10-replay-') as directory:
        temporary = pathlib.Path(directory)
        extract(archive, temporary)
        root = temporary / 'phase10-remediation-20261026'
        inventory = json.loads((root / 'sha256.json').read_text())
        def check_inventory():
            for name, digest in inventory.items():
                path = pathlib.PurePosixPath(name)
                if path.is_absolute() or '..' in path.parts or sha(root / name) != digest:
                    raise ValueError('inventory mismatch: ' + name)
        check_inventory()
        results = {}
        for release, candidate in [('release-1', 'candidate'), ('release-2-ready-3', 'candidate-2')]:
            source = temporary / (release + '-source')
            source.mkdir()
            extract(root / release / 'source.tar', source)
            env = dict(os.environ, ADE_BENCH_BIN=str(root / candidate / 'bin/ade-bench'),
                       PYTHONDONTWRITEBYTECODE='1')
            # Calling verify(), rather than its CLI, leaves every archived byte unchanged.
            code = ("import importlib.util,json,pathlib,sys;sys.path.insert(0,sys.argv[1]);"
                    "s=importlib.util.spec_from_file_location('archived',sys.argv[1]+'/phase10-verify.py');"
                    "m=importlib.util.module_from_spec(s);s.loader.exec_module(m);"
                    "print(json.dumps(m.verify(pathlib.Path(sys.argv[2])),indent=2))")
            command = ['python3', '-c', code, str(source / 'scripts'), str(root / release)]
            completed = subprocess.run(command, env=env, text=True, capture_output=True, check=False)
            (output / (release + '-stdout.json')).write_text(completed.stdout)
            (output / (release + '-stderr.log')).write_text(completed.stderr)
            if completed.returncode:
                raise RuntimeError('archived verification failed: ' + completed.stderr)
            result = json.loads(completed.stdout)
            if result != json.loads((root / release / 'verification.json').read_text()):
                raise ValueError('replay differs from original: ' + release)
            results[release] = {k: v for k, v in result.items() if k != 'summaries'}
        check_inventory()
        results['archive'] = dict(sha256=expected_sha256, entries=len(inventory), unchanged_after_replay=True)
        (output / 'replay.json').write_text(json.dumps(results, indent=2) + '\n')
        return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive', type=pathlib.Path)
    parser.add_argument('output', type=pathlib.Path)
    parser.add_argument('--sha256', required=True)
    arguments = parser.parse_args()
    print(json.dumps(replay(arguments.archive.resolve(), arguments.sha256, arguments.output.resolve()), indent=2))
