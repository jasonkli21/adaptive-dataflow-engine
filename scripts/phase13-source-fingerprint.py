#!/usr/bin/env python3
"""Hash the committed Phase 13 remediation tree, excluding its own summary."""
import hashlib
import json
import subprocess
from pathlib import Path


SUMMARY = 'docs/reports/phase-13-review-remediation-evidence.json'


def run(*args):
    return subprocess.run(args, check=True, stdout=subprocess.PIPE).stdout


def fingerprint(root=Path('.')):
    root = root.resolve()
    paths = run('git', '-C', str(root), 'ls-files', '-z').split(b'\0')
    paths = sorted(path.decode('utf-8') for path in paths if path and path.decode('utf-8') != SUMMARY)
    digest = hashlib.sha256()
    for path in paths:
        content = run('git', '-C', str(root), 'cat-file', 'blob', f'HEAD:{path}')
        name = path.encode('utf-8')
        digest.update(len(name).to_bytes(8, 'big'))
        digest.update(name)
        digest.update(hashlib.sha256(content).digest())
    head = run('git', '-C', str(root), 'rev-parse', 'HEAD').decode().strip()
    tree = run('git', '-C', str(root), 'rev-parse', 'HEAD^{tree}').decode().strip()
    return {
        'head': head,
        'tree': tree,
        'tracked_files_excluding_summary': len(paths),
        'tracked_inputs_sha256': digest.hexdigest(),
        'excluded_self_referential_summary': SUMMARY,
    }


if __name__ == '__main__':
    print(json.dumps(fingerprint(), indent=2, sort_keys=True))
