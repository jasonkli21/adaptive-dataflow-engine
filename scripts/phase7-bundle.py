#!/usr/bin/env python3
"""Package a verified Phase 7 run as a checksummed, self-identifying archive."""
import hashlib
import json
import pathlib
import shutil
import sys
import tarfile
import tempfile


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def main():
    if len(sys.argv) != 3:
        raise SystemExit('usage: phase7-bundle.py RESULTS_DIR OUTPUT.tar.gz')
    results = pathlib.Path(sys.argv[1]).resolve()
    output = pathlib.Path(sys.argv[2]).resolve()
    if not results.is_dir():
        raise SystemExit(f'results directory not found: {results}')

    commit_path = results / 'source-commit.txt'
    verification_path = results / 'verification.json'
    if not commit_path.is_file() or not verification_path.is_file():
        raise SystemExit('results must contain source-commit.txt and verification.json')
    commit = commit_path.read_text().strip()
    verification = json.loads(verification_path.read_text())
    if not verification.get('passed') or verification.get('source_commit') != commit:
        raise SystemExit('verification must pass and match source-commit.txt')
    if verification.get('object_request_cost_scope') != 'put_head_get_proxy_v1':
        raise SystemExit('verification must declare the Phase 7 request-cost scope')

    run_files = sorted(results.glob('*-result.json'))
    if not run_files:
        raise SystemExit('results contain no measured result files')
    identity = None
    for run_file in run_files:
        run = json.loads(run_file.read_text())
        repository = run.get('repository', {})
        if repository.get('git_commit') != commit or repository.get('git_dirty'):
            raise SystemExit(f'{run_file.name} does not identify the clean source commit')
        current = {
            'build': run.get('build'),
            'environment': run.get('environment'),
            'workload': run.get('workload'),
            'dataset_manifest_fingerprint': run.get('dataset_manifest_fingerprint'),
        }
        if identity is None:
            identity = current
        elif current != identity:
            raise SystemExit(f'{run_file.name} has a different benchmark identity')

    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='ade-phase7-bundle-') as temporary:
        stage = pathlib.Path(temporary) / 'phase7-evidence'
        stage.mkdir()
        files = [path for path in results.rglob('*') if path.is_file()]
        for source in files:
            if source.name == '.DS_Store':
                continue
            destination = stage / source.relative_to(results)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)

        inventory = []
        for path in sorted(stage.rglob('*')):
            if path.is_file():
                inventory.append({
                    'path': str(path.relative_to(stage)),
                    'size_bytes': path.stat().st_size,
                    'sha256': sha256(path),
                })
        manifest = {
            'schema_version': 1,
            'source_commit': commit,
            'verification': verification,
            'benchmark_identity': identity,
            'files': inventory,
        }
        (stage / 'bundle-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        checksums = []
        for path in sorted(stage.rglob('*')):
            if path.is_file():
                checksums.append(f'{sha256(path)}  {path.relative_to(stage)}')
        (stage / 'SHA256SUMS').write_text('\n'.join(checksums) + '\n')

        with tarfile.open(output, 'w:gz') as archive:
            archive.add(stage, arcname='phase7-evidence')

    digest = sha256(output)
    output.with_suffix(output.suffix + '.sha256').write_text(f'{digest}  {output.name}\n')
    print(json.dumps({
        'archive': str(output),
        'sha256': digest,
        'source_commit': commit,
        'file_count': len(inventory),
    }, indent=2))


if __name__ == '__main__':
    main()
