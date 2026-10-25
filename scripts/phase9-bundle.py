#!/usr/bin/env python3
"""Package successful or failed Phase 9 evidence, source archive and checksums."""
import hashlib
import json
import pathlib
import shutil
import subprocess
import sys
import tarfile
import tempfile

results, output = map(lambda s: pathlib.Path(s).resolve(), sys.argv[1:])
commit = (results/'source-commit.txt').read_text().strip()
verification = json.loads((results/'verification.json').read_text()) if (results/'verification.json').exists() else dict(passed=False,correctness_passed=False,error='suite stopped before verification')
with tempfile.TemporaryDirectory(prefix='ade-phase9-bundle-') as temporary:
    stage = pathlib.Path(temporary)/'phase9-evidence'
    shutil.copytree(results,stage)
    with (stage/'source.tar').open('wb') as stream:
        subprocess.run(['git','archive','--format=tar',commit],stdout=stream,check=True)
    source_identity=json.loads((stage/'source-identity.json').read_text())
    source_hash=hashlib.sha256((stage/'source.tar').read_bytes()).hexdigest()
    if source_identity['source_commit'] != commit or source_identity['source_archive_sha256'] != source_hash:
        raise SystemExit('source archive does not match the measured implementation identity')
    (stage/'implementation-identity.json').write_text(json.dumps(source_identity,indent=2)+'\n')
    # Preserve the exact snapshot commit as Git objects, so reviewers can rebuild
    # with the original clean repository identity rather than inventing a new SHA.
    subprocess.run(['git','bundle','create',str(stage/'source.bundle'),'--all',commit],check=True)
    inventory = {str(p.relative_to(stage)): hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in sorted(stage.rglob('*')) if p.is_file()}
    (stage/'inventory.json').write_text(json.dumps(dict(source_commit=commit,
        empirical_passed=verification.get('passed',False),sha256=inventory),indent=2)+'\n')
    output.parent.mkdir(parents=True,exist_ok=True)
    with tarfile.open(output,'w:gz') as archive:
        archive.add(stage,arcname=stage.name)
checksum=hashlib.sha256(output.read_bytes()).hexdigest()
output.with_suffix(output.suffix+'.sha256').write_text(f'{checksum}  {output.name}\n')
print(json.dumps(dict(archive=str(output),sha256=checksum,source_commit=commit,
                     empirical_passed=verification.get('passed',False))))
