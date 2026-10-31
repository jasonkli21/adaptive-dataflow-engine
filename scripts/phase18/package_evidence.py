#!/usr/bin/env python3
"""Package immutable portable evidence after independent semantic verification."""
import argparse,hashlib,io,json,pathlib,tarfile
from verify_evidence import verify

def package(campaign,destination,qualified=True):
    destination.mkdir(parents=True,exist_ok=False);result=verify(campaign) if qualified else dict(status='FAILED_OR_PREMEASUREMENT_PRESERVED');files={}
    for path in sorted(campaign.rglob('*')):
        rel=path.relative_to(campaign)
        if rel.parts[0] in ['broker-data','minio-data','fixture'] or rel.parts[0].endswith('-pinned') or path.is_symlink():continue
        if path.is_file():files[str(rel)]=path
    archive=destination/'acceptance.tar.gz'
    with tarfile.open(archive,'w:gz') as tar:
        for name,path in files.items():
            b=path.read_bytes();m=tarfile.TarInfo(name);m.size=len(b);m.mode=0o600;tar.addfile(m,io.BytesIO(b))
    manifest=dict(version=1,qualified=qualified,archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),files={n:hashlib.sha256(p.read_bytes()).hexdigest() for n,p in files.items()},result=result);(destination/'manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n');return manifest
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--campaign',required=True,type=pathlib.Path);p.add_argument('--destination',required=True,type=pathlib.Path);p.add_argument('--preserve-unqualified',action='store_true');a=p.parse_args();m=package(a.campaign,a.destination,not a.preserve_unqualified);print(json.dumps(dict(result=m['result'],archive_sha256=m['archive_sha256'],files=len(m['files'])),sort_keys=True))
