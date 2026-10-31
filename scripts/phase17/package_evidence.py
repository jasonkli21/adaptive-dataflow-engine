#!/usr/bin/env python3
"""Freeze raw observations and exact sources. Omit disposable services and executables."""
import argparse,hashlib,json,pathlib,tarfile
from verify_evidence import verify
from service_fixture import dump

def package(campaign,destination,qualified=True):
    campaign=campaign.resolve();destination.mkdir(parents=True,exist_ok=False)
    result=verify(campaign) if qualified else dict(status='FAILED_OR_PREMEASUREMENT_PRESERVED')
    files={};attacks={}
    for p in sorted(campaign.rglob('*')):
        rel=p.relative_to(campaign)
        if rel.parts[0] in ['broker-data','minio-data','fixture'] or rel.parts[0].endswith('-pinned'):continue
        if p.is_symlink():attacks[str(rel)]=str(p.readlink());continue
        if p.is_file():files[str(rel)]=p
    inert=destination/'symlink-attacks.json';dump(inert,attacks);files['symlink-attacks.json']=inert
    archive=destination/'acceptance.tar.gz'
    with tarfile.open(archive,'w:gz',compresslevel=9) as tar:
        for name,p in sorted(files.items()):
            # Add bytes explicitly, so a hardlink attack remains an inert ordinary file.
            b=p.read_bytes();info=tarfile.TarInfo(name);info.size=len(b);info.mode=0o600
            import io
            tar.addfile(info,io.BytesIO(b))
    manifest=dict(version=3,qualified=qualified,archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),files={name:hashlib.sha256(p.read_bytes()).hexdigest() for name,p in sorted(files.items())},result=result)
    dump(destination/'manifest.json',manifest);return manifest
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--campaign',required=True,type=pathlib.Path);p.add_argument('--destination',required=True,type=pathlib.Path);p.add_argument('--preserve-unqualified',action='store_true');a=p.parse_args();m=package(a.campaign,a.destination,not a.preserve_unqualified);print(json.dumps(dict(result=m['result'],files=len(m['files']),archive_sha256=m['archive_sha256']),sort_keys=True))
