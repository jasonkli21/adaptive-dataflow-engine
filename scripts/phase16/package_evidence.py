#!/usr/bin/env python3
"""Package raw witnesses and exact source; excludes broker data and executable copies."""
import argparse, hashlib, json, pathlib, tarfile
from verify_evidence import verify

p=argparse.ArgumentParser();p.add_argument('--campaign',type=pathlib.Path,required=True);p.add_argument('--destination',type=pathlib.Path,required=True);a=p.parse_args()
result=verify(a.campaign.resolve());a.destination.mkdir(parents=True,exist_ok=False)
files={};symlinks={}
for path in sorted(a.campaign.rglob('*')):
    relative=path.relative_to(a.campaign)
    if relative.parts[0] in ('broker-data','minio-data','fixture','ade-state-stream-pinned'):continue
    if path.is_symlink():symlinks[str(relative)]=str(path.readlink());continue
    if path.is_file():files[str(relative)]=path
# Safety attacks retain their requested target as inert text, never dereference/extract it.
inert=a.destination/'symlink-attacks.json';inert.write_text(json.dumps(symlinks,indent=2,sort_keys=True));files['symlink-attacks.json']=inert
archive=a.destination/'acceptance.tar.gz'
with tarfile.open(archive,'w:gz',compresslevel=9) as tar:
    for name,path in sorted(files.items()):tar.add(path,arcname=name,recursive=False)
manifest=dict(version=1,archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),files={name:hashlib.sha256(path.read_bytes()).hexdigest() for name,path in sorted(files.items())})
(a.destination/'manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')
print(json.dumps(dict(**result,archive_sha256=manifest['archive_sha256'],files=len(files)),sort_keys=True))
