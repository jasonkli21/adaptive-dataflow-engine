#!/usr/bin/env python3
"""Package a raw Phase10 outcome, including failed or zero-measurement attempts."""
import hashlib,json,pathlib,sys,tarfile
root=pathlib.Path(sys.argv[1]).resolve()
files={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(root.rglob('*')) if p.is_file() and p.name!='sha256.json'}
(root/'sha256.json').write_text(json.dumps(files,indent=2)+'\n')
output=root.with_name(root.name+'-bundle.tar.gz')
with tarfile.open(output,'w:gz') as archive:archive.add(root,arcname=root.name)
print(json.dumps(dict(path=str(output),sha256=hashlib.sha256(output.read_bytes()).hexdigest(),files=len(files))))
