#!/usr/bin/env python3
"""Verify historical evidence against the explicitly pinned baseline Git objects."""
import argparse,hashlib,json,pathlib,subprocess
BASE='fbe0e2b7739fd7d61c45c58706faa56b63a945be'
def verify(repo):
    lines=subprocess.check_output(['git','ls-tree','-r',BASE,'--','docs/evidence'],cwd=repo,text=True).splitlines();inventory={}
    for line in lines:
        meta,name=line.split('\t',1);blob=meta.split()[2];path=repo/name
        actual=subprocess.check_output(['git','hash-object',str(path)],cwd=repo,text=True).strip()
        if actual!=blob:raise ValueError('historical evidence changed: '+name)
        h=hashlib.sha256()
        with path.open('rb') as f:
            for b in iter(lambda:f.read(1048576),b''):h.update(b)
        inventory[name]=dict(git_blob=blob,sha256=h.hexdigest(),bytes=path.stat().st_size)
    return dict(base=BASE,files=inventory)
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--repo',type=pathlib.Path,default=pathlib.Path(__file__).resolve().parents[2]);p.add_argument('--output',type=pathlib.Path,required=True);a=p.parse_args();r=verify(a.repo);a.output.write_text(json.dumps(r,indent=2,sort_keys=True)+'\n');print(json.dumps(dict(historical_files_unchanged=len(r['files']))))
