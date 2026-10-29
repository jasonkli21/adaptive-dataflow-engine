import pathlib,tarfile,io,json,hashlib,tempfile,subprocess,sys
repo=pathlib.Path.cwd();archive=repo/'docs/evidence/phase15/acceptance.tar.gz';manifest=json.loads((repo/'docs/evidence/phase15/manifest.json').read_text());members={}
with tarfile.open(archive,'r:gz') as tar:
 for item in tar:members[item.name]=(tar.extractfile(item).read(),item.mode)
with tempfile.TemporaryDirectory(prefix='ade15-archive-attacks-') as temp:
 root=pathlib.Path(temp)
 for name in ['negative-output','window-sum','version','byte-hash']:
  changed=dict(members);descriptor=json.loads(json.dumps(manifest));dest=root/(name+'.tar.gz')
  if name=='negative-output':changed['wrong-job.output.jsonl']=(b'{}\n',0o600)
  if name=='window-sum':
   frames=[json.loads(line) for line in changed['final-close.output.jsonl'][0].splitlines()]
   next(row for row in frames if row['type']=='window')['sum_i64']+=1
   changed['final-close.output.jsonl']=(''.join(json.dumps(row)+'\n' for row in frames).encode(),0o600)
  if name in ['negative-output','window-sum']:
   with tarfile.open(dest,'w:gz') as tar:
    for path,(data,mode) in sorted(changed.items()):
     item=tarfile.TarInfo(path);item.size=len(data);item.mode=mode;tar.addfile(item,io.BytesIO(data))
   descriptor['files']={path:hashlib.sha256(data).hexdigest() for path,(data,mode) in sorted(changed.items())}
   descriptor['archive_sha256']=hashlib.sha256(dest.read_bytes()).hexdigest()
  elif name=='version':dest.write_bytes(archive.read_bytes());descriptor['version']=2
  else:
   data=bytearray(archive.read_bytes());data[len(data)//2]^=1;dest.write_bytes(data)
  mp=root/(name+'.json');mp.write_text(json.dumps(descriptor));result=subprocess.run([sys.executable,'scripts/phase15/verify_evidence.py','--archive',str(dest),'--manifest',str(mp)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
  required={'negative-output':'wrong-job output before full validation','window-sum':'window oracle mismatch','version':'archive identity/version','byte-hash':'archive identity/version'}[name]
  if result.returncode==0 or required not in result.stderr:raise RuntimeError((name,result.returncode,result.stdout,result.stderr[-1000:]))
  print('REFUSED',name,required,flush=True)
