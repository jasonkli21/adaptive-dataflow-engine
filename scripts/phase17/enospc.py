#!/usr/bin/env python3
"""Actual ENOSPC on an owned disposable HFS+ disk image; never formats a real disk."""
import argparse,errno,hashlib,json,pathlib,shutil,subprocess,tempfile,os
from service_fixture import private,dump,require

def run(a):
    root=a.output.resolve();private(root);mount=private(root/'mount');image=root/'owned-only.dmg';commands=[]
    def call(cmd,label,expected=0):
        commands.append(dict(label=label,argv=list(map(str,cmd))));dump(root/'commands.json',commands)
        result=subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=60);(root/(label+'.stdout')).write_bytes(result.stdout);(root/(label+'.stderr')).write_bytes(result.stderr);require(result.returncode==expected,f'{label} failed {result.returncode}: {result.stderr[-1000:]}');return result.stdout
    binary=a.campaign/'txn-pinned';env=json.loads((a.campaign/'environment.json').read_text());require(hashlib.sha256(binary.read_bytes()).hexdigest()==env['binaries']['txn'],'matched binary hash')
    call(['/usr/bin/hdiutil','create','-size','16m','-fs','HFS+','-volname','ADE17_DISPOSABLE','-type','UDIF',str(image)],'create-image')
    attached=False
    try:
        call(['/usr/bin/hdiutil','attach','-nobrowse','-mountpoint',str(mount),str(image)],'attach-owned-image');attached=True
        c=json.loads((a.campaign/'atomic-seed.config3.json').read_text());retired=json.loads(pathlib.Path(c['state']['checkpoint_root'],'CURRENT3').read_text())['blake3'];c['state']['checkpoint_root']=str(private(mount/'check'));c['state']['working_root']=str(private(root/'work'))
        cfg=root/'config3.json';dump(cfg,c);trace=[json.loads(line) for line in (a.campaign/'atomic-export.stderr').read_text().splitlines() if line.startswith('{')];cap=next(r for r in trace if r.get('status')=='EXPORTED')
        require(retired==json.loads((pathlib.Path(cap['path'])/'SAVEPOINT3').read_text())['selected']['blake3'],'source savepoint is stale')
        call([str(binary),str(cfg),'import',cap['path'],cap['blake3'],retired],'import')
        check=pathlib.Path(c['state']['checkpoint_root']);before=(check/'CURRENT3').read_bytes();old=call([str(binary),str(cfg),'read'],'reader-before')
        stats_before=os.statvfs(mount);dump(root/'statvfs-before.json',dict(blocks=stats_before.f_blocks,available=stats_before.f_bavail,block_size=stats_before.f_frsize))
        filler=mount/'owned-filler.bin';written=0;failure=None
        with open(filler,'wb',buffering=0) as file:
            try:
                while True:written+=file.write(b'x'*65536)
            except OSError as error:
                failure=error.errno;(root/'fill-error.stderr').write_text(repr(error))
            if failure==errno.ENOSPC:
                try:
                    while True:written+=file.write(b'x'*4096)
                except OSError as error:
                    failure=error.errno;(root/'fill-error.stderr').write_text(repr(error))
        require(failure==errno.ENOSPC,'image did not reach genuine ENOSPC')
        stats_full=os.statvfs(mount);dump(root/'statvfs-full.json',dict(blocks=stats_full.f_blocks,available=stats_full.f_bavail,block_size=stats_full.f_frsize))
        call([str(binary),str(cfg),'transition','2','2'],'transition-refused',expected=1)
        require((check/'CURRENT3').read_bytes()==before,'ENOSPC advanced selector');require(call([str(binary),str(cfg),'read'],'reader-after')==old,'ENOSPC changed committed output')
        shutil.copytree(check,root/'selected-and-orphans')
        dump(root/'outcome.json',dict(status='ACTUAL_ENOSPC_REFUSED',errno=failure,filler_bytes=written,filesystem='HFS+ owned 16MiB image',binary_sha256=env['binaries']['txn'],selector_sha256=hashlib.sha256(before).hexdigest(),savepoint_blake3=cap['blake3'],retired_predecessor_head=retired,source_campaign=str(a.campaign),objects=len(list(check.rglob('*')))))
    finally:
        if attached:call(['/usr/bin/hdiutil','detach',str(mount)],'detach-owned-image')
    print((root/'outcome.json').read_text())
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--campaign',required=True,type=pathlib.Path);p.add_argument('--output',required=True,type=pathlib.Path);run(p.parse_args())
