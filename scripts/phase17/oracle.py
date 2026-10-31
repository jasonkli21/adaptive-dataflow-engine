"""Independent ordered source/window/state oracle and manifest reader. Stdlib only."""
import copy,json,pathlib,struct,uuid
from digest import digest,raw
from service_fixture import require

def fingerprint(c):
    s=c['state'];src=s['source']
    native=dict(version=1,job=s['job_id'],schema='EventV1/u64/i64/i64',operator=s['operator'],width=s['window_ms'],late=s['late_policy'],hash=s['hash_version'],cluster=src['expected_cluster_id'],topic=src['topic'],partitions=[v['partition'] for v in src['starts']],topic_partitions=s['topic_partitions'],key_less_than=src['key_less_than'],allowance=src['allowance_ms'],history='immutable-uncompressed-32768-v1')
    encode=lambda value:json.dumps(value,separators=(',',':'),sort_keys=True).encode()
    return digest(encode(dict(version=3,native=digest(encode(native)),starts=src['starts'],history=c['trusted_history'])))

def shard(key):return raw(struct.pack('<Q',key))[0]&31
def checked(value):require(-(1<<63)<=value<(1<<63),'sum/time overflow');return value
class Model:
    def __init__(self,c):
        self.c=c;self.state={};self.outputs=[];self.total=0;self.late=0;self.closed_late=0;self.frontier=None
        self.cursors={s['partition']:dict(next=s['offset'],maximum=None,watermark=None,forwarded=None) for s in c['state']['source']['starts']}
    def consume(self,item,ledger):
        p,off=item['partition'],item['offset'];source=self.c['state']['source'];e=ledger[(p,off)];cursor=self.cursors[p]
        require(off>=cursor['next'],'oracle backward offset');time=e['event_time_ms'];width=self.c['state']['window_ms'];start=(time//width)*width;end=checked(start+width)
        self.late+=int(cursor['watermark'] is not None and time<cursor['watermark'])
        if e['key']<source['key_less_than']:
            if self.frontier is not None and end<=self.frontier:self.closed_late+=1
            else:
                key=(start,e['key']);old=self.state.get(key,(0,0));self.state[key]=(checked(old[0]+e['value_i64']),old[1]+1)
        cursor.update(next=off+1,forwarded=off,maximum=max(time,cursor['maximum'] if cursor['maximum'] is not None else time))
        cursor['watermark']=checked(cursor['maximum']-source['allowance_ms']);self.total+=1
        waters=[v['watermark'] for v in self.cursors.values()];self.frontier=None if None in waters else min(waters)
        if self.frontier is not None:
            for start,key in sorted(list(self.state)):
                if start+width<=self.frontier:
                    amount,count=self.state.pop((start,key));job=self.c['state']['job_id']
                    self.outputs.append(dict(output_version=1,job_id=job,key=key,window_start=start,window_end=start+width,sum_i64=amount,count_u64=count,output_id=f'{job}:{key}:{start}:{start+width}:1',payload_fingerprint=f'window-payload-v1:{amount}:{count}'))
    def compare(self,m,entries,outputs):
        p=m['progress'];require(p['forwarded_records']==self.total and p['late_records']==self.late and p['frontier']==self.frontier and m['closed_window_late']==self.closed_late,'oracle progress/frontier/late differs')
        for part,cursor in self.cursors.items():
            actual=p['partitions'][str(part)];require(actual['next_offset_suggestion']==cursor['next'] and actual['watermark']==cursor['watermark'] and actual['max_forwarded_time']==cursor['maximum'] and actual['forwarded']==cursor['forwarded'],'oracle partition cursor differs')
        require(entries==self.state,'full hot/cold logical state differs');require(outputs==self.outputs,'exact committed output payload/history differs')

def chain(root,config,pointer=None):
    root=pathlib.Path(root);p=pointer or json.loads((root/'CURRENT3').read_text());result=[]
    while p:
        require(len(result)<config['limits']['epochs'] and p['version']==3,'invalid chain/cap');uuid.UUID(p['id']);require('/' not in p['id'],'unsafe id')
        folder=root/p['id'];b=(folder/'epoch.json').read_bytes();require(digest(b)==p['blake3'],'epoch hash mismatch');m=json.loads(b)
        require(m['id']==p['id'] and m['epoch']==p['epoch'] and m['version']==3,'epoch identity mismatch');require(m['fingerprint']==fingerprint(config) and m['job_id']==config['state']['job_id'],'independent semantic/source fingerprint mismatch')
        if m['parent']:require(m['parent']['epoch']+1==m['epoch'],'broken parent')
        else:require(m['epoch']==1,'missing parent')
        entries={};canonical=bytearray()
        for s,ref in enumerate(m['shards']):
            b=(folder/f'shard-{s:02}.bin').read_bytes();require(len(b)==ref['object']['bytes'] and digest(b)==ref['object']['blake3'],'shard hash mismatch')
            require(ref['shard']==s and ref['owner']==s%m['owners'],'owner mismatch');n=64 if m['codec']==1 else 80;require(len(b)==ref['count']*n,'shard count');previous=None
            for offset in range(0,len(b),n):
                record=b[offset:offset+n];require(raw(record[:-32])==record[-32:],'record checksum');start,key,amount,count=struct.unpack('<qQqQ',record[:32]);require(shard(key)==s and count>0 and (previous is None or previous<(start,key)),'ownership/order/count');previous=(start,key)
                if n==80:require(record[40:48]==b'ADE3REC2' and struct.unpack('<q',record[32:40])[0]==start+config['state']['window_ms'],'actual codec2 transform invalid')
                logical=record[:32];canonical+=logical+raw(logical);entries[(start,key)]=(amount,count)
        require(digest(canonical)==m['state_blake3'],'logical state digest')
        b=(folder/'output.jsonl').read_bytes();require(len(b)==m['output']['bytes'] and digest(b)==m['output']['blake3'] and (not b or b.endswith(b'\n')),'output digest/frame')
        outputs=[json.loads(line) for line in b.splitlines()];result.append((m,entries,outputs));p=m['parent']
    result.reverse();history=[];ids={}
    for m,entries,outputs in result:
        for o in outputs:
            require(o['output_id'] not in ids,'duplicate/conflicting committed output');ids[o['output_id']]=o['payload_fingerprint']
        history+=outputs
    require(sum(len(json.dumps(o)) for o in history)<=config['limits']['history_bytes']*2,'history bound')
    return result

def verify_trace(root,config,ledgers,attempts,observed,pointer=None):
    cuts=chain(root,config,pointer);model=Model(config);history=[];topic=config['state']['source']['topic'];ledger={(r['partition'],r['offset']):r['event'] for r in ledgers[topic]}
    for m,entries,outputs in cuts:
        session=m['progress']['session'];trace=attempts.get(session,[]);target=m['progress']['forwarded_records']
        inputs=[r for r in trace if r.get('type')=='input' and model.total<r['sequence']<=target]
        for r in inputs:require(r['sequence']==model.total+1,'trace gap');model.consume(r,ledger)
        history+=outputs;model.compare(m,entries,history)
    require(history==observed,'separate committed reader differs from manifest and oracle')
    return dict(cuts=len(cuts),records=model.total,outputs=len(history),state_entries=len(model.state),codec=cuts[-1][0]['codec'],owners=cuts[-1][0]['owners'])
