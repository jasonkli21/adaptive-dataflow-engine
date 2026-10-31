"""Independent semantic inference, choice arithmetic and ordered Kafka oracle."""
import copy,json,pathlib,sys,struct
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]/'phase17'))
from digest import digest,raw
from service_fixture import require,rows
import importlib.util
_spec=importlib.util.spec_from_file_location('phase17_reference',pathlib.Path(__file__).resolve().parents[1]/'phase17/oracle.py')
_ref=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(_ref)
Model,chain,verify_trace=_ref.Model,_ref.chain,_ref.verify_trace

def ordered(v,keys):return {k:v[k] for k in keys}
def canonical(p):
    c=p['computation'];kind=c['kind']
    if kind=='kafka':
        c=ordered(c,['kind','source','job','key_less_than','allowance_ms','projection','window_ms','late_policy','hash_version','timers'])
        s=ordered(c['source'],['cluster','topic','topic_partitions','original_starts','immutable_history','mode']);c['source']=s
        s['original_starts']=[ordered(o,['partition','offset']) for o in s['original_starts']]
        mode=s['mode'];s['mode']=ordered(mode,['kind']+(['cut'] if mode['kind']=='history_then_live' else []))
        if 'cut' in s['mode']:s['mode']['cut']=[ordered(o,['partition','offset']) for o in s['mode']['cut']]
        c['timers']=[ordered(t,['kind','deadline_ms']) for t in c['timers']]
    else:
        c=ordered(c,['kind','sources','operators','max_output_rows','max_output_bytes'])
        c['max_output_rows']=1000000;c['max_output_bytes']=67108864
        c['sources']=[ordered(s,['id','table','manifest','logical','schema_identity','schema','files','replayable']) for s in c['sources']]
        for s in c['sources']:
            s['schema']=[ordered(f,['name','data_type','nullable']) for f in s['schema']]
            s['files']=[ordered(f,['name','bytes','rows','fingerprint']) for f in s['files']]
        keys={'scan':['source'],'filter':['input','column','less_than'],'project':['input','columns'],'group_sum_count':['input','key','value'],'sort':['input','key','row_id','value'],'limit':['input','rows'],'inner_join':['left','right','left_key','right_key','left_value','right_value']}
        c['operators']=[ordered(o,['op']+keys[o['op']]) for o in c['operators']]
        for o in c['operators']:
            if o['op']=='project':o['columns']=[ordered(x,['column','alias']) for x in o['columns']]
    return json.dumps(dict(version=p['version'],computation=c,sink=p['sink']),separators=(',',':'),ensure_ascii=False).encode()
def field(name,ty,nullable=False):return dict(name=name,data_type=ty,nullable=nullable)
def keys(value,expected):require(set(value)==set(expected),'noncanonical/unknown plan fields')
def strict(p):
    keys(p,['version','computation','sink']);require(type(p['version']) is int and p['version']==1,'logical version');c=p['computation']
    if c['kind']=='kafka':
        keys(c,['kind','source','job','key_less_than','allowance_ms','projection','window_ms','late_policy','hash_version','timers']);s=c['source'];keys(s,['cluster','topic','topic_partitions','original_starts','immutable_history','mode'])
        for token,cap in [(s['cluster'],128),(s['topic'],128),(s['immutable_history'],256),(c['job'],128)]:
            require(isinstance(token,str) and 0<len(token.encode())<=cap and token not in ['.','..'] and all(ch.isascii() and (ch.isalnum() or ch in '._:-') for ch in token),'source/job/history token')
        require(type(c['allowance_ms']) is int and 0<=c['allowance_ms']<2**63 and type(c['key_less_than']) is int and 0<=c['key_less_than']<2**64 and type(c['hash_version']) is int,'numeric semantics')
        require(c['window_ms'] is None or type(c['window_ms']) is int and 0<c['window_ms']<2**63,'window integer')
        require(all(type(part) is int for part in s['topic_partitions']),'partition integer')
        require(1<=len(s['topic_partitions'])<=4 and s['topic_partitions']==list(range(len(s['topic_partitions']))),'topic inventory property')
        starts=s['original_starts'];assigned=[o['partition'] for o in starts];require(1<=len(starts)<=4 and assigned==sorted(set(assigned)) and all(p in s['topic_partitions'] for p in assigned),'assigned subset property')
        for o in starts:keys(o,['partition','offset']);require(type(o['partition']) is int and type(o['offset']) is int and 0<=o['offset']<2**63-1,'original offset')
        mode=s['mode'];require(mode['kind'] in ['assigned','history_then_live'],'finite Kafka range refused');keys(mode,['kind']+(['cut'] if mode['kind']=='history_then_live' else []))
        if mode['kind']=='history_then_live':
            require(p['sink']!='committed_local_v3' and [o['partition'] for o in mode['cut']]==assigned,'cut/delivery property')
            for end,start in zip(mode['cut'],starts):keys(end,['partition','offset']);require(type(end['partition']) is int and type(end['offset']) is int and start['offset']<=end['offset']<2**63-1,'exclusive cut')
        require(c['allowance_ms']>=0 and c['hash_version']==1 and c['projection']==['value_i64','key','event_time_ms'],'time/hash/projection property')
        require((c['window_ms'] is None and c['late_policy'] is None) or (c['window_ms']>0 and c['late_policy']=='receipt_drop_closed_v1'),'window/late property')
        require(p['sink'] in ['receipts','committed_local_v3'] and (p['sink']!='committed_local_v3' or c['window_ms'] is not None),'delivery property')
        require(len(c['timers'])<=64 and (c['window_ms'] is None or not c['timers']),'timer property')
        for t in c['timers']:keys(t,['kind','deadline_ms']);require(t['kind'] in ['event','processing'] and (t['kind']!='processing' or t['deadline_ms']>=0),'timer deadline')
    else:
        keys(c,['kind','sources','operators','max_output_rows','max_output_bytes']);require(c['kind']=='parquet_manifest' and p['sink']=='collect' and 1<=len(c['sources'])<=2 and 1<=len(c['operators'])<=64,'finite tree/property bounds')
        for s in c['sources']:
            keys(s,['id','table','manifest','logical','schema_identity','schema','files','replayable'])
            for f in s['schema']:keys(f,['name','data_type','nullable'])
            for f in s['files']:keys(f,['name','bytes','rows','fingerprint'])
        operands={'scan':['source'],'filter':['input','column','less_than'],'project':['input','columns'],'group_sum_count':['input','key','value'],'sort':['input','key','row_id','value'],'limit':['input','rows'],'inner_join':['left','right','left_key','right_key','left_value','right_value']}
        for o in c['operators']:
            require(o['op'] in operands,'operator property');keys(o,['op']+operands[o['op']])
            if o['op']=='project':
                for v in o['columns']:keys(v,['column','alias'])

def decision(p,d):
    strict(p);c=p['computation'];require(d['semantic_digest']==digest(canonical(p)),'independent semantic digest')
    props=d['properties'];require(props['deterministic_for_observed_trace'] is True,'determinism property')
    if c['kind']=='kafka':
        state=c['window_ms'] is not None
        schema=[field('key','u64'),field('window_start','i64'),field('window_end','i64'),field('sum_i64','i64'),field('count_u64','u64')] if state else [field('value_i64','i64'),field('key','u64'),field('event_time_ms','i64')]
        require(props==dict(termination='non_terminating',schema=schema,ordering=[],deterministic_for_observed_trace=True,replay='operator_attested_trace_relative',phase='history_then_live' if c['source']['mode']['kind']=='history_then_live' else 'live',event_time='signed_ms_receipts_including_filtered',frontier='min_confirmed_unknown_until_all_no_idle_advance',distribution='fixed_assigned_partitions',delivery='local_committed_prefix_v3' if p['sink']=='committed_local_v3' else 'at_least_once_receipts',stateful=state),'stream property lattice')
        legal={'native_kafka':0};choices=['native_kafka'];n=0
    else:
        schemas=[];orders=[];ops=c['operators'];join=any(o['op']=='inner_join' for o in ops)
        for i,o in enumerate(ops):
            require(all(o.get(k,-1)<i for k in ['input','left','right']),'cycle/dangling')
            op=o['op'];order=[]
            if op=='scan':schema=c['sources'][o['source']]['schema']
            elif op=='project':
                schema=[dict(schemas[o['input']][v['column']],name=v['alias']) for v in o['columns']]
                try:order=[next(j for j,v in enumerate(o['columns']) if v['column']==k) for k in orders[o['input']]]
                except StopIteration:order=[]
            elif op=='group_sum_count':schema=[field('key','u64'),field('sum','i64',True),field('count','i64')]
            elif op=='inner_join':schema=[field('key','u64'),field('left_value','i64'),field('right_value','i64')]
            elif op=='sort':schema=[schemas[o['input']][o[k]] for k in ['key','row_id','value']];order=[0,1]
            else:schema=schemas[o['input']];order=orders[o['input']]
            schemas.append(schema);orders.append(order)
        require(props==dict(termination='finite',schema=schemas[-1],ordering=orders[-1],deterministic_for_observed_trace=True,replay='native_join_no_recovery' if join else 'immutable_manifest',phase='finite',event_time='ordinary_column',frontier='finite_eof',distribution='join_key' if join else 'single_or_fixed_hash',delivery='validated_finite_result',stateful=False),'finite property lattice')
        n=sum(f['rows'] for s in c['sources'] for f in s['files']);legal={}
        if not join:legal['local']=n*4
        base=next((o for o in reversed(ops) if o['op']!='project'),None)
        if d['controls']['distributed_available'] and 2<=d['controls']['workers']<=4:
            if join:legal['distributed_join']=20000+n*4
            if base and base['op']=='group_sum_count' and ops[base['input']]['op']=='scan' and all(o['op'] in ['scan','group_sum_count','project'] for o in ops) and len(c['sources'])==1 and c['sources'][0]['schema'][base['key']]['name']=='key' and c['sources'][0]['schema'][base['value']]['name']=='value_i64':legal['distributed_aggregate']=20000+n*2
        choices=['local','distributed_aggregate','distributed_join']
    require(d['source_rows']==n and len(d['candidates'])==len(choices)<=3,'candidate/search bound')
    ctrl=d['controls']
    for choice in list(legal):
        if ctrl['forced'] is not None and ctrl['forced']!=choice:del legal[choice]
        elif not ctrl['enabled'] and choice!=ctrl['static_choice']:del legal[choice]
    for choice,entry in zip(choices,d['candidates']):
        require(entry['choice']==choice,'candidate order')
        require((entry['rejection'] is None)==(choice in legal),'independent legality')
        require(entry['estimated_units']==legal.get(choice),'integer proxy score')
    require(d['chosen']==min(legal,key=lambda k:(legal[k],choices.index(k))),'independent selected choice')
    return props

def physical_digest(plan,choice):
    c=plan['computation'];canonical_comp=json.loads(canonical(plan))['computation'];nodes=[]
    for i,o in enumerate(canonical_comp['operators']):
        op=o['op'];props=dict(nodes[o.get('input',o.get('left',0))]['properties']) if op!='scan' else dict(bounded=True,deterministic=True,replayable=c['sources'][o['source']]['replayable'],ordering=[],distribution='single')
        if op=='scan':schema=c['sources'][o['source']]['schema']
        elif op=='project':
            schema=[dict(nodes[o['input']]['schema'][v['column']],name=v['alias']) for v in o['columns']]
            try:props['ordering']=[next(j for j,v in enumerate(o['columns']) if v['column']==k) for k in props['ordering']]
            except StopIteration:props['ordering']=[]
            if choice!='local':props['distribution']='gather'
        elif op=='group_sum_count':
            schema=[field('key','u64'),field('sum','i64',True),field('count','i64')];props['ordering']=[]
            if choice=='distributed_aggregate':props['distribution']={'hash':[0]}
        elif op=='inner_join':schema=[field('key','u64'),field('left_value','i64'),field('right_value','i64')];props.update(replayable=False,ordering=[],distribution={'hash_or_broadcast':0})
        elif op=='sort':schema=[nodes[o['input']]['schema'][o[k]] for k in ['key','row_id','value']];props['ordering']=[0,1]
        else:schema=nodes[o['input']]['schema']
        nodes.append(dict(id=i,operator=o,schema=[ordered(f,['name','data_type','nullable']) for f in schema],properties=props))
    physical=dict(version=1,target=choice,sources=canonical_comp['sources'],nodes=nodes,output=len(nodes)-1,max_output_rows=c['max_output_rows'],max_output_bytes=c['max_output_bytes'])
    return digest(json.dumps(physical,separators=(',',':'),ensure_ascii=False).encode())

def native_binding(plan,config,mode):
    c=plan['computation'];src=c['source'];state=mode!='stateless';native=config['source'] if state else config
    require(src['cluster']==native['expected_cluster_id'] and src['topic']==native['topic'] and src['original_starts']==native['starts'],'logical/native source identity differs')
    require(c['key_less_than']==native['key_less_than'] and c['allowance_ms']==native['allowance_ms'] and c['timers']==native['timers'],'logical/native transform differs')
    require(plan['sink']==('committed_local_v3' if mode=='v3' else 'receipts'),'logical/native delivery differs')
    if state:require(src['topic_partitions']==config['topic_partitions'] and c['window_ms']==config['window_ms'] and c['late_policy']==config['late_policy'] and c['hash_version']==config['hash_version']==1 and config['operator']=='tumbling_sum_count_v1','logical/native state semantics differ')
    else:require(c['window_ms'] is None and c['late_policy'] is None,'state silently lowered to stateless')

def replay(root,case,ledgers,initial=None):
    root=pathlib.Path(root);c=json.loads((root/case['config']).read_text());state=case['mode']!='stateless';source=c['source'] if state else c
    config=dict(state=c if state else dict(source=source,job_id='stateless',window_ms=100))
    model=copy.deepcopy(initial) if initial else Model(config)
    model.c=config
    if not hasattr(model,'history'):model.history=[]
    ledger={(r['partition'],r['offset']):r['event'] for r in ledgers[source['topic']]}
    trace=rows(root/(case['name']+'.stderr'));out=rows(root/(case['name']+'.stdout'));audits=[r['record'] for r in trace if r.get('type')=='unified_confirmed'];inputs=[];expected_windows=[]
    admissions=[r for r in trace if r.get('type')=='admitted'];require(len(admissions)==1,'one native source/session admission');admitted=admissions[0];session=admitted['session']
    require(admitted['progress']['forwarded_records']==model.total,'restore reset confirmed progress')
    if initial is not None:require(session!=initial.session,'restart reused attempt session')
    starts={part:cur['next'] for part,cur in model.cursors.items()}
    for part,start in starts.items():require(admitted['progress']['partitions'][str(part)]['start']==start,'original/saved source start differs')
    acknowledgments=[r['progress'] for r in trace if r.get('type')=='forwarded'];model.session=session

    for frame in out:
        if frame.get('type')=='window':
            require(expected_windows,'window emitted without frontier obligation');expected=expected_windows.pop(0);actual=dict(frame);actual.pop('type');require(actual==expected,'independent native window payload');continue
        require(not expected_windows,'input crossed unsettled window closure')
        receipt=frame['receipt'] if frame.get('type')=='input' else frame
        part,off=receipt['partition'],receipt['offset'];e=ledger[(part,off)];cur=model.cursors[part]
        require(receipt['session']==session,'cut replaced native source session')
        require(receipt['event_time_ms']==e['event_time_ms'] and receipt['late']==(cur['watermark'] is not None and e['event_time_ms']<cur['watermark']),'receipt time/late differs from original producer')
        expected=None if e['key']>=source['key_less_than'] else ordered(e,['value_i64','key','event_time_ms'])
        require(receipt['projected']==expected,'native filter/project differs from producer')
        before=model.closed_late;old_outputs=len(model.outputs);model.consume(dict(partition=part,offset=off),ledger)
        require(len(inputs)<len(acknowledgments),'missing actual sink acknowledgment')
        ack=acknowledgments[len(inputs)];require(ack['session']==session and ack['forwarded_records']==model.total and ack['frontier']==model.frontier and ack['late_records']==model.late,'intermediate ACK/frontier differs')
        for part_id,cursor in model.cursors.items():
            actual=ack['partitions'][str(part_id)];require(actual['start']==starts[part_id] and actual['next_offset_suggestion']==cursor['next'] and actual['watermark']==cursor['watermark'] and actual['forwarded']==cursor['forwarded'] and actual['seen']==cursor['forwarded'],'ACK cursor/reseek differs')
        if state:
            require(frame['closed_window_late']==(model.closed_late>before),'closed-window receipt differs')
            expected_windows+=model.outputs[old_outputs:]
        inputs.append((part,off));model.history.append((part,off))
    require(not expected_windows,'window closure omitted')
    require(len(acknowledgments)==len(inputs),'unpaired acknowledgment')
    terminal=next((r for r in reversed(trace) if r.get('type')=='terminal'),None)
    require(terminal is not None,'missing terminal diagnostics')
    progress=terminal['progress'];require(progress['forwarded_records']==model.total and progress['late_records']==model.late and progress['frontier']==model.frontier,'sink-confirmed progress mismatch')
    for part,cur in model.cursors.items():
        actual=progress['partitions'][str(part)];require(actual['next_offset_suggestion']==cur['next'] and actual['watermark']==cur['watermark'],'confirmed cursor mismatch')
    if case.get('logical'):
        p=json.loads((root/case['logical']).read_text());d=next(r['decision'] for r in trace if r.get('type')=='unified_decision');decision(p,d);native_binding(p,c,case['mode'])
        require(len(audits)==len(inputs),'per-record classification coverage')
        for a,(part,off) in zip(audits,inputs):
            src=p['computation']['source'];cut={v['partition']:v['offset'] for v in src['mode'].get('cut',[])}
            require(a==dict(semantic_digest=d['semantic_digest'],partition=part,offset=off,phase='historical' if off<cut.get(part,-1) else 'live'),'actual offset-cut classification')
    return model,inputs

def checkpoint(root,case,model):
    c=json.loads((root/case['config']).read_text());folder=root/case['snapshot'];p=json.loads((folder/'CURRENT').read_text());b=(folder/p['generation']/'manifest.json').read_bytes();require(digest(b)==p['manifest_blake3'],'checkpoint manifest hash');m=json.loads(b);entries={}
    if model.total!=m['progress']['forwarded_records']:
        history=model.history;fresh=Model(dict(state=c));ledger={(r['partition'],r['offset']):r['event'] for r in json.loads((root/'ledgers.json').read_text())[c['source']['topic']]}
        for part,off in history[:m['progress']['forwarded_records']]:fresh.consume(dict(partition=part,offset=off),ledger)
        model=fresh
    for shard in m['shards']:
        sid=shard['shard'];path=folder/p['generation']/f'shard-{sid:02}.bin';b=path.read_bytes()
        require(digest(b)==shard['blake3'] and len(b)==shard['bytes'],'checkpoint shard hash')
        for off in range(0,len(b),64):
            record=b[off:off+64];require(raw(record[:32])==record[32:],'state checksum');start,key,amount,count=struct.unpack('<qQqQ',record[:32]);require(raw(struct.pack('<Q',key))[0]&31==sid,'state ownership');entries[(start,key)]=(amount,count)
    model.compare(m,entries,model.outputs)
