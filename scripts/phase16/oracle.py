"""Independent arithmetic/state contract evaluator. Does not import ADE code."""
import importlib.util, json, pathlib, struct
_spec=importlib.util.spec_from_file_location('phase15_contract',pathlib.Path(__file__).resolve().parents[1]/'phase15/oracle.py')
_module=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(_module)
Model,require,rows=_module.Model,_module.require,_module.rows


def evaluate(p,o,action):
    c=p['constraints'];place=action['placement'];fmt=action['format'];copies=2 if place=='both' else 1
    result=dict(action=action,score=None,recovery_ms=None,rejection=None)
    loss=c[place+'_loss_ppm']
    if copies<c['min_copies'] or c['require_remote'] and place=='local' or loss>c['max_loss_ppm']:result['rejection']='durability_floor';return result
    if place not in p['placements']:result['rejection']='placement_disabled';return result
    if fmt=='incremental' and (o['depth']>=p['max_depth'] or o['changed_shards']==32):result['rejection']='full_required';return result
    if o['changed_bytes']>o['logical_bytes'] or o['changed_shards']>32:result['rejection']='invalid_observation';return result
    ceil=lambda a,b:(a+b-1)//b
    physical=o['logical_bytes'] if fmt=='full' else o['changed_bytes']
    objects=(32 if fmt=='full' else o['changed_shards'])+4
    depth=0 if fmt=='full' else o['depth']+1
    requests=objects*3+66*(depth+1)
    if place!='remote' and physical>o['local_available_bytes'] or place!='local' and (physical+65560>o['remote_available_bytes'] or objects>o['remote_available_objects'] or requests>o['remaining_requests'] or requests*1048576>o['remaining_response_bytes']):result['rejection']='capacity_floor';return result
    local=place!='remote';remote=place!='local';prices=p['prices']
    restore=ceil(o['logical_bytes']*1000,o['local_bytes_per_second'] if local else o['remote_bytes_per_second'])
    recovery=restore+ceil(action['records']*o['replay_ns_per_record'],1000000)
    result['recovery_ms']=recovery
    if recovery>c['recovery_slo_ms']:result['rejection']='recovery_slo';return result
    write=(ceil(o['logical_bytes']*1000,o['scan_bytes_per_second'])+(ceil(physical*1000,o['local_bytes_per_second']) if local else 0)+(ceil(physical*1000,o['remote_bytes_per_second']) if remote else 0))*prices['compute_ms']
    if local:write+=physical*prices['local_write_byte']
    if remote:write+=physical*prices['remote_write_byte']+((32 if fmt=='full' else o['changed_shards'])*3+6)*prices['remote_request']
    write+=o['logical_bytes']*copies*c['retention_ms']*prices['capacity_byte_ms']
    score=ceil(write*1000000,action['records'])+c['failure_ppm']*(ceil(action['records'],2)*prices['replay_record']+restore*prices['compute_ms'])
    require(score<2**64,'score overflow');result['score']=score;return result


def check_decision(p,d):
    o=dict(d['observation']);calibrated=o['samples']>=p['min_samples'] and all(o[x]>0 for x in ('scan_bytes_per_second','replay_ns_per_record')) and (not any(x!='remote' for x in p['placements']) or o['local_bytes_per_second']>0) and (not any(x!='local' for x in p['placements']) or o['remote_bytes_per_second']>0)
    if not calibrated:o.update(scan_bytes_per_second=1000,local_bytes_per_second=1000,remote_bytes_per_second=1000,replay_ns_per_record=1000000)
    require(d['model_inputs']==o,'policy input provenance differs')
    for candidate in d['candidates']:require(candidate==evaluate(p,o,candidate['action']),'independent policy arithmetic/rejection mismatch')
    if d['reason'].startswith('minimum_'):
        best=min((x for x in d['candidates'] if x['score'] is not None),key=lambda x:x['score'])['action'].copy()
        if ';self_contained_required' in d['reason']:best['format']='full'
        require(d['chosen']==best,'policy did not execute minimum admissible action')


def check_snapshot(root,manifest,model,get):
    model.check_progress(manifest['progress'])
    require(manifest['version']==2 and len(manifest['shards'])==32 and manifest['pending_closures']==0 and not model.due(),'v2 format/quiescence')
    require(manifest['closed_window_late']==model.closed_late,'closed late mismatch')
    state={};physical=0
    for index,meta in enumerate(manifest['shards']):
        require(meta['shard']==index and meta['owner']==index%manifest['owners'],'duplicate owner/shard')
        raw=get(meta['object_generation'],f'shard-{index:02}.bin')
        require(len(raw)==meta['bytes']==64*meta['count'],'shard bytes/count')
        previous=None
        for off in range(0,len(raw),64):
            start,key,amount,count=struct.unpack('<qQqQ',raw[off:off+32]);identity=(start,key)
            require(identity not in state and (previous is None or previous<identity),'duplicate/unsorted windows')
            state[identity]=(amount,count);previous=identity
        if meta['object_generation']==manifest['generation']:physical+=len(raw)
    require(state==model.state and len(state)==manifest['windows'],'canonical v2 state differs from producer-ledger prefix')
    return physical
