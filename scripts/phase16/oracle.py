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
    reusable=d.get('incremental_placements',p['placements'])
    for candidate in d['candidates']:
        expected=evaluate(p,o,candidate['action'])
        if candidate['action']['format']=='incremental' and candidate['action']['placement'] not in reusable and expected['rejection'] is None:
            expected=dict(action=candidate['action'],score=None,recovery_ms=None,rejection='self_contained_required')
        require(candidate==expected,'independent policy arithmetic/rejection mismatch')
    if d['reason'].startswith('minimum_'):
        best=min((x for x in d['candidates'] if x['score'] is not None),key=lambda x:x['score'])['action'].copy()
        # Historical source-bound logs scored an incremental choice then
        # converted it to full. Keep that old evidence replayable; new traces
        # carry an executable-placement set and score the effective action.
        if 'incremental_placements' not in d and ';self_contained_required' in d['reason']:best['format']='full'
        require(d['chosen']==best,'policy did not execute minimum admissible action')


def choose_recovery(replay_eligible,replay_cost,restore_eligible,restore_cost):
    """Independent feasibility-first recovery ranking used by new regressions."""
    if not replay_eligible and not restore_eligible:
        return None
    if replay_eligible and not restore_eligible:
        return 'full_replay_explicit_conflict_consent'
    if restore_eligible and not replay_eligible:
        return 'checkpoint_restore'
    return ('full_replay_explicit_conflict_consent'
            if replay_cost is not None and replay_cost<restore_cost
            else 'checkpoint_restore')


def check_recovery(p,record,config,remote_copy=False,saved_offsets=None):
    """Independently check feasibility-first recovery trace fields.

    Old source-bound archives omit `sources` and retain their original cost-only
    policy. New traces must provide live ranges for both source vectors.
    """
    m=record['model'];policy=p['recovery'];slo=p['constraints']['recovery_slo_ms']
    if 'sources' not in m:
        replay=m['replay_estimated_ms']*p['prices']['compute_ms']+m['confirmed_records']*p['prices']['replay_record'] if policy['allow_replay'] else None
        restore=m['restore_estimated_ms']*p['prices']['compute_ms']+m['logical_bytes']*(p['prices']['remote_read_byte'] if remote_copy else p['prices']['local_read_byte'])+(66*p['prices']['remote_request'] if remote_copy else 0)
        require(m['restore_cost_nano']==restore and m['replay_cost_nano']==replay,'legacy recovery cost proxy mismatch')
        chosen='full_replay_explicit_conflict_consent' if replay is not None and replay<restore else 'checkpoint_restore'
        require(m['chosen']==chosen==record['path'],'legacy recovery recommendation was not executed')
        return

    sources=m['sources'];expected=sorted(config['topic_partitions'])
    require(sources['cluster_id']==config['source']['expected_cluster_id'],'recovery source cluster differs')
    require(sources['partition_inventory']==expected,'recovery partition inventory differs')
    original={x['partition']:x['offset'] for x in policy['original_starts']}
    def ranges(candidate,offsets,label):
        rs=candidate['ranges'];parts=[x['partition'] for x in rs]
        if candidate['reason']=='partition_inventory':
            require(not candidate['eligible'],'partition mismatch marked eligible')
            return
        require(parts==expected and len(parts)==len(set(parts)),label+' range inventory differs')
        for row in rs:
            require(row['requested_offset']==offsets[row['partition']],label+' requested offset differs')
            require(row['low']>=0 and row['high']>=row['low'],'invalid observed watermark')
            if candidate['eligible']:
                require(row['low']<=row['requested_offset']<=row['high'],label+' eligible range excludes offset')
        require(candidate['eligible']==(candidate['reason'] is None),label+' eligibility/reason mismatch')
        if candidate['reason'] in ('origin_retention','saved_offset_retention'):
            require(any(x['low']>x['requested_offset'] for x in rs),label+' retention refusal lacks expired offset')
        elif candidate['reason']=='offset_outside_range':
            require(any(x['requested_offset']>x['high'] for x in rs),label+' range refusal lacks out-of-range offset')
        elif candidate['reason']=='invalid_watermark':
            require(any(x['low']<0 or x['high']<x['low'] for x in rs),label+' invalid-watermark refusal lacks invalid range')
        elif candidate['reason']=='watermark_unavailable':
            require(len(rs)<len(expected),label+' missing-watermark refusal has complete ranges')
    ranges(sources['replay'],original,'replay')
    restore_offsets={x['partition']:x['requested_offset'] for x in sources['restore']['ranges']}
    ranges(sources['restore'],restore_offsets,'restore')
    if saved_offsets is not None:
        require(restore_offsets==saved_offsets,'restore eligibility did not use the selected manifest vector')
    if record['path']=='checkpoint_restore' and record['progress'].get('partitions'):
        saved={int(k):v['start'] for k,v in record['progress']['partitions'].items()}
        require(restore_offsets==saved,'restore did not use the exact saved offset vector')

    replay_refusal=None
    if not policy['allow_replay']:replay_refusal='replay_ineligible_not_enabled'
    elif not policy['accept_duplicate_and_conflicting_outputs']:replay_refusal='replay_ineligible_conflict_consent'
    elif not policy['trusted_immutable_history']:replay_refusal='replay_ineligible_history_untrusted'
    elif not sources['replay']['eligible']:
        replay_refusal={
            'origin_retention':'replay_ineligible_origin_retention',
            'offset_outside_range':'replay_ineligible_offset_outside_range',
            'watermark_unavailable':'replay_ineligible_watermark_unavailable',
            'partition_inventory':'replay_ineligible_partition_inventory',
            'invalid_watermark':'replay_ineligible_invalid_watermark',
        }.get(sources['replay']['reason'],'replay_ineligible_source_range')
    elif m['replay_estimated_ms']>slo:replay_refusal='replay_ineligible_slo'
    restore_refusal=None
    if not sources['restore']['eligible']:
        restore_refusal={
            'saved_offset_retention':'restore_ineligible_saved_offset_retention',
            'offset_outside_range':'restore_ineligible_offset_outside_range',
            'watermark_unavailable':'restore_ineligible_watermark_unavailable',
            'partition_inventory':'restore_ineligible_partition_inventory',
            'invalid_watermark':'restore_ineligible_invalid_watermark',
        }.get(sources['restore']['reason'],'restore_ineligible_source_range')
    elif m['restore_estimated_ms']>slo:restore_refusal='restore_ineligible_slo'
    replay_eligible=replay_refusal is None;restore_eligible=restore_refusal is None
    require(m['replay_refusal']==replay_refusal and m['restore_refusal']==restore_refusal,'recovery refusal provenance differs')
    require(m['replay_eligible']==replay_eligible and m['restore_eligible']==restore_eligible,'recovery eligibility differs')
    replay=m['replay_estimated_ms']*p['prices']['compute_ms']+m['confirmed_records']*p['prices']['replay_record'] if replay_eligible else None
    restore=m['restore_estimated_ms']*p['prices']['compute_ms']+m['logical_bytes']*(p['prices']['remote_read_byte'] if remote_copy else p['prices']['local_read_byte'])+(66*p['prices']['remote_request'] if remote_copy else 0)
    require(m['restore_cost_nano']==restore and m['replay_cost_nano']==replay,'recovery cost proxy mismatch')
    chosen=choose_recovery(replay_eligible,replay,restore_eligible,restore)
    require(chosen is not None and m['chosen']==chosen==record['path'],'recovery recommendation was not executed')
    chosen_reason=(restore_refusal if chosen=='full_replay_explicit_conflict_consent' else replay_refusal) or 'minimum_estimated_cost'
    require(m['chosen_reason']==chosen_reason,'recovery choice reason differs')


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
