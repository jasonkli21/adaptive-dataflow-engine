#!/usr/bin/env python3
"""Verify portable Phase14 source/log archive without extracting arbitrary paths."""
import argparse, hashlib, io, json, pathlib, tarfile

if not __debug__:
    raise SystemExit('Evidence checks require Python assertions enabled; do not use -O/PYTHONOPTIMIZE.')


def verify(path, manifest):
    expected = json.loads(manifest.read_text())
    data = path.read_bytes()
    assert hashlib.sha256(data).hexdigest() == expected['archive_sha256'], 'archive identity'
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:gz') as archive:
        actual = {}
        for member in archive.getmembers():
            p = pathlib.PurePosixPath(member.name)
            assert not p.is_absolute() and '..' not in p.parts, 'unsafe archive path'
            assert member.isfile(), 'only regular evidence files admitted'
            assert member.name not in actual and member.size <= 16*1024*1024, 'duplicate/large file'
            actual[member.name] = archive.extractfile(member).read()
    assert set(actual) == set(expected['files']), 'archive coverage'
    for name, digest in expected['files'].items():
        assert hashlib.sha256(actual[name]).hexdigest() == digest, name
    result = json.loads(actual[expected['acceptance_result']])
    assert result['status'] == 'PASS' and all(c['status'] == 'PASS' for c in result['cases'])
    assert len({c['pid'] for c in result['cases']}) == len(result['cases']), 'fresh reader processes'
    source_prefix = expected['acceptance_result'].rsplit('/', 1)[0]
    inventory = json.loads(actual[source_prefix+'/source-inventory.json'])
    with tarfile.open(fileobj=io.BytesIO(actual[source_prefix+'/source.tar.gz']), mode='r:gz') as source:
        members = {m.name:m for m in source.getmembers()}
        assert set(members) == set(inventory), 'source inventory coverage'
        for name, digest in inventory.items():
            member = members[name]
            p = pathlib.PurePosixPath(name)
            assert member.isfile() and not p.is_absolute() and '..' not in p.parts
            assert hashlib.sha256(source.extractfile(member).read()).hexdigest() == digest, name
    labels = {c['case'] for c in result['cases']}
    required = {'native','middle-replay','legal-gap','quiet','sasl-ssl','bad-credentials','backpressure',
        'cancel-poll','cancel-stall','epipe','kill','kill-replay','bad-schema','oversize','underflow',
        'out-of-range','missing-topic','missing-partition','wrong-cluster','remote-plaintext','timer-cap',
        'tls-refusal','compression-refusal','retention-drift','partition-drift','compression-drift',
        'broker-outage','broker-restart-explicit','config-fifo','nested-unknown'}
    assert required <= labels, 'mandatory independent case coverage'
    for case in result['cases']:
        assert case['exit'] == (-9 if case['case']=='kill' else (0 if case['case'] in {
            'native','middle-replay','sasl-ssl','legal-gap','quiet','backpressure','kill-replay','broker-restart-explicit'} else 1)), 'exact child exit'
    for label in ('native','middle-replay','sasl-ssl','legal-gap','quiet','backpressure','kill-replay','broker-restart-explicit'):
        config=json.loads(actual[source_prefix+'/'+label+'.config.json'])
        topic=config['topic']; starts={x['partition']:x['offset'] for x in config['starts']}
        ledger={}
        for name, contents in actual.items():
            if name.startswith(source_prefix+'/') and name.endswith('-produce-'+topic+'.stdout'):
                for line in contents.decode().splitlines():
                    part, offset, payload=line.split('\t',2)
                    if int(offset)>=starts[int(part)]: ledger[int(part),int(offset)]=json.loads(payload)
        rows=[json.loads(line) for line in actual[source_prefix+'/'+label+'.output.jsonl'].decode().splitlines()]
        assert len(rows)==len(ledger) and ledger, 'independent producer coverage'
        last={};watermarks={};unique=set()
        for row in rows:
            key=(row['partition'],row['offset']); assert key in ledger and key not in unique
            unique.add(key);part,offset=key;event=ledger[key]
            assert offset>last.get(part,-1);last[part]=offset
            assert row['event_time_ms']==event['event_time_ms']
            assert row['late']==(part in watermarks and event['event_time_ms']<watermarks[part])
            expected={k:event[k] for k in ('value_i64','key','event_time_ms')} if event['key']<config['key_less_than'] else None
            assert row['projected']==expected, 'native filter/projection oracle'
            watermarks[part]=max(watermarks.get(part,event['event_time_ms']-config['allowance_ms']),event['event_time_ms']-config['allowance_ms'])
        trace=[json.loads(line) for line in actual[source_prefix+'/'+label+'.trace.jsonl'].decode().splitlines()]
        forwarded=[t['progress'] for t in trace if t['type']=='forwarded']
        assert len(forwarded)==len(rows)
        published=None;timer_ids=[]
        for item in trace:
            if 'progress' in item: published=item['progress']['frontier']
            if item['type']=='timer':
                timer_ids.append(item['timer']['id'])
                if item['timer']['kind']=='processing': assert item['processing_ms']>=item['timer']['deadline_ms']
                if item['timer']['kind']=='event': assert published is not None and published>=item['timer']['deadline_ms']
        assert len(timer_ids)==len(set(timer_ids)) and timer_ids[:2]==[0,1]
        maxima={};known={}
        for row, progress in zip(rows,forwarded):
            part=row['partition'];maxima[part]=max(maxima.get(part,row['event_time_ms']),row['event_time_ms']);known[part]=maxima[part]-config['allowance_ms']
            assert progress['partitions'][str(part)]['watermark']==known[part]
            assert progress['frontier']==(min(known.values()) if len(known)==len(starts) else None)
        terminal=next(t for t in reversed(trace) if t['type']=='terminal')
        assert terminal['status']=='STOPPED' and terminal['progress']['forwarded_records']==len(rows)
        assert terminal['managed_in_flight_peak_bytes']<=16384
    pressure = next(c for c in result['cases'] if c['case']=='backpressure')
    assert pressure['blocked_seconds'] >= 30
    print(json.dumps({'status':'PASS','cases':len(result['cases']),'files':len(actual),'source_files':len(inventory)}))


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--archive',type=pathlib.Path,default=pathlib.Path('docs/evidence/phase14/acceptance.tar.gz'))
    parser.add_argument('--manifest',type=pathlib.Path,default=pathlib.Path('docs/evidence/phase14/manifest.json'))
    args=parser.parse_args()
    verify(args.archive,args.manifest)
