"""Release evidence v2. Hash references detect alteration, not a dishonest host.

Legacy v1 bundles require the verifier archived with their original source. They
cannot establish v2 readiness or stage identity and are never silently upgraded.
"""
import hashlib
import json
import math
import pathlib
import platform
import re
import subprocess
import time
import uuid


class VerificationError(RuntimeError):
    pass


def require(condition, context):
    if not condition:
        raise VerificationError(str(context))


COMMANDS = {
    'top': ['/usr/bin/top', '-l', '1', '-n', '0'],
    'vm': ['/usr/bin/vm_stat'],
    'uptime': ['/usr/bin/uptime'],
    'processes': ['/bin/ps', '-A', '-o', 'pid=,comm='],
}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(root, name, value):
    (root / name).write_text(json.dumps(value, indent=2) + '\n')


def capture(session):
    require(platform.system() == 'Darwin' and platform.machine() == 'arm64',
            'UNVERIFIED: release capture requires macOS arm64')
    start = time.monotonic_ns()
    readings = {key: subprocess.check_output(cmd, text=True) for key, cmd in COMMANDS.items()}
    return dict(session=session, platform='macos-arm64-v1', commands=COMMANDS,
                start_ns=start, end_ns=time.monotonic_ns(), time_ns=time.time_ns(),
                host=platform.node(), kernel=platform.release(), readings=readings)


def number(text, pattern):
    match = re.search(pattern, text)
    require(match is not None, 'UNVERIFIED: malformed host reading')
    value = float(match[1])
    require(math.isfinite(value), 'UNVERIFIED: nonfinite host reading')
    return value


def inspect_sample(sample, session, host):
    require(sample['session'] == session and sample['platform'] == 'macos-arm64-v1'
            and sample['host'] == host and bool(host), 'UNVERIFIED: host/session/platform')
    require(sample['commands'] == COMMANDS, 'UNVERIFIED: host command identity')
    for key in ['start_ns', 'end_ns', 'time_ns']:
        require(type(sample[key]) is int and sample[key] > 0, 'UNVERIFIED: malformed time')
    require(sample['start_ns'] <= sample['end_ns'], 'UNVERIFIED: time order')
    r = sample['readings']
    idle = number(r['top'], r'CPU usage:.*?([\d.]+)% idle')
    load = number(r['uptime'], r'load averages?:\s*([\d.]+)')
    swap = number(r['vm'], r'Swapouts:\s*(\d+)\.')
    require(0 <= idle <= 100 and load >= 0, 'UNVERIFIED: numeric host reading')
    require('Mach Virtual Memory Statistics: (page size of ' in r['vm'], 'UNVERIFIED: vm format')
    lines = r['processes'].splitlines()
    require(bool(lines), 'UNVERIFIED: empty process inventory')
    for line in lines:
        # Darwin can report a kernel command name in parentheses for an exiting
        # process, or a bare login-shell/daemon name instead of an absolute path.
        match = re.fullmatch(r'\s*([1-9]\d*)\s+(/[^\n]+|-?[A-Za-z0-9_.-]+|\([A-Za-z0-9_.-]+\))', line)
        require(match is not None, 'UNVERIFIED: malformed process line')
        name = pathlib.PurePosixPath(match[2]).name.strip('()')
        require(name not in {'cargo', 'rustc', 'ade-bench', 'ade-worker'},
                'UNVERIFIED: concurrent compiler/benchmark/worker')
        require(re.search(r'(?:^|/)(?:cargo|rustc|ade-bench|ade-worker)(?:\s|$)',
                          match[2].strip('()')) is None, 'UNVERIFIED: disguised competitor process')
    return dict(idle=idle, load1=load, swapouts=int(swap))


def check_readiness(samples, session, host):
    require(len(samples) == 3, 'UNVERIFIED: exactly three readiness samples required')
    values = [inspect_sample(s, session, host) for s in samples]
    for previous, current in zip(samples, samples[1:]):
        require(current['start_ns'] - previous['end_ns'] >= 10_000_000_000,
                'UNVERIFIED: readiness intervals must be at least 10s')
        require(current['time_ns'] > previous['time_ns'], 'UNVERIFIED: wall clock order')
        require(abs((current['time_ns'] - previous['time_ns']) -
                    (current['end_ns'] - previous['end_ns'])) <= 5_000_000_000,
                'UNVERIFIED: inconsistent wall/monotonic clocks')
    require(all(v['idle'] >= 70 and v['load1'] <= 8 for v in values),
            'UNVERIFIED: host idle/load threshold')
    swaps = [v['swapouts'] for v in values]
    require(swaps == sorted(swaps) and swaps[-1] - swaps[0] <= 1000,
            'UNVERIFIED: swapout delta')
    return values


def seal(root, session, commands):
    """No circular inventory: verification and the seal itself are derived files."""
    files = {p.name: sha(p) for p in sorted(root.iterdir())
             if p.is_file() and p.name not in {'release-evidence.json', 'verification.json', 'sha256.json'}}
    runs = {p.stem.removesuffix('-result'): json.loads(p.read_text())['run_id']
            for p in root.glob('*-result.json')}
    write(root, 'release-evidence.json', dict(schema_version=2, session=session,
          source=json.loads((root / 'source-identity.json').read_text()),
          host=platform.node(), platform='macos-arm64-v1', files=files,
          runs=runs, commands=commands))


def check_release(root):
    path = root / 'release-evidence.json'
    require(path.is_file(), 'UNVERIFIED: v2 release seal missing; use archived verifier for legacy v1')
    seal = json.loads(path.read_text())
    require(seal['schema_version'] == 2 and seal['platform'] == 'macos-arm64-v1',
            'UNVERIFIED: release version/platform')
    uuid.UUID(seal['session'])
    require(seal['source'] == json.loads((root / 'source-identity.json').read_text()), 'source seal')
    required = {'readiness.json', 'postflight.json', 'statuses.json', 'inventory.json',
                'source-identity.json', 'binary-sha256.json', 'source.tar', 'source.bundle',
                'environment-stdout.log', 'declaration.md'}
    inventory = json.loads((root / 'inventory.json').read_text())
    for item in inventory:
        required.update(item['stem'] + suffix for suffix in
                        ['-result.json', '-metrics.jsonl', '-command.json', '-stdout.log', '-stderr.log'])
    for regime in ['imbalanced', 'uniform']:
        for kind in ['manifest', 'oracle']:
            required.update(f'{regime}-{kind}{suffix}' for suffix in
                            ['-stdout.log', '-stderr.log', '-command.json'])
    for kind in ['exit', 'expiry', 'notice']:
        required.update(f'negative-{kind}{suffix}' for suffix in
                        ['-stdout.log', '-stderr.log', '-command.json'])
    require(required <= seal['files'].keys(), 'UNVERIFIED: incomplete sealed evidence')
    for name, digest in seal['files'].items():
        require(pathlib.PurePath(name).name == name and (root / name).is_file(), 'unsafe/missing sealed path')
        require(sha(root / name) == digest, 'altered sealed artifact: ' + name)
    samples = json.loads((root / 'readiness.json').read_text())
    values = check_readiness(samples, seal['session'], seal['host'])
    post = json.loads((root / 'postflight.json').read_text())
    inspect_sample(post, seal['session'], seal['host'])
    require(post['start_ns'] > samples[-1]['end_ns'], 'UNVERIFIED: postflight time')
    require(post['time_ns'] > samples[-1]['time_ns'] and
            abs((post['time_ns'] - samples[-1]['time_ns']) -
                (post['end_ns'] - samples[-1]['end_ns'])) <= 5_000_000_000,
            'UNVERIFIED: stale postflight clock')
    env = json.loads((root / 'environment-stdout.log').read_text())
    require(env['environment']['os'] == 'macos' and env['environment']['arch'] == 'aarch64', 'environment platform')
    require(all(s['kernel'] == env['environment']['kernel'] for s in [*samples, post]), 'environment kernel')
    require(env['repository'] == dict(git_commit=seal['source']['source_commit'], git_dirty=False), 'environment source')
    for name in required:
        if name.endswith('-command.json'):
            stem = name.removesuffix('-command.json')
            cmd = json.loads((root / name).read_text())
            event = seal['commands'][stem]
            require(event['argv'] == cmd and samples[-1]['end_ns'] < event['start_ns']
                    < event['end_ns'] < post['start_ns'], 'UNVERIFIED: command/time binding')
            statuses = json.loads((root / 'statuses.json').read_text())
            if stem in statuses:
                require(event['status'] == statuses[stem], 'command exit status binding')
    runs = {i['stem']: json.loads((root / (i['stem'] + '-result.json')).read_text())['run_id'] for i in inventory}
    require(seal['runs'] == runs and len(set(runs.values())) == len(runs), 'release run identity')
    return dict(schema_version=2, session=seal['session'], readiness=values)


def check_negative(root, stem, status):
    require(status != 0, 'negative unexpectedly succeeded')
    require(not (root / (stem + '-stdout.log')).read_text().strip(), 'negative emitted stdout')
    text = (root / (stem + '-stderr.log')).read_text()
    records = [json.loads(line.split('ADE_SCHEDULER_FAULT ', 1)[1])
               for line in text.splitlines() if line.startswith('ADE_SCHEDULER_FAULT ')]
    require(len(records) == 1, 'missing/ambiguous reached fault evidence')
    r = records[0]
    cmd = json.loads((root / (stem + '-command.json')).read_text())
    options = [cmd[i + 1] for i, arg in enumerate(cmd) if arg == '--engine-option']
    expected = {'negative-exit': ('exit_after_frame', 'scheduler_research_exit_after_emit_worker=0'),
                'negative-expiry': ('expired_live_lease', 'scheduler_research_no_heartbeat_worker=0'),
                'negative-notice': ('spot_deadline', 'scheduler_research_delay_worker=0')}
    stage, hook = expected[stem]
    require(hook in options and r['stage'] == stage and r['schema_version'] == 1, 'negative hook/stage')
    require('--spec' in cmd and '--dataset' in cmd and 'validate' in cmd, 'negative command')
    uuid.UUID(r['fence']['execution'])
    require(r['fence']['epoch'] == 1 and r['fence']['job'] == 0
            and re.fullmatch('[0-9a-f]{64}', r['fence']['digest']), 'negative execution fence')
    lease = r['lease']
    require(lease['worker'] == 0 and lease['nonce'] > 0 and r['process_id'] > 0, 'negative live lease')
    manifest = json.loads((root / 'uniform-manifest-stdout.log').read_text())
    morsel = lease['morsel']
    file = manifest['files'][morsel['id']]
    dataset = pathlib.Path(cmd[cmd.index('--dataset') + 1])
    require(morsel['fingerprint'] == file['fingerprint'] and morsel['bytes'] == file['bytes']
            and pathlib.Path(morsel['path']) == dataset / file['relative_path'], 'negative source')
    require(r['at_ms'] >= r['granted_ms'], 'negative grant time')
    if stage == 'exit_after_frame':
        require(r['emitted_rows'] > 0 and r['emitted_frames'] == 1, 'death before output')
    elif stage == 'expired_live_lease':
        require('ambiguous expired source lease' in text, 'unrelated expiry failure')
        require('scheduler_lease_ms=80' in options and 'scheduler_heartbeat_ms=20' in options
                and r['at_ms'] >= r['expires_ms'] >= r['granted_ms'] + 80
                and r['renewals'] == 0, 'expiry before live lease deadline')
    else:
        require('spot notice expired before producer drain' in text, 'unrelated spot failure')
        require(r['notice_ms'] >= r['granted_ms'] and r['at_ms'] >= r['deadline_ms']
                == r['notice_ms'] + 1 and r['completed'] >= 1, 'spot deadline not reached')
        events = next(o.split('=', 1)[1] for o in options if o.startswith('scheduler_participation='))
        require(json.loads(events) == [dict(after_completed=1, worker=0,
                    action=dict(spot_notice=dict(grace_ms=1)))], 'notice injection')
    return r
