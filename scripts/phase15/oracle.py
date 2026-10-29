"""Independent contract oracle. No ADE code, codec or hash implementation imports."""
import copy, json, pathlib, struct


def require(condition, message):
    if not condition:
        raise ValueError(message)


def rows(path):
    return [json.loads(line) for line in pathlib.Path(path).read_text().splitlines() if line.strip()]


class Model:
    def __init__(self, config):
        self.config = config
        self.state = {}
        self.maxima = {}
        self.offsets = {s['partition']: s['offset'] for s in config['source']['starts']}
        self.frontier = None
        self.total = 0
        self.late = 0
        self.closed_late = 0
        self.outputs = []
        self.inputs = set()

    def due(self):
        return sorted(k for k in self.state if self.frontier is not None and k[0] + self.config['window_ms'] <= self.frontier)

    def consume(self, frame, ledger):
        if frame['type'] == 'window':
            pending = self.due()
            require(bool(pending), 'unexpected/reopened window')
            start, key = pending[0]
            amount, count = self.state.pop((start, key))
            stop = start + self.config['window_ms']
            want = dict(type='window', output_version=1, job_id=self.config['job_id'], key=key,
                        window_start=start, window_end=stop, sum_i64=amount, count_u64=count,
                        output_id=f"{self.config['job_id']}:{key}:{start}:{stop}:1")
            if 'payload_fingerprint' in frame:
                want['payload_fingerprint'] = f'window-payload-v1:{amount}:{count}'
            require(frame == want, f'window oracle mismatch: {frame} != {want}')
            self.outputs.append(frame)
            return
        require(frame['type'] == 'input', 'unknown output kind')
        require(not self.due(), 'source demand bypassed pending window output')
        r = frame['receipt']; identity = (r['partition'], r['offset']); p = r['partition']
        require(identity in ledger and identity not in self.inputs, 'foreign/duplicate input within lineage')
        event = ledger[identity]
        require(r['offset'] >= self.offsets[p], 'backward or skipped resume')
        # A finite producer ledger independently proves that no visible record was skipped.
        eligible = sorted(off for part, off in ledger if part == p and off >= self.offsets[p])
        require(eligible and eligible[0] == r['offset'], 'lost visible Kafka record (holes are permitted)')
        ts = event['event_time_ms']; old_bound = self.maxima.get(p)
        is_late = old_bound is not None and ts < old_bound - self.config['source']['allowance_ms']
        require(r['event_time_ms'] == ts and r['late'] == is_late and r['version'] == 1, 'receipt time/late/version')
        accepted = event['key'] < self.config['source']['key_less_than']
        projection = {k: event[k] for k in ('value_i64', 'key', 'event_time_ms')} if accepted else None
        require(r['projected'] == projection, 'native transform mismatch')
        # Python // on signed integers is floor division, derived separately from the window contract.
        start = (ts // self.config['window_ms']) * self.config['window_ms']
        closed = accepted and self.frontier is not None and start + self.config['window_ms'] <= self.frontier
        require(frame['closed_window_late'] == closed, 'closed-window late decision')
        if closed:
            self.closed_late += 1
        elif accepted:
            amount, count = self.state.get((start, event['key']), (0, 0))
            self.state[(start, event['key'])] = (amount + event['value_i64'], count + 1)
        self.inputs.add(identity); self.offsets[p] = r['offset'] + 1
        self.maxima[p] = max(self.maxima.get(p, ts), ts)
        self.frontier = min(self.maxima.values()) - self.config['source']['allowance_ms'] if len(self.maxima) == len(self.offsets) else None
        self.total += 1; self.late += int(is_late)

    def check_progress(self, progress):
        require(progress['forwarded_records'] == self.total and progress['late_records'] == self.late and progress['frontier'] == self.frontier, 'progress oracle mismatch')
        for p, off in self.offsets.items():
            cursor = progress['partitions'][str(p)]
            require(cursor['next_offset_suggestion'] == off, 'checkpoint ahead/behind confirmed inputs')
            require(cursor['max_forwarded_time'] == self.maxima.get(p), 'max time changed on restore')
            want = self.maxima[p] - self.config['source']['allowance_ms'] if p in self.maxima else None
            require(cursor['watermark'] == want, 'watermark changed on restore')

    def check_checkpoint(self, directory):
        directory = pathlib.Path(directory)
        manifest = json.loads((directory/'manifest.json').read_text())
        self.check_progress(manifest['progress'])
        require(not self.due() and manifest['pending_closures'] == 0 and manifest['closed_window_late'] == self.closed_late, 'nonquiescent checkpoint')
        state = {}
        for index, meta in enumerate(manifest['shards']):
            require(meta['shard'] == index and meta['owner'] == index % manifest['owners'], 'owner/shard mismatch')
            raw = (directory/f'shard-{index:02}.bin').read_bytes()
            require(len(raw) == meta['bytes'] == 64*meta['count'], 'checkpoint count/length')
            previous = None
            for offset in range(0, len(raw), 64):
                start, key, amount, count = struct.unpack('<qQqQ', raw[offset:offset+32])
                identity = (start, key)
                require(identity not in state and (previous is None or identity > previous), 'duplicate/unsorted accumulator')
                state[identity] = (amount, count); previous = identity
        require(state == self.state and len(state) == manifest['windows'], 'checkpoint state does not match confirmed prefix')
        return manifest

    def clone(self):
        return copy.deepcopy(self)
