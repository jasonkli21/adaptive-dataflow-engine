"""Independent Phase 19 byte, bag, and decision model for frozen local evidence."""
import collections
import json
import struct
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "phase17"))
from digest import digest as blake3  # portable implementation from the BLAKE3 spec

SHARDS = 128
ROUTES = ("restore", "incremental_update", "full_recompute")


def check(condition, reason):
    if not condition:
        raise ValueError(reason)


def zip_exact(*values):
    check(all(len(v) == len(values[0]) for v in values), "zip length mismatch")
    return zip(*values)


def compact(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def strict(data):
    def pairs(entries):
        values = dict(entries)
        check(len(values) == len(entries), "duplicate JSON field")
        return values
    return json.loads(data, object_pairs_hook=pairs)


def identity(domain, data):
    return blake3(domain.encode() + b"\0" + data)


class Thrift:
    """The compact protocol subset needed for uncompressed PLAIN Parquet fixtures."""

    def __init__(self, data, offset):
        self.data = data
        self.offset = offset

    def byte(self):
        value = self.data[self.offset]
        self.offset += 1
        return value

    def unsigned(self):
        value = 0
        for shift in range(0, 70, 7):
            byte = self.byte()
            value |= (byte & 127) << shift
            if byte < 128:
                return value
        raise ValueError("Thrift varint")

    def signed(self):
        value = self.unsigned()
        return (value >> 1) ^ -(value & 1)

    def field(self, code):
        if code in (1, 2):
            return code == 1
        if code == 3:
            return self.byte()
        if code in (4, 5, 6):
            return self.signed()
        if code == 8:
            size = self.unsigned()
            start = self.offset
            self.offset += size
            return bytes(self.data[start:self.offset])
        if code in (9, 10):
            header = self.byte()
            size = header >> 4
            if size == 15:
                size = self.unsigned()
            check(size <= len(self.data), "Thrift collection bounds")
            return [self.field(header & 15) for _ in range(size)]
        if code == 12:
            return self.fields()
        raise ValueError(f"unsupported Thrift type {code}")

    def fields(self):
        result = {}
        index = 0
        while (header := self.byte()) != 0:
            delta = header >> 4
            index = index + delta if delta else self.signed()
            check(index not in result, "duplicate Thrift field")
            result[index] = self.field(header & 15)
        return result


def parquet_rows(data):
    check(data[:4] == data[-4:] == b"PAR1", "Parquet magic")
    footer = len(data) - 8 - struct.unpack_from("<I", data, len(data) - 8)[0]
    parser = Thrift(data, footer)
    meta = parser.fields()
    check(parser.offset == len(data) - 8, "Parquet footer closure")
    schema = meta[2]
    check(len(schema) == 3 and schema[0][5] == 2, "Parquet schema width")
    check([(f[1], f[3], f[4]) for f in schema[1:]] ==
          [(2, 0, b"key"), (2, 0, b"value")], "Parquet physical schema")
    check(schema[1].get(6) == 14 and schema[2].get(6) in (None, 18), "Parquet signedness")
    count = 0
    for group in meta[4]:
        columns = []
        for chunk, spec in zip_exact(group[1], ("<Q", "<q")):
            m = chunk[3]
            check(m[4] == 0 and 11 not in m, "fixture must use uncompressed PLAIN pages")
            start, end = m[9], m[9] + m[7]
            check(4 <= start <= end <= footer, "Parquet column extent")
            values = []
            while start < end:
                parser = Thrift(data, start)
                header = parser.fields()
                start = parser.offset
                n = header[5][1]
                check(header[1] == 0 and header[5][2] == 0 and
                      header[2] == header[3] == n * 8, "Parquet PLAIN page")
                values.extend(value for (value,) in struct.iter_unpack(spec, data[start:start + n * 8]))
                start += n * 8
            check(start == end and len(values) == m[5] == group[3], "Parquet row coverage")
            columns.append(values)
        count += group[3]
        yield from zip_exact(*columns)
    check(count == meta[3], "Parquet total rows")


def plan_digest(plan):
    check(set(plan) == {"version", "namespace", "threshold", "baseline"} and plan["version"] == 1,
          "plan version or shape")
    fields = ("name", "bytes", "rows", "fingerprint")
    check(all(set(f) == set(fields) for f in plan["baseline"]), "plan file shape")
    canonical = {k: plan[k] for k in ("version", "namespace", "threshold", "baseline")}
    canonical["baseline"] = [{k: f[k] for k in fields} for f in plan["baseline"]]
    return identity("maintained-plan-v1", compact(canonical))


def delta(data, plan_id, epoch, parent):
    envelope = strict(data)
    check(set(envelope) == {"version", "plan", "epoch", "parent", "rows"} and
          (envelope["version"], envelope["plan"], envelope["epoch"], envelope["parent"]) ==
          (1, plan_id, epoch, parent), "delta envelope")
    check(len(data) <= 2 * 1024 * 1024 and len(envelope["rows"]) <= 32768, "delta caps")
    rows = collections.Counter()
    for key, value, weight in envelope["rows"]:
        check(0 <= key < 2**64 and -(2**63) <= value < 2**63 and -(2**63) <= weight < 2**63,
              "delta domain")
        rows[key, value] += weight
    return identity("maintained-delta-v1", data), envelope["rows"], {k: n for k, n in rows.items() if n}


def view_rows_and_shards(bag, threshold):
    groups = collections.defaultdict(list)
    for (key, value), count in sorted(bag.items()):
        check(0 < count < 2**64, "multiplicity")
        groups[key].append((value, count))
    views = []
    shards = [bytearray() for _ in range(SHARDS)]
    keys = [0] * SHARDS
    view_counts = [0] * SHARDS
    for key in sorted(groups, key=lambda k: (k % SHARDS, k)):
        values = groups[key]
        subset = [(v, n) for v, n in values if v >= threshold]
        all_count = sum(n for _, n in values)
        all_sum = sum(v * n for v, n in values)
        sub_count = sum(n for _, n in subset)
        sub_sum = sum(v * n for v, n in subset)
        shard = key % SHARDS
        keys[shard] += 1
        shards[shard] += struct.pack("<QQqQqI", key, all_count, all_sum, sub_count, sub_sum, len(values))
        for value, count in values:
            shards[shard] += struct.pack("<qQ", value, count)
        for name, count, total in (("all_v1", all_count, all_sum), ("filtered_v1", sub_count, sub_sum)):
            if count:
                views.append({"view": name, "key": key, "count": count, "sum": total})
                view_counts[shard] += 1
    return views, [b"ADE19S01" + struct.pack("<Q", n) + b if n else None
                   for n, b in zip(keys, shards)], view_counts


def from_fixture(plan_bytes, file_bytes, epoch_bytes, check_baseline=True):
    plan = strict(plan_bytes)
    plan_id = plan_digest(plan)
    baseline = collections.Counter()
    for spec, data in zip_exact(plan["baseline"], file_bytes):
        check(len(data) == spec["bytes"], "baseline byte inventory")
        if check_baseline:
            check(blake3(data) == spec["fingerprint"], "baseline byte fingerprint")
        rows = collections.Counter(parquet_rows(data))
        check(sum(rows.values()) == spec["rows"], "baseline decoded row inventory")
        baseline.update(rows)
    chain = identity("maintained-genesis-v1", plan_id.encode())
    return replay(plan, plan_id, chain, baseline, epoch_bytes)


def replay(plan, plan_id, chain, bag, epoch_bytes):
    epochs = []
    history = []
    raw_rows = []
    changes = []
    for epoch in range(len(epoch_bytes) + 1):
        if epoch:
            chain, raw, change = delta(epoch_bytes[epoch - 1], plan_id, epoch, chain)
            raw_rows.append(raw)
            changes.append(change)
            for row, weight in change.items():
                after = bag.get(row, 0) + weight
                check(after >= 0, "negative replay bag")
                if after:
                    bag[row] = after
                else:
                    bag.pop(row, None)
        history.append(chain)
        views, shards, counts = view_rows_and_shards(bag, plan["threshold"])
        fingerprints = [blake3(s) if s is not None else None for s in shards]
        result = identity("maintained-result-v1", compact([plan_id, epoch, chain, fingerprints]))
        epochs.append({"plan": plan_id, "chain": chain, "result": result, "views": views,
                       "history": history[1:].copy(), "shards": shards, "hashes": fingerprints,
                       "view_counts": counts})
    return epochs, raw_rows, changes


def expected_decision(model, file_sizes, rows, raw_rows, changes, epoch, parent_epoch, policy):
    existing = parent_epoch == epoch
    can_increment = parent_epoch == epoch - 1 and parent_epoch is not None
    parent_sizes = [len(s or b"") for s in model[parent_epoch]["shards"]] if parent_epoch is not None else []
    zeros = dict(scan_rows=0, delta_rows=0, touched_keys=0, verify_bytes=0, write_bytes=0)
    reserve = 2 * 32768 + 16 * SHARDS
    work = [
        zeros | {"verify_bytes": sum(parent_sizes)} if existing else None,
        None,
        zeros | {"scan_rows": rows, "delta_rows": sum(map(len, raw_rows[:epoch])),
                 "verify_bytes": sum(file_sizes),
                 "write_bytes": 0 if existing else reserve + 60 * (rows + sum(map(len, raw_rows[:epoch])))},
    ]
    if can_increment:
        keys = {key for key, _ in changes[epoch - 1]}
        touched = {key % SHARDS for key in keys}
        delta_rows = len(raw_rows[epoch - 1])
        work[1] = zeros | {"delta_rows": delta_rows, "touched_keys": len(keys),
                           "verify_bytes": sum(parent_sizes),
                           "write_bytes": reserve + 60 * delta_rows + sum(parent_sizes[i] for i in touched)}
    scores = [None if w is None else 4*w["scan_rows"] + w["delta_rows"] + 6*w["touched_keys"]
              + w["verify_bytes"]//64 + w["write_bytes"]//32 for w in work]
    routes = [(score, i) for i, score in enumerate(scores) if score is not None]
    winner = ROUTES[min(routes)[1]] if policy == "adaptive" else policy
    return winner, work, scores


def verify_trace(trace, expected, epoch, selection):
    check((trace["plan"], trace["epoch"], trace["chain"], trace["result"]) ==
          (expected["plan"], epoch, expected["chain"], expected["result"]), "trace identity")
    route, estimates, scores = selection
    decision = trace["decision"]
    check(decision["selected"] == route, "independent policy winner")
    for c, name, estimate, score in zip_exact(decision["candidates"], ROUTES, estimates, scores):
        check((c["route"], c["estimate"], c["work_units"], c["projected_ms"], c["refusal"]) ==
              (name, estimate, score, None if score is None else (score + 99999) // 100000,
               "missing compatible verified inputs" if estimate is None else None),
              "independent policy candidate")
