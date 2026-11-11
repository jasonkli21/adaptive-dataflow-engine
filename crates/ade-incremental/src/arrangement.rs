use crate::{Error, Guard, Metrics, Result, require};
use ade_logical::maintained::{Changes, SHARDS, shard};
use std::{collections::BTreeMap, io::Read};

const MAGIC: &[u8; 8] = b"ADE19S01";
const HEADER: u64 = 16;
const KEY_BYTES: u64 = 44;
const VALUE_BYTES: u64 = 16;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Total {
    pub count: u64,
    pub sum: i64,
}

#[derive(Default)]
struct Wide {
    count: u64,
    sum: i128,
}
impl From<Total> for Wide {
    fn from(value: Total) -> Self {
        Self {
            count: value.count,
            sum: i128::from(value.sum),
        }
    }
}
impl Wide {
    fn change(&mut self, value: i64, weight: i128) -> Result<()> {
        self.count = u64::try_from(i128::from(self.count) + weight)
            .map_err(|_| Error::Invalid("view count overflow or underflow"))?;
        // Retractions precede inserts. Each intermediate is a valid bag with
        // at most u64::MAX rows, so its exact signed sum fits i128.
        self.sum = self
            .sum
            .checked_add(i128::from(value) * weight)
            .ok_or(Error::Invalid("wide sum overflow"))?;
        Ok(())
    }
    fn finish(self) -> Result<Total> {
        Ok(Total {
            count: self.count,
            sum: i64::try_from(self.sum).map_err(|_| Error::Invalid("view sum overflow"))?,
        })
    }
}

#[derive(Default)]
pub(crate) struct Group {
    values: BTreeMap<i64, u64>,
    pub all: Total,
    pub filtered: Total,
}
impl Group {
    fn totals(&self, threshold: i64) -> Result<(Total, Total)> {
        let mut all = Wide::default();
        let mut filtered = Wide::default();
        for (&value, &weight) in &self.values {
            all.change(value, i128::from(weight))?;
            if value >= threshold {
                filtered.change(value, i128::from(weight))?;
            }
        }
        Ok((all.finish()?, filtered.finish()?))
    }
}

#[derive(Default)]
pub(crate) struct Shard {
    pub groups: BTreeMap<u64, Group>,
    entries: usize,
}

pub(crate) struct Hot {
    used: usize,
    cap: usize,
    pub peak: usize,
}
impl Hot {
    pub fn new(cap: usize) -> Self {
        Self {
            used: 0,
            cap,
            peak: 0,
        }
    }

    pub fn add(&mut self, entries: usize) -> Result<()> {
        let next = self
            .used
            .checked_add(entries)
            .ok_or(Error::Quota("hot entries"))?;
        if next > self.cap {
            return Err(Error::Quota("hot entries"));
        }
        self.used = next;
        self.peak = self.peak.max(next);
        Ok(())
    }

    pub fn release(&mut self, entries: usize) {
        self.used -= entries;
    }
}

pub(crate) type Partitioned = [Vec<(u64, i64, i128)>; SHARDS];

pub(crate) fn partition(changes: &Changes) -> Partitioned {
    let mut buckets: Partitioned = std::array::from_fn(|_| Vec::new());
    for (&(key, value), &weight) in changes {
        buckets[shard(key)].push((key, value, weight));
    }
    buckets
}

impl Shard {
    pub fn entries(&self) -> usize {
        self.entries + self.groups.len()
    }

    pub fn baseline_insert(&mut self, key: u64, value: i64, hot: &mut Hot) -> Result<()> {
        use std::collections::btree_map::Entry;
        let group = match self.groups.entry(key) {
            Entry::Vacant(entry) => {
                hot.add(1)?;
                entry.insert(Group::default())
            }
            Entry::Occupied(entry) => entry.into_mut(),
        };
        match group.values.entry(value) {
            Entry::Vacant(entry) => {
                hot.add(1)?;
                entry.insert(1);
                self.entries += 1;
            }
            Entry::Occupied(mut entry) => *entry.get_mut() += 1, // finite physical source rows
        }
        Ok(())
    }

    pub fn baseline_finalize(&mut self, threshold: i64, guard: &Guard) -> Result<()> {
        for group in self.groups.values_mut() {
            guard.check()?;
            (group.all, group.filtered) = group.totals(threshold)?;
        }
        Ok(())
    }

    pub fn apply(
        &mut self,
        changes: &[(u64, i64, i128)],
        threshold: i64,
        hot: &mut Hot,
        guard: &Guard,
        metrics: &mut Metrics,
    ) -> Result<()> {
        for rows in changes.chunk_by(|a, b| a.0 == b.0) {
            guard.check()?;
            self.apply_key(rows[0].0, rows, threshold, hot)?;
            metrics.keys_updated += 1;
            metrics.view_updates += 2;
        }
        Ok(())
    }

    fn apply_key(
        &mut self,
        key: u64,
        rows: &[(u64, i64, i128)],
        threshold: i64,
        hot: &mut Hot,
    ) -> Result<()> {
        use std::collections::btree_map::Entry;
        let group = match self.groups.entry(key) {
            Entry::Vacant(entry) => {
                hot.add(1)?;
                entry.insert(Group::default())
            }
            Entry::Occupied(entry) => entry.into_mut(),
        };
        let mut all = Wide::from(group.all);
        let mut filtered = Wide::from(group.filtered);
        let ordered = rows
            .iter()
            .filter(|row| row.2 < 0)
            .chain(rows.iter().filter(|row| row.2 > 0));
        for &(_, value, weight) in ordered {
            let before = group.values.get(&value).copied().unwrap_or_default();
            let after = u64::try_from(i128::from(before) + weight)
                .map_err(|_| Error::Invalid("negative or overflowing row multiplicity"))?;
            if after == 0 {
                group.values.remove(&value);
                self.entries -= 1;
                hot.release(1);
            } else {
                if before == 0 {
                    hot.add(1)?;
                    self.entries += 1;
                }
                group.values.insert(value, after);
            }
            all.change(value, weight)?;
            if value >= threshold {
                filtered.change(value, weight)?;
            }
        }
        if group.values.is_empty() {
            self.groups.remove(&key);
            hot.release(1);
        } else {
            group.all = all.finish()?;
            group.filtered = filtered.finish()?;
        }
        Ok(())
    }

    pub fn view_rows(&self) -> u64 {
        self.groups.len() as u64
            + self
                .groups
                .values()
                .filter(|g| g.filtered.count > 0)
                .count() as u64
    }

    pub fn len_bytes(&self) -> u64 {
        HEADER + KEY_BYTES * self.groups.len() as u64 + VALUE_BYTES * self.entries as u64
    }

    pub fn encode(&self, guard: &Guard) -> Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(
            usize::try_from(self.len_bytes()).map_err(|_| Error::Quota("shard bytes"))?,
        );
        bytes.extend(MAGIC);
        bytes.extend((self.groups.len() as u64).to_le_bytes());
        for (&key, group) in &self.groups {
            guard.check()?;
            bytes.extend(key.to_le_bytes());
            bytes.extend(group.all.count.to_le_bytes());
            bytes.extend(group.all.sum.to_le_bytes());
            bytes.extend(group.filtered.count.to_le_bytes());
            bytes.extend(group.filtered.sum.to_le_bytes());
            let values =
                u32::try_from(group.values.len()).map_err(|_| Error::Quota("key values"))?;
            bytes.extend(values.to_le_bytes());
            for (&value, &count) in &group.values {
                bytes.extend(value.to_le_bytes());
                bytes.extend(count.to_le_bytes());
            }
        }
        Ok(bytes)
    }

    pub fn decode(
        mut input: impl Read,
        bytes: u64,
        index: usize,
        threshold: Option<i64>,
        hot: &mut Hot,
        guard: &Guard,
    ) -> Result<Self> {
        let mut out = Self::default();
        require(read(&mut input)? == *MAGIC, "shard format")?;
        let keys = u64::from_le_bytes(read(&mut input)?);
        require(
            keys <= bytes.saturating_sub(HEADER) / (KEY_BYTES + VALUE_BYTES),
            "shard key count",
        )?;
        let mut remaining = bytes - HEADER;
        for _ in 0..keys {
            guard.check()?;
            let (key, group) = decode_group(&mut input, &mut remaining, hot, guard)?;
            require(
                shard(key) == index
                    && out
                        .groups
                        .last_key_value()
                        .is_none_or(|(&prev, _)| prev < key),
                "shard key ordering or routing",
            )?;
            if let Some(t) = threshold {
                require(
                    group.totals(t)? == (group.all, group.filtered),
                    "shard view semantics",
                )?;
            }
            out.entries += group.values.len();
            out.groups.insert(key, group);
        }
        require(remaining == 0, "shard trailing bytes")?;
        Ok(out)
    }
}

fn read<const N: usize>(input: &mut impl Read) -> Result<[u8; N]> {
    let mut result = [0; N];
    input.read_exact(&mut result)?;
    Ok(result)
}

fn decode_group(
    input: &mut impl Read,
    remaining: &mut u64,
    hot: &mut Hot,
    guard: &Guard,
) -> Result<(u64, Group)> {
    require(*remaining >= KEY_BYTES, "shard group length")?;
    let key = u64::from_le_bytes(read(input)?);
    let all = Total {
        count: u64::from_le_bytes(read(input)?),
        sum: i64::from_le_bytes(read(input)?),
    };
    let filtered = Total {
        count: u64::from_le_bytes(read(input)?),
        sum: i64::from_le_bytes(read(input)?),
    };
    let values = u32::from_le_bytes(read(input)?);
    let charge = KEY_BYTES + u64::from(values) * VALUE_BYTES;
    require(
        values > 0 && charge <= *remaining && all.count > 0 && filtered.count <= all.count,
        "shard group accounting",
    )?;
    *remaining -= charge;
    hot.add(1 + values as usize)?;
    let mut bag = BTreeMap::new();
    for i in 0..values {
        if i % 1024 == 0 {
            guard.check()?;
        }
        let value = i64::from_le_bytes(read(input)?);
        let count = u64::from_le_bytes(read(input)?);
        require(
            count > 0 && bag.last_key_value().is_none_or(|(&prev, _)| prev < value),
            "shard value ordering or multiplicity",
        )?;
        bag.insert(value, count);
    }
    Ok((
        key,
        Group {
            values: bag,
            all,
            filtered,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CancellationToken;

    #[test]
    fn signed_minimum_retracts_exactly_and_count_overflow_refuses() {
        let guard = Guard::new(CancellationToken::default(), 1000);
        let mut hot = Hot::new(16);
        let mut shard = Shard::default();
        let mut metrics = Metrics::default();
        let insert = [(0, 0, i128::from(u64::MAX))];
        shard
            .apply(&insert, 0, &mut hot, &guard, &mut metrics)
            .unwrap();
        assert_eq!(
            shard.groups[&0].all,
            Total {
                count: u64::MAX,
                sum: 0
            }
        );
        let min = [(0, 0, i128::from(i64::MIN))];
        shard
            .apply(&min, 0, &mut hot, &guard, &mut metrics)
            .unwrap();
        assert_eq!(shard.groups[&0].all.count, i64::MAX as u64);
        assert!(matches!(
            shard.apply(
                &[(0, 1, i128::from(u64::MAX))],
                0,
                &mut hot,
                &guard,
                &mut metrics
            ),
            Err(Error::Invalid("view count overflow or underflow"))
        ));
    }

    #[test]
    fn threshold_change_and_valid_final_sum_ignore_transient_narrow_overflow() {
        let guard = Guard::new(CancellationToken::default(), 1000);
        let mut hot = Hot::new(16);
        let mut shard = Shard::default();
        let mut metrics = Metrics::default();
        let original = [(0, i64::MIN, 1), (0, i64::MAX, 1)];
        shard
            .apply(&original, 0, &mut hot, &guard, &mut metrics)
            .unwrap();
        assert_eq!(shard.groups[&0].all, Total { count: 2, sum: -1 });
        assert_eq!(
            shard.groups[&0].filtered,
            Total {
                count: 1,
                sum: i64::MAX
            }
        );
        let replace = [(0, i64::MIN, -1), (0, -1, 1), (0, 0, 1), (0, i64::MAX, -1)];
        shard
            .apply(&replace, 0, &mut hot, &guard, &mut metrics)
            .unwrap();
        let encoded = shard.encode(&guard).unwrap();
        let mut decoded_hot = Hot::new(3);
        let restored = Shard::decode(
            encoded.as_slice(),
            encoded.len() as u64,
            0,
            Some(0),
            &mut decoded_hot,
            &guard,
        )
        .unwrap();
        assert_eq!(restored.groups[&0].all, Total { count: 2, sum: -1 });
        assert_eq!(restored.groups[&0].filtered, Total { count: 1, sum: 0 });
        assert!(
            Shard::decode(
                encoded.as_slice(),
                encoded.len() as u64,
                0,
                Some(1),
                &mut Hot::new(3),
                &guard
            )
            .is_err()
        );
    }
}
