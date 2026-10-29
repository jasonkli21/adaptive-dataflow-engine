//! Bounded sorted cold files; no cardinality-sized in-memory index.
use super::{Config, Entry, check, end, shard};
use crate::{Error, Result};
use ade_exec::CancellationToken;
use serde::Serialize;
use std::{
    fs::{self, File},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
};

pub const CHARGE: usize = std::mem::size_of::<Entry>();
pub const RECORD: u64 = 64;
pub const SHARDS: usize = 32;
#[derive(Default, Debug, Serialize)]
pub struct Metrics {
    pub hot_peak_bytes: usize,
    pub disk_peak_bytes: u64,
    pub evictions: u64,
    pub cold_hits: u64,
    pub hot_hits: u64,
    pub windows: u64,
    pub io_buffer_bytes: usize,
    pub checkpoint_peak_bytes: u64,
    pub checkpoint_metadata_peak_bytes: usize,
}
// Each logical owner has a distinct hot allocation, cold inventory and directory.
// Store only arbitrates their shared byte quota and exclusive virtual-shard routing.
struct OwnerStore {
    root: PathBuf,
    hot: Vec<Entry>,
    counts: [u64; SHARDS],
}
pub struct Store {
    root: PathBuf,
    owners: Vec<OwnerStore>,
    disk_cap: u64,
    pub metrics: Metrics,
    pub token: CancellationToken,
    poisoned: bool,
    injected_write_bytes: Option<usize>,
}
/// # Errors
/// Refuses filesystem failure, unsafe inputs, corruption or quota violation.
pub fn private(path: &Path) -> Result<()> {
    let canonical = fs::canonicalize(path).map_err(|_| Error::State("private root missing"))?;
    check(canonical == path, "noncanonical/symlink root")?;
    let m = fs::symlink_metadata(path).map_err(|_| Error::State("root metadata"))?;
    check(
        m.is_dir() && m.permissions().mode() & 0o777 == 0o700,
        "private directory mode 0700",
    )
}
/// # Errors
/// Refuses filesystem failure, unsafe inputs, corruption or quota violation.
pub fn create_file(path: &Path) -> Result<File> {
    private(path.parent().ok_or(Error::State("file parent"))?)?;
    let fd = rustix::fs::open(
        path,
        rustix::fs::OFlags::WRONLY
            | rustix::fs::OFlags::CREATE
            | rustix::fs::OFlags::EXCL
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::from_bits_truncate(0o600),
    )
    .map_err(|_| Error::State("create private file"))?;
    Ok(File::from(fd))
}
/// # Errors
/// Refuses filesystem failure, unsafe inputs, corruption or quota violation.
pub fn regular(path: &Path, cap: u64) -> Result<File> {
    private(path.parent().ok_or(Error::State("file parent"))?)?;
    let fd = rustix::fs::open(
        path,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|_| Error::State("open regular file"))?;
    let f = File::from(fd);
    let m = f.metadata().map_err(|_| Error::State("file metadata"))?;
    check(
        m.is_file() && m.len() <= cap && m.nlink() == 1 && m.permissions().mode() & 0o777 == 0o600,
        "regular private file size/link/mode",
    )?;
    Ok(f)
}
/// # Errors
/// Refuses filesystem failure, unsafe inputs, corruption or quota violation.
pub fn io<T>(r: std::io::Result<T>) -> Result<T> {
    r.map_err(|_| Error::State("filesystem I/O"))
}
/// # Errors
/// Refuses filesystem failure, unsafe inputs, corruption or quota violation.
pub fn sync_dir(path: &Path) -> Result<()> {
    private(path)?;
    io(io(File::open(path))?.sync_all())
}
// Read exactly one fixed record, including checksum. A partial record is never EOF.
/// # Errors
/// Refuses filesystem failure, unsafe inputs, corruption or quota violation.
/// # Panics
/// Fixed-size slices are converted to arrays; their lengths are statically fixed.
pub fn read(f: &mut File) -> Result<Option<Entry>> {
    let mut b = [0u8; 64];
    let n = io(f.read(&mut b[..1]))?;
    if n == 0 {
        return Ok(None);
    }
    io(f.read_exact(&mut b[1..]))?;
    check(
        blake3::hash(&b[..32]).as_bytes() == &b[32..],
        "working/checkpoint record checksum",
    )?;
    let e = Entry {
        start: i64::from_le_bytes(b[0..8].try_into().expect("fixed")),
        key: u64::from_le_bytes(b[8..16].try_into().expect("fixed")),
        sum: i64::from_le_bytes(b[16..24].try_into().expect("fixed")),
        count: u64::from_le_bytes(b[24..32].try_into().expect("fixed")),
    };
    check(e.count > 0, "zero accumulator count")?;
    Ok(Some(e))
}
#[must_use]
pub fn encode(e: Entry) -> [u8; 64] {
    let mut b = [0; 64];
    b[..8].copy_from_slice(&e.start.to_le_bytes());
    b[8..16].copy_from_slice(&e.key.to_le_bytes());
    b[16..24].copy_from_slice(&e.sum.to_le_bytes());
    b[24..32].copy_from_slice(&e.count.to_le_bytes());
    let h = blake3::hash(&b[..32]);
    b[32..].copy_from_slice(h.as_bytes());
    b
}
impl Store {
    /// # Errors
    /// Refuses cancellation, corrupt state, filesystem failure or exceeded quotas.
    pub fn new(config: &Config, session: uuid::Uuid, token: CancellationToken) -> Result<Self> {
        config.validate()?;
        private(&config.working_root)?;
        check(
            io(fs::read_dir(&config.working_root))?.next().is_none(),
            "working root not empty; prove prior attempt dead and remove orphan state",
        )?;
        let root = config.working_root.join(session.to_string());
        io(fs::create_dir(&root))?;
        io(fs::set_permissions(
            &root,
            fs::Permissions::from_mode(0o700),
        ))?;
        let mut owners = Vec::with_capacity(config.owners);
        let slots = config.hot_bytes / CHARGE;
        for owner in 0..config.owners {
            let dir = root.join(format!("owner-{owner}"));
            io(fs::create_dir(&dir))?;
            io(fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)))?;
            owners.push(OwnerStore {
                root: dir,
                hot: Vec::with_capacity(
                    slots / config.owners + usize::from(owner < slots % config.owners),
                ),
                counts: [0; SHARDS],
            });
        }
        let store = Self {
            root,
            owners,
            disk_cap: config.cold_bytes,
            metrics: Metrics {
                hot_peak_bytes: config.hot_bytes / CHARGE * CHARGE,
                io_buffer_bytes: 4096,
                ..Metrics::default()
            },
            token,
            poisoned: false,
            injected_write_bytes: None,
        };
        for s in 0..SHARDS {
            create_file(&store.path(s))?;
        }
        Ok(store)
    }
    #[must_use]
    pub fn path(&self, s: usize) -> PathBuf {
        self.owners[s % self.owners.len()]
            .root
            .join(format!("shard-{s:02}.bin"))
    }
    fn owner(&self, key: u64) -> usize {
        shard(key) % self.owners.len()
    }
    fn cold_count(&self, s: usize) -> u64 {
        self.owners[s % self.owners.len()].counts[s]
    }
    fn live(&self) -> Result<()> {
        self.token.check().map_err(|_| Error::Cancelled)?;
        check(!self.poisoned, "poisoned working store")
    }
    #[must_use]
    pub fn disk_bytes(&self) -> u64 {
        self.owners
            .iter()
            .map(|owner| owner.counts.iter().sum::<u64>())
            .sum::<u64>()
            * RECORD
    }
    #[must_use]
    pub fn hot_bytes(&self) -> usize {
        self.owners
            .iter()
            .map(|owner| owner.hot.capacity() * CHARGE)
            .sum()
    }
    /// # Errors
    /// Refuses cancellation, corrupt state, filesystem failure or exceeded quotas.
    /// # Panics
    /// Iterator peek and next must agree; both borrow the same immutable vector.
    pub fn visit_shard(&self, s: usize, mut visit: impl FnMut(Entry) -> Result<()>) -> Result<()> {
        self.live()?;
        check(s < SHARDS, "shard index")?;
        let mut file = regular(&self.path(s), self.disk_cap)?;
        check(
            io(file.metadata())?.len() == self.cold_count(s) * RECORD,
            "cold length/count",
        )?;
        let mut hot = self.owners[s % self.owners.len()]
            .hot
            .iter()
            .filter(|e| shard(e.key) == s)
            .peekable();
        let mut cold = read(&mut file)?;
        let mut previous = None;
        let mut count = 0;
        while cold.is_some() || hot.peek().is_some() {
            self.live()?;
            let use_hot = hot
                .peek()
                .is_some_and(|h| cold.is_none_or(|c| h.id() < c.id()));
            let e = if use_hot {
                *hot.next().expect("peeked")
            } else {
                let e = cold.take().expect("present");
                count += 1;
                cold = read(&mut file)?;
                e
            };
            check(
                previous.is_none_or(|p| p < e.id()) && shard(e.key) == s,
                "duplicate/unsorted/foreign shard record",
            )?;
            previous = Some(e.id());
            visit(e)?;
        }
        check(count == self.cold_count(s), "cold count")
    }
    /// # Errors
    /// Refuses cancellation, corrupt state, filesystem failure or exceeded quotas.
    pub fn first_closed(&self, frontier: Option<i64>, width: i64) -> Result<Option<Entry>> {
        let Some(f) = frontier else { return Ok(None) };
        let mut first = None;
        for s in 0..SHARDS {
            self.visit_shard(s, |e| {
                if end(e.start, width)? <= f && first.is_none_or(|old: Entry| e.id() < old.id()) {
                    first = Some(e);
                }
                Ok(())
            })?;
        }
        Ok(first)
    }
    /// Inject a real partial cold-file write followed by a structured I/O failure.
    /// Per-store, explicit test seam; absent in every normal runtime instance.
    pub fn inject_write_failure_after(&mut self, bytes: usize) {
        self.injected_write_bytes = Some(bytes);
    }
    fn cold_write(&mut self, file: &mut File, entry: Entry) -> Result<()> {
        let encoded = encode(entry);
        if let Some(remaining) = self.injected_write_bytes {
            let n = remaining.min(encoded.len());
            io(file.write_all(&encoded[..n]))?;
            self.injected_write_bytes = Some(remaining - n);
            if n < encoded.len() {
                return Err(Error::State("injected cold partial write/no space"));
            }
            Ok(())
        } else {
            io(file.write_all(&encoded))
        }
    }
    fn rewrite(&mut self, s: usize, target: Entry, insert: bool) -> Result<()> {
        self.live()?;
        self.poisoned = true;
        let old = self.cold_count(s);
        let new = if insert {
            old.checked_add(1).ok_or(Error::Overflow("cold records"))?
        } else {
            old.checked_sub(1)
                .ok_or(Error::State("missing cold record"))?
        };
        let peak = self
            .disk_bytes()
            .checked_add(new * RECORD)
            .ok_or(Error::Overflow("cold bytes"))?;
        check(peak <= self.disk_cap, "cold disk quota including rewrite")?;
        self.metrics.disk_peak_bytes = self.metrics.disk_peak_bytes.max(peak);
        let path = self.path(s);
        let tmp = path.with_extension("tmp");
        let mut input = regular(&path, self.disk_cap)?;
        check(io(input.metadata())?.len() == old * RECORD, "cold length")?;
        let mut out = create_file(&tmp)?;
        let mut found = false;
        let mut previous = None;
        let mut count = 0;
        while let Some(e) = read(&mut input)? {
            self.token.check().map_err(|_| Error::Cancelled)?;
            check(
                shard(e.key) == s && previous.is_none_or(|p| p < e.id()),
                "cold ownership/order",
            )?;
            previous = Some(e.id());
            count += 1;
            if insert && !found && target.id() < e.id() {
                self.cold_write(&mut out, target)?;
                found = true;
            }
            if e.id() == target.id() {
                check(!insert && !found, "duplicate cold record")?;
                found = true;
            } else {
                self.cold_write(&mut out, e)?;
            }
        }
        check(count == old, "cold count")?;
        if insert && !found {
            self.cold_write(&mut out, target)?;
            found = true;
        }
        check(found, "cold record missing")?;
        io(fs::rename(tmp, path))?;
        let owner = s % self.owners.len();
        self.owners[owner].counts[s] = new;
        self.poisoned = false;
        Ok(())
    }
    /// # Errors
    /// Refuses cancellation, corrupt state, filesystem failure or exceeded quotas.
    pub fn get(&mut self, id: (i64, u64)) -> Result<Option<Entry>> {
        self.live()?;
        let owner = self.owner(id.1);
        if let Ok(index) = self.owners[owner]
            .hot
            .binary_search_by_key(&id, |entry| entry.id())
        {
            self.metrics.hot_hits += 1;
            return Ok(Some(self.owners[owner].hot[index]));
        }
        let mut found = None;
        self.visit_shard(shard(id.1), |e| {
            if e.id() == id {
                found = Some(e);
            }
            Ok(())
        })?;
        if let Some(e) = found {
            self.rewrite(shard(e.key), e, false)?;
            self.insert_hot(e)?;
            self.metrics.cold_hits += 1;
        }
        Ok(found)
    }
    fn insert_hot(&mut self, e: Entry) -> Result<()> {
        let owner = self.owner(e.key);
        if let Ok(index) = self.owners[owner]
            .hot
            .binary_search_by_key(&e.id(), |entry| entry.id())
        {
            self.owners[owner].hot[index] = e;
            return Ok(());
        }
        if self.owners[owner].hot.len() == self.owners[owner].hot.capacity() {
            let victim = *self.owners[owner]
                .hot
                .first()
                .ok_or(Error::State("hot owner cap"))?;
            self.rewrite(shard(victim.key), victim, true)?;
            self.owners[owner].hot.remove(0);
            self.metrics.evictions += 1;
        }
        let hot = &mut self.owners[owner].hot;
        let position = hot
            .binary_search_by_key(&e.id(), |entry| entry.id())
            .unwrap_or_else(|index| index);
        check(hot.len() < hot.capacity(), "hot allocation cap")?;
        hot.insert(position, e);
        Ok(())
    }
    /// # Errors
    /// Refuses cancellation, corrupt state, filesystem failure or exceeded quotas.
    pub fn put(&mut self, e: Entry, new: bool) -> Result<()> {
        self.live()?;
        self.insert_hot(e)?;
        if new {
            self.metrics.windows = self
                .metrics
                .windows
                .checked_add(1)
                .ok_or(Error::Overflow("windows"))?;
        }
        Ok(())
    }
    /// # Errors
    /// Refuses cancellation, corrupt state, filesystem failure or exceeded quotas.
    pub fn remove(&mut self, e: Entry) -> Result<()> {
        self.live()?;
        let owner = self.owner(e.key);
        if let Ok(index) = self.owners[owner]
            .hot
            .binary_search_by_key(&e.id(), |entry| entry.id())
        {
            self.owners[owner].hot.remove(index);
        } else {
            self.rewrite(shard(e.key), e, false)?;
        }
        self.metrics.windows = self
            .metrics
            .windows
            .checked_sub(1)
            .ok_or(Error::State("window count"))?;
        Ok(())
    }
    /// # Errors
    /// Refuses cancellation, corrupt state, filesystem failure or exceeded quotas.
    pub fn restore_entry(&mut self, e: Entry) -> Result<()> {
        check(self.get(e.id())?.is_none(), "duplicate restored entry")?;
        self.put(e, true)
    }
}
impl Drop for Store {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
