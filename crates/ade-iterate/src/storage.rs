use crate::{Error, Guard, Result, fault, require, source::no_symlinks};
use ade_exec::iterative::Cut;
use ade_logical::iterative::{digest, is_digest};
use ade_physical::cycle::MAX_ROUNDS;
use rustix::fs::{AtFlags, Dir, Mode, OFlags, linkat, open, openat, unlinkat};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{DirBuilder, File, Metadata, TryLockError},
    io::{self, Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt},
    path::Path,
};

const LOCK: &str = "LEASE20";
const ANCHOR: &str = "CYCLE20";
const STAGE_OBJECT: &str = ".stage20-object";
const STAGE_WITNESS: &str = ".stage20-witness";
const MAX_ENTRIES: usize = 3 * MAX_ROUNDS as usize + 5;
const OBJECT_CAP: usize = 128 * 1024;
const WITNESS_CAP: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Location {
    device: u64,
    directory: u64,
    lease: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pin {
    pub sequence: u32,
    pub round: u32,
    pub chain: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Anchor<'a> {
    version: u8,
    location: Location,
    plan: &'a str,
    source: &'a str,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Witness {
    version: u8,
    sequence: u32,
    round: u32,
    parent: String,
    object: String,
    bytes: usize,
    terminal: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Snapshot {
    pub version: u8,
    pub plan: String,
    pub source: String,
    pub cut: Cut,
    pub result: Option<crate::Footer>,
}
impl Snapshot {
    fn check(&self, plan: &str, source: &str, witness: &Witness) -> Result<()> {
        require(
            self.version == 1
                && self.plan == plan
                && self.source == source
                && self.cut.next_round == witness.round
                && self.cut.in_flight == 0
                && (self.cut.frontier.is_empty() == witness.terminal)
                && (self.result.is_some() == witness.terminal),
            "checkpoint identity, round or terminal mismatch",
        )
    }
}

pub(crate) struct Root {
    directory: File,
    _lease: File,
    pub pin: Pin,
    pub selected: Option<Snapshot>,
    pub bytes: u64,
    pub entries: usize,
    limit: u64,
}

fn io_error(e: rustix::io::Errno) -> io::Error {
    io::Error::from(e)
}

fn part(kind: char, sequence: u32) -> String {
    format!("{kind}20-{sequence:04}")
}
fn ordinal(name: &str) -> Option<(char, u32)> {
    let (prefix, digits) = name.split_once('-')?;
    let kind = match prefix {
        "s20" => 's',
        "o20" => 'o',
        "w20" => 'w',
        _ => return None,
    };
    let seq: u32 = digits.parse().ok()?;
    (seq > 0 && seq <= MAX_ROUNDS && part(kind, seq) == name).then_some((kind, seq))
}

fn owned(meta: &Metadata, mode: u32) -> bool {
    meta.uid() == rustix::process::geteuid().as_raw() && meta.mode() & 0o7777 == mode
}
fn directory(path: &Path) -> Result<File> {
    no_symlinks(path)?;
    let fd = File::from(
        open(
            path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(io_error)?,
    );
    require(
        owned(&fd.metadata()?, 0o700),
        "private cycle20 directory required",
    )?;
    Ok(fd)
}

fn read_file(dir: &File, name: &str) -> Result<File> {
    let file = File::from(
        openat(
            dir,
            name,
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(io_error)?,
    );
    let meta = file.metadata()?;
    require(
        meta.is_file() && owned(&meta, 0o600),
        "private regular cycle20 file required",
    )?;
    Ok(file)
}

fn new_file(dir: &File, name: &str) -> Result<File> {
    Ok(File::from(
        openat(
            dir,
            name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(io_error)?,
    ))
}
fn read(dir: &File, name: &str, cap: usize, guard: &Guard) -> Result<Vec<u8>> {
    let mut file = read_file(dir, name)?;
    let len =
        usize::try_from(file.metadata()?.len()).map_err(|_| Error::Limit("root file bytes"))?;
    if len > cap {
        return Err(Error::Limit("root file bytes"));
    }
    let mut bytes = vec![0; len];
    for chunk in bytes.chunks_mut(16 * 1024) {
        guard.check()?;
        file.read_exact(chunk)?;
    }
    require(file.read(&mut [0])? == 0, "root file grew")?;
    Ok(bytes)
}

fn link(dir: &File, source: &str, target: &str) -> Result<()> {
    linkat(dir, source, dir, target, AtFlags::empty()).map_err(io_error)?;
    Ok(())
}
fn unlink(dir: &File, name: &str) -> Result<()> {
    unlinkat(dir, name, AtFlags::empty()).map_err(io_error)?;
    Ok(())
}
fn fence(lease: &File, writer: bool) -> Result<()> {
    match if writer {
        lease.try_lock()
    } else {
        lease.try_lock_shared()
    } {
        Ok(()) => Ok(()),
        Err(TryLockError::WouldBlock) => {
            Err(Error::Refused("cycle20 root is fenced by another process"))
        }
        Err(TryLockError::Error(e)) => Err(e.into()),
    }
}
fn anchor(dir: &File, lease: &File, plan: &str, source: &str) -> Result<Vec<u8>> {
    let dir = dir.metadata()?;
    let lease = lease.metadata()?;
    Ok(serde_json::to_vec(&Anchor {
        version: 1,
        location: Location {
            device: dir.dev(),
            directory: dir.ino(),
            lease: lease.ino(),
        },
        plan,
        source,
    })?)
}

struct Inventory {
    names: BTreeSet<String>,
    selectors: u32,
    bytes: u64,
}
impl Inventory {
    fn scan(dir: &File, limit: u64, guard: &Guard) -> Result<Self> {
        let mut names = BTreeSet::new();
        let mut seen = BTreeMap::<u64, (u64, String)>::new();
        let mut bytes = 0;
        let mut selectors = 0;
        for entry in Dir::read_from(dir).map_err(io_error)? {
            guard.check()?;
            let entry = entry.map_err(io_error)?;
            let name = entry
                .file_name()
                .to_str()
                .map_err(|_| Error::Refused("non-UTF8 cycle20 name"))?;
            if name == "." || name == ".." {
                continue;
            }
            if names.len() == MAX_ENTRIES {
                return Err(Error::Limit("root metadata entries"));
            }
            let kind = ordinal(name);
            require(
                matches!(name, LOCK | ANCHOR | STAGE_OBJECT | STAGE_WITNESS) || kind.is_some(),
                "foreign cycle20 root entry",
            )?;
            if let Some(('s', seq)) = kind {
                selectors = selectors.max(seq);
            }
            let meta = read_file(dir, name)?.metadata()?;
            if let Some((links, first)) = seen.get_mut(&meta.ino()) {
                require(
                    *links == 1 && legal_pair(first, name),
                    "unexpected cycle20 alias",
                )?;
                *links = 0;
            } else {
                seen.insert(
                    meta.ino(),
                    (
                        meta.nlink()
                            .checked_sub(1)
                            .ok_or(Error::Refused("unlinked cycle20 file"))?,
                        name.into(),
                    ),
                );
                bytes += meta.len();
                if bytes > limit {
                    return Err(Error::Limit("retained disk bytes"));
                }
            }
            names.insert(name.into());
        }
        require(
            seen.values().all(|(links, _)| *links == 0),
            "external cycle20 file alias",
        )?;
        Ok(Self {
            names,
            selectors,
            bytes,
        })
    }
}
fn legal_pair(first: &str, second: &str) -> bool {
    match (ordinal(first), ordinal(second)) {
        (Some((x, i)), Some((y, j))) => i == j && matches!((x, y), ('s', 'w') | ('w', 's')),
        (Some((kind, _)), None) => {
            matches!((kind, second), ('o', STAGE_OBJECT) | ('w', STAGE_WITNESS))
        }
        (None, Some(_)) => legal_pair(second, first),
        (None, None) => false,
    }
}

impl Root {
    pub fn create(
        path: &Path,
        plan: &str,
        source: &str,
        limit: u64,
        guard: &Guard,
    ) -> Result<Self> {
        no_symlinks(path)?;
        require(path.parent().is_some(), "cycle20 root parent required")?;
        if limit < 256 {
            return Err(Error::Limit("retained disk bytes for anchor"));
        }
        guard.check()?;
        DirBuilder::new().mode(0o700).create(path)?;
        let dir = directory(path)?;
        let lease = new_file(&dir, LOCK)?;
        fence(&lease, true)?;
        let raw = anchor(&dir, &lease, plan, source)?;
        let bytes = raw.len() as u64;
        if bytes > limit {
            return Err(Error::Limit("retained disk bytes for anchor"));
        }
        let mut file = new_file(&dir, ANCHOR)?;
        file.write_all(&raw)?;
        file.sync_all()?;
        lease.sync_all()?;
        dir.sync_all()?;
        File::open(
            path.parent()
                .ok_or(Error::Refused("cycle20 root parent required"))?,
        )?
        .sync_all()?;
        let pin = Pin {
            sequence: 0,
            round: 0,
            chain: digest("ade-cycle20-anchor-v1", &raw),
        };
        Ok(Self {
            directory: dir,
            _lease: lease,
            pin,
            selected: None,
            bytes,
            entries: 2,
            limit,
        })
    }

    pub fn open(
        path: &Path,
        plan: &str,
        source: &str,
        expected: &Pin,
        limit: u64,
        writer: bool,
        guard: &Guard,
    ) -> Result<Self> {
        require(
            expected.sequence <= MAX_ROUNDS
                && expected.round <= MAX_ROUNDS
                && is_digest(&expected.chain),
            "invalid independently pinned cycle20 head",
        )?;
        let dir = directory(path)?;
        let lease = read_file(&dir, LOCK)?;
        fence(&lease, writer)?;
        let inventory = Inventory::scan(&dir, limit, guard)?;
        let raw = read(&dir, ANCHOR, WITNESS_CAP, guard)?;
        require(
            raw == anchor(&dir, &lease, plan, source)?,
            "cycle20 root/plan/source binding changed",
        )?;
        require(
            inventory.selectors == expected.sequence,
            "cycle20 selector differs from exact pin",
        )?;
        let mut pin = Pin {
            sequence: 0,
            round: 0,
            chain: digest("ade-cycle20-anchor-v1", &raw),
        };
        let mut selected = None;
        for seq in 1..=expected.sequence {
            let (new_pin, snapshot) = selected_cut(&dir, plan, source, &pin, seq, guard)?;
            require(
                selected
                    .as_ref()
                    .is_none_or(|s: &Snapshot| s.result.is_none()),
                "descendant after terminal cut",
            )?;
            pin = new_pin;
            selected = Some(snapshot);
        }
        require(
            &pin == expected,
            "cycle20 selected chain/round differs from exact pin",
        )?;
        let mut root = Self {
            directory: dir,
            _lease: lease,
            pin,
            selected,
            bytes: inventory.bytes,
            entries: inventory.names.len(),
            limit,
        };
        root.pending(&inventory, writer, guard)?;
        Ok(root)
    }

    fn pending(&mut self, inventory: &Inventory, writer: bool, guard: &Guard) -> Result<()> {
        let next = self.pin.sequence + 1;
        let mut pending = Vec::new();
        for name in &inventory.names {
            if matches!(name.as_str(), STAGE_OBJECT | STAGE_WITNESS) {
                pending.push(name);
            } else if let Some((kind, sequence)) = ordinal(name)
                && sequence > self.pin.sequence
            {
                require(
                    sequence == next && matches!(kind, 'o' | 'w'),
                    "unselected cycle20 fork",
                )?;
                pending.push(name);
            }
        }
        if writer && !pending.is_empty() {
            for name in pending {
                guard.check()?;
                unlink(&self.directory, name)?;
            }
            self.directory.sync_all()?;
            let inventory = Inventory::scan(&self.directory, self.limit, guard)?;
            self.bytes = inventory.bytes;
            self.entries = inventory.names.len();
        }
        Ok(())
    }

    pub fn commit(
        &mut self,
        snapshot: &Snapshot,
        guard: &Guard,
        prepared: impl FnOnce(&Pin) -> Result<()>,
        attest: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        let seq = self.pin.sequence + 1;
        let round = snapshot.cut.next_round;
        require(
            seq <= MAX_ROUNDS
                && round <= MAX_ROUNDS
                && round > self.pin.round
                && snapshot.cut.in_flight == 0,
            "cycle20 publication round",
        )?;
        let object = serde_json::to_vec(snapshot)?;
        let witness = serde_json::to_vec(&Witness {
            version: 1,
            sequence: seq,
            round,
            parent: self.pin.chain.clone(),
            object: digest("ade-cycle20-object-v1", &object),
            bytes: object.len(),
            terminal: snapshot.result.is_some(),
        })?;
        let total = self.bytes + (object.len() + witness.len()) as u64;
        if object.len() > OBJECT_CAP
            || witness.len() > WITNESS_CAP
            || total > self.limit
            || self.entries + 4 > MAX_ENTRIES
        {
            return Err(Error::Limit("retained disk bytes or root metadata"));
        }
        guard.check()?;
        fault("enospc_continue", round, guard)?;
        self.stage(&object, round, guard)?;
        self.publish_witness(&witness, seq, round, guard)?;
        let next = Pin {
            sequence: seq,
            round,
            chain: digest("ade-cycle20-witness-v1", &witness),
        };
        prepared(&next)?;
        fault("before_select", round, guard)?;
        attest()?;
        guard.check()?;
        link(&self.directory, &part('w', seq), &part('s', seq))?;
        fault("selected", round, guard)?;
        self.directory.sync_all()?;
        fault("directory_synced", round, guard)?;
        self.pin = next;
        self.bytes = total;
        self.entries += 3;
        Ok(())
    }

    fn stage(&self, object: &[u8], round: u32, guard: &Guard) -> Result<()> {
        let mut file = new_file(&self.directory, STAGE_OBJECT)?;
        fault("stage_open", round, guard)?;
        let half = object.len() / 2;
        file.write_all(&object[..half])?;
        fault("short_write", round, guard)?;
        file.write_all(&object[half..])?;
        fault("stage_written", round, guard)?;
        file.sync_all()?;
        fault("stage_synced", round, guard)?;
        Ok(())
    }

    fn publish_witness(&self, witness: &[u8], seq: u32, round: u32, guard: &Guard) -> Result<()> {
        link(&self.directory, STAGE_OBJECT, &part('o', seq))?;
        fault("object_linked", round, guard)?;
        unlink(&self.directory, STAGE_OBJECT)?;
        let mut stage = new_file(&self.directory, STAGE_WITNESS)?;
        stage.write_all(witness)?;
        stage.sync_all()?;
        link(&self.directory, STAGE_WITNESS, &part('w', seq))?;
        unlink(&self.directory, STAGE_WITNESS)?;
        self.directory.sync_all()?;
        fault("witness_synced", round, guard)
    }
}

fn selected_cut(
    dir: &File,
    plan: &str,
    source: &str,
    previous: &Pin,
    seq: u32,
    guard: &Guard,
) -> Result<(Pin, Snapshot)> {
    let select = part('s', seq);
    let witness_path = part('w', seq);
    require(
        read_file(dir, &select)?.metadata()?.ino()
            == read_file(dir, &witness_path)?.metadata()?.ino(),
        "selector and durable witness are not one inode",
    )?;
    let raw = read(dir, &select, WITNESS_CAP, guard)?;
    let witness: Witness = serde_json::from_slice(&raw)?;
    require(
        witness.version == 1
            && witness.sequence == seq
            && witness.round > previous.round
            && witness.round <= MAX_ROUNDS
            && witness.parent == previous.chain,
        "cycle20 witness sequence, round or parent mismatch",
    )?;
    let object = read(dir, &part('o', seq), OBJECT_CAP, guard)?;
    require(
        object.len() == witness.bytes && digest("ade-cycle20-object-v1", &object) == witness.object,
        "cycle20 checkpoint object digest mismatch",
    )?;
    let snapshot: Snapshot = serde_json::from_slice(&object)?;
    snapshot.check(plan, source, &witness)?;
    Ok((
        Pin {
            sequence: seq,
            round: witness.round,
            chain: digest("ade-cycle20-witness-v1", &raw),
        },
        snapshot,
    ))
}
