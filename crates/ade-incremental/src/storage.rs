use crate::{Error, Guard, Limits, Result, fault, require};
use ade_logical::maintained::{MAX_EPOCH, MaintainedPlanV1, SHARDS, identity, is_digest};
use rustix::fs::{Mode, OFlags};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions, TryLockError},
    io::{self, Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Component, Path, PathBuf},
};

const LEASE: &str = "LEASE19";
const STAGE: &str = ".stage19";
const MANIFEST: &str = "MAINTAINED19";
pub(crate) const MANIFEST_CAP: usize = 32 * 1024;
const MIN_SHARD_BYTES: u64 = 76;

pub(crate) fn no_symlinks(path: &Path) -> Result<()> {
    let mut prefix = PathBuf::new();
    for part in path.components() {
        require(
            !matches!(part, Component::ParentDir),
            "parent path component",
        )?;
        prefix.push(part);
        match fs::symlink_metadata(&prefix) {
            Ok(m) => require(!m.is_symlink(), "symlink path")?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => break,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

pub(crate) fn regular(path: &Path) -> Result<File> {
    let fd = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(io::Error::from)?;
    let file = File::from(fd);
    require(file.metadata()?.is_file(), "regular input file required")?;
    Ok(file)
}

fn private_file(path: &Path) -> Result<File> {
    let file = regular(path)?;
    require(
        file.metadata()?.permissions().mode() & 0o777 == 0o600,
        "private file required",
    )?;
    Ok(file)
}

fn private_dir(path: &Path) -> Result<File> {
    let fd = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(io::Error::from)?;
    let file = File::from(fd);
    require(
        file.metadata()?.permissions().mode() & 0o777 == 0o700,
        "private directory mode 0700 required",
    )?;
    Ok(file)
}

pub(crate) fn read_cap(path: &Path, cap: usize, guard: &Guard) -> Result<Vec<u8>> {
    read_bounded(regular(path)?, cap, guard)
}

fn read_bounded(mut file: File, cap: usize, guard: &Guard) -> Result<Vec<u8>> {
    let len = usize::try_from(file.metadata()?.len()).map_err(|_| Error::Quota("file bytes"))?;
    if len > cap {
        return Err(Error::Quota("file bytes"));
    }
    let mut bytes = vec![0; len];
    for slice in bytes.chunks_mut(16 * 1024) {
        guard.check()?;
        file.read_exact(slice)?;
    }
    require(file.read(&mut [0; 1])? == 0, "input grew during read")?;
    Ok(bytes)
}

fn new_file(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?)
}

fn ordinal(kind: char, epoch: u32) -> String {
    format!("{kind}-{epoch:08}")
}
fn shard_name(index: usize) -> String {
    format!("s-{index:03}.bin")
}
fn parse_ordinal(kind: char, name: &str) -> Option<u32> {
    let (_, digits) = name.split_once('-')?;
    let epoch = digits.parse().ok()?;
    (epoch <= MAX_EPOCH && name == ordinal(kind, epoch)).then_some(epoch)
}
fn parse_shard(name: &str) -> Option<usize> {
    let index = name
        .strip_prefix("s-")?
        .strip_suffix(".bin")?
        .parse()
        .ok()?;
    (index < SHARDS && name == shard_name(index)).then_some(index)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Location {
    device: u64,
    directory: u64,
    lease: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Object {
    pub epoch: u32,
    pub bytes: u64,
    pub hash: String,
    pub view_rows: u64,
    pub hot_entries: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    version: u32,
    location: Location,
    pub plan: String,
    pub epoch: u32,
    pub chain: String,
    pub history: Vec<String>,
    previous: Option<String>,
    pub result: String,
    pub objects: Vec<Option<Object>>,
}
impl Manifest {
    pub fn result(
        plan: &str,
        epoch: u32,
        chain: &str,
        objects: &[Option<Object>],
    ) -> Result<String> {
        let digests = objects
            .iter()
            .map(|o| o.as_ref().map(|o| &o.hash))
            .collect::<Vec<_>>();
        Ok(identity(
            "maintained-result-v1",
            &serde_json::to_vec(&(plan, epoch, chain, digests))?,
        ))
    }

    pub fn state_bytes(&self) -> u64 {
        self.objects.iter().flatten().map(|o| o.bytes).sum()
    }

    pub fn check_limits(&self, limits: Limits) -> Result<()> {
        if self.state_bytes() > limits.state_bytes {
            return Err(Error::Quota("arrangement encoded bytes"));
        }
        if self
            .objects
            .iter()
            .flatten()
            .any(|o| o.hot_entries > limits.hot_entries)
        {
            return Err(Error::Quota("committed shard hot entries"));
        }
        Ok(())
    }
}

pub(crate) struct Root {
    dir: PathBuf,
    fd: File,
    lease: Option<File>,
    location: Location,
    pub used: u64,
    pub committed: BTreeMap<u32, Manifest>,
    last_digest: Option<String>,
    pub pending: Option<Manifest>,
    stage: bool,
}
impl Root {
    pub fn open(
        path: &Path,
        writer: bool,
        initialize: bool,
        plan: &MaintainedPlanV1,
        limits: Limits,
        guard: &Guard,
    ) -> Result<Self> {
        no_symlinks(path)?;
        let fd = private_dir(path)?;
        let lockpath = path.join(LEASE);
        let lease = match private_file(&lockpath) {
            Ok(file) => Some(file),
            Err(Error::Io(e)) if e.kind() == io::ErrorKind::NotFound => {
                for entry in fs::read_dir(path)? {
                    require(
                        entry?.file_name() == LEASE,
                        "unrecognized or legacy root without lease",
                    )?;
                }
                if writer && initialize {
                    match new_file(&lockpath) {
                        Ok(file) => {
                            file.sync_all()?;
                            fd.sync_all()?;
                        }
                        Err(Error::Io(e)) if e.kind() == io::ErrorKind::AlreadyExists => {}
                        Err(e) => return Err(e),
                    }
                    Some(private_file(&lockpath)?)
                } else {
                    None
                }
            }
            Err(e) => return Err(e),
        };
        if let Some(lease) = &lease {
            let lock = if writer {
                lease.try_lock()
            } else {
                lease.try_lock_shared()
            };
            match lock {
                Ok(()) => {}
                Err(TryLockError::WouldBlock) => return Err(Error::Busy),
                Err(TryLockError::Error(e)) => return Err(e.into()),
            }
            let m = lease.metadata()?;
            require(m.nlink() == 1 && m.len() == 0, "invalid writer lease")?;
        }
        let meta = fd.metadata()?;
        let lease_inode = lease
            .as_ref()
            .map(File::metadata)
            .transpose()?
            .map_or(0, |m| m.ino());
        let location = Location {
            device: meta.dev(),
            directory: meta.ino(),
            lease: lease_inode,
        };
        let mut root = Self {
            dir: path.into(),
            fd,
            lease,
            location,
            used: 0,
            committed: BTreeMap::new(),
            last_digest: None,
            pending: None,
            stage: false,
        };
        root.inspect(plan, limits, guard)?;
        Ok(root)
    }

    fn inspect(&mut self, plan: &MaintainedPlanV1, limits: Limits, guard: &Guard) -> Result<()> {
        let mut generations = BTreeSet::new();
        let mut witnesses = BTreeSet::new();
        for item in fs::read_dir(&self.dir)? {
            guard.check()?;
            let item = item?;
            let name = item.file_name();
            let name = name.to_str().ok_or(Error::Invalid("root entry name"))?;
            if let Some(epoch) = parse_ordinal('g', name) {
                generations.insert(epoch);
                self.charge(inspect_dir(&item.path(), guard)?, limits)?;
            } else if let Some(epoch) = parse_ordinal('w', name) {
                witnesses.insert(epoch);
                let len = private_file(&item.path())?.metadata()?.len();
                require(len <= MANIFEST_CAP as u64, "witness bytes")?;
                self.charge(len, limits)?;
            } else if name == STAGE {
                self.stage = true;
                self.charge(inspect_dir(&item.path(), guard)?, limits)?;
            } else {
                require(
                    name == LEASE && self.lease.is_some(),
                    "foreign or legacy root entry",
                )?;
            }
        }
        for epoch in generations {
            guard.check()?;
            require(witnesses.remove(&epoch), "generation witness missing")?;
            let (manifest, digest) =
                self.inspect_manifest(epoch, &self.generation(epoch), plan, limits, guard)?;
            self.committed.insert(epoch, manifest);
            self.last_digest = Some(digest);
        }
        if let Some(&epoch) = witnesses.first() {
            require(
                witnesses.len() == 1 && self.stage,
                "selector rollback or missing witnessed stage",
            )?;
            self.pending = Some(
                self.inspect_manifest(epoch, &self.stage_path(), plan, limits, guard)?
                    .0,
            );
        }
        Ok(())
    }

    fn inspect_manifest(
        &self,
        epoch: u32,
        dir: &Path,
        plan: &MaintainedPlanV1,
        limits: Limits,
        guard: &Guard,
    ) -> Result<(Manifest, String)> {
        let file = private_file(&dir.join(MANIFEST))?;
        let w = private_file(&self.witness(epoch))?.metadata()?;
        let f = file.metadata()?;
        require(
            f.dev() == w.dev() && f.ino() == w.ino() && f.nlink() == 2,
            "manifest and witness inode mismatch",
        )?;
        let bytes = read_bounded(file, MANIFEST_CAP, guard)?;
        let manifest: Manifest = serde_json::from_slice(&bytes)?;
        self.verify_manifest(&manifest, epoch, plan)?;
        manifest.check_limits(limits)?;
        check_shard_inventory(dir, &manifest, guard)?;
        Ok((manifest, identity("maintained-manifest-v1", &bytes)))
    }

    fn verify_manifest(
        &self,
        manifest: &Manifest,
        epoch: u32,
        plan: &MaintainedPlanV1,
    ) -> Result<()> {
        require(
            manifest.version == 1
                && manifest.location == self.location
                && manifest.plan == plan.digest()
                && manifest.epoch == epoch
                && manifest.history.len() == epoch as usize
                && manifest.previous == self.last_digest
                && manifest.objects.len() == SHARDS,
            "foreign root, generation or semantic plan",
        )?;
        let genesis = plan.genesis();
        let chain = manifest.history.last().unwrap_or(&genesis);
        require(
            manifest.chain == *chain
                && manifest.history.iter().all(|h| is_digest(h))
                && self.head().is_none_or(|p| {
                    p.epoch + 1 == epoch && manifest.history.starts_with(&p.history)
                }),
            "generation history fork or rollback",
        )?;
        for (i, object) in manifest.objects.iter().enumerate() {
            if let Some(o) = object {
                require(
                    is_digest(&o.hash)
                        && (MIN_SHARD_BYTES..=crate::MAX_STATE_BYTES).contains(&o.bytes)
                        && (1..=2 * ((o.bytes - 16) / 60)).contains(&o.view_rows)
                        && (2..=(o.bytes - 16) / 16).contains(&(o.hot_entries as u64))
                        && (o.epoch == epoch
                            || self
                                .committed
                                .get(&o.epoch)
                                .is_some_and(|p| p.objects[i].as_ref() == Some(o))),
                    "invalid shard reference",
                )?;
            }
        }
        require(
            manifest.result == Manifest::result(&manifest.plan, epoch, chain, &manifest.objects)?,
            "generation result identity",
        )
    }

    pub fn head(&self) -> Option<&Manifest> {
        self.committed.last_key_value().map(|(_, m)| m)
    }

    fn generation(&self, epoch: u32) -> PathBuf {
        self.dir.join(ordinal('g', epoch))
    }
    fn witness(&self, epoch: u32) -> PathBuf {
        self.dir.join(ordinal('w', epoch))
    }
    fn stage_path(&self) -> PathBuf {
        self.dir.join(STAGE)
    }

    pub fn verify_objects(&self, m: &Manifest, guard: &Guard) -> Result<()> {
        for (i, obj) in m.objects.iter().enumerate() {
            if let Some(obj) = obj {
                self.verified_object(i, obj, guard)?;
            }
        }
        Ok(())
    }

    pub fn verified_object(&self, index: usize, object: &Object, guard: &Guard) -> Result<File> {
        let dir = if self
            .pending
            .as_ref()
            .is_some_and(|m| m.epoch == object.epoch)
        {
            self.stage_path()
        } else {
            self.generation(object.epoch)
        };
        verified_file(&dir.join(shard_name(index)), object, guard)
    }

    fn charge(&mut self, amount: u64, limits: Limits) -> Result<()> {
        self.used = self
            .used
            .checked_add(amount)
            .ok_or(Error::Quota("root disk bytes"))?;
        if self.used > limits.disk_bytes {
            return Err(Error::Quota("root disk bytes"));
        }
        Ok(())
    }

    pub fn clean_unwitnessed(&mut self, guard: &Guard) -> Result<()> {
        if self.stage && self.pending.is_none() {
            let stage = self.stage_path();
            let removed = inspect_dir(&stage, guard)?;
            fs::remove_dir_all(stage)?;
            self.fd.sync_all()?;
            self.stage = false;
            self.used -= removed;
        }
        Ok(())
    }

    pub fn start_stage(&mut self) -> Result<()> {
        require(!self.stage && self.pending.is_none(), "unresolved stage")?;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(self.stage_path())?;
        self.stage = true;
        Ok(())
    }

    pub fn write_shard(
        &mut self,
        index: usize,
        shard: &crate::arrangement::Shard,
        epoch: u32,
        limits: Limits,
        guard: &Guard,
    ) -> Result<Object> {
        let len = shard.len_bytes();
        if len > limits.state_bytes {
            return Err(Error::Quota("shard bytes"));
        }
        let bytes = shard.encode(guard)?;
        self.charge(len, limits)?;
        let mut file = new_file(&self.stage_path().join(shard_name(index)))?;
        let (first, rest) = bytes.split_at(16);
        file.write_all(first)?;
        fault("shard_partial")?;
        for chunk in rest.chunks(16 * 1024) {
            guard.check()?;
            file.write_all(chunk)?;
        }
        file.sync_all()?;
        Ok(Object {
            epoch,
            bytes: len,
            hash: ade_physical::fingerprint(&bytes),
            view_rows: shard.view_rows(),
            hot_entries: shard.entries(),
        })
    }

    pub fn manifest(
        &self,
        plan: &MaintainedPlanV1,
        history: Vec<String>,
        epoch: u32,
        objects: Vec<Option<Object>>,
    ) -> Result<Manifest> {
        let chain = history.last().cloned().unwrap_or_else(|| plan.genesis());
        let result = Manifest::result(plan.digest(), epoch, &chain, &objects)?;
        let manifest = Manifest {
            version: 1,
            location: self.location.clone(),
            plan: plan.digest().into(),
            epoch,
            chain,
            history,
            previous: self.last_digest.clone(),
            result,
            objects,
        };
        self.verify_manifest(&manifest, epoch, plan)?;
        Ok(manifest)
    }

    pub fn publish(
        &mut self,
        m: &Manifest,
        limits: Limits,
        guard: &Guard,
        attest: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        fault("objects_synced")?;
        let bytes = serde_json::to_vec(m)?;
        require(bytes.len() <= MANIFEST_CAP, "generation manifest cap")?;
        self.charge(2 * bytes.len() as u64, limits)?;
        let stage = self.stage_path();
        let path = stage.join(MANIFEST);
        let mut file = new_file(&path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        private_dir(&stage)?.sync_all()?;
        fault("manifest_synced")?;
        check_shard_inventory(&stage, m, guard)?;
        for (i, object) in m.objects.iter().enumerate() {
            if let Some(o) = object.as_ref().filter(|o| o.epoch == m.epoch) {
                verified_file(&stage.join(shard_name(i)), o, guard)?;
            }
        }
        require(
            read_bounded(private_file(&path)?, MANIFEST_CAP, guard)? == bytes,
            "staged manifest drift",
        )?;
        attest()?;
        guard.check()?;
        self.check_lease()?;
        // If the link operation itself reports an ambiguous I/O failure,
        // leave the bounded stage for the next opener to inspect.
        self.pending = Some(m.clone());
        fs::hard_link(&path, self.witness(m.epoch))?;
        self.fd.sync_all()?;
        fault("witness_synced")?;
        self.finish_witness(m.epoch)
    }

    pub fn finish_witness(&self, epoch: u32) -> Result<()> {
        self.check_lease()?;
        rustix::fs::renameat_with(
            &self.fd,
            STAGE,
            &self.fd,
            ordinal('g', epoch),
            rustix::fs::RenameFlags::NOREPLACE,
        )
        .map_err(io::Error::from)?;
        fault("selected")?;
        self.fd.sync_all()?;
        fault("selection_synced")
    }

    fn check_lease(&self) -> Result<()> {
        let lease = private_file(&self.dir.join(LEASE))?.metadata()?;
        let m = private_dir(&self.dir)?.metadata()?;
        require(
            m.dev() == self.location.device
                && m.ino() == self.location.directory
                && lease.ino() == self.location.lease
                && lease.nlink() == 1
                && lease.len() == 0,
            "writer lease or root changed",
        )
    }
}

fn verified_file(path: &Path, object: &Object, guard: &Guard) -> Result<File> {
    use std::io::Seek;
    let mut file = private_file(path)?;
    require(file.metadata()?.len() == object.bytes, "shard byte drift")?;
    let mut hash = blake3::Hasher::new();
    let mut remaining = object.bytes;
    let mut buffer = [0; 16 * 1024];
    while remaining > 0 {
        guard.check()?;
        let n = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| Error::Quota("shard bytes"))?;
        file.read_exact(&mut buffer[..n])?;
        hash.update(&buffer[..n]);
        remaining -= n as u64;
    }
    require(
        file.read(&mut [0; 1])? == 0 && hash.finalize().to_hex().as_str() == object.hash,
        "shard fingerprint drift",
    )?;
    file.rewind()?;
    Ok(file)
}

fn inspect_dir(path: &Path, guard: &Guard) -> Result<u64> {
    private_dir(path)?;
    let mut used = 0u64;
    for item in fs::read_dir(path)? {
        guard.check()?;
        let item = item?;
        require(
            item.file_name()
                .to_str()
                .is_some_and(|n| n == MANIFEST || parse_shard(n).is_some()),
            "foreign generation entry",
        )?;
        used = used
            .checked_add(private_file(&item.path())?.metadata()?.len())
            .ok_or(Error::Quota("generation bytes"))?;
    }
    Ok(used)
}

fn check_shard_inventory(dir: &Path, m: &Manifest, guard: &Guard) -> Result<()> {
    let mut seen = 0usize;
    for entry in fs::read_dir(dir)? {
        guard.check()?;
        let entry = entry?;
        if entry.file_name() == MANIFEST {
            continue;
        }
        let index = entry
            .file_name()
            .to_str()
            .and_then(parse_shard)
            .ok_or(Error::Invalid("invalid shard filename"))?;
        let object = m.objects[index]
            .as_ref()
            .ok_or(Error::Invalid("unclaimed shard object"))?;
        let meta = private_file(&entry.path())?.metadata()?;
        require(
            object.epoch == m.epoch && meta.len() == object.bytes && meta.nlink() == 1,
            "shard inventory drift",
        )?;
        seen += 1;
    }
    require(
        seen == m
            .objects
            .iter()
            .flatten()
            .filter(|o| o.epoch == m.epoch)
            .count(),
        "missing shard object",
    )
}
