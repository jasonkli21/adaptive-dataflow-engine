//! A single selector binds state, progress, lineage and output. No directory discovery.
use super::{Config, migration, sink};
use crate::{
    Engine, Error, Progress, Result,
    state::{
        self, check,
        store::{self, io, private, regular, sync_dir},
    },
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
};
use uuid::Uuid;
pub const META: u64 = 65_536;
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Pointer {
    pub version: u32,
    pub epoch: u64,
    pub id: Uuid,
    pub blake3: String,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Operation {
    Commit,
    Paused,
    Transition,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Object {
    pub bytes: u64,
    pub blake3: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Shard {
    pub shard: usize,
    pub owner: usize,
    pub count: u64,
    pub object: Object,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub version: u32,
    pub epoch: u64,
    pub id: Uuid,
    pub parent: Option<Pointer>,
    pub fingerprint: String,
    pub job_id: Uuid,
    pub writer: Uuid,
    pub codec: u32,
    pub owners: usize,
    pub operation: Operation,
    pub progress: Progress,
    pub closed_window_late: u64,
    pub shards: Vec<Shard>,
    pub state_blake3: String,
    pub output: Object,
}
pub struct Verified {
    pub pointer: Pointer,
    pub manifest: Manifest,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Root {
    version: u32,
    path: PathBuf,
    job: Uuid,
    fingerprint: String,
}
/// Permanent inode: never unlink/recreate a lock to recover a crashed writer.
pub struct Lease {
    file: File,
    root: PathBuf,
    writer: Uuid,
}
impl Lease {
    /// # Errors
    /// Refuses unsafe/mixed/cloned roots and a competing writer.
    pub fn acquire(c: &Config) -> Result<Self> {
        c.validate()?;
        private(&c.state.checkpoint_root)?;
        let root = &c.state.checkpoint_root;
        check(
            !root.join("CURRENT").exists() && !root.join("CURRENT2").exists(),
            "mixed checkpoint formats",
        )?;
        let path = root.join("WRITER3");
        let fd = rustix::fs::open(
            &path,
            rustix::fs::OFlags::RDWR
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::from_bits_truncate(0o600),
        )
        .map_err(|_| Error::State("writer lock open"))?;
        let file = File::from(fd);
        let m = io(file.metadata())?;
        check(
            m.is_file() && m.nlink() == 1 && m.permissions().mode() & 0o777 == 0o600,
            "writer lock inode",
        )?;
        rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
            .map_err(|_| Error::State("writer busy/fenced"))?;
        if !root.join("ROOT3").exists() {
            // A root without identity is only fresh if no preexisting object exists.
            let names = io(fs::read_dir(root))?
                .collect::<std::io::Result<Vec<_>>>()
                .map_err(|_| Error::State("root inventory"))?;
            check(
                names.len() == 1 && names[0].file_name() == "WRITER3",
                "nonempty unidentified root",
            )?;
            write(
                &root.join("ROOT3"),
                &encode(&Root {
                    version: 3,
                    path: root.clone(),
                    job: c.state.job_id,
                    fingerprint: c.fingerprint()?,
                })?,
            )?;
            sync_dir(root)?;
        }
        root_identity(c)?;
        occupied(root, c.limits.root_bytes)?;
        let lease = Self {
            file,
            root: root.clone(),
            writer: Uuid::new_v4(),
        };
        lease.fence()?;
        Ok(lease)
    }
    pub(crate) fn fence(&self) -> Result<()> {
        private(&self.root)?;
        let named = regular(&self.root.join("WRITER3"), 512)?;
        let a = io(named.metadata())?;
        let b = io(self.file.metadata())?;
        check(
            a.dev() == b.dev() && a.ino() == b.ino(),
            "stale writer inode/fence",
        )
    }
}
/// # Errors
/// Root binding prevents implicit writable cloning; explicit savepoint import rebinds.
pub fn root_identity(c: &Config) -> Result<()> {
    private(&c.state.checkpoint_root)?;
    check(
        !c.state.checkpoint_root.join("CURRENT").exists()
            && !c.state.checkpoint_root.join("CURRENT2").exists(),
        "mixed checkpoint formats",
    )?;
    let r: Root = json(&bytes(&c.state.checkpoint_root.join("ROOT3"), 4096)?)?;
    check(
        r.version == 3
            && r.path == c.state.checkpoint_root
            && r.job == c.state.job_id
            && r.fingerprint == c.fingerprint()?,
        "foreign/cloned root identity",
    )
}
/// # Errors
/// Bounded strict JSON parsing.
pub fn json<T: DeserializeOwned>(b: &[u8]) -> Result<T> {
    serde_json::from_slice(b).map_err(|_| Error::State("strict v3 JSON"))
}
/// # Errors
/// Canonical struct/ordered-map serialization.
pub fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(v).map_err(|_| Error::State("v3 encode"))
}
#[must_use]
pub fn hash(b: &[u8]) -> String {
    blake3::hash(b).to_hex().to_string()
}
/// # Errors
/// Bounded nofollow private single-link read.
pub fn bytes(path: &Path, cap: u64) -> Result<Vec<u8>> {
    let file = regular(path, cap)?;
    let mut b = Vec::new();
    io(file.take(cap + 1).read_to_end(&mut b))?;
    check(b.len() as u64 <= cap, "v3 byte cap")?;
    Ok(b)
}
pub(crate) fn write(path: &Path, b: &[u8]) -> Result<()> {
    let mut f = store::create_file(path)?;
    io(f.write_all(b))?;
    io(f.sync_all())
}
pub(crate) fn directory(p: &Path) -> Result<()> {
    io(fs::create_dir(p))?;
    io(fs::set_permissions(p, fs::Permissions::from_mode(0o700)))
}
/// # Errors
/// Malformed authoritative selector is fatal; absence never triggers discovery.
pub fn selection(root: &Path) -> Result<Option<Pointer>> {
    private(root)?;
    match fs::symlink_metadata(root.join("CURRENT3")) {
        Ok(_) => {
            let p: Pointer = json(&bytes(&root.join("CURRENT3"), 512)?)?;
            pointer(&p)?;
            Ok(Some(p))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(Error::State("selector metadata")),
    }
}
fn pointer(p: &Pointer) -> Result<()> {
    check(
        p.version == 3
            && p.epoch > 0
            && !p.id.is_nil()
            && p.blake3.len() == 64
            && p.blake3
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
        "v3 pointer identity",
    )
}
fn dir(c: &Config, p: &Pointer) -> PathBuf {
    c.state.checkpoint_root.join(p.id.to_string())
}
fn object(b: &[u8]) -> Object {
    Object {
        bytes: b.len() as u64,
        blake3: hash(b),
    }
}
fn verify_object(path: &Path, o: &Object, cap: u64) -> Result<Vec<u8>> {
    check(o.bytes <= cap, "object capacity")?;
    let b = bytes(path, cap)?;
    check(
        b.len() as u64 == o.bytes && hash(&b) == o.blake3,
        "object digest/length",
    )?;
    Ok(b)
}
/// # Errors
/// Entire immutable chain, every record, state and output is validated.
pub fn verify(c: &Config, p: &Pointer) -> Result<Vec<Verified>> {
    verify_codec(c, p, true)
}
#[allow(
    clippy::too_many_lines,
    reason = "Bounded complete closure verification before adoption"
)]
pub(crate) fn verify_codec(c: &Config, p: &Pointer, enforce_codec: bool) -> Result<Vec<Verified>> {
    c.validate()?;
    pointer(p)?;
    let mut chain = Vec::new();
    let mut next = Some(p.clone());
    let mut total_output = 0u64;
    while let Some(p) = next.take() {
        check(chain.len() < c.limits.epochs, "epoch chain cap")?;
        pointer(&p)?;
        let b = bytes(&dir(c, &p).join("epoch.json"), META)?;
        check(hash(&b) == p.blake3, "epoch digest")?;
        let m: Manifest = json(&b)?;
        check(
            m.version == 3
                && m.epoch == p.epoch
                && m.id == p.id
                && m.job_id == c.state.job_id
                && m.fingerprint == c.fingerprint()?
                && !m.writer.is_nil()
                && [1, 2].contains(&m.codec)
                && [1, 2, 4].contains(&m.owners)
                && m.shards.len() == 32
                && m.closed_window_late <= m.progress.forwarded_records,
            "epoch identity/schema",
        )?;
        state::checkpoint::validate_progress(&c.state, &m.progress)?;
        let mut possible = 0u64;
        for start in &c.state.source.starts {
            let cursor = &m.progress.partitions[&start.partition];
            check(cursor.start >= start.offset, "origin progress")?;
            possible = possible
                .checked_add(
                    u64::try_from(cursor.next_offset_suggestion - start.offset)
                        .map_err(|_| Error::State("offset envelope"))?,
                )
                .ok_or(Error::Overflow("offset envelope"))?;
        }
        check(
            m.progress.forwarded_records <= possible
                && (m.progress.forwarded_records == 0)
                    == m.progress
                        .partitions
                        .values()
                        .all(|c| c.forwarded.is_none()),
            "record/source conservation",
        )?;
        check(
            m.parent
                .as_ref()
                .map_or(p.epoch == 1, |x| x.epoch.checked_add(1) == Some(p.epoch)),
            "broken epoch parent",
        )?;
        total_output = total_output
            .checked_add(m.output.bytes)
            .ok_or(Error::Overflow("output history"))?;
        check(
            total_output <= c.limits.history_bytes && m.output.bytes <= c.limits.output_bytes,
            "output history quota",
        )?;
        next.clone_from(&m.parent);
        chain.push(Verified {
            pointer: p,
            manifest: m,
        });
    }
    chain.reverse();
    if enforce_codec {
        check(
            chain.last().is_some_and(|e| e.manifest.codec <= c.codec),
            "old codec reader refuses new format",
        )?;
    }
    let mut ids = BTreeMap::new();
    let mut previous: Option<&Manifest> = None;
    let mut emitted_count = 0u64;
    for e in &chain {
        let mut h = blake3::Hasher::new();
        let mut state_count = 0u64;
        entries(c, e, |record| {
            h.update(&store::encode(record));
            state_count = state_count
                .checked_add(record.count)
                .ok_or(Error::Overflow("state count"))?;
            Ok(())
        })?;
        check(
            h.finalize().to_hex().as_str() == e.manifest.state_blake3,
            "logical state digest",
        )?;
        let out = output(c, e)?;
        for o in &out {
            o.validate(c)?;
            check(
                e.manifest
                    .progress
                    .frontier
                    .is_some_and(|f| o.window_end <= f),
                "output beyond frontier",
            )?;
            sink::unique(&mut ids, o)?;
            emitted_count = emitted_count
                .checked_add(o.count_u64)
                .ok_or(Error::Overflow("output count"))?;
        }
        check(
            state_count
                .checked_add(emitted_count)
                .and_then(|n| n.checked_add(e.manifest.closed_window_late))
                .is_some_and(|n| n <= e.manifest.progress.forwarded_records),
            "state/output/source conservation",
        )?;
        if let Some(old) = previous {
            let m = &e.manifest;
            check(
                m.progress.forwarded_records >= old.progress.forwarded_records
                    && m.progress.late_records >= old.progress.late_records
                    && m.closed_window_late >= old.closed_window_late
                    && old
                        .progress
                        .frontier
                        .is_none_or(|f| m.progress.frontier.is_some_and(|n| n >= f)),
                "lineage progress rollback",
            )?;
            for (p, a) in &old.progress.partitions {
                check(
                    m.progress.partitions[p].next_offset_suggestion >= a.next_offset_suggestion,
                    "source rollback",
                )?;
            }
            if m.operation == Operation::Transition {
                check(
                    m.state_blake3 == old.state_blake3
                        && out.is_empty()
                        && same_progress(&m.progress, &old.progress)
                        && m.closed_window_late == old.closed_window_late,
                    "migration semantic mismatch",
                )?;
            } else {
                check(
                    m.codec == old.codec && m.owners == old.owners,
                    "implicit migration refused",
                )?;
            }
        }
        previous = Some(&e.manifest);
    }
    Ok(chain)
}
fn same_progress(a: &Progress, b: &Progress) -> bool {
    a.frontier == b.frontier
        && a.forwarded_records == b.forwarded_records
        && a.late_records == b.late_records
        && a.partitions.iter().all(|(p, c)| {
            b.partitions.get(p).is_some_and(|d| {
                c.next_offset_suggestion == d.next_offset_suggestion
                    && c.forwarded == d.forwarded
                    && c.max_forwarded_time == d.max_forwarded_time
                    && c.watermark == d.watermark
            })
        })
}
/// # Errors
/// Streaming full-state validation; fixed one-record scratch per shard.
pub fn entries(
    c: &Config,
    e: &Verified,
    mut visit: impl FnMut(state::Entry) -> Result<()>,
) -> Result<()> {
    let m = &e.manifest;
    let mut total = 0u64;
    for (s, ref_shard) in m.shards.iter().enumerate() {
        check(
            ref_shard.shard == s
                && ref_shard.owner == s % m.owners
                && ref_shard.count.checked_mul(migration::width(m.codec))
                    == Some(ref_shard.object.bytes),
            "shard ownership/count",
        )?;
        total = total
            .checked_add(ref_shard.object.bytes)
            .ok_or(Error::Overflow("state size"))?;
        check(total <= c.limits.stage_bytes, "state size quota")?;
        let mut f = regular(
            &dir(c, &e.pointer).join(format!("shard-{s:02}.bin")),
            c.limits.stage_bytes,
        )?;
        check(
            io(f.metadata())?.len() == ref_shard.object.bytes,
            "state object length",
        )?;
        let mut raw_hash = blake3::Hasher::new();
        let mut previous = None;
        let mut count = 0u64;
        while let Some(record) = migration::read(&mut f, m.codec, c.state.window_ms)? {
            check(
                state::shard(record.key) == s
                    && record.key < c.state.source.key_less_than
                    && previous.is_none_or(|p| p < record.id())
                    && m.progress.frontier.is_none_or(|frontier| {
                        record
                            .start
                            .checked_add(c.state.window_ms)
                            .is_some_and(|end| end > frontier)
                    }),
                "state ownership/order/closure",
            )?;
            let mut encoded = Vec::with_capacity(80);
            migration::write(&mut encoded, record, m.codec, c.state.window_ms)?;
            raw_hash.update(&encoded);
            previous = Some(record.id());
            count += 1;
            visit(record)?;
        }
        check(
            count == ref_shard.count
                && raw_hash.finalize().to_hex().as_str() == ref_shard.object.blake3,
            "shard digest/count",
        )?;
    }
    Ok(())
}
/// # Errors
/// Exact immutable output segment, bounded per epoch and per record.
pub fn output(c: &Config, e: &Verified) -> Result<Vec<sink::Output>> {
    let b = verify_object(
        &dir(c, &e.pointer).join("output.jsonl"),
        &e.manifest.output,
        c.limits.output_bytes,
    )?;
    check(b.is_empty() || b.ends_with(b"\n"), "partial output segment")?;
    b.split(|v| *v == b'\n')
        .filter(|b| !b.is_empty())
        .map(|b| {
            check(b.len() <= 2048, "output record cap")?;
            json(b)
        })
        .collect()
}
/// # Errors
/// Fully validated state is rebuilt in independent owner stores.
pub fn hydrate(c: &Config, e: &Verified, engine: &mut Engine) -> Result<()> {
    let state = engine.state.as_mut().ok_or(Error::State("native state"))?;
    entries(c, e, |record| state.store.restore_entry(record))?;
    state.closed_window_late = e.manifest.closed_window_late;
    let mut p = e.manifest.progress.clone();
    p.session = engine.progress.session;
    for cursor in p.partitions.values_mut() {
        cursor.start = cursor.next_offset_suggestion;
        cursor.seen = cursor.forwarded;
    }
    engine.progress = p;
    Ok(())
}
/// Count every retained object, temporary file and crash orphan, never delete automatically.
/// # Errors
/// Refuses unsafe paths, excessive entries/depth and byte quota overflow.
pub fn occupied(root: &Path, cap: u64) -> Result<(u64, usize)> {
    fn scan(root: &Path, cap: u64, total: &mut u64, count: &mut usize, depth: usize) -> Result<()> {
        private(root)?;
        check(depth <= 2, "root nesting cap")?;
        for item in io(fs::read_dir(root))? {
            let item = io(item)?;
            *count += 1;
            check(*count <= 8192, "root entry cap")?;
            let m = io(fs::symlink_metadata(item.path()))?;
            if m.is_dir() {
                scan(&item.path(), cap, total, count, depth + 1)?;
            } else {
                let _file = regular(&item.path(), cap)?;
                *total = total
                    .checked_add(m.len())
                    .ok_or(Error::Overflow("root bytes"))?;
                check(*total <= cap, "retained and orphan root quota")?;
            }
        }
        Ok(())
    }
    let (mut total, mut count) = (0, 0);
    scan(root, cap, &mut total, &mut count, 0)?;
    Ok((total, count))
}
/// # Errors
/// One atomic CURRENT3 selection. Any failure poisons the caller, including after rename.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::similar_names,
    reason = "One auditable atomic publication sequence"
)]
pub fn publish(
    c: &Config,
    lease: &Lease,
    parent: Option<&Pointer>,
    engine: &Engine,
    outputs: &[sink::Output],
    operation: Operation,
    hook: &mut dyn FnMut(&str) -> Result<()>,
) -> Result<Pointer> {
    lease.fence()?;
    root_identity(c)?;
    check(
        selection(&c.state.checkpoint_root)?.as_ref() == parent,
        "writer parent mismatch",
    )?;
    check(!engine.has_unconfirmed_input(), "quiescent input")?;
    let state = engine.state.as_ref().ok_or(Error::State("native state"))?;
    check(
        !state.has_pending_close()
            && state
                .store
                .first_closed(engine.progress.frontier, c.state.window_ms)?
                .is_none(),
        "quiescent closure",
    )?;
    state::checkpoint::validate_progress(&c.state, &engine.progress)?;
    let prior = parent.map(|p| verify_codec(c, p, false)).transpose()?;
    let number = parent.map_or(Ok(1), |p| {
        p.epoch.checked_add(1).ok_or(Error::Overflow("epoch"))
    })?;
    let root = &c.state.checkpoint_root;
    let (used, _) = occupied(root, c.limits.root_bytes)?;
    let generations = io(fs::read_dir(root))?.try_fold(0usize, |n, item| {
        let item = io(item)?;
        Ok::<_, Error>(n + usize::from(io(item.file_type())?.is_dir()))
    })?;
    check(
        generations < c.limits.epochs && number <= c.limits.epochs as u64,
        "retained epoch/orphan cap",
    )?;
    let mut output_bytes = Vec::new();
    let mut ids = BTreeMap::new();
    let mut history = 0u64;
    if let Some(prior) = &prior {
        for e in prior {
            history += e.manifest.output.bytes;
            for o in output(c, e)? {
                sink::unique(&mut ids, &o)?;
            }
        }
    }
    for o in outputs {
        o.validate(c)?;
        sink::unique(&mut ids, o)?;
        output_bytes.extend(encode(o)?);
        output_bytes.push(b'\n');
    }
    check(
        output_bytes.len() as u64 <= c.limits.output_bytes
            && history + output_bytes.len() as u64 <= c.limits.history_bytes,
        "output/history quota",
    )?;
    let state_size = state
        .store
        .metrics
        .windows
        .checked_mul(migration::width(c.codec))
        .ok_or(Error::Overflow("stage state bytes"))?;
    let reserve = state_size
        .checked_add(output_bytes.len() as u64)
        .and_then(|n| n.checked_add(META + 512))
        .ok_or(Error::Overflow("stage reserve"))?;
    check(
        reserve <= c.limits.stage_bytes
            && used
                .checked_add(reserve)
                .is_some_and(|n| n <= c.limits.root_bytes),
        "stage/retained quota before write",
    )?;
    hook("before_stage")?;
    let id = Uuid::new_v4();
    let stage = root.join(id.to_string());
    directory(&stage)?;
    hook("stage_created")?;
    let mut out = store::create_file(&stage.join("output.jsonl"))?;
    hook("before_output_write")?;
    let split = output_bytes.len() / 2;
    io(out.write_all(&output_bytes[..split]))?;
    hook("output_half_written")?;
    io(out.write_all(&output_bytes[split..]))?;
    hook("before_output_sync")?;
    io(out.sync_all())?;
    hook("output_synced")?;
    let mut shards = Vec::with_capacity(32);
    let mut logical = blake3::Hasher::new();
    for s in 0..32 {
        let path = stage.join(format!("shard-{s:02}.bin"));
        let mut file = store::create_file(&path)?;
        let mut hasher = blake3::Hasher::new();
        let mut count = 0;
        state.store.visit_shard(s, |e| {
            let mut b = Vec::with_capacity(80);
            migration::write(&mut b, e, c.codec, c.state.window_ms)?;
            io(file.write_all(&b))?;
            hasher.update(&b);
            logical.update(&store::encode(e));
            count += 1;
            Ok(())
        })?;
        hook("state_written")?;
        io(file.sync_all())?;
        hook("state_synced")?;
        shards.push(Shard {
            shard: s,
            owner: s % c.state.owners,
            count,
            object: Object {
                bytes: count * migration::width(c.codec),
                blake3: hasher.finalize().to_hex().to_string(),
            },
        });
    }
    let manifest = Manifest {
        version: 3,
        epoch: number,
        id,
        parent: parent.cloned(),
        fingerprint: c.fingerprint()?,
        job_id: c.state.job_id,
        writer: lease.writer,
        codec: c.codec,
        owners: c.state.owners,
        operation,
        progress: engine.progress.clone(),
        closed_window_late: state.closed_window_late,
        shards,
        state_blake3: logical.finalize().to_hex().to_string(),
        output: object(&output_bytes),
    };
    let b = encode(&manifest)?;
    check(b.len() as u64 <= META, "manifest cap")?;
    hook("before_manifest")?;
    write(&stage.join("epoch.json"), &b)?;
    hook("manifest_synced")?;
    sync_dir(&stage)?;
    sync_dir(root)?;
    hook("epoch_durable")?;
    let p = Pointer {
        version: 3,
        epoch: number,
        id,
        blake3: hash(&b),
    };
    verify_codec(c, &p, false)?;
    hook("epoch_verified")?;
    lease.fence()?;
    check(
        selection(root)?.as_ref() == parent,
        "parent changed before selector",
    )?;
    let tmp = root.join(format!("select-{}", lease.writer));
    let mut f = store::create_file(&tmp)?;
    let b = encode(&p)?;
    hook("before_selector")?;
    io(f.write_all(&b[..b.len() / 2]))?;
    hook("selector_half_written")?;
    io(f.write_all(&b[b.len() / 2..]))?;
    io(f.sync_all())?;
    hook("selector_synced")?;
    io(fs::rename(&tmp, root.join("CURRENT3")))?;
    hook("selector_renamed")?;
    sync_dir(root)?;
    hook("selector_durable")?;
    check(
        selection(root)?.as_ref() == Some(&p),
        "UNKNOWN_AFTER_COMMIT; reopen",
    )?;
    hook("before_telemetry")?;
    Ok(p)
}
