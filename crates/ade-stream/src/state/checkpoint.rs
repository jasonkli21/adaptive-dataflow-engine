//! Full local quiescent generations. Never discover a generation by directory order.
use super::{
    Config, State, check, end, shard,
    store::{self, SHARDS, encode, io, private, regular, sync_dir},
};
use crate::{Error, Progress, Result};
use ade_exec::CancellationToken;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;
const MANIFEST_CAP: u64 = 32768;
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Pointer {
    pub version: u32,
    pub generation: Uuid,
    pub manifest_blake3: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Shard {
    pub shard: usize,
    pub owner: usize,
    pub count: u64,
    pub bytes: u64,
    pub blake3: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub version: u32,
    pub codec: String,
    pub hash_version: u32,
    pub generation: Uuid,
    pub prior: Option<Pointer>,
    pub logical_fingerprint: String,
    pub job_id: Uuid,
    pub owners: usize,
    pub progress: Progress,
    pub closed_window_late: u64,
    pub pending_closures: u64,
    pub windows: u64,
    pub shards: Vec<Shard>,
}
pub struct Lock(PathBuf);
impl Lock {
    /// # Errors
    /// Refuses unsafe roots or an existing exclusive lock.
    pub fn acquire(config: &Config) -> Result<Self> {
        private(&config.checkpoint_root)?;
        let path = config.checkpoint_root.join("LOCK");
        let mut file = store::create_file(&path)?;
        io(file.write_all(Uuid::new_v4().to_string().as_bytes()))?;
        io(file.sync_all())?;
        Ok(Self(path))
    }
}
impl Drop for Lock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
fn bytes(path: &Path, cap: u64) -> Result<Vec<u8>> {
    let file = regular(path, cap)?;
    let mut encoded = Vec::new();
    io(file.take(cap + 1).read_to_end(&mut encoded))?;
    check(encoded.len() as u64 <= cap, "metadata byte cap")?;
    Ok(encoded)
}
fn json<T: serde::de::DeserializeOwned>(encoded: &[u8]) -> Result<T> {
    serde_json::from_slice(encoded).map_err(|_| Error::State("strict checkpoint JSON/version"))
}
fn digest(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|encoded| encoded.is_ascii_digit() || (b'a'..=b'f').contains(&encoded))
}
fn pointer(position: &Pointer) -> Result<()> {
    check(
        position.version == 1 && !position.generation.is_nil() && digest(&position.manifest_blake3),
        "pointer identity/version",
    )
}
fn validate_progress(config: &Config, position: &Progress) -> Result<()> {
    check(
        !position.session.is_nil()
            && position.partitions.len() == config.source.starts.len()
            && position.partitions.keys().copied().eq(config
                .source
                .starts
                .iter()
                .map(|s| s.partition))
            && position.late_records <= position.forwarded_records,
        "progress inventory/counters",
    )?;
    for cursor in position.partitions.values() {
        check(
            cursor.start >= 0
                && cursor.next_offset_suggestion >= cursor.start
                && cursor.next_offset_suggestion < i64::MAX
                && cursor.seen == cursor.forwarded,
            "confirmed offset vector",
        )?;
        if let Some(off) = cursor.forwarded {
            check(
                off >= 0
                    && off.checked_add(1) == Some(cursor.next_offset_suggestion)
                    && cursor.max_forwarded_time.is_some(),
                "offset successor",
            )?;
        } else {
            check(
                cursor.next_offset_suggestion == cursor.start
                    && cursor.max_forwarded_time.is_none(),
                "initial offset/time",
            )?;
        }
        check(
            cursor.watermark
                == cursor
                    .max_forwarded_time
                    .map(|t| {
                        t.checked_sub(config.source.allowance_ms)
                            .ok_or(Error::Overflow("restore watermark"))
                    })
                    .transpose()?,
            "watermark mismatch",
        )?;
    }
    let frontier = position
        .partitions
        .values()
        .map(|config| config.watermark)
        .collect::<Option<Vec<_>>>()
        .and_then(|v| v.into_iter().min());
    check(position.frontier == frontier, "frontier mismatch")
}
/// Validate all bytes and exclusive shard ownership before returning any state.
/// # Errors
/// Refuses missing/corrupt/foreign generations, unsafe paths and impossible progress.
pub fn load(config: &Config, token: &CancellationToken) -> Result<(Pointer, Manifest)> {
    private(&config.checkpoint_root)?;
    let position: Pointer = json(&bytes(&config.checkpoint_root.join("CURRENT"), 512)?)?;
    pointer(&position)?;
    let dir = config.checkpoint_root.join(position.generation.to_string());
    let manifest = verify_generation(config, &position, &dir, token)?;
    Ok((position, manifest))
}
fn verify_generation(
    config: &Config,
    position: &Pointer,
    dir: &Path,
    token: &CancellationToken,
) -> Result<Manifest> {
    private(dir)?;
    let encoded = bytes(&dir.join("manifest.json"), MANIFEST_CAP)?;
    check(
        blake3::hash(&encoded).to_hex().as_str() == position.manifest_blake3,
        "manifest checksum",
    )?;
    let manifest: Manifest = json(&encoded)?;
    check(
        manifest.version == 1
            && manifest.codec == "le-fixed64-blake3-v1"
            && manifest.hash_version == 1
            && manifest.generation == position.generation
            && manifest.job_id == config.job_id
            && manifest.logical_fingerprint == config.fingerprint()?
            && [1, 2, 4].contains(&manifest.owners)
            && manifest.pending_closures == 0
            && manifest.shards.len() == SHARDS,
        "checkpoint identity/codec/owner inventory",
    )?;
    if let Some(prior) = &manifest.prior {
        pointer(prior)?;
        check(
            prior.generation != manifest.generation,
            "self-linked generation",
        )?;
    }
    validate_progress(config, &manifest.progress)?;
    check(
        manifest.closed_window_late <= manifest.progress.forwarded_records,
        "closed late counter",
    )?;
    let mut windows = 0u64;
    let mut total = 0u64;
    let mut active_count = 0u64;
    for (s, meta) in manifest.shards.iter().enumerate() {
        token.check().map_err(|_| Error::Cancelled)?;
        check(
            meta.shard == s
                && meta.owner == s % manifest.owners
                && meta.bytes <= config.checkpoint_bytes
                && meta.count.checked_mul(store::RECORD) == Some(meta.bytes)
                && digest(&meta.blake3),
            "shard count/owner/bytes",
        )?;
        total = total
            .checked_add(meta.bytes)
            .ok_or(Error::Overflow("snapshot bytes"))?;
        check(total <= config.checkpoint_bytes, "snapshot byte cap")?;
        let mut file = regular(&dir.join(format!("shard-{s:02}.bin")), meta.bytes)?;
        check(io(file.metadata())?.len() == meta.bytes, "shard length")?;
        let mut hasher = blake3::Hasher::new();
        let mut count = 0;
        let mut previous = None;
        while let Some(e) = store::read(&mut file)? {
            token.check().map_err(|_| Error::Cancelled)?;
            check(
                previous.is_none_or(|old| old < e.id())
                    && shard(e.key) == s
                    && e.start.rem_euclid(config.window_ms) == 0
                    && manifest.progress.frontier.is_none_or(|frontier| {
                        end(e.start, config.window_ms).is_ok_and(|n| n > frontier)
                    }),
                "duplicate/foreign/closed window state",
            )?;
            end(e.start, config.window_ms)?;
            previous = Some(e.id());
            hasher.update(&encode(e));
            count += 1;
            active_count = active_count
                .checked_add(e.count)
                .ok_or(Error::Overflow("active counts"))?;
        }
        check(
            count == meta.count && hasher.finalize().to_hex().as_str() == meta.blake3,
            "shard checksum/count",
        )?;
        windows = windows
            .checked_add(count)
            .ok_or(Error::Overflow("windows"))?;
    }
    check(
        windows == manifest.windows && active_count <= manifest.progress.forwarded_records,
        "state/progress conservation",
    )?;
    Ok(manifest)
}
// Count retained files without retaining a directory listing. Foreign entries refuse.
fn occupied(config: &Config) -> Result<(usize, u64)> {
    let mut count = 0;
    let mut entries = 0;
    let mut total = 0u64;
    for item in io(fs::read_dir(&config.checkpoint_root))? {
        let item = io(item)?;
        entries += 1;
        check(
            entries <= config.max_generations * 2 + 4,
            "checkpoint directory entry cap",
        )?;
        let name = item.file_name();
        let name = name
            .to_str()
            .ok_or(Error::State("checkpoint path encoding"))?;
        let meta = io(fs::symlink_metadata(item.path()))?;
        check(!meta.is_symlink(), "checkpoint symlink")?;
        if meta.is_dir() {
            let id = name.strip_prefix("stage-").unwrap_or(name);
            check(Uuid::parse_str(id).is_ok(), "foreign checkpoint directory")?;
            private(&item.path())?;
            count += 1;
            let mut files = 0;
            for file in io(fs::read_dir(item.path()))? {
                let file = io(file)?;
                files += 1;
                check(files <= SHARDS + 1, "generation entry cap")?;
                let file = regular(&file.path(), config.checkpoint_bytes)?;
                total = total
                    .checked_add(io(file.metadata())?.len())
                    .ok_or(Error::Overflow("retained bytes"))?;
            }
        } else {
            check(
                name == "LOCK" || name == "CURRENT" || name.starts_with("pointer-"),
                "foreign checkpoint file",
            )?;
            let file = regular(&item.path(), 512)?;
            total = total
                .checked_add(io(file.metadata())?.len())
                .ok_or(Error::Overflow("retained bytes"))?;
        }
        check(
            total <= config.checkpoint_bytes,
            "retained checkpoint byte quota",
        )?;
    }
    Ok((count, total))
}
/// Full immutable generation publication. Hook boundaries are before/after critical
/// operations; any hook failure poisons this attempt and emits no success event.
/// # Errors
/// Refuses nonquiescent/corrupt state, quota exhaustion, cancellation or filesystem failure.
#[allow(
    clippy::too_many_lines,
    reason = "Atomic publication order is audited as one transition"
)]
pub fn publish(
    state: &mut State,
    position: &Progress,
    hook: &mut dyn FnMut(&str) -> Result<()>,
) -> Result<Pointer> {
    let config = &state.config;
    validate_progress(config, position)?;
    private(&config.checkpoint_root)?;
    if let Some(expected) = &state.generation {
        let actual: Pointer = json(&bytes(&config.checkpoint_root.join("CURRENT"), 512)?)?;
        check(&actual == expected, "current changed since restore")?;
    }
    let (count, used) = occupied(config)?;
    check(
        count < config.max_generations,
        "checkpoint generation quota; retain prior, operator GC required",
    )?;
    let reserve = state
        .store
        .metrics
        .windows
        .checked_mul(store::RECORD)
        .and_then(|n| n.checked_add(65536))
        .and_then(|n| n.checked_add(used))
        .ok_or(Error::Overflow("checkpoint quota"))?;
    check(
        reserve <= config.checkpoint_bytes,
        "total checkpoint byte quota including staging",
    )?;
    state.store.metrics.checkpoint_peak_bytes =
        state.store.metrics.checkpoint_peak_bytes.max(reserve);
    // Fixed metadata domain, charged before manifest serialization/revalidation.
    state.store.metrics.checkpoint_metadata_peak_bytes = 128 * 1024;
    let id = Uuid::new_v4();
    let staging_dir = config.checkpoint_root.join(format!("stage-{id}"));
    io(fs::create_dir(&staging_dir))?;
    io(fs::set_permissions(
        &staging_dir,
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    ))?;
    let point = |name: &str, hook: &mut dyn FnMut(&str) -> Result<()>| {
        state.store.token.check().map_err(|_| Error::Cancelled)?;
        hook(name)
    };
    point("stage_created", hook)?;
    let mut metas = Vec::with_capacity(SHARDS);
    for s in 0..SHARDS {
        point("shard_write", hook)?;
        let mut file = store::create_file(&staging_dir.join(format!("shard-{s:02}.bin")))?;
        let mut hasher = blake3::Hasher::new();
        let mut count = 0;
        state.store.visit_shard(s, |e| {
            let encoded = encode(e);
            io(file.write_all(&encoded))?;
            hasher.update(&encoded);
            count += 1;
            Ok(())
        })?;
        point("shard_checksum", hook)?;
        io(file.sync_all())?;
        point("shard_synced", hook)?;
        metas.push(Shard {
            shard: s,
            owner: s % config.owners,
            count,
            bytes: count * store::RECORD,
            blake3: hasher.finalize().to_hex().to_string(),
        });
    }
    let manifest = Manifest {
        version: 1,
        codec: "le-fixed64-blake3-v1".into(),
        hash_version: 1,
        generation: id,
        prior: state.generation.clone(),
        logical_fingerprint: config.fingerprint()?,
        job_id: config.job_id,
        owners: config.owners,
        progress: position.clone(),
        closed_window_late: state.closed_window_late,
        pending_closures: 0,
        windows: state.store.metrics.windows,
        shards: metas,
    };
    let encoded =
        serde_json::to_vec(&manifest).map_err(|_| Error::State("manifest serialization"))?;
    check(encoded.len() as u64 <= MANIFEST_CAP, "manifest cap")?;
    let mut file = store::create_file(&staging_dir.join("manifest.json"))?;
    point("manifest_write", hook)?;
    io(file.write_all(&encoded))?;
    point("manifest_checksum", hook)?;
    io(file.sync_all())?;
    point("manifest_synced", hook)?;
    let pointer = Pointer {
        version: 1,
        generation: id,
        manifest_blake3: blake3::hash(&encoded).to_hex().to_string(),
    };
    verify_generation(config, &pointer, &staging_dir, &state.store.token)?;
    point("staging_verified", hook)?;
    sync_dir(&staging_dir)?;
    point("stage_synced", hook)?;
    let published = config.checkpoint_root.join(id.to_string());
    check(!published.exists(), "generation collision")?;
    io(fs::rename(&staging_dir, &published))?;
    point("generation_renamed", hook)?;
    sync_dir(&config.checkpoint_root)?;
    point("generation_synced", hook)?;
    let encoded =
        serde_json::to_vec(&pointer).map_err(|_| Error::State("pointer serialization"))?;
    check(encoded.len() <= 512, "pointer cap")?;
    let path = config.checkpoint_root.join(format!("pointer-{id}"));
    let mut file = store::create_file(&path)?;
    point("pointer_write", hook)?;
    io(file.write_all(&encoded))?;
    io(file.sync_all())?;
    point("pointer_synced", hook)?;
    // This is the only publication selection point. Old generations remain intact.
    io(fs::rename(path, config.checkpoint_root.join("CURRENT")))?;
    point("pointer_renamed", hook)?;
    sync_dir(&config.checkpoint_root)?;
    point("pointer_durable", hook)?;
    point("cleanup", hook)?;
    Ok(pointer)
}
