//! Full logical v2 index, physically changed immutable shards, exact certificates.
#![allow(
    clippy::many_single_char_names,
    reason = "Bounded codec and canonical manifest notation"
)]
use super::{
    Adaptive, Remote,
    policy::{self, Format, Placement},
};
use crate::{
    Error, Progress, Result, Start,
    state::{
        Config, Entry, State, check,
        checkpoint::{self, Pointer},
        end, shard,
        store::{self, SHARDS, io, private, regular, sync_dir},
    },
};
use ade_exec::CancellationToken;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{Read, Write},
    os::unix::fs::PermissionsExt,
    path::Path,
    time::Instant,
};
use uuid::Uuid;
pub const META_CAP: u64 = 32768;
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metrics {
    pub canonical_scanned_bytes: u64,
    pub physical_shard_bytes: u64,
    pub physical_shard_objects: u64,
    pub retained_local_bytes: u64,
    pub elapsed_ns: u64,
    pub scan_ns: u64,
    pub object_buffer_charge_bytes: u64,
    pub metadata_charge_bytes: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reference {
    pub generation: Uuid,
    pub bytes: u64,
    pub blake3: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Shard {
    pub shard: usize,
    pub owner: usize,
    pub object_generation: Uuid,
    pub count: u64,
    pub bytes: u64,
    pub blake3: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub version: u32,
    pub codec: String,
    pub generation: Uuid,
    pub prior: Option<Reference>,
    pub depth: u64,
    pub logical_fingerprint: String,
    pub job_id: Uuid,
    pub owners: usize,
    pub origin: Vec<Start>,
    pub progress: Progress,
    pub closed_window_late: u64,
    pub pending_closures: u64,
    pub windows: u64,
    pub placement: Placement,
    pub remote: Option<Remote>,
    pub policy_blake3: String,
    pub min_copies: u64,
    pub require_remote: bool,
    pub max_loss_ppm: u64,
    pub shards: Vec<Shard>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Certificate {
    pub version: u32,
    pub manifest: Reference,
    pub logical_fingerprint: String,
    pub placement: Placement,
    pub remote: Option<Remote>,
    pub min_copies: u64,
    pub require_remote: bool,
    pub max_loss_ppm: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Selector {
    pub version: u32,
    pub certificate: Certificate,
    pub certificate_blake3: String,
}
#[must_use]
pub fn digest(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
#[must_use]
pub fn hash(b: &[u8]) -> String {
    blake3::hash(b).to_hex().to_string()
}
/// # Errors
/// Rejects duplicate keys and unsupported typed schemas.
pub fn json<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T> {
    let _: Unique = serde_json::from_slice(b).map_err(|_| Error::State("duplicate v2 JSON key"))?;
    serde_json::from_slice(b).map_err(|_| Error::State("strict v2 JSON/version"))
}
/// # Errors
/// Rejects oversized metadata and serialization failures.
pub fn encode(v: &impl Serialize) -> Result<Vec<u8>> {
    let b = serde_json::to_vec(v).map_err(|_| Error::State("v2 serialization"))?;
    check(b.len() as u64 <= META_CAP, "v2 metadata cap")?;
    Ok(b)
}
/// # Errors
/// Rejects unsafe regular files and excess bytes.
pub fn bytes(path: &Path, cap: u64) -> Result<Vec<u8>> {
    let f = regular(path, cap)?;
    let mut b = Vec::new();
    io(f.take(cap + 1).read_to_end(&mut b))?;
    check(b.len() as u64 <= cap, "v2 read cap")?;
    Ok(b)
}
fn write(path: &Path, b: &[u8]) -> Result<()> {
    let mut f = store::create_file(path)?;
    io(f.write_all(b))?;
    io(f.sync_all())
}
fn directory(p: &Path) -> Result<()> {
    io(fs::create_dir(p))?;
    io(fs::set_permissions(p, fs::Permissions::from_mode(0o700)))
}
fn object_name(s: usize) -> String {
    format!("shard-{s:02}.bin")
}
fn source_key(g: Uuid, name: &str) -> String {
    format!("{g}/{name}")
}
fn fetch(
    a: &mut Adaptive,
    c: &Config,
    g: Uuid,
    name: &str,
    n: u64,
    h: &str,
    from: Placement,
) -> Result<Vec<u8>> {
    check(
        !g.is_nil() && digest(h) && n <= a.policy.limits.object_bytes,
        "v2 exact object envelope",
    )?;
    let b = if from == Placement::Local {
        bytes(&c.checkpoint_root.join(g.to_string()).join(name), n)?
    } else {
        #[cfg(feature = "remote-checkpoint")]
        {
            a.client
                .as_mut()
                .ok_or(Error::State("explicit remote client required"))?
                .fetch(&source_key(g, name), n, h)?
        }
        #[cfg(not(feature = "remote-checkpoint"))]
        {
            return Err(Error::State("remote feature required"));
        }
    };
    check(
        b.len() as u64 == n && hash(&b) == h,
        "v2 object length/checksum",
    )?;
    Ok(b)
}
fn reference(r: &Reference) -> Result<()> {
    check(
        !r.generation.is_nil() && (1..=META_CAP).contains(&r.bytes) && digest(&r.blake3),
        "v2 manifest reference",
    )
}
fn entry(b: &[u8]) -> Result<Entry> {
    check(
        b.len() == 64 && blake3::hash(&b[..32]).as_bytes() == &b[32..],
        "v2 record checksum",
    )?;
    let e = Entry {
        start: i64::from_le_bytes(b[0..8].try_into().map_err(|_| Error::State("record"))?),
        key: u64::from_le_bytes(b[8..16].try_into().map_err(|_| Error::State("record"))?),
        sum: i64::from_le_bytes(b[16..24].try_into().map_err(|_| Error::State("record"))?),
        count: u64::from_le_bytes(b[24..32].try_into().map_err(|_| Error::State("record"))?),
    };
    check(e.count > 0, "v2 zero count")?;
    Ok(e)
}
fn same_origins(a: &[Start], b: &[Start]) -> bool {
    a.iter()
        .map(|s| (s.partition, s.offset))
        .eq(b.iter().map(|s| (s.partition, s.offset)))
}
fn validate_manifest(
    c: &Config,
    a: &Adaptive,
    cert: &Certificate,
    r: &Reference,
    m: &Manifest,
) -> Result<()> {
    check(
        m.version == 2
            && m.codec == "le-fixed64-blake3-v1"
            && m.generation == r.generation
            && m.logical_fingerprint == c.fingerprint()?
            && m.job_id == c.job_id
            && [1, 2, 4].contains(&m.owners)
            && m.shards.len() == SHARDS
            && m.pending_closures == 0
            && m.closed_window_late <= m.progress.forwarded_records
            && m.depth <= a.policy.max_depth
            && digest(&m.policy_blake3)
            && m.placement == cert.placement
            && m.remote == cert.remote
            && m.min_copies == cert.min_copies
            && m.require_remote == cert.require_remote
            && m.max_loss_ppm == cert.max_loss_ppm
            && same_origins(&m.origin, &a.policy.recovery.original_starts),
        "v2 semantic manifest identity",
    )?;
    checkpoint::validate_progress(c, &m.progress)?;
    for (p, s) in m.progress.partitions.values().zip(&m.origin) {
        check(
            p.start >= s.offset && p.forwarded.is_none_or(|o| o >= s.offset),
            "v2 progress origin",
        )?;
    }
    Ok(())
}
/// Verify the complete bounded manifest chain and every canonical shard before adoption.
/// # Errors
/// Refuses missing/corrupt ancestry, forged ownership/progress, mixed formats and cycles.
#[allow(
    clippy::too_many_lines,
    reason = "Complete ancestry and state verification before adoption"
)]
pub fn verify(
    a: &mut Adaptive,
    c: &Config,
    cert: &Certificate,
    from: Placement,
    token: &CancellationToken,
) -> Result<Manifest> {
    check(
        cert.version == 2
            && [1, 2].contains(&cert.min_copies)
            && cert.max_loss_ppm <= 1_000_000
            && cert.placement.copies() >= cert.min_copies
            && (!cert.require_remote || cert.placement.remote())
            && cert.logical_fingerprint == c.fingerprint()?
            && cert.placement.copies() >= a.policy.constraints.min_copies
            && a.policy.constraints.min_copies >= cert.min_copies
            && (!cert.require_remote || a.policy.constraints.require_remote)
            && a.policy.constraints.max_loss_ppm <= cert.max_loss_ppm
            && (!a.policy.constraints.require_remote || cert.placement.remote())
            && (from == Placement::Local && cert.placement.local()
                || from == Placement::Remote && cert.placement.remote()),
        "v2 certificate/explicit copy/durability",
    )?;
    check(
        cert.remote.is_some() == cert.placement.remote(),
        "v2 placement/remote descriptor mismatch",
    )?;
    if let Some(remote) = &cert.remote {
        remote.validate()?;
    }
    reference(&cert.manifest)?;
    let mut chain: Vec<Manifest> = Vec::with_capacity(5);
    let mut next = Some(cert.manifest.clone());
    while let Some(r) = next.take() {
        token.check().map_err(|_| Error::Cancelled)?;
        reference(&r)?;
        check(
            chain.len() <= 4 && !chain.iter().any(|m| m.generation == r.generation),
            "v2 ancestry cycle/depth",
        )?;
        let m: Manifest = json(&fetch(
            a,
            c,
            r.generation,
            "manifest.json",
            r.bytes,
            &r.blake3,
            from,
        )?)?;
        validate_manifest(c, a, cert, &r, &m)?;
        let mut possible = 0u64;
        for (cursor, origin) in m.progress.partitions.values().zip(&m.origin) {
            possible = possible
                .checked_add(
                    u64::try_from(cursor.next_offset_suggestion - origin.offset)
                        .map_err(|_| Error::State("v2 original progress"))?,
                )
                .ok_or(Error::Overflow("v2 offset envelope"))?;
        }
        check(
            m.progress.forwarded_records <= possible
                && (m.progress.forwarded_records == 0)
                    == m.progress
                        .partitions
                        .values()
                        .all(|c| c.forwarded.is_none()),
            "v2 record/offset conservation",
        )?;
        if let Some(child) = chain.last() {
            check(
                m.depth.checked_add(1) == Some(child.depth)
                    && m.progress.forwarded_records < child.progress.forwarded_records,
                "v2 ancestor depth/progress",
            )?;
        }
        check((m.depth == 0) == m.prior.is_none(), "v2 full/depth")?;
        next.clone_from(&m.prior);
        chain.push(m);
    }
    for i in (0..chain.len()).rev() {
        let m = &chain[i];
        let mut logical = 0u64;
        let mut windows = 0u64;
        let mut counts = 0u64;
        for (s, meta) in m.shards.iter().enumerate() {
            token.check().map_err(|_| Error::Cancelled)?;
            check(
                meta.shard == s
                    && meta.owner == s % m.owners
                    && meta.count.checked_mul(64) == Some(meta.bytes)
                    && meta.bytes <= a.policy.limits.object_bytes
                    && !meta.object_generation.is_nil()
                    && digest(&meta.blake3),
                "v2 shard inventory/owner/size",
            )?;
            if meta.object_generation != m.generation {
                let parent = chain
                    .get(i + 1)
                    .ok_or(Error::State("v2 full has ancestor shard"))?;
                let old = &parent.shards[s];
                check(
                    old.object_generation == meta.object_generation
                        && old.count == meta.count
                        && old.bytes == meta.bytes
                        && old.blake3 == meta.blake3,
                    "v2 foreign ancestor pin",
                )?;
            }
            logical = logical
                .checked_add(meta.bytes)
                .ok_or(Error::Overflow("v2 logical bytes"))?;
            check(logical <= c.checkpoint_bytes, "v2 logical quota")?;
            let b = fetch(
                a,
                c,
                meta.object_generation,
                &object_name(s),
                meta.bytes,
                &meta.blake3,
                from,
            )?;
            let mut previous = None;
            for raw in b.as_chunks::<64>().0 {
                let e = entry(raw)?;
                check(
                    previous.is_none_or(|p| p < e.id())
                        && shard(e.key) == s
                        && e.key < c.source.key_less_than
                        && e.start.rem_euclid(c.window_ms) == 0
                        && m.progress
                            .partitions
                            .values()
                            .filter_map(|c| c.max_forwarded_time)
                            .max()
                            .is_some_and(|time| e.start <= time)
                        && m.progress
                            .frontier
                            .is_none_or(|f| end(e.start, c.window_ms).is_ok_and(|n| n > f)),
                    "v2 duplicate/foreign/closed state",
                )?;
                end(e.start, c.window_ms)?;
                previous = Some(e.id());
                counts = counts
                    .checked_add(e.count)
                    .ok_or(Error::Overflow("v2 counts"))?;
            }
            windows = windows
                .checked_add(meta.count)
                .ok_or(Error::Overflow("v2 windows"))?;
        }
        check(
            windows == m.windows
                && counts
                    .checked_add(m.closed_window_late)
                    .is_some_and(|n| n <= m.progress.forwarded_records),
            "v2 state/progress conservation",
        )?;
    }
    chain
        .into_iter()
        .next()
        .ok_or(Error::State("v2 empty ancestry"))
}
/// # Errors
/// Explicit selected-copy load only; never falls back to another generation/copy.
pub fn selection(c: &Config) -> Result<Selector> {
    check(
        !c.checkpoint_root.join("CURRENT").exists(),
        "v1/v2 mixed root refused",
    )?;
    let s: Selector = json(&bytes(&c.checkpoint_root.join("CURRENT2"), META_CAP)?)?;
    check(
        s.version == 2
            && digest(&s.certificate_blake3)
            && hash(&encode(&s.certificate)?) == s.certificate_blake3,
        "corrupt CURRENT2 certificate",
    )?;
    Ok(s)
}
/// # Errors
/// Revalidates each immutable object while rehydrating separate logical owner stores.
pub fn hydrate(a: &mut Adaptive, state: &mut State, m: &Manifest, from: Placement) -> Result<()> {
    for meta in &m.shards {
        let b = fetch(
            a,
            &state.config,
            meta.object_generation,
            &object_name(meta.shard),
            meta.bytes,
            &meta.blake3,
            from,
        )?;
        for raw in b.as_chunks::<64>().0 {
            state.store.restore_entry(entry(raw)?)?;
        }
    }
    Ok(())
}
// Bounded retained/staging inventory, including orphan stages. No automatic deletion.
fn occupied(c: &Config) -> Result<(usize, u64)> {
    private(&c.checkpoint_root)?;
    let mut count = 0;
    let mut used = 0u64;
    let mut entries = 0;
    for item in io(fs::read_dir(&c.checkpoint_root))? {
        let item = io(item)?;
        entries += 1;
        check(entries <= c.max_generations * 2 + 8, "v2 root entry cap")?;
        let name = item
            .file_name()
            .to_str()
            .ok_or(Error::State("v2 path"))?
            .to_owned();
        if io(fs::symlink_metadata(item.path()))?.is_dir() {
            check(
                Uuid::parse_str(name.strip_prefix("stage-").unwrap_or(&name)).is_ok(),
                "v2 foreign directory",
            )?;
            private(&item.path())?;
            count += 1;
            let mut files = 0;
            for f in io(fs::read_dir(item.path()))? {
                let f = io(f)?;
                files += 1;
                check(files <= 34, "v2 generation entry cap")?;
                used = used
                    .checked_add(io(regular(&f.path(), c.checkpoint_bytes)?.metadata())?.len())
                    .ok_or(Error::Overflow("v2 occupied"))?;
            }
        } else {
            check(
                ["LOCK", "CURRENT2", "REMOTE-LEDGER"].contains(&name.as_str())
                    || name.starts_with("selector-")
                    || name == "ledger-next",
                "v2 foreign root file",
            )?;
            used = used
                .checked_add(io(regular(&item.path(), META_CAP)?.metadata())?.len())
                .ok_or(Error::Overflow("v2 occupied"))?;
        }
        check(used <= c.checkpoint_bytes, "v2 retained quota")?;
    }
    Ok((count, used))
}
fn canonical(state: &State, s: usize, cap: u64) -> Result<Vec<u8>> {
    let mut b = Vec::with_capacity(
        usize::try_from(cap).map_err(|_| Error::State("object allocation cap"))?,
    );
    state.store.visit_shard(s, |e| {
        check(
            (b.len() as u64).checked_add(64).is_some_and(|n| n <= cap),
            "v2 shard object cap",
        )?;
        b.extend_from_slice(&store::encode(e));
        Ok(())
    })?;
    Ok(b)
}
/// Quiescence is enforced by State; publication requires every required copy first.
/// # Errors
/// Any failed operation leaves old selection or a whole new selection; no false success.
#[allow(
    clippy::too_many_lines,
    reason = "Audited immutable-copy and selector order"
)]
pub fn publish(
    state: &mut State,
    a: &mut Adaptive,
    p: &Progress,
    ms: u64,
    hook: &mut dyn FnMut(&str) -> Result<()>,
) -> Result<Pointer> {
    #[cfg(feature = "remote-checkpoint")]
    if let Some(client) = &mut a.client {
        client.begin_cut();
    }
    let begin = Instant::now();
    let c = state.config.clone();
    let token = state.store.token.clone();
    checkpoint::validate_progress(&c, p)?;
    check(p.forwarded_records > 0, "v2 empty cut")?;
    if let Some(expected) = &a.current_hash {
        check(
            selection(&c)?.certificate_blake3 == *expected,
            "v2 current changed",
        )?;
    }
    let (count, used) = occupied(&c)?;
    check(count < c.max_generations, "v2 generation quota")?;
    let reserve = state
        .store
        .metrics
        .windows
        .checked_mul(64)
        .and_then(|n| n.checked_add(131_072))
        .and_then(|n| n.checked_add(used))
        .ok_or(Error::Overflow("v2 staging reservation"))?;
    check(
        reserve <= c.checkpoint_bytes,
        "v2 retained plus staging quota",
    )?;
    state.store.metrics.checkpoint_peak_bytes =
        state.store.metrics.checkpoint_peak_bytes.max(reserve);
    state.store.metrics.checkpoint_metadata_peak_bytes = 4 * 1024 * 1024;
    let scan = Instant::now();
    let mut metas = Vec::with_capacity(32);
    let mut total = 0u64;
    for s in 0..SHARDS {
        let b = canonical(state, s, a.policy.limits.object_bytes)?;
        total = total
            .checked_add(b.len() as u64)
            .ok_or(Error::Overflow("canonical"))?;
        metas.push(Shard {
            shard: s,
            owner: s % c.owners,
            object_generation: Uuid::nil(),
            count: b.len() as u64 / 64,
            bytes: b.len() as u64,
            blake3: hash(&b),
        });
    }
    let scan_ns =
        u64::try_from(scan.elapsed().as_nanos()).map_err(|_| Error::Overflow("scan time"))?;
    let mut old = None;
    if let Some(cert) = a.current.clone() {
        // Before policy can reference an ancestor verify the explicit required copy.
        let from = a.selected_copy;
        old = Some(verify(a, &c, &cert, from, &token)?);
    }
    let mut observation = a.observation.clone();
    observation.logical_bytes = total;
    observation.changed_bytes = 0;
    observation.changed_shards = 0;
    observation.depth = old.as_ref().map_or(0, |m| m.depth);
    for (s, meta) in metas.iter().enumerate() {
        if old
            .as_ref()
            .is_none_or(|m| m.shards[s].bytes != meta.bytes || m.shards[s].blake3 != meta.blake3)
        {
            observation.changed_bytes += meta.bytes;
            observation.changed_shards += 1;
        }
    }
    observation.local_available_bytes = c
        .checkpoint_bytes
        .saturating_sub(used)
        .saturating_sub(131_072);
    observation.remote_available_bytes = a.policy.limits.remote_write_bytes;
    observation.remote_available_objects = a.policy.limits.remote_objects;
    observation.remaining_requests = a.policy.limits.requests;
    observation.remaining_response_bytes = a.policy.limits.response_bytes;
    #[cfg(feature = "remote-checkpoint")]
    if let Some(remote) = &a.policy.remote {
        let ledger = super::remote::reservations(&c.checkpoint_root, remote)?;
        observation.remote_available_bytes = observation
            .remote_available_bytes
            .saturating_sub(ledger.write_bytes);
        observation.remote_available_objects = observation
            .remote_available_objects
            .saturating_sub(ledger.objects);
        observation.remaining_requests = observation
            .remaining_requests
            .saturating_sub(ledger.requests);
        observation.remaining_response_bytes = observation
            .remaining_response_bytes
            .saturating_sub(ledger.reserved_response_bytes);
    }
    // Incremental is executable only when this exact placement can reuse the
    // verified previous closure with an unchanged durability floor. Filter
    // infeasible incremental variants before their costs are ranked; the
    // eventual physical writer must never transform a scored incremental
    // action into a full one after admission.
    let incremental_placements = [Placement::Local, Placement::Remote, Placement::Both]
        .into_iter()
        .filter(|placement| {
            !a.force_full_next
                && old.as_ref().is_some_and(|m| {
                    m.placement == *placement
                        && m.remote
                            == if placement.remote() {
                                a.policy.remote.clone()
                            } else {
                                None
                            }
                        && m.depth < a.policy.max_depth
                        && m.min_copies == a.policy.constraints.min_copies
                        && m.require_remote == a.policy.constraints.require_remote
                        && m.max_loss_ppm == a.policy.constraints.max_loss_ppm
                        && m.progress.forwarded_records < p.forwarded_records
                })
        })
        .collect::<Vec<_>>();
    let decision = policy::decide_with_incremental_placements(
        &a.policy,
        &observation,
        a.last_action.as_ref().map(|(x, t)| (x, *t)),
        ms,
        &incremental_placements,
    )?;
    if a.disk_loss_restore && decision.chosen.placement.remote() {
        check(
            a.current.as_ref().and_then(|c| c.remote.as_ref()) != a.policy.remote.as_ref(),
            "disk-loss writer requires fresh remote namespace/budget",
        )?;
    }
    #[cfg(feature = "remote-checkpoint")]
    if decision.chosen.placement.remote()
        && (!a
            .client
            .as_ref()
            .is_some_and(super::remote::Client::is_writer)
            || a.disk_loss_restore
            || a.client.as_ref().map(super::remote::Client::config) != a.policy.remote.as_ref())
    {
        let mut client = super::remote::Client::new(
            a.policy
                .remote
                .as_ref()
                .ok_or(Error::State("write remote"))?,
            &a.policy.limits,
            Some(&c.checkpoint_root),
            token.clone(),
        )?;
        client.probe()?;
        a.client = Some(client);
    }
    a.decision = Some(decision.clone());
    hook("policy_decided")?;
    let id = Uuid::new_v4();
    let dir = c.checkpoint_root.join(format!("stage-{id}"));
    directory(&dir)?;
    hook("stage_created")?;
    let mut metrics = Metrics {
        canonical_scanned_bytes: total,
        object_buffer_charge_bytes: a.policy.limits.object_bytes,
        metadata_charge_bytes: 4 * 1024 * 1024,
        scan_ns,
        retained_local_bytes: used,
        ..Metrics::default()
    };
    for (s, meta) in metas.iter_mut().enumerate() {
        token.check().map_err(|_| Error::Cancelled)?;
        if decision.chosen.format == Format::Incremental
            && old.as_ref().is_some_and(|m| {
                m.shards[s].bytes == meta.bytes && m.shards[s].blake3 == meta.blake3
            })
        {
            meta.object_generation =
                old.as_ref().ok_or(Error::State("ancestor"))?.shards[s].object_generation;
            continue;
        }
        meta.object_generation = id;
        let b = canonical(state, s, a.policy.limits.object_bytes)?;
        check(hash(&b) == meta.blake3, "canonical changed during cut")?;
        if decision.chosen.placement.local() {
            hook("shard_write")?;
            write(&dir.join(object_name(s)), &b)?;
            hook("shard_synced")?;
        }
        if decision.chosen.placement.remote() {
            hook("remote_shard_put")?;
            put(a, &source_key(id, &object_name(s)), b)?;
            hook("remote_shard_verified")?;
        }
        metrics.physical_shard_bytes += meta.bytes;
        metrics.physical_shard_objects += 1;
    }
    let prior = if decision.chosen.format == Format::Incremental {
        a.current.as_ref().map(|c| c.manifest.clone())
    } else {
        None
    };
    let depth = if prior.is_some() {
        old.as_ref().ok_or(Error::State("ancestor"))?.depth + 1
    } else {
        0
    };
    let manifest = Manifest {
        version: 2,
        codec: "le-fixed64-blake3-v1".into(),
        generation: id,
        prior,
        depth,
        logical_fingerprint: c.fingerprint()?,
        job_id: c.job_id,
        owners: c.owners,
        origin: a.policy.recovery.original_starts.clone(),
        progress: p.clone(),
        closed_window_late: state.closed_window_late,
        pending_closures: 0,
        windows: state.store.metrics.windows,
        placement: decision.chosen.placement,
        remote: if decision.chosen.placement.remote() {
            a.policy.remote.clone()
        } else {
            None
        },
        policy_blake3: hash(&encode(&a.policy)?),
        min_copies: a.policy.constraints.min_copies,
        require_remote: a.policy.constraints.require_remote,
        max_loss_ppm: a.policy.constraints.max_loss_ppm,
        shards: metas,
    };
    let b = encode(&manifest)?;
    let r = Reference {
        generation: id,
        bytes: b.len() as u64,
        blake3: hash(&b),
    };
    hook("manifest_write")?;
    write(&dir.join("manifest.json"), &b)?;
    hook("manifest_synced")?;
    if manifest.placement.remote() {
        hook("remote_manifest_put")?;
        put(a, &source_key(id, "manifest.json"), b)?;
        hook("remote_manifest_verified")?;
    }
    sync_dir(&dir)?;
    hook("stage_synced")?;
    io(fs::rename(&dir, c.checkpoint_root.join(id.to_string())))?;
    hook("generation_renamed")?;
    sync_dir(&c.checkpoint_root)?;
    hook("generation_synced")?;
    let cert = Certificate {
        version: 2,
        manifest: r,
        logical_fingerprint: c.fingerprint()?,
        placement: manifest.placement,
        remote: manifest.remote.clone(),
        min_copies: a.policy.constraints.min_copies,
        require_remote: a.policy.constraints.require_remote,
        max_loss_ppm: a.policy.constraints.max_loss_ppm,
    };
    if cert.placement.local() {
        verify(a, &c, &cert, Placement::Local, &token)?;
    }
    if cert.placement.remote() {
        verify(a, &c, &cert, Placement::Remote, &token)?;
    }
    hook("copies_verified")?;
    let b = encode(&cert)?;
    let h = hash(&b);
    if cert.placement.remote() {
        hook("remote_certificate_put")?;
        put(a, &source_key(id, "certificate.json"), b.clone())?;
        hook("remote_certificate_verified")?;
    }
    write(
        &c.checkpoint_root
            .join(id.to_string())
            .join("certificate.json"),
        &b,
    )?;
    sync_dir(&c.checkpoint_root.join(id.to_string()))?;
    hook("certificate_synced")?;
    // Source and diagnostic failure at this hook prevents selector publication.
    hook("before_selector")?;
    let selector = Selector {
        version: 2,
        certificate: cert.clone(),
        certificate_blake3: h.clone(),
    };
    let path = c.checkpoint_root.join(format!("selector-{id}"));
    write(&path, &encode(&selector)?)?;
    hook("selector_synced")?;
    io(fs::rename(path, c.checkpoint_root.join("CURRENT2")))?;
    hook("selector_renamed")?;
    sync_dir(&c.checkpoint_root)?;
    hook("selector_durable")?;
    metrics.elapsed_ns = u64::try_from(begin.elapsed().as_nanos())
        .map_err(|_| Error::Overflow("checkpoint time"))?;
    a.observation.samples = a
        .observation
        .samples
        .checked_add(1)
        .ok_or(Error::Overflow("samples"))?;
    a.observation.scan_bytes_per_second = total
        .checked_mul(1_000_000_000)
        .ok_or(Error::Overflow("measured rate"))?
        .checked_div(scan_ns.max(1))
        .unwrap_or(0);
    // Conservative whole-cut throughput includes verification and metadata overhead.
    let rate = metrics
        .physical_shard_bytes
        .checked_mul(1_000_000_000)
        .ok_or(Error::Overflow("measured rate"))?
        .checked_div(metrics.elapsed_ns.max(1))
        .unwrap_or(0);
    if cert.placement.local() {
        a.observation.local_bytes_per_second = rate;
    }
    if cert.placement.remote() {
        a.observation.remote_bytes_per_second = rate;
    }
    a.last_metrics = metrics;
    a.selected_copy = if cert.placement.local() {
        Placement::Local
    } else {
        Placement::Remote
    };
    a.current = Some(cert);
    a.current_hash = Some(h);
    a.last_action = Some((decision.chosen, ms));
    a.disk_loss_restore = false;
    a.force_full_next = false;
    Ok(Pointer {
        version: 2,
        generation: id,
        manifest_blake3: selector.certificate.manifest.blake3,
    })
}
fn put(a: &mut Adaptive, key: &str, b: Vec<u8>) -> Result<()> {
    #[cfg(feature = "remote-checkpoint")]
    {
        a.client
            .as_mut()
            .ok_or(Error::State("remote client required"))?
            .put(key, b)
    }
    #[cfg(not(feature = "remote-checkpoint"))]
    {
        let _ = (a, key, b);
        Err(Error::State("remote feature required"))
    }
}

/// Install an operator-retained certificate into an empty scratch root.
/// # Errors
/// Refuses collisions or unsafe files; no state objects are copied.
pub fn install_certificate(c: &Config, s: &Selector) -> Result<()> {
    let path = c
        .checkpoint_root
        .join(format!("selector-{}", s.certificate.manifest.generation));
    write(&path, &encode(s)?)?;
    io(fs::rename(path, c.checkpoint_root.join("CURRENT2")))?;
    sync_dir(&c.checkpoint_root)
}

// Progress contains a BTreeMap: serde's default map visitor would overwrite a
// repeated partition. Validate recursively before typed decoding, under META_CAP.
struct Unique;
impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Unique;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("unique JSON fields")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Unique, A::Error> {
                let mut keys = std::collections::BTreeSet::new();
                while let Some(key) = map.next_key::<String>()? {
                    if !keys.insert(key) {
                        return Err(serde::de::Error::custom("duplicate key"));
                    }
                    map.next_value::<Unique>()?;
                }
                Ok(Unique)
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Unique, A::Error> {
                while seq.next_element::<Unique>()?.is_some() {}
                Ok(Unique)
            }
            fn visit_bool<E: serde::de::Error>(self, _: bool) -> std::result::Result<Unique, E> {
                Ok(Unique)
            }
            fn visit_i64<E: serde::de::Error>(self, _: i64) -> std::result::Result<Unique, E> {
                Ok(Unique)
            }
            fn visit_u64<E: serde::de::Error>(self, _: u64) -> std::result::Result<Unique, E> {
                Ok(Unique)
            }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> std::result::Result<Unique, E> {
                Ok(Unique)
            }
            fn visit_str<E: serde::de::Error>(self, _: &str) -> std::result::Result<Unique, E> {
                Ok(Unique)
            }
            fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Unique, E> {
                Ok(Unique)
            }
        }
        d.deserialize_any(Visitor)
    }
}
