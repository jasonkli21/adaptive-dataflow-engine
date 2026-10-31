//! Named self-contained exports. The exact top digest is an operator capability.
use super::{
    Config,
    epoch::{self, Lease, Object, Pointer, bytes, encode, hash, json, occupied},
};
use crate::{
    Error, Result,
    state::{
        check,
        store::{io, private, sync_dir},
    },
};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::{
    fs,
    path::{Path, PathBuf},
};
use uuid::Uuid;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileRef {
    pub path: String,
    pub object: Object,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Export {
    pub version: u32,
    pub fingerprint: String,
    pub selected: Pointer,
    pub files: Vec<FileRef>,
}
fn name(s: &str) -> Result<()> {
    check(
        !s.is_empty()
            && s.len() <= 64
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "savepoint name",
    )
}
fn separate(c: &Config, p: &Path) -> Result<()> {
    private(p)?;
    check(
        !p.starts_with(&c.state.checkpoint_root)
            && !c.state.checkpoint_root.starts_with(p)
            && !p.starts_with(&c.state.working_root)
            && !c.state.working_root.starts_with(p),
        "separate savepoint root",
    )
}
fn paths(p: &Pointer) -> Vec<String> {
    let mut result = vec![
        format!("{}/epoch.json", p.id),
        format!("{}/output.jsonl", p.id),
    ];
    result.extend((0..32).map(|s| format!("{}/shard-{s:02}.bin", p.id)));
    result
}
/// # Errors
/// Atomic no-clobber named export; failures leave only charged, invalid stage orphans.
pub fn export(
    c: &Config,
    parent: &Path,
    label: &str,
    hook: &mut dyn FnMut(&str) -> Result<()>,
) -> Result<(PathBuf, String)> {
    name(label)?;
    separate(c, parent)?;
    let _lease = Lease::acquire(c)?;
    let pointer = epoch::selection(&c.state.checkpoint_root)?
        .ok_or(Error::State("savepoint needs committed epoch"))?;
    let epochs = epoch::verify(c, &pointer)?;
    let (used, _) = occupied(parent, c.limits.savepoint_bytes)?;
    let names = io(fs::read_dir(parent))?.count();
    check(
        names < c.limits.savepoints,
        "savepoint count including orphans",
    )?;
    check(
        fs::symlink_metadata(parent.join(label)).is_err(),
        "savepoint collision",
    )?;
    let mut files = Vec::new();
    let mut needed = 0u64;
    for e in &epochs {
        for path in paths(&e.pointer) {
            let b = bytes(&c.state.checkpoint_root.join(&path), c.limits.stage_bytes)?;
            needed = needed
                .checked_add(b.len() as u64)
                .ok_or(Error::Overflow("savepoint size"))?;
            files.push(FileRef {
                path,
                object: Object {
                    bytes: b.len() as u64,
                    blake3: hash(&b),
                },
            });
        }
    }
    let manifest = Export {
        version: 3,
        fingerprint: c.fingerprint()?,
        selected: pointer,
        files,
    };
    let top = encode(&manifest)?;
    check(
        top.len() <= 1_048_576
            && used
                .checked_add(needed)
                .and_then(|n| n.checked_add(top.len() as u64))
                .is_some_and(|n| n <= c.limits.savepoint_bytes),
        "savepoint byte quota",
    )?;
    let stage = parent.join(format!("export-{}", Uuid::new_v4()));
    epoch::directory(&stage)?;
    hook("export_created")?;
    for e in &epochs {
        epoch::directory(&stage.join(e.pointer.id.to_string()))?;
    }
    for f in &manifest.files {
        let b = bytes(&c.state.checkpoint_root.join(&f.path), c.limits.stage_bytes)?;
        check(hash(&b) == f.object.blake3, "savepoint source changed")?;
        epoch::write(&stage.join(&f.path), &b)?;
        hook("export_object")?;
    }
    for e in &epochs {
        sync_dir(&stage.join(e.pointer.id.to_string()))?;
    }
    epoch::write(&stage.join("SAVEPOINT3"), &top)?;
    sync_dir(&stage)?;
    hook("export_synced")?;
    // Parent is private, operations serialized by the lineage lease; no replacing names.
    check(
        fs::symlink_metadata(parent.join(label)).is_err(),
        "savepoint collision",
    )?;
    hook("export_before_publish")?;
    let parent_fd = io(File::open(parent))?;
    let stage_name = stage
        .file_name()
        .ok_or(Error::State("savepoint stage name"))?;
    // renameatx_np(RENAME_EXCL) on macOS and renameat2(RENAME_NOREPLACE)
    // on Linux make admission atomic even if another actor creates an empty
    // destination after our friendly preflight check.
    rustix::fs::renameat_with(
        &parent_fd,
        stage_name,
        &parent_fd,
        label,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(|_| Error::State("savepoint atomic no-replace publish"))?;
    sync_dir(parent)?;
    hook("export_durable")?;
    Ok((parent.join(label), hash(&top)))
}
/// # Errors
/// Verifies all paths/objects/semantic closure before any adoption or Kafka assignment.
pub fn verify(c: &Config, path: &Path, digest: &str) -> Result<Export> {
    private(path)?;
    let (used, _) = occupied(path, c.limits.savepoint_bytes)?;
    let top = bytes(&path.join("SAVEPOINT3"), 1_048_576)?;
    check(hash(&top) == digest, "operator savepoint digest")?;
    let m: Export = json(&top)?;
    check(
        m.version == 3
            && m.fingerprint == c.fingerprint()?
            && m.files.len() <= c.limits.epochs * 34,
        "savepoint identity/cap",
    )?;
    let mut verify_config = c.clone();
    verify_config.state.checkpoint_root = path.to_path_buf();
    let epochs = epoch::verify(&verify_config, &m.selected)?;
    let expected: Vec<String> = epochs.iter().flat_map(|e| paths(&e.pointer)).collect();
    check(
        m.files.iter().map(|f| &f.path).eq(expected.iter()),
        "savepoint exact path inventory",
    )?;
    let mut total = top.len() as u64;
    for f in &m.files {
        let b = bytes(&path.join(&f.path), c.limits.stage_bytes)?;
        check(
            b.len() as u64 == f.object.bytes && hash(&b) == f.object.blake3,
            "savepoint object digest",
        )?;
        total = total
            .checked_add(f.object.bytes)
            .ok_or(Error::Overflow("savepoint bytes"))?;
    }
    check(total == used, "unexpected savepoint objects")?;
    Ok(m)
}
/// # Errors
/// Fresh-root import preserves the complete output lineage. No rollback/adoption into an old root.
pub fn import(
    c: &Config,
    path: &Path,
    digest: &str,
    retired_head_digest: &str,
    hook: &mut dyn FnMut(&str) -> Result<()>,
) -> Result<Pointer> {
    c.validate()?;
    separate(c, path)?;
    let manifest = verify(c, path, digest)?;
    check(
        retired_head_digest == manifest.selected.blake3,
        "retired predecessor authoritative head attestation",
    )?;
    let mut source_config = c.clone();
    source_config.state.checkpoint_root = path.to_path_buf();
    let closure = epoch::verify(&source_config, &manifest.selected)?;
    let current = &closure
        .last()
        .ok_or(Error::State("savepoint empty lineage"))?
        .manifest;
    check(
        current.codec == c.codec && current.owners == c.state.owners,
        "import requires exact codec/owners; use explicit transition",
    )?;
    check(
        io(fs::read_dir(&c.state.checkpoint_root))?.next().is_none(),
        "savepoint import requires fresh empty root",
    )?;
    let lease = Lease::acquire(c)?;
    check(
        occupied(&c.state.checkpoint_root, c.limits.root_bytes)?.0
            + manifest.files.iter().map(|f| f.object.bytes).sum::<u64>()
            + 512
            <= c.limits.root_bytes,
        "import root quota",
    )?;
    // Any interrupted import is no longer admissible as a fresh empty lineage.
    // This witness never selects data; CURRENT3 remains the sole commit point.
    epoch::mark_activated(c)?;
    for f in &manifest.files {
        let dir = c.state.checkpoint_root.join(
            f.path
                .split('/')
                .next()
                .ok_or(Error::State("export path"))?,
        );
        if !dir.exists() {
            epoch::directory(&dir)?;
        }
        let b = bytes(&path.join(&f.path), c.limits.stage_bytes)?;
        check(hash(&b) == f.object.blake3, "import source mutation")?;
        epoch::write(&c.state.checkpoint_root.join(&f.path), &b)?;
        hook("import_object")?;
    }
    let chain = epoch::verify(c, &manifest.selected)?;
    for e in chain {
        sync_dir(&c.state.checkpoint_root.join(e.pointer.id.to_string()))?;
    }
    sync_dir(&c.state.checkpoint_root)?;
    lease.fence()?;
    let temp = c.state.checkpoint_root.join("IMPORT3");
    epoch::write(&temp, &encode(&manifest.selected)?)?;
    hook("import_before_selector")?;
    io(fs::rename(temp, c.state.checkpoint_root.join("CURRENT3")))?;
    sync_dir(&c.state.checkpoint_root)?;
    hook("import_durable")?;
    Ok(manifest.selected)
}
