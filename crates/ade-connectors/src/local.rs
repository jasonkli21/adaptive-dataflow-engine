//! Pinned local inventory with descriptor-relative, symlink-free reads.
use crate::{
    CatalogEntryV1, ConnectorSpecV1, Context, Error, PinnedSplit, ResolvedSnapshotV1, Result,
    Source, contract::relative_key,
};
use rustix::fs::{Mode, OFlags, open, openat};
use std::{fs::File, io::Read, path::Path};

pub struct Local {
    root: File,
    snapshot: ResolvedSnapshotV1,
}

pub(crate) fn root(path: &Path) -> Result<File> {
    Ok(open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?
    .into())
}

pub(crate) fn regular_at(root: &File, key: &str) -> Result<File> {
    if !relative_key(key) {
        return Err(Error::Admission("unsafe local key"));
    }
    let mut parent = root.try_clone()?;
    let mut parts = key.split('/').peekable();
    while let Some(part) = parts.next() {
        let mut flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
        if parts.peek().is_some() {
            flags |= OFlags::DIRECTORY;
        }
        parent = openat(&parent, part, flags, Mode::empty())?.into();
    }
    if !parent.metadata()?.is_file() {
        return Err(Error::Admission("regular local file required"));
    }
    Ok(parent)
}

impl Local {
    /// # Errors
    /// Refuses a bad snapshot or inaccessible source root.
    pub fn new(path: &Path, snapshot: ResolvedSnapshotV1) -> Result<Self> {
        snapshot.digest()?;
        if snapshot.entry.connector.kind != "local_parquet" {
            return Err(Error::Admission("local Parquet connector kind"));
        }
        Ok(Self {
            root: root(path)?,
            snapshot,
        })
    }
}

impl Source for Local {
    fn validate(&self, spec: &ConnectorSpecV1) -> Result<()> {
        if spec != &self.snapshot.entry.connector {
            return Err(Error::Identity("local connector spec"));
        }
        Ok(())
    }

    fn resolve(&self, entry: &CatalogEntryV1, context: &Context) -> Result<ResolvedSnapshotV1> {
        context.check()?;
        if entry != &self.snapshot.entry {
            return Err(Error::Identity("local catalog entry"));
        }
        Ok(self.snapshot.clone())
    }

    fn open(&self, split: &PinnedSplit, context: &Context) -> Result<Box<dyn Read + '_>> {
        context.check()?;
        let file = regular_at(&self.root, &split.key)?;
        if file.metadata()?.len() != split.bytes {
            return Err(Error::Identity("local split length"));
        }
        Ok(Box::new(file))
    }
}
