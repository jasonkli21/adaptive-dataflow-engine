use crate::{Error, Guard, Request, Result, fault, require};
use ade_exec::iterative::{Graph, managed_bytes, scan_graph_file};
use ade_logical::iterative::ParquetFileV1;
use ade_physical::cycle::{BoundCycle, Role};
use rustix::fs::{Mode, OFlags, open};
use std::{
    collections::BTreeSet,
    fs::{self, File, Metadata},
    io::{self, Read, Seek, SeekFrom},
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
};

pub(crate) fn no_symlinks(path: &Path) -> Result<()> {
    require(
        path.is_absolute() && path.as_os_str().len() <= 4096,
        "absolute bounded path required",
    )?;
    require(
        !path.components().any(|part| part == Component::ParentDir),
        "parent path component",
    )?;
    let mut prefix = PathBuf::new();
    for part in path.components() {
        prefix.push(part);
        match fs::symlink_metadata(&prefix) {
            Ok(meta) => require(!meta.is_symlink(), "symlink path")?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => break,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

pub(crate) fn regular(path: &Path) -> Result<File> {
    let fd = open(
        path,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(io::Error::from)?;
    let file = File::from(fd);
    require(file.metadata()?.is_file(), "regular graph file required")?;
    Ok(file)
}

#[derive(PartialEq, Eq)]
struct Stamp {
    device: u64,
    inode: u64,
    bytes: u64,
    changed: (i64, i64),
    modified: (i64, i64),
}
impl From<Metadata> for Stamp {
    fn from(m: Metadata) -> Self {
        Self {
            device: m.dev(),
            inode: m.ino(),
            bytes: m.len(),
            changed: (m.ctime(), m.ctime_nsec()),
            modified: (m.mtime(), m.mtime_nsec()),
        }
    }
}

struct BoundFile<'a> {
    path: PathBuf,
    file: File,
    stamp: Stamp,
    expected: &'a ParquetFileV1,
}

impl BoundFile<'_> {
    fn attest(&self, guard: &Guard) -> Result<()> {
        guard.check()?;
        no_symlinks(&self.path)?;
        require(
            self.stamp == Stamp::from(regular(&self.path)?.metadata()?)
                && self.stamp == Stamp::from(self.file.metadata()?),
            "graph file identity changed",
        )?;
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(0))?;
        let mut hash = blake3::Hasher::new();
        let mut bytes = 0u64;
        let mut buf = [0; 16 * 1024];
        loop {
            guard.check()?;
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            bytes += n as u64;
            require(bytes <= self.expected.bytes, "graph file grew")?;
            hash.update(&buf[..n]);
        }
        require(
            bytes == self.expected.bytes
                && hash.finalize().to_hex().as_str() == self.expected.blake3,
            "graph file bytes or BLAKE3 changed",
        )?;
        require(
            self.stamp == Stamp::from(self.file.metadata()?)
                && self.stamp == Stamp::from(regular(&self.path)?.metadata()?),
            "graph file identity changed",
        )
    }
}

pub(crate) struct Source<'a> {
    files: Vec<BoundFile<'a>>,
    pub graph: Graph,
    pub bytes: u64,
    pub batch_peak: usize,
}

impl<'a> Source<'a> {
    pub fn bind(request: &'a Request, cycle: BoundCycle, guard: &Guard) -> Result<Self> {
        let source_bytes = request.inventory.admit()?;
        if source_bytes > request.limits.source_bytes {
            return Err(Error::Limit("source bytes"));
        }
        let vertex_rows = request.inventory.vertices.iter().map(|f| f.rows).sum();
        let edge_rows = request.inventory.edges.iter().map(|f| f.rows).sum();
        if managed_bytes(vertex_rows, edge_rows, request.limits.credits)
            .is_none_or(|bytes| bytes > request.limits.state_bytes)
        {
            return Err(Error::Limit("managed state bytes"));
        }
        no_symlinks(&request.source_dir)?;
        let mut vertices = Vec::with_capacity(vertex_rows);
        let mut edges = Vec::with_capacity(edge_rows);
        let mut files = Vec::new();
        let mut inodes = BTreeSet::new();
        let mut batch_peak = 0;
        for (role, entries) in [
            (Role::Vertices, &request.inventory.vertices),
            (Role::Edges, &request.inventory.edges),
        ] {
            for expected in entries {
                let path = request.source_dir.join(&expected.name);
                no_symlinks(&path)?;
                let file = regular(&path)?;
                let stamp = Stamp::from(file.metadata()?);
                require(
                    stamp.bytes == expected.bytes && inodes.insert((stamp.device, stamp.inode)),
                    "graph file length or duplicate inode",
                )?;
                let binding = BoundFile {
                    path,
                    file,
                    stamp,
                    expected,
                };
                binding.attest(guard)?;
                batch_peak = batch_peak.max(scan_graph_file(
                    binding.file.try_clone()?,
                    role,
                    expected.rows,
                    request.limits.batch_rows,
                    &guard.cancel,
                    guard.deadline,
                    |a, b| {
                        if let Some(b) = b {
                            edges.push((a, b));
                        } else {
                            vertices.push(a);
                        }
                    },
                )?);
                files.push(binding);
            }
        }
        fault("source_decoded", 0, guard)?;
        let graph = Graph::new(
            vertices,
            edges,
            cycle,
            request.limits.credits,
            request.limits.state_bytes,
        )?;
        let source = Self {
            files,
            graph,
            bytes: source_bytes,
            batch_peak,
        };
        source.attest(guard)?;
        Ok(source)
    }

    pub fn attest(&self, guard: &Guard) -> Result<()> {
        for file in &self.files {
            file.attest(guard)?;
        }
        Ok(())
    }
}
