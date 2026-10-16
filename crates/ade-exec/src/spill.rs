//! Lossy execution-scoped local storage; `TempDir` also removes partial files.
use crate::{OperatorId, Result};
use std::{
    collections::BTreeMap,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
#[derive(Debug, Clone, Default)]
pub struct SpillMetrics {
    pub files: u64,
    pub events: u64,
    pub sort_runs: u64,
    pub sort_merges: u64,
    pub write_bytes: u64,
    pub read_bytes: u64,
    pub operator_events: BTreeMap<OperatorId, u64>,
}
#[derive(Debug)]
pub struct SpillManager {
    root: tempfile::TempDir,
    pub metrics: SpillMetrics,
    read_bytes: Arc<AtomicU64>,
}
impl SpillManager {
    pub fn new(parent: Option<&Path>) -> Result<Self> {
        let root = if let Some(parent) = parent {
            tempfile::tempdir_in(parent)?
        } else {
            tempfile::tempdir()?
        };
        Ok(Self {
            root,
            metrics: SpillMetrics::default(),
            read_bytes: Arc::new(AtomicU64::new(0)),
        })
    }
    pub fn create(&mut self, id: OperatorId) -> Result<(PathBuf, File)> {
        let path = self
            .root
            .path()
            .join(format!("op-{}-{}.spill", id.0, self.metrics.files));
        let file = File::options().write(true).create_new(true).open(&path)?;
        self.metrics.files += 1;
        Ok((path, file))
    }
    pub fn event(&mut self, id: OperatorId) {
        self.metrics.events += 1;
        *self.metrics.operator_events.entry(id).or_default() += 1;
    }
    pub fn wrote(&mut self, path: &Path) -> Result<()> {
        self.metrics.write_bytes += std::fs::metadata(path)?.len();
        Ok(())
    }
    pub fn reading(&self, path: &Path) -> Result<SpillReader> {
        Ok(SpillReader {
            file: File::open(path)?,
            bytes: self.read_bytes.clone(),
        })
    }
    pub fn snapshot(&self) -> SpillMetrics {
        let mut metrics = self.metrics.clone();
        metrics.read_bytes = self.read_bytes.load(Ordering::Relaxed);
        metrics
    }
    pub fn close(self) -> Result<()> {
        self.root.close()?;
        Ok(())
    }
}

/// Counts bytes actually returned by read, including codec read-ahead.
pub(crate) struct SpillReader {
    file: File,
    bytes: Arc<AtomicU64>,
}
impl SpillReader {
    pub fn metadata(&self) -> std::io::Result<std::fs::Metadata> {
        self.file.metadata()
    }
}
impl Read for SpillReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let size = self.file.read(buffer)?;
        self.bytes
            .fetch_add(u64::try_from(size).expect("read size"), Ordering::Relaxed);
        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    #[test]
    fn read_metrics_count_consumed_bytes_and_root_is_removed() {
        let parent = tempfile::tempdir().unwrap();
        let mut spill = SpillManager::new(Some(parent.path())).unwrap();
        let (path, mut file) = spill.create(OperatorId(0)).unwrap();
        file.write_all(&[1; 100]).unwrap();
        drop(file);
        let mut reader = spill.reading(&path).unwrap();
        reader.read_exact(&mut [0; 8]).unwrap();
        assert_eq!(spill.snapshot().read_bytes, 8);
        drop(reader);
        spill.close().unwrap();
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }
}
