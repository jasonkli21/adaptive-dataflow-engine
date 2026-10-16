//! Versioned, file-granular lineage for the fixed aggregate graph.
use crate::{
    Result,
    ids::{AttemptId, LogicalTransferId, MorselId, TaskId},
    require,
};
use serde::{Deserialize, Serialize};
use std::{
    io::Read,
    path::{Path, PathBuf},
};

/// Streaming file hashing does not retain source contents.
/// # Errors
/// Returns file I/O errors.
pub fn fingerprint(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut hash = blake3::Hasher::new();
    let mut bytes = [0; 8192];
    loop {
        let n = file.read(&mut bytes)?;
        if n == 0 {
            break;
        }
        hash.update(&bytes[..n]);
    }
    Ok(hash.finalize().to_hex().to_string())
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceMorsel {
    pub id: MorselId,
    pub path: PathBuf,
    pub fingerprint: String,
    pub bytes: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSnapshot {
    pub version: u32,
    pub manifest_fingerprint: String,
    pub schema_fingerprint: String,
    pub logical_fingerprint: String,
    pub replayable: bool,
    pub morsels: Vec<SourceMorsel>,
}
impl SourceSnapshot {
    /// Validate descriptor structure and exact ordered job inputs, without I/O.
    /// # Errors
    /// Rejects missing identity, duplicate inputs and unsupported lineage versions.
    pub fn validate(&self, files: &[PathBuf]) -> Result<()> {
        require(
            self.version == 1
                && !self.manifest_fingerprint.is_empty()
                && !self.schema_fingerprint.is_empty()
                && !self.logical_fingerprint.is_empty(),
            "invalid source snapshot identity/version",
        )?;
        require(
            !files.is_empty() && files.len() == self.morsels.len(),
            "snapshot input coverage mismatch",
        )?;
        let mut unique = std::collections::BTreeSet::new();
        for (i, (m, file)) in self.morsels.iter().zip(files).enumerate() {
            require(
                m.id == MorselId(i as u64)
                    && &m.path == file
                    && m.fingerprint.len() == 64
                    && unique.insert(file),
                "snapshot morsel identity mismatch",
            )?;
        }
        Ok(())
    }
    #[must_use]
    pub fn producer_morsels(&self, producer: TaskId, workers: usize) -> Vec<SourceMorsel> {
        self.morsels
            .iter()
            .skip(usize::try_from(producer.0).unwrap_or(usize::MAX))
            .step_by(workers)
            .cloned()
            .collect()
    }
    /// Check physical input bytes immediately before executing a closure.
    /// # Errors
    /// Rejects source drift or unreadable inputs.
    pub fn verify(morsels: &[SourceMorsel]) -> Result<()> {
        for m in morsels {
            require(
                std::fs::metadata(&m.path)?.len() == m.bytes
                    && fingerprint(&m.path)? == m.fingerprint,
                "source snapshot fingerprint mismatch",
            )?;
        }
        Ok(())
    }
}
/// A terminal manifest exists only after the destination acknowledges completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferManifest {
    pub logical: LogicalTransferId,
    pub attempt: AttemptId,
    pub segments: u64,
    pub rows: u64,
    pub payload_bytes: u64,
    pub fingerprint: String,
}
impl TransferManifest {
    /// # Errors
    /// Rejects unexpected attempts, logical drift, terminal or content mismatches.
    pub fn validate_replay(&self, replay: &Self) -> Result<()> {
        let mut expected = self.clone();
        expected.attempt = AttemptId(1);
        require(
            self.attempt == AttemptId(0) && &expected == replay,
            "replay manifest/fingerprint mismatch",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{JobId, SegmentId, transfer};
    #[test]
    fn snapshot_morsels_and_attempt_independent_segments() {
        let root = tempfile::tempdir().unwrap();
        let files: Vec<_> = (0..4).map(|i| root.path().join(format!("{i}"))).collect();
        for file in &files {
            std::fs::write(file, b"stable").unwrap();
        }
        let snapshot = SourceSnapshot {
            version: 1,
            manifest_fingerprint: "manifest".into(),
            schema_fingerprint: "schema".into(),
            logical_fingerprint: "logical".into(),
            replayable: true,
            morsels: files
                .iter()
                .enumerate()
                .map(|(i, path)| SourceMorsel {
                    id: MorselId(i as u64),
                    path: path.clone(),
                    bytes: 6,
                    fingerprint: fingerprint(path).unwrap(),
                })
                .collect(),
        };
        snapshot.validate(&files).unwrap();
        assert_eq!(
            snapshot
                .producer_morsels(TaskId(1), 2)
                .iter()
                .map(|m| m.id)
                .collect::<Vec<_>>(),
            [MorselId(1), MorselId(3)]
        );
        SourceSnapshot::verify(&snapshot.morsels).unwrap();
        std::fs::write(&files[0], b"change").unwrap();
        assert!(SourceSnapshot::verify(&snapshot.morsels).is_err());
        assert!(
            snapshot
                .validate(&files.iter().rev().cloned().collect::<Vec<_>>())
                .is_err()
        );
        let a = transfer(JobId(3), 0, 1, 0);
        let b = a.with_attempt(AttemptId(1));
        assert_ne!(a, b);
        assert_eq!(
            SegmentId {
                transfer: a.logical(),
                sequence: 7
            },
            SegmentId {
                transfer: b.logical(),
                sequence: 7
            }
        );
    }
}
