//! Pure admission of finite graph inventories and cyclic semantic identities.
use crate::{Error, Result, require};
use ade_physical::cycle::{CyclicPlanV1, MAX_EDGES, MAX_VERTICES, Role};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const MAX_SOURCE_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_FILES: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetFileV1 {
    pub name: String,
    pub bytes: u64,
    pub rows: usize,
    pub blake3: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphInventoryV1 {
    pub vertices: Vec<ParquetFileV1>,
    pub edges: Vec<ParquetFileV1>,
}

#[must_use]
pub fn is_digest(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

#[must_use]
pub fn digest(domain: &str, bytes: &[u8]) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(domain.as_bytes());
    hash.update(b"\0");
    hash.update(bytes);
    hash.finalize().to_hex().to_string()
}

impl GraphInventoryV1 {
    /// # Errors
    /// Refuses invalid or incomplete manifests and out-of-envelope rows/bytes.
    pub fn admit(&self) -> Result<u64> {
        let mut names = BTreeSet::new();
        let mut bytes = 0u64;
        for (files, max_rows) in [(&self.vertices, MAX_VERTICES), (&self.edges, MAX_EDGES)] {
            require(
                !files.is_empty() && files.len() <= MAX_FILES,
                "graph files per role",
            )?;
            let mut rows = 0usize;
            for file in files {
                require(
                    file.name.len() <= 128
                        && file.name.ends_with(".parquet")
                        && file
                            .name
                            .bytes()
                            .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
                        && names.insert(&file.name)
                        && file.bytes >= 12
                        && is_digest(&file.blake3),
                    "invalid or duplicate graph file inventory entry",
                )?;
                bytes = bytes
                    .checked_add(file.bytes)
                    .ok_or_else(|| Error("graph source bytes overflow".into()))?;
                rows = rows
                    .checked_add(file.rows)
                    .ok_or_else(|| Error("graph rows overflow".into()))?;
            }
            require(rows <= max_rows, "graph row cap")?;
        }
        require(bytes <= MAX_SOURCE_BYTES, "graph source byte cap")?;
        Ok(bytes)
    }

    /// # Errors
    /// Refuses malformed graph inventories and cyclic plans.
    pub fn identities(&self, plan: &CyclicPlanV1) -> Result<(String, String)> {
        self.admit()?;
        let cycle = plan.bind()?;
        let encode = |e: serde_json::Error| Error(e.to_string());
        let source = digest(
            "ade-cycle20-source-v1",
            &serde_json::to_vec(&(Role::Vertices.schema(), Role::Edges.schema(), self))
                .map_err(encode)?,
        );
        let id = digest(
            "ade-cycle20-plan-v1",
            &serde_json::to_vec(&(
                plan,
                &source,
                cycle.algorithm().output_schema(),
                "simple_directed_strict_round_v1",
            ))
            .map_err(encode)?,
        );
        Ok((source, id))
    }
}
