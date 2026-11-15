use crate::{Control, Error, Result, fs, is_digest};
use ade_exec::physical::{Binding, Catalog};
use ade_physical::{MAX_PLAN_BYTES, Operator, PlanV1, Schema, Source, SourceFile, Target, Type};
use rustix::fs::OFlags;
use serde::{Deserialize, Serialize};
use std::{fs::File, os::unix::fs::MetadataExt, path::Path};

pub(crate) const SEMANTICS: &str = "ade.shared-prefix.v1/scan-filter-lt-project-1/parquet-1";
pub(crate) const PAYLOAD: &str = "payload.parquet";

/// Operator-pinned catalog. The external digest is over the exact UTF-8 JSON bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub version: u32,
    pub logical: String,
    pub schema: Schema,
    pub files: Vec<SourceFile>,
}

impl Snapshot {
    /// Derive the `PlanV1` source declaration from this exact snapshot file.
    /// # Errors
    /// Refuses an invalid catalog version, digest, schema or inventory.
    pub fn source(&self, pin: &str) -> Result<Source> {
        if self.version != 1 || !is_digest(pin) {
            return Err(Error::Request("catalog version or pin"));
        }
        let source = Source {
            id: 0,
            table: "input".into(),
            manifest: pin.into(),
            logical: self.logical.clone(),
            schema_identity: schema_identity(&self.schema)?,
            schema: self.schema.clone(),
            files: self.files.clone(),
            replayable: true,
        };
        source
            .validate()
            .map_err(|_| Error::Request("catalog inventory"))?;
        Ok(source)
    }
}

pub(crate) fn schema_identity(schema: &Schema) -> Result<String> {
    Ok(ade_physical::fingerprint(&serde_json::to_vec(schema)?))
}

pub(crate) struct Original<'a> {
    root: &'a Path,
    pin: &'a str,
    pub(crate) catalog: Catalog,
    stamps: Vec<fs::Stamp>,
}

impl<'a> Original<'a> {
    pub(crate) fn open(
        root: &'a Path,
        pin: &'a str,
        plan: &PlanV1,
        control: &Control,
    ) -> Result<Self> {
        let dir = fs::directory(root, false)?;
        let manifest = fs::data(&dir, "catalog.json", OFlags::RDONLY, false)?;
        let stamps = vec![manifest.metadata()?.into()];
        let bytes = fs::read_bounded(manifest, MAX_PLAN_BYTES, control)?;
        if !is_digest(pin) || ade_physical::fingerprint(&bytes) != pin {
            return Err(Error::Snapshot);
        }
        let source = serde_json::from_slice::<Snapshot>(&bytes)?.source(pin)?;
        if plan.sources.as_slice() != std::slice::from_ref(&source) {
            return Err(Error::Snapshot);
        }
        let mut original = Self {
            root,
            pin,
            catalog: Catalog::default(),
            stamps,
        };
        let mut files = Vec::new();
        for info in &source.files {
            let file = source_file(&dir, &info.name)?;
            original.stamps.push(file.metadata()?.into());
            files.push(root.join(&info.name));
        }
        original.catalog.bindings.push(Binding { source, files });
        Ok(original)
    }

    pub(crate) fn attest(&self, control: &Control) -> Result<()> {
        let dir = fs::directory(self.root, false).map_err(|_| Error::Snapshot)?;
        let catalog =
            fs::data(&dir, "catalog.json", OFlags::RDONLY, false).map_err(|_| Error::Snapshot)?;
        if fs::Stamp::from(catalog.metadata()?) != self.stamps[0]
            || ade_physical::fingerprint(&fs::read_bounded(catalog, MAX_PLAN_BYTES, control)?)
                != self.pin
        {
            return Err(Error::Snapshot);
        }
        for (file, expected) in self.catalog.bindings[0]
            .source
            .files
            .iter()
            .zip(&self.stamps[1..])
        {
            control.check()?;
            let f = source_file(&dir, &file.name).map_err(|_| Error::Snapshot)?;
            if fs::Stamp::from(f.metadata()?) != *expected {
                return Err(Error::Snapshot);
            }
        }
        Ok(())
    }

    pub(crate) fn verify(&self, plan: &PlanV1, control: &Control, preflight: bool) -> Result<()> {
        self.attest(control)?;
        self.catalog
            .verify(plan, &control.token, control.deadline, preflight)
            .map_err(|e| match e {
                ade_exec::Error::Cancelled | ade_exec::Error::Deadline(_) => e.into(),
                _ => Error::Snapshot,
            })?;
        self.attest(control)
    }
}

fn source_file(root: &File, key: &str) -> Result<File> {
    let mut dir = root.try_clone()?;
    let mut parts = key.split('/').peekable();
    while let Some(part) = parts.next() {
        if part.is_empty() || part == "." || part == ".." {
            return Err(Error::Access);
        }
        if parts.peek().is_none() {
            return fs::data(&dir, part, OFlags::RDONLY, false);
        }
        dir = fs::open_dir(&dir, Path::new(part))?;
        let m = dir.metadata()?;
        if (m.uid() != fs::uid() && m.uid() != 0) || m.mode() & 0o022 != 0 {
            return Err(Error::Access);
        }
    }
    Err(Error::Access)
}

pub(crate) struct Cut {
    pub(crate) prefix: PlanV1,
    pub(crate) schema: Schema,
    suffix: Vec<Operator>,
}

impl Cut {
    pub(crate) fn new(original: &PlanV1, cut: u32) -> Result<Self> {
        let unsupported = || Error::Request("unsupported native cut or suffix");
        if !original.sources[0].replayable {
            return Err(Error::Request(
                "cache requires a replayable source snapshot",
            ));
        }
        if original.target != Target::Local
            || !matches!(cut, 1 | 2)
            || original.output <= cut
            || original.sources[0]
                .schema
                .iter()
                .any(|f| !matches!(f.data_type, Type::U64 | Type::I64))
            || !matches!(original.nodes[0].operator, Operator::Scan { source: 0 })
            || !matches!(
                original.nodes[1].operator,
                Operator::Filter { input: 0, .. }
            )
            || (cut == 2
                && !matches!(
                    original.nodes[2].operator,
                    Operator::Project { input: 1, .. }
                ))
        {
            return Err(unsupported());
        }
        let mut prefix = PlanV1::new(Target::Local, original.sources.clone());
        for node in &original.nodes[..=cut as usize] {
            prefix
                .push(node.operator.clone())
                .map_err(|_| unsupported())?;
        }
        prefix.validate().map_err(|_| unsupported())?;
        let schema = prefix.nodes[cut as usize].schema.clone();
        let suffix = original.nodes[cut as usize + 1..]
            .iter()
            .map(|node| {
                Ok(match &node.operator {
                    Operator::GroupSumCount { input, key, value } => Operator::GroupSumCount {
                        input: input - cut,
                        key: *key,
                        value: *value,
                    },
                    Operator::Project { input, columns } => Operator::Project {
                        input: input - cut,
                        columns: columns.clone(),
                    },
                    _ => return Err(unsupported()),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if !matches!(suffix[0], Operator::GroupSumCount { .. })
            || suffix[1..]
                .iter()
                .any(|op| !matches!(op, Operator::Project { .. }))
        {
            return Err(unsupported());
        }
        Ok(Self {
            prefix,
            schema,
            suffix,
        })
    }

    pub(crate) fn key(&self, scope: &str) -> Result<String> {
        let mut hasher = blake3::Hasher::new_derive_key(SEMANTICS);
        hasher.update(scope.as_bytes());
        hasher.update(&serde_json::to_vec(&self.prefix)?);
        Ok(hasher.finalize().to_hex().to_string())
    }

    pub(crate) fn bind(
        &self,
        original: &PlanV1,
        entry_path: &Path,
        key: &str,
        bytes: u64,
        rows: u64,
        digest: &str,
    ) -> Result<(PlanV1, Catalog)> {
        let source = Source {
            id: 0,
            table: "input".into(),
            manifest: key.into(),
            logical: digest.into(),
            schema_identity: schema_identity(&self.schema)?,
            schema: self.schema.clone(),
            files: vec![SourceFile {
                name: PAYLOAD.into(),
                bytes,
                rows,
                fingerprint: digest.into(),
            }],
            replayable: true,
        };
        let mut rewritten = PlanV1::new(Target::Local, vec![source.clone()]);
        rewritten.max_output_rows = original.max_output_rows;
        rewritten.max_output_bytes = original.max_output_bytes;
        rewritten
            .push(Operator::Scan { source: 0 })
            .map_err(|_| Error::Integrity("cached scan"))?;
        for op in &self.suffix {
            rewritten
                .push(op.clone())
                .map_err(|_| Error::Integrity("cached suffix"))?;
        }
        rewritten
            .validate()
            .map_err(|_| Error::Integrity("cached plan"))?;
        if rewritten.nodes.last().map(|n| (&n.schema, &n.properties))
            != original.nodes.last().map(|n| (&n.schema, &n.properties))
        {
            return Err(Error::Integrity("suffix semantics"));
        }
        Ok((
            rewritten,
            Catalog {
                bindings: vec![Binding {
                    source,
                    files: vec![entry_path.join(PAYLOAD)],
                }],
            },
        ))
    }
}
