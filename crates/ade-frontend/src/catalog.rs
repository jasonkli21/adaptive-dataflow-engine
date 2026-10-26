//! Manifest-only local catalog. `DataFusion` receives schemas, never file providers.
use crate::{Result, require};
use ade_exec::{
    CancellationToken,
    physical::{Binding, Catalog as NativeCatalog},
};
use ade_physical::{Field, Operator, PlanV1, Source, SourceFile, Target, Type};
use serde::Deserialize;
use std::{
    io::Read,
    path::Path,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Default)]
pub struct Catalog {
    pub(crate) native: NativeCatalog,
}
#[derive(Deserialize)]
struct Manifest {
    schema_version: u32,
    dataset_version: u32,
    required_schema: String,
    schema_fingerprint: String,
    logical_fingerprint: String,
    format: String,
    row_count: u64,
    files: Vec<SourceFileManifest>,
}
#[derive(Deserialize)]
struct SourceFileManifest {
    relative_path: String,
    bytes: u64,
    rows: u64,
    fingerprint: String,
}
impl Catalog {
    /// Register only the established manifest format and allowlisted table names.
    /// All listed files are verified, including physical schemas/nulls/row counts.
    /// # Errors
    /// Refuses traversal, symlink escape, forged metadata and source/resource drift.
    pub fn register_manifest(&mut self, table: &str, root: &Path, manifest: &Path) -> Result<()> {
        self.register_manifest_until(
            table,
            root,
            manifest,
            Instant::now() + Duration::from_secs(120),
        )
    }

    /// Register a manifest under a caller-owned absolute deadline.
    /// OS file operations are not preemptible; source verification checks the
    /// deadline between bounded reads and decode batches.
    /// # Errors
    /// Refuses expired deadlines, traversal, symlink escape, forged metadata and source drift.
    pub fn register_manifest_until(
        &mut self,
        table: &str,
        root: &Path,
        manifest: &Path,
        deadline: Instant,
    ) -> Result<()> {
        require(Instant::now() < deadline, "manifest registration deadline")?;
        require(
            self.native.bindings.len() < 2
                && !self.native.bindings.iter().any(|b| b.source.table == table),
            "duplicate/excess table",
        )?;
        let file = std::fs::File::open(manifest)?;
        require(
            file.metadata()?.is_file(),
            "manifest must be a regular file",
        )?;
        let mut bytes = Vec::new();
        file.take(1024 * 1024 + 1).read_to_end(&mut bytes)?;
        require(bytes.len() <= 1024 * 1024, "manifest cap 1MiB")?;
        require(Instant::now() < deadline, "manifest registration deadline")?;
        let m: Manifest = serde_json::from_slice(&bytes)?;
        require(
            m.schema_version == 1 && m.dataset_version == 1 && m.format == "parquet",
            "manifest version/format",
        )?;
        let schema = source_schema(table, &m.required_schema)?;
        require(
            canonical_schema_fingerprint(&m.required_schema)
                .is_some_and(|expected| m.schema_fingerprint == expected),
            "manifest schema fingerprint does not match allowlisted v1 schema",
        )?;
        require(
            m.files.iter().try_fold(0u64, |a, f| a.checked_add(f.rows)) == Some(m.row_count),
            "manifest total rows",
        )?;
        require(m.files.len() <= 128, "manifest file cap 128")?;
        let prefix = match table {
            "ade_left" => Some("left/"),
            "ade_right" => Some("right/"),
            _ => None,
        };
        require(
            m.files.iter().all(|f| {
                prefix.is_none()
                    || f.relative_path.starts_with("left/")
                    || f.relative_path.starts_with("right/")
            }),
            "join manifest side names",
        )?;
        let source = Source {
            id: u32::try_from(self.native.bindings.len())
                .map_err(|_| crate::Error::Unsupported("source ID overflow".into()))?,
            table: table.into(),
            manifest: blake3_hash(&bytes),
            logical: m.logical_fingerprint,
            schema_identity: m.schema_fingerprint,
            schema,
            files: m
                .files
                .into_iter()
                .filter(|f| prefix.is_none_or(|p| f.relative_path.starts_with(p)))
                .map(|f| SourceFile {
                    name: f.relative_path,
                    bytes: f.bytes,
                    rows: f.rows,
                    fingerprint: f.fingerprint,
                })
                .collect(),
            replayable: table == "input",
        };
        source.validate()?;
        let root = root.canonicalize()?;
        let files = source
            .files
            .iter()
            .map(|f| {
                let path = root.join(&f.name).canonicalize()?;
                require(path.starts_with(&root), "manifest symlink escapes root")?;
                Ok(path)
            })
            .collect::<Result<Vec<_>>>()?;
        // Reuse native preflight even for registration of a single join side.
        let mut check_source = source.clone();
        check_source.id = 0;
        check_source.table = "input".into();
        let binding = Binding {
            source: check_source.clone(),
            files: files.clone(),
        };
        let mut plan = PlanV1::new(Target::Local, vec![check_source]);
        plan.push(Operator::Scan { source: 0 })?;
        NativeCatalog {
            bindings: vec![binding],
        }
        .verify(&plan, &CancellationToken::default(), deadline, true)?;
        self.native.bindings.push(Binding { source, files });
        Ok(())
    }
    #[must_use]
    pub fn native(&self) -> &NativeCatalog {
        &self.native
    }
    /// Build a plan from only the needed tables, in stable declared order.
    /// # Errors
    /// Rejects unregistered tables.
    pub fn plan(&self, target: Target, tables: &[&str]) -> Result<(PlanV1, NativeCatalog)> {
        let mut bindings = vec![];
        for (i, table) in tables.iter().enumerate() {
            let mut binding = self
                .native
                .bindings
                .iter()
                .find(|b| b.source.table == *table)
                .ok_or_else(|| crate::Error::Unsupported(format!("unregistered table {table}")))?
                .clone();
            binding.source.id = u32::try_from(i)
                .map_err(|_| crate::Error::Unsupported("source ID overflow".into()))?;
            bindings.push(binding);
        }
        Ok((
            PlanV1::new(target, bindings.iter().map(|b| b.source.clone()).collect()),
            NativeCatalog { bindings },
        ))
    }
    /// Resolve the exact immutable source catalog for a compiled graph.
    /// # Errors
    /// Rejects source identity changes or missing tables.
    pub fn bind(&self, plan: &PlanV1) -> Result<NativeCatalog> {
        plan.validate()?;
        let tables = plan
            .sources
            .iter()
            .map(|s| s.table.as_str())
            .collect::<Vec<_>>();
        let (_, catalog) = self.plan(plan.target, &tables)?;
        require(
            catalog
                .bindings
                .iter()
                .map(|b| &b.source)
                .eq(plan.sources.iter()),
            "plan source does not match registered manifest",
        )?;
        Ok(catalog)
    }
}
fn blake3_hash(bytes: &[u8]) -> String {
    ade_physical::fingerprint(bytes)
}

fn canonical_schema_fingerprint(required_schema: &str) -> Option<String> {
    let canonical = match required_schema {
        "synthetic-events-v1" => {
            b"synthetic-events-v1:row_id=u64,key=u64,secondary=u64,value_i64=i64,value_f64=f64,category=u32,payload=utf8,event_time=timestamp_ms;all_nonnull".as_slice()
        }
        "synthetic-join-v1" => {
            b"left-v1:key=u64,value_i64=i64;right-v1:key=u64,value_i64=i64;all_nonnull".as_slice()
        }
        _ => return None,
    };
    Some(blake3_hash(canonical))
}

fn source_schema(table: &str, required_schema: &str) -> Result<Vec<Field>> {
    Ok(match (table, required_schema) {
        ("input", "synthetic-events-v1") => vec![
            Field::new("row_id", Type::U64, false),
            Field::new("key", Type::U64, false),
            Field::new("secondary", Type::U64, false),
            Field::new("value_i64", Type::I64, false),
            Field::new("value_f64", Type::F64, false),
            Field::new("category", Type::U32, false),
            Field::new("payload", Type::Utf8, false),
            Field::new("event_time", Type::TimestampMs, false),
        ],
        ("ade_left" | "ade_right", "synthetic-join-v1") => vec![
            Field::new("key", Type::U64, false),
            Field::new("value_i64", Type::I64, false),
        ],
        _ => {
            return Err(crate::Error::Unsupported(
                "table/schema allowlist: input events or ade_left/ade_right join".into(),
            ));
        }
    })
}
