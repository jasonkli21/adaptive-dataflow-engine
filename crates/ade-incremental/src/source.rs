use crate::{
    Error, Guard, Limits, Metrics, Result,
    arrangement::{Hot, Shard},
    require, storage,
};
use ade_exec::{
    ExecutionConfig,
    physical::{Binding, Catalog},
};
use ade_logical::maintained::{DELTA_BYTES, DeltaBatchV1, MaintainedPlanV1, SHARDS, shard};
use ade_physical::{Operator, PlanV1, Target};
use arrow::array::{Int64Array, UInt64Array};
use std::{
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

pub(crate) const BATCH_ROWS: usize = 1024;
pub(crate) const SCAN_BYTES: usize = 1024 * 1024;

pub(crate) struct Baseline {
    plan: PlanV1,
    catalog: Catalog,
    stamps: Vec<FileStamp>,
    pub rows: u64,
    pub bytes: u64,
}
#[derive(PartialEq, Eq)]
struct FileStamp {
    device: u64,
    inode: u64,
    bytes: u64,
    changed: (i64, i64),
    modified: (i64, i64),
}
impl From<&std::fs::Metadata> for FileStamp {
    fn from(meta: &std::fs::Metadata) -> Self {
        Self {
            device: meta.dev(),
            inode: meta.ino(),
            bytes: meta.len(),
            changed: (meta.ctime(), meta.ctime_nsec()),
            modified: (meta.mtime(), meta.mtime_nsec()),
        }
    }
}
impl Baseline {
    pub fn open(root: &Path, descriptor: &MaintainedPlanV1, guard: &Guard) -> Result<Self> {
        storage::no_symlinks(root)?;
        let source = descriptor.source().clone();
        let mut files = Vec::new();
        let mut stamps = Vec::new();
        for entry in &source.files {
            let path = root.join(&entry.name);
            storage::no_symlinks(&path)?;
            let file = storage::regular(&path)?;
            stamps.push(FileStamp::from(&file.metadata()?));
            files.push(path);
        }
        let rows = source.files.iter().map(|file| file.rows).sum();
        let bytes = source.files.iter().map(|file| file.bytes).sum();
        let mut plan = PlanV1::new(Target::Local, vec![source.clone()]);
        plan.push(Operator::Scan { source: 0 })?;
        let catalog = Catalog {
            bindings: vec![Binding { source, files }],
        };
        let baseline = Self {
            plan,
            catalog,
            stamps,
            rows,
            bytes,
        };
        baseline.attest(guard)?;
        Ok(baseline)
    }

    pub fn attest(&self, guard: &Guard) -> Result<()> {
        self.catalog
            .verify(&self.plan, &guard.cancel, guard.until, false)?;
        for (path, expected) in self.catalog.bindings[0].files.iter().zip(&self.stamps) {
            guard.check()?;
            storage::no_symlinks(path)?;
            let actual = FileStamp::from(&storage::regular(path)?.metadata()?);
            require(
                &actual == expected,
                "baseline identity changed during operation",
            )?;
        }
        Ok(())
    }

    pub fn scan(
        &self,
        shards: &mut [Shard; SHARDS],
        hot: &mut Hot,
        threshold: i64,
        guard: &Guard,
        metrics: &mut Metrics,
    ) -> Result<()> {
        let mut failure = None;
        let config = ExecutionConfig {
            memory_limit_bytes: SCAN_BYTES,
            spill_enabled: false,
            prefetch_batches: 0,
            output_batch_rows: BATCH_ROWS,
        };
        let schema = ade_exec::physical::arrow_schema(&self.plan.sources[0].schema);
        let local = ade_exec::physical::lower(&self.plan, &self.catalog, BATCH_ROWS)?;
        self.catalog
            .verify(&self.plan, &guard.cancel, guard.until, true)?;
        let outcome = ade_exec::execute_to_sink(local, config, guard.cancel.clone(), |batch| {
            let result = (|| -> Result<()> {
                guard.check()?;
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or(Error::Invalid("native key type"))?;
                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or(Error::Invalid("native value type"))?;
                require(
                    batch.schema() == schema && batch.columns().iter().all(|a| a.null_count() == 0),
                    "native scan schema or null drift",
                )?;
                let bytes = batch.get_array_memory_size();
                require(
                    batch.num_rows() <= BATCH_ROWS && bytes <= SCAN_BYTES,
                    "native decoded batch cap",
                )?;
                require(
                    metrics.baseline_rows + batch.num_rows() as u64 <= self.rows,
                    "native scan exceeded pinned row count",
                )?;
                metrics.decoded_batch_peak = metrics.decoded_batch_peak.max(bytes);
                for (&key, &value) in keys.values().iter().zip(values.values()) {
                    shards[shard(key)].baseline_insert(key, value, hot)?;
                }
                metrics.baseline_rows += batch.num_rows() as u64;
                Ok(())
            })();
            if let Err(error) = result {
                failure = Some(error);
                return Err(ade_exec::Error::Invalid(
                    "maintained baseline consumer".into(),
                ));
            }
            Ok(())
        });
        if let Some(error) = failure {
            return Err(error);
        }
        outcome?;
        require(
            metrics.baseline_rows == self.rows,
            "native scan row coverage",
        )?;
        crate::fault("baseline_scanned")?;
        self.attest(guard)?;
        metrics.baseline_scan_bytes = self.bytes;
        for shard in shards {
            shard.baseline_finalize(threshold, guard)?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct DeltaMeta {
    pub chain: String,
    pub rows: u64,
    pub keys: u64,
    pub touched_shards: Vec<usize>,
    pub bytes: u64,
}

pub(crate) struct History {
    dir: Option<PathBuf>,
    tail: Option<PathBuf>,
    pub genesis: String,
    pub deltas: Vec<DeltaMeta>,
}
impl DeltaMeta {
    fn verified(batch: &DeltaBatchV1, bytes: u64) -> Self {
        Self {
            chain: batch.chain().into(),
            rows: batch.rows() as u64,
            keys: batch.keys() as u64,
            touched_shards: batch.touched_shards().collect(),
            bytes,
        }
    }
}

impl History {
    pub fn open(
        dir: Option<&Path>,
        target: u32,
        expected: &str,
        plan: &MaintainedPlanV1,
        limits: Limits,
        guard: &Guard,
    ) -> Result<Self> {
        let mut history = Self {
            dir: dir.map(Path::to_owned),
            tail: None,
            genesis: plan.genesis(),
            deltas: Vec::new(),
        };
        if let Some(dir) = dir {
            storage::no_symlinks(dir)?;
            for epoch in 1..=target {
                let parent = history.tip();
                let (batch, bytes) = load(dir, epoch, parent, plan, limits, guard)?;
                history.deltas.push(DeltaMeta::verified(&batch, bytes));
            }
        }
        if dir.is_some() || target == 0 {
            require(
                history.tip() == expected,
                "independently pinned source chain differs",
            )?;
        }
        Ok(history)
    }

    pub fn bind_tail(
        &mut self,
        path: &Path,
        anchor: &crate::storage::Manifest,
        request: &crate::Request,
        plan: &MaintainedPlanV1,
        guard: &Guard,
    ) -> Result<()> {
        let epoch = request.epoch;
        require(
            epoch > 0 && (anchor.epoch == epoch - 1 || anchor.epoch == epoch),
            "tail requires compatible parent or target",
        )?;
        storage::no_symlinks(path)?;
        let prefix = &anchor.history[..epoch as usize - 1];
        let parent = prefix.last().unwrap_or(&self.genesis);
        let (batch, bytes) = load_file(path, epoch, parent, plan, request.limits, guard)?;
        require(
            batch.chain() == request.chain,
            "independently pinned tail chain differs",
        )?;
        self.tail = Some(path.into());
        self.deltas = prefix
            .iter()
            .map(|chain| DeltaMeta {
                chain: chain.clone(),
                rows: 0,
                keys: 0,
                touched_shards: Vec::new(),
                bytes: 0,
            })
            .collect();
        self.deltas.push(DeltaMeta::verified(&batch, bytes));
        Ok(())
    }

    pub fn full(&self, epoch: u32) -> bool {
        self.tail.is_none() && self.covers(epoch)
    }

    pub fn tip(&self) -> &str {
        self.deltas.last().map_or(&self.genesis, |m| &m.chain)
    }

    pub fn parent(&self, epoch: u32) -> &str {
        if epoch <= 1 {
            &self.genesis
        } else {
            &self.deltas[epoch as usize - 2].chain
        }
    }

    pub fn covers(&self, epoch: u32) -> bool {
        self.deltas.len() == epoch as usize
    }

    pub fn load(
        &self,
        epoch: u32,
        plan: &MaintainedPlanV1,
        limits: Limits,
        guard: &Guard,
    ) -> Result<DeltaBatchV1> {
        let parent = self.parent(epoch);
        let (batch, _) = if let Some(path) = &self.tail {
            require(self.covers(epoch), "only target delta is locally available")?;
            load_file(path, epoch, parent, plan, limits, guard)?
        } else {
            let dir = self
                .dir
                .as_ref()
                .ok_or(Error::Invalid("delta directory required"))?;
            load(dir, epoch, parent, plan, limits, guard)?
        };
        require(
            batch.chain() == self.deltas[epoch as usize - 1].chain,
            "delta bytes changed after admission",
        )?;
        Ok(batch)
    }

    pub fn attest(&self, plan: &MaintainedPlanV1, limits: Limits, guard: &Guard) -> Result<()> {
        for epoch in
            1..=u32::try_from(self.deltas.len()).map_err(|_| Error::Quota("delta epochs"))?
        {
            if self.deltas[epoch as usize - 1].bytes > 0 {
                self.load(epoch, plan, limits, guard)?;
            }
        }
        Ok(())
    }
}

fn load(
    dir: &Path,
    epoch: u32,
    parent: &str,
    plan: &MaintainedPlanV1,
    limits: Limits,
    guard: &Guard,
) -> Result<(DeltaBatchV1, u64)> {
    load_file(
        &dir.join(format!("{epoch:08}.json")),
        epoch,
        parent,
        plan,
        limits,
        guard,
    )
}

fn load_file(
    path: &Path,
    epoch: u32,
    parent: &str,
    plan: &MaintainedPlanV1,
    limits: Limits,
    guard: &Guard,
) -> Result<(DeltaBatchV1, u64)> {
    let bytes = storage::read_cap(path, DELTA_BYTES, guard)?;
    let batch = DeltaBatchV1::from_json(&bytes, plan, epoch, parent)?;
    if batch.rows().saturating_mul(2) > limits.hot_entries {
        return Err(Error::Quota("delta hot entries"));
    }
    Ok((batch, bytes.len() as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CancellationToken, Guard};
    use ade_logical::maintained::PLAN_BYTES;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn explicitly_bound_missing_baseline_file_is_an_error() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join(format!("ade19-baseline-{}-{unique}", std::process::id()));
        fs::create_dir(&root).unwrap();
        let document = serde_json::json!({
            "version": 1,
            "namespace": "missing-baseline-test",
            "threshold": 0,
            "baseline": [{
                "name": "missing.parquet",
                "bytes": 4,
                "rows": 1,
                "fingerprint": "0".repeat(64)
            }]
        });
        let bytes = serde_json::to_vec(&document).unwrap();
        assert!(bytes.len() < PLAN_BYTES);
        let plan = MaintainedPlanV1::from_json(&bytes).unwrap();
        let guard = Guard::new(CancellationToken::default(), 1000);
        let Err(error) = Baseline::open(&root, &plan, &guard) else {
            panic!("missing explicit baseline unexpectedly admitted")
        };
        assert!(
            matches!(error, Error::Io(ref e) if e.kind() == std::io::ErrorKind::NotFound),
            "{error:?}"
        );
        fs::remove_dir_all(root).unwrap();
    }
}
