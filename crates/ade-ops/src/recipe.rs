use crate::{Error, Result, check_time, require};
use ade_physical::{PlanV1, Target};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::Read,
    net::SocketAddr,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    time::Instant,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Input {
    pub table: String,
    pub root: PathBuf,
    pub manifest: PathBuf,
}
/// Declared fixed worker identity. Registration does not contact or trust a worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Worker {
    pub id: u64,
    pub control: SocketAddr,
    pub exchange: SocketAddr,
    pub instance: uuid::Uuid,
    pub scratch_root: PathBuf,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Recipe {
    pub version: u32,
    pub plan: PlanV1,
    pub inputs: Vec<Input>,
    pub workers: Vec<Worker>,
    pub timeout_ms: u64,
    /// Initial execution plus at most two safely fenced whole-job retries.
    pub max_attempts: u32,
    #[serde(
        default = "default_runtime",
        skip_serializing_if = "default_runtime_value"
    )]
    pub runtime_bytes: u64,
    /// Hard combined IPC data plus publication-manifest staging allowance.
    pub staging_bytes: u64,
}
fn default_runtime() -> u64 {
    ade_distributed::managed::DISK_BYTES
}
#[allow(clippy::trivially_copy_pass_by_ref)] // serde skip_serializing_if signature.
fn default_runtime_value(n: &u64) -> bool {
    *n == default_runtime()
}
impl Recipe {
    /// Validate metadata shape. Does not admit execution or contact workers.
    /// # Errors
    /// Refuses invalid IR, local targets, non-loopback cohorts and unsupported bounds.
    pub fn validate(&self) -> Result<()> {
        require(self.version == 1, "recipe version")?;
        self.plan
            .validate()
            .map_err(|e| Error::Invalid(e.to_string()))?;
        require(
            self.plan.target != Target::Local,
            "operations only describes distributed targets",
        )?;
        require(
            (1..=120_000).contains(&self.timeout_ms),
            "timeout 1..120000ms",
        )?;
        require((1..=3).contains(&self.max_attempts), "attempt cap 1..3")?;
        require(
            (1..=default_runtime()).contains(&self.runtime_bytes),
            "runtime byte cap 1..64MiB per worker/coordinator per attempt",
        )?;
        require(
            (1..=64 * 1024 * 1024).contains(&self.staging_bytes),
            "staging cap 1..64MiB",
        )?;
        require(
            self.inputs.len() == self.plan.sources.len(),
            "input coverage",
        )?;
        for (input, source) in self.inputs.iter().zip(&self.plan.sources) {
            require(input.table == source.table, "input table order")?;
            absolute(&input.root)?;
            absolute(&input.manifest)?;
        }
        require((2..=4).contains(&self.workers.len()), "fixed workers 2..4")?;
        let mut endpoints = BTreeSet::new();
        let mut instances = BTreeSet::new();
        let mut scratch = BTreeSet::new();
        for (i, w) in self.workers.iter().enumerate() {
            absolute(&w.scratch_root)?;
            require(
                w.id == i as u64 && !w.instance.is_nil() && instances.insert(w.instance),
                "worker identities",
            )?;
            require(
                scratch.insert(&w.scratch_root),
                "worker scratch roots must differ",
            )?;
            for endpoint in [w.control, w.exchange] {
                require(
                    endpoint.ip().is_loopback()
                        && endpoint.port() != 0
                        && endpoints.insert(endpoint),
                    "fixed unique loopback endpoints",
                )?;
            }
        }
        Ok(())
    }
    pub(crate) fn digest(&self) -> Result<String> {
        Ok(ade_physical::fingerprint(&serde_json::to_vec(self)?))
    }
    pub(crate) fn freeze(&self, until: Instant) -> Result<Vec<FrozenFile>> {
        self.validate()?;
        check_time(until)?;
        let mut inventory = BTreeMap::new();
        for (input, source) in self.inputs.iter().zip(&self.plan.sources) {
            // Resolve only existing, nonsymlink paths. Roots must already be canonical.
            require(
                input.root.canonicalize()? == input.root,
                "input root must be canonical",
            )?;
            nonsymlink(&input.root)?;
            let manifest = FrozenFile::capture(&input.manifest, 1024 * 1024, until)?;
            require(
                manifest.hash == source.manifest,
                "manifest identity differs from IR",
            )?;
            inventory.insert(input.manifest.clone(), manifest);
            for file in &source.files {
                let path = input.root.join(&file.name);
                let captured = FrozenFile::capture(&path, file.bytes, until)?;
                require(
                    captured.bytes == file.bytes && captured.hash == file.fingerprint,
                    "source identity differs from IR",
                )?;
                inventory.insert(path, captured);
            }
        }
        let mut catalog = ade_frontend::Catalog::default();
        for input in &self.inputs {
            catalog
                .register_manifest_until(&input.table, &input.root, &input.manifest, until)
                .map_err(|e| Error::Invalid(e.to_string()))?;
        }
        catalog
            .bind(&self.plan)
            .map_err(|e| Error::Invalid(e.to_string()))?;
        let files: Vec<_> = inventory.into_values().collect();
        for file in &files {
            file.verify(until)?;
        }
        check_time(until)?;
        Ok(files)
    }
}
fn absolute(path: &Path) -> Result<()> {
    require(
        path.is_absolute()
            && path.as_os_str().len() <= 4096
            && path.components().all(|c| {
                !matches!(
                    c,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            }),
        "bounded absolute normalized path",
    )
}
pub(crate) fn nonsymlink(path: &Path) -> Result<()> {
    absolute(path)?;
    let mut prefix = PathBuf::new();
    for component in path.components() {
        prefix.push(component);
        require(
            !fs::symlink_metadata(&prefix)?.file_type().is_symlink(),
            "symlink path refused",
        )?;
    }
    Ok(())
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FrozenFile {
    pub path: PathBuf,
    pub device: u64,
    pub inode: u64,
    pub modified_sec: i64,
    pub modified_nsec: i64,
    pub bytes: u64,
    pub hash: String,
}
impl FrozenFile {
    fn capture(path: &Path, limit: u64, until: Instant) -> Result<Self> {
        Self::capture_token(path, limit, until, None, "registration")
    }
    fn capture_token(
        path: &Path,
        limit: u64,
        until: Instant,
        token: Option<&ade_exec::CancellationToken>,
        stage: &str,
    ) -> Result<Self> {
        check_time(until)?;
        nonsymlink(path)?;
        let entry = fs::metadata(path)?;
        require(
            entry.is_file() && entry.len() <= limit,
            "source regular file/byte bound",
        )?;
        let mut file = File::open(path)?;
        let before = file.metadata()?;
        require(
            before.is_file() && before.len() <= limit,
            "source regular file/byte bound",
        )?;
        let mut hash = blake3::Hasher::new();
        let mut read = 0u64;
        let mut buf = [0; 8192];
        loop {
            if let Some(token) = token {
                token.check().map_err(|e| Error::Invalid(e.to_string()))?;
            }
            check_time(until)?;
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            read = read
                .checked_add(n as u64)
                .ok_or(Error::Quota("source byte overflow"))?;
            if read > limit {
                return Err(Error::Quota("source bytes"));
            }
            hash.update(&buf[..n]);
            #[cfg(feature = "fault-injection")]
            if token.is_some() && std::env::var("ADE_OPS_HASH_STAGE").as_deref() == Ok(stage) {
                eprintln!("ADE_OPS_HASH_WITNESS {stage}");
                let pause = Instant::now() + std::time::Duration::from_secs(4);
                while Instant::now() < pause {
                    check_time(until)?;
                    if let Some(token) = token {
                        token.check().map_err(|e| Error::Invalid(e.to_string()))?;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
            }
            let _ = stage;
        }
        let result = Self {
            path: path.into(),
            device: before.dev(),
            inode: before.ino(),
            modified_sec: before.mtime(),
            modified_nsec: before.mtime_nsec(),
            bytes: read,
            hash: hash.finalize().to_hex().to_string(),
        };
        let after = fs::metadata(path)?;
        require(
            read == before.len()
                && after.dev() == result.device
                && after.ino() == result.inode
                && after.len() == read
                && after.mtime() == result.modified_sec
                && after.mtime_nsec() == result.modified_nsec,
            "source changed during hash",
        )?;
        check_time(until)?;
        Ok(result)
    }
    pub(crate) fn verify_token(
        &self,
        until: Instant,
        token: &ade_exec::CancellationToken,
        stage: &str,
    ) -> Result<()> {
        require(
            Self::capture_token(&self.path, self.bytes, until, Some(token), stage)? == *self,
            "frozen source drift/replacement",
        )
    }
    pub(crate) fn verify(&self, until: Instant) -> Result<()> {
        require(
            Self::capture(&self.path, self.bytes, until)? == *self,
            "frozen source drift/replacement",
        )
    }
}
