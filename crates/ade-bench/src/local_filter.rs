//! Adapter-owned profile admission. The runtime only receives the admitted cost model.
use crate::{Error, Result, dataset, dataset::DatasetManifest, require, spec::BenchmarkCase};
use ade_exec::{
    ExecutionConfig,
    filter_kernel::{AutoSelection, Isa, Measurements, ProfileRefusal, Request, Shape},
};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{Read, copy},
    path::{Path, PathBuf},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

const MAX_PROFILE_BYTES: u64 = 64 * 1024;
const MAX_AGE_SECONDS: u64 = 90 * 24 * 3600;

#[derive(Debug, Clone, Default)]
pub(crate) struct Options {
    policy: Option<String>,
    profile: Option<PathBuf>,
    pub(crate) digest: Option<String>,
}

impl Options {
    pub(crate) fn set(&mut self, key: &str, value: &str) -> Result<()> {
        require(!value.is_empty(), "empty filter kernel option")?;
        match key {
            "kernel_policy" => {
                require(
                    matches!(
                        value,
                        "scalar" | "arrow" | "force-neon" | "force-avx2" | "measured-auto"
                    ),
                    "unknown kernel_policy",
                )?;
                self.policy = Some(value.into());
            }
            "kernel_profile" => self.profile = Some(value.into()),
            "kernel_profile_blake3" => {
                require(digest(value), "invalid kernel_profile_blake3")?;
                self.digest = Some(value.into());
            }
            _ => unreachable!("filter option"),
        }
        Ok(())
    }

    pub(crate) fn label(&self) -> Option<&str> {
        self.policy.as_deref()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        require(
            match self.label() {
                Some("measured-auto") => self.profile.is_some() == self.digest.is_some(),
                _ => self.profile.is_none() && self.digest.is_none(),
            },
            "kernel_profile and kernel_profile_blake3 must be supplied together for measured-auto",
        )?;
        let unavailable = match self.label() {
            Some("force-neon") if !ade_exec::filter_kernel::neon_available() => Some(Isa::Neon),
            Some("force-avx2") => Some(Isa::Avx2),
            _ => None,
        };
        if let Some(isa) = unavailable {
            return Err(ade_exec::Error::UnsupportedFilterIsa(isa).into());
        }
        Ok(())
    }

    pub(crate) fn prepare(
        &self,
        root: &Path,
        manifest: &DatasetManifest,
        case: &BenchmarkCase,
        batch_size: usize,
        config: &ExecutionConfig,
    ) -> Option<Request> {
        match self.label() {
            None => None,
            Some("scalar") => Some(Request::Scalar),
            Some("arrow") => Some(Request::Arrow),
            Some("force-neon") => Some(Request::Force(Isa::Neon)),
            Some("force-avx2") => Some(Request::Force(Isa::Avx2)),
            Some("measured-auto") => {
                let policy = match (&self.profile, &self.digest) {
                    (Some(path), Some(pin)) => {
                        let start = Instant::now();
                        let profile = load(path, pin).and_then(|profile| {
                            profile.admit(root, manifest, case, batch_size, config)
                        });
                        let ns = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
                        profile
                            .map_or_else(AutoSelection::unavailable, |p| p.with_admission_cost(ns))
                    }
                    _ => AutoSelection::unavailable(ProfileRefusal::Missing),
                };
                Some(Request::MeasuredAuto(policy))
            }
            _ => unreachable!("validated filter policy"),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Profile {
    version: u32,
    mechanism: String,
    created_unix_s: u64,
    binding: Binding,
    shape: Shape,
    full_batches: usize,
    calibration_sha256: String,
    sample_ids: [u32; 7],
    scalar: Measurements,
    arrow: Measurements,
    neon: Measurements,
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    executable_blake3: String,
    cpu_class: String,
    os_release: String,
    target: String,
    dataset: String,
    workload: String,
    batch_size: usize,
    memory_limit_bytes: usize,
    output_batch_rows: usize,
    spill_enabled: bool,
    prefetch_batches: usize,
}

impl Binding {
    fn current(
        manifest: &DatasetManifest,
        case: &BenchmarkCase,
        batch_size: usize,
        config: &ExecutionConfig,
    ) -> Result<Self> {
        let executable = std::env::current_exe()?;
        let mut hasher = blake3::Hasher::new();
        copy(&mut File::open(executable)?, &mut hasher)?;
        let cpu_class = if cfg!(target_os = "macos") {
            mac_model()?
        } else {
            return Err(Error::Invalid(
                "no calibrated CPU class probe for this OS".into(),
            ));
        };
        let semantics = case.semantic_identity();
        let workload = serde_json::to_vec(&(
            semantics.schema_version,
            semantics.id,
            semantics.version,
            semantics.dataset,
            semantics.query,
            semantics.validation,
        ))?;
        Ok(Self {
            executable_blake3: hasher.finalize().to_hex().to_string(),
            cpu_class,
            os_release: probe("/usr/bin/uname", &["-r"])?,
            target: crate::environment::BuildInfo::default().target,
            dataset: manifest.identity()?,
            workload: blake3::hash(&workload).to_hex().to_string(),
            batch_size,
            memory_limit_bytes: config.memory_limit_bytes,
            output_batch_rows: config.output_batch_rows,
            spill_enabled: config.spill_enabled,
            prefetch_batches: config.prefetch_batches,
        })
    }
}

fn mac_model() -> Result<String> {
    // Match the public product model. The other ioreg fields are discarded.
    let output = std::process::Command::new("/usr/sbin/ioreg")
        .args(["-rd1", "-c", "IOPlatformExpertDevice"])
        .output()?;
    require(output.status.success(), "CPU class probe failed")?;
    let text = String::from_utf8_lossy(&output.stdout);
    let model = text
        .lines()
        .find_map(|line| {
            line.trim()
                .strip_prefix("\"model\" = <\"")?
                .strip_suffix("\">")
        })
        .ok_or_else(|| Error::Invalid("CPU class unavailable".into()))?;
    require(
        (1..=24).contains(&model.len())
            && model
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b','),
        "invalid CPU class",
    )?;
    Ok(model.to_owned())
}

fn probe(binary: &str, args: &[&str]) -> Result<String> {
    let output = std::process::Command::new(binary).args(args).output()?;
    let text = String::from_utf8_lossy(&output.stdout);
    let value = text.trim();
    require(
        output.status.success()
            && (1..=80).contains(&value.len())
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b" ._-".contains(&b)),
        "hardware or OS identity unavailable",
    )?;
    Ok(value.to_owned())
}

impl Profile {
    fn admit(
        &self,
        root: &Path,
        manifest: &DatasetManifest,
        case: &BenchmarkCase,
        batch_size: usize,
        config: &ExecutionConfig,
    ) -> std::result::Result<AutoSelection, ProfileRefusal> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ProfileRefusal::Incompatible)?
            .as_secs();
        let policy = self.validate(now)?;
        if self.binding
            != Binding::current(manifest, case, batch_size, config)
                .map_err(|_| ProfileRefusal::Incompatible)?
            || self.shape.rows != batch_size
            || self.shape.nulls != 0
            || !matches!(case.query, crate::spec::Query::ScanFilterProject { threshold } if threshold == self.shape.threshold)
            || u64::try_from(
                self.full_batches
                    .checked_mul(batch_size)
                    .ok_or(ProfileRefusal::Invalid)?,
            )
            .ok()
                != Some(manifest.row_count)
            || manifest
                .files
                .iter()
                .any(|file| !file.rows.is_multiple_of(batch_size as u64))
        {
            return Err(ProfileRefusal::Incompatible);
        }

        // The benchmark catalog trusts immutable files for the life of the plan.
        // Auto with a profile additionally verifies the actual bytes before use.
        let pinned = dataset::inspect(root, true)
            .and_then(|data| data.identity())
            .map_err(|_| ProfileRefusal::Incompatible)?;
        if pinned != self.binding.dataset {
            return Err(ProfileRefusal::Incompatible);
        }
        Ok(policy)
    }

    fn validate(&self, now: u64) -> std::result::Result<AutoSelection, ProfileRefusal> {
        if self.version != 1
            || self.mechanism != "arrow-59.3.1-u64-lt-neon-v1"
            || now
                .checked_sub(self.created_unix_s)
                .is_none_or(|age| age > MAX_AGE_SECONDS)
        {
            return Err(ProfileRefusal::Incompatible);
        }
        if !digest(&self.calibration_sha256) || self.sample_ids != [1, 2, 3, 4, 5, 6, 7] {
            return Err(ProfileRefusal::Invalid);
        }
        AutoSelection::from_verified_samples(
            self.shape,
            self.full_batches,
            &self.scalar,
            &self.arrow,
            &self.neon,
        )
        .map_err(|_| ProfileRefusal::Invalid)
    }
}

fn load(path: &Path, pin: &str) -> std::result::Result<Profile, ProfileRefusal> {
    // Trusted operator-owned regular file. Reject devices, pipes and unbounded reads.
    if !path
        .symlink_metadata()
        .is_ok_and(|m| m.is_file() && m.len() <= MAX_PROFILE_BYTES)
    {
        return Err(ProfileRefusal::Invalid);
    }
    let file = File::open(path).map_err(|_| ProfileRefusal::Invalid)?;
    let mut content = Vec::new();
    file.take(MAX_PROFILE_BYTES + 1)
        .read_to_end(&mut content)
        .map_err(|_| ProfileRefusal::Invalid)?;
    if content.len() as u64 > MAX_PROFILE_BYTES || blake3::hash(&content).to_hex().as_str() != pin {
        return Err(ProfileRefusal::Invalid);
    }
    serde_json::from_slice(&content).map_err(|_| ProfileRefusal::Invalid)
}

fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
