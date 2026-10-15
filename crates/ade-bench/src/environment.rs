//! Best-effort metadata restricted to non-sensitive hardware/source/build facts.
use serde::{Deserialize, Serialize};
use std::process::Command;
fn command(program: &str, args: &[&str]) -> Option<String> {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .filter(|s| !s.is_empty())
}
/// Host identity deliberately excludes hostname, usernames, paths and network identifiers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentInfo {
    pub os: String,
    pub arch: String,
    pub kernel: Option<String>,
    pub cpu_model: Option<String>,
    pub physical_cores: Option<u64>,
    pub logical_cores: Option<usize>,
    pub memory_bytes: Option<u64>,
}
impl EnvironmentInfo {
    #[must_use]
    pub fn collect() -> Self {
        let sysctl = |key| command("sysctl", &["-n", key]);
        let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").ok();
        let memory = std::fs::read_to_string("/proc/meminfo").ok().and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("MemTotal:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|n| n.parse::<u64>().ok())
                .map(|n| n * 1024)
        });
        Self {
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            kernel: command("uname", &["-r"]),
            cpu_model: sysctl("machdep.cpu.brand_string").or_else(|| {
                cpuinfo.and_then(|s| {
                    s.lines()
                        .find(|l| l.starts_with("model name"))
                        .and_then(|l| l.split_once(':'))
                        .map(|(_, v)| v.trim().into())
                })
            }),
            physical_cores: sysctl("hw.physicalcpu").and_then(|s| s.parse().ok()),
            logical_cores: std::thread::available_parallelism()
                .ok()
                .map(std::num::NonZero::get),
            memory_bytes: sysctl("hw.memsize").and_then(|s| s.parse().ok()).or(memory),
        }
    }
}
/// Compiler settings baked into the executable rather than guessed from runtime environment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BuildInfo {
    pub harness_version: String,
    pub rustc: String,
    pub profile: String,
    pub target: String,
    pub debug_assertions: bool,
    pub allocator: String,
    pub codegen_overrides: Vec<String>,
}
impl Default for BuildInfo {
    fn default() -> Self {
        let mut codegen_overrides = env!("ADE_BUILD_SETTINGS")
            .split(';')
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if !codegen_overrides
            .iter()
            .any(|setting| setting.starts_with("debug-assertions="))
        {
            codegen_overrides.push(format!("debug-assertions={}", cfg!(debug_assertions)));
        }
        codegen_overrides.sort();
        Self {
            harness_version: env!("CARGO_PKG_VERSION").into(),
            rustc: env!("ADE_RUSTC").into(),
            profile: env!("ADE_PROFILE").into(),
            target: env!("ADE_TARGET").into(),
            debug_assertions: cfg!(debug_assertions),
            allocator: "system-default".into(),
            codegen_overrides,
        }
    }
}
/// Null Git fields explicitly represent a source archive/non-Git context.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RepositoryInfo {
    pub git_commit: Option<String>,
    pub git_dirty: Option<bool>,
}
impl RepositoryInfo {
    #[must_use]
    pub fn collect() -> Self {
        Self::from_build_values(option_env!("ADE_GIT_COMMIT"), option_env!("ADE_GIT_DIRTY"))
    }

    fn from_build_values(commit: Option<&str>, dirty: Option<&str>) -> Self {
        Self {
            git_commit: commit.filter(|value| !value.is_empty()).map(str::to_owned),
            git_dirty: dirty.and_then(|value| match value {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            }),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn metadata_serializes() {
        let value = serde_json::to_value(EnvironmentInfo::collect()).unwrap();
        assert!(value.get("hostname").is_none());
        assert!(value.get("os").is_some());
        serde_json::to_string(&RepositoryInfo::collect()).unwrap();
    }

    #[test]
    fn build_provenance_stays_unknown_for_source_archives() {
        let repository = RepositoryInfo::from_build_values(None, None);
        assert_eq!(repository.git_commit, None);
        assert_eq!(repository.git_dirty, None);
    }

    #[test]
    fn repository_info_uses_compiled_values_not_runtime_git_context() {
        let repository = RepositoryInfo::collect();
        assert_eq!(
            repository.git_commit.as_deref(),
            option_env!("ADE_GIT_COMMIT").filter(|value| !value.is_empty())
        );
        assert_eq!(
            repository.git_dirty,
            option_env!("ADE_GIT_DIRTY").and_then(|value| match value {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            })
        );
    }

    #[test]
    fn build_metadata_includes_effective_known_settings() {
        let build = BuildInfo::default();
        assert!(
            build
                .codegen_overrides
                .iter()
                .any(|s| s.starts_with("opt-level="))
        );
        assert!(
            build
                .codegen_overrides
                .iter()
                .any(|s| s.starts_with("debug-assertions="))
        );
        if matches!(build.profile.as_str(), "release" | "bench") {
            assert!(build.codegen_overrides.iter().any(|s| s == "lto=thin"));
            assert!(
                build
                    .codegen_overrides
                    .iter()
                    .any(|s| s == "codegen-units=1")
            );
            assert!(build.codegen_overrides.iter().any(|s| s == "debug-info=1"));
        }
    }
}
