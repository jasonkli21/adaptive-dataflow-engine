use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
};

const PROFILE_KEYS: &[&str] = &[
    "debug",
    "debug-assertions",
    "lto",
    "codegen-units",
    "opt-level",
    "target-cpu",
    "target-feature",
];

fn git(root: &Path, args: &[&str]) -> Option<String> {
    Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn emit_rerun(path: &Path) {
    println!("cargo:rerun-if-changed={}", path.display());
}

fn watch_tree(root: &Path) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    emit_rerun(root);
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            watch_tree(&path);
        } else {
            emit_rerun(&path);
        }
    }
}

fn repository_provenance(root: &Path) -> (Option<String>, Option<bool>) {
    let Some(top) = git(root, &["rev-parse", "--show-toplevel"]) else {
        return (None, None);
    };
    let Ok(top) = fs::canonicalize(top) else {
        return (None, None);
    };
    let Ok(root) = fs::canonicalize(root) else {
        return (None, None);
    };
    // A source archive nested inside some other checkout must not inherit that checkout's identity.
    if top != root {
        return (None, None);
    }

    for git_path in ["HEAD", "index", "packed-refs"] {
        if let Some(path) = git(root.as_path(), &["rev-parse", "--git-path", git_path]) {
            let path = PathBuf::from(path);
            let path = if path.is_absolute() {
                path
            } else {
                root.join(path)
            };
            emit_rerun(&path);
        }
    }
    if let Some(reference) = git(root.as_path(), &["symbolic-ref", "--quiet", "HEAD"])
        && let Some(path) = git(root.as_path(), &["rev-parse", "--git-path", &reference])
    {
        let path = PathBuf::from(path);
        let path = if path.is_absolute() {
            path
        } else {
            root.join(path)
        };
        emit_rerun(&path);
    }

    let commit = git(root.as_path(), &["rev-parse", "HEAD"]);
    let dirty = git(
        root.as_path(),
        &["status", "--porcelain", "--untracked-files=all"],
    )
    .map(|status| !status.is_empty());
    (commit, dirty)
}

fn profile_chain(
    profiles: &toml::value::Table,
    profile: &str,
    seen: &mut BTreeSet<String>,
    chain: &mut Vec<String>,
) {
    if !seen.insert(profile.to_owned()) {
        return;
    }
    if let Some(parent) = profiles
        .get(profile)
        .and_then(toml::Value::as_table)
        .and_then(|table| table.get("inherits"))
        .and_then(toml::Value::as_str)
    {
        profile_chain(profiles, parent, seen, chain);
    }
    chain.push(profile.to_owned());
}

fn setting_string(value: &toml::Value) -> Option<String> {
    value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_bool().map(|v| v.to_string()))
        .or_else(|| value.as_integer().map(|v| v.to_string()))
}

fn profile_settings(root: &Path, profile: &str) -> BTreeMap<String, String> {
    let Ok(source) = fs::read_to_string(root.join("Cargo.toml")) else {
        return BTreeMap::new();
    };
    let Ok(document) = toml::from_str::<toml::Table>(&source) else {
        return BTreeMap::new();
    };
    let Some(profiles) = document.get("profile").and_then(toml::Value::as_table) else {
        return BTreeMap::new();
    };

    let mut chain = Vec::new();
    profile_chain(profiles, profile, &mut BTreeSet::new(), &mut chain);
    let mut settings = BTreeMap::new();
    for current in chain {
        if let Some(table) = profiles.get(&current).and_then(toml::Value::as_table) {
            for key in PROFILE_KEYS {
                if let Some(value) = table.get(*key).and_then(setting_string) {
                    settings.insert((*key).into(), value);
                }
            }
        }
        let profile_env = current.to_ascii_uppercase().replace('-', "_");
        for key in PROFILE_KEYS {
            let env_key = format!(
                "CARGO_PROFILE_{profile_env}_{}",
                key.to_ascii_uppercase().replace('-', "_")
            );
            if let Ok(value) = std::env::var(&env_key) {
                settings.insert((*key).into(), value);
            }
            println!("cargo:rerun-if-env-changed={env_key}");
        }
    }
    if let Ok(opt_level) = std::env::var("OPT_LEVEL") {
        settings.insert("opt-level".into(), opt_level);
    }
    settings
}

fn add_codegen_flag(settings: &mut BTreeMap<String, String>, value: &str) {
    if let Some((key, value)) = value.split_once('=') {
        let key = match key {
            "debuginfo" => "debug",
            "debug-assertions" => "debug-assertions",
            "target-cpu" | "target-feature" | "lto" | "codegen-units" | "opt-level" => key,
            _ => return,
        };
        if value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "_+-.,".contains(c))
        {
            settings.insert(key.into(), value.trim().into());
        }
    }
}

fn emit_repository_metadata(root: &Path) {
    let (commit, dirty) = repository_provenance(root);
    println!(
        "cargo:rustc-env=ADE_GIT_COMMIT={}",
        commit.as_deref().unwrap_or("")
    );
    println!(
        "cargo:rustc-env=ADE_GIT_DIRTY={}",
        dirty.map_or_else(String::new, |value| value.to_string())
    );
    emit_rerun(root);
    if let Some(tracked) = git(root, &["ls-files", "-z"]) {
        for name in tracked.split('\0').filter(|name| !name.is_empty()) {
            let mut path = root.join(name);
            emit_rerun(&path);
            while let Some(parent) = path.parent() {
                if parent == root {
                    emit_rerun(parent);
                    break;
                }
                emit_rerun(parent);
                path = parent.to_path_buf();
            }
        }
    }
}

fn emit_build_metadata(root: &Path) {
    for key in [
        "PROFILE",
        "TARGET",
        "RUSTC",
        "OPT_LEVEL",
        "DEBUG",
        "CARGO_ENCODED_RUSTFLAGS",
    ] {
        println!("cargo:rerun-if-env-changed={key}");
    }
    let profile = std::env::var("PROFILE").unwrap_or_default();
    let target = std::env::var("TARGET").unwrap_or_default();
    let rustc = std::env::var("RUSTC")
        .ok()
        .and_then(|rustc| Command::new(rustc).arg("--version").output().ok())
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_default();

    let profile_key = if profile == "debug" { "dev" } else { &profile };
    let mut settings = profile_settings(root, profile_key);
    settings.entry("opt-level".into()).or_insert_with(|| {
        if matches!(profile.as_str(), "release" | "bench") {
            "3"
        } else {
            "0"
        }
        .into()
    });
    settings.entry("debug".into()).or_insert_with(|| {
        if matches!(profile.as_str(), "release") {
            "0"
        } else {
            "2"
        }
        .into()
    });
    settings
        .entry("lto".into())
        .or_insert_with(|| "false".into());
    let flags = std::env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();
    let mut tokens = flags.split('\u{1f}');
    while let Some(token) = tokens.next() {
        let option = if token == "-C" {
            tokens.next()
        } else {
            token.strip_prefix("-C")
        };
        if let Some(option) = option {
            add_codegen_flag(&mut settings, option);
        }
    }
    let mut effective = settings
        .into_iter()
        .map(|(key, value)| {
            let key = if key == "debug" { "debug-info" } else { &key };
            format!("{key}={value}")
        })
        .collect::<Vec<_>>();
    effective.sort();
    let effective = effective.join(";");

    for (key, value) in [
        ("ADE_PROFILE", profile),
        ("ADE_TARGET", target),
        ("ADE_RUSTC", rustc),
        ("ADE_BUILD_SETTINGS", effective),
    ] {
        println!("cargo:rustc-env={key}={value}");
    }
}

fn main() {
    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("Cargo sets CARGO_MANIFEST_DIR"));
    let root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("ade-bench manifest is two directories below the workspace root");

    for path in [
        root.join("Cargo.toml"),
        root.join("Cargo.lock"),
        manifest_dir.join("Cargo.toml"),
        manifest_dir.join("build.rs"),
    ] {
        emit_rerun(&path);
    }
    watch_tree(&manifest_dir.join("src"));
    emit_repository_metadata(root);
    emit_build_metadata(root);
}
