use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

fn collect_rust_sources(root: &Path, files: &mut Vec<PathBuf>) -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed={}", root.display());
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_rust_sources(&path, files)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let package = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").ok_or("manifest")?);
    let workspace = package.parent().and_then(Path::parent).ok_or("workspace")?;
    let frontend = workspace.join("crates/ade-frontend");
    let mut files = vec![
        workspace.join("Cargo.lock"),
        workspace.join("Cargo.toml"),
        package.join("Cargo.toml"),
        package.join("build.rs"),
        frontend.join("Cargo.toml"),
    ];
    collect_rust_sources(&package.join("src"), &mut files)?;
    collect_rust_sources(&frontend.join("src"), &mut files)?;
    files.sort();

    let mut hash = blake3::Hasher::new();
    hash.update(b"ade-ops-coordinator-compatibility-v1");
    hash.update(b"explicit-semantic-epoch-1");
    for file in files {
        println!("cargo:rerun-if-changed={}", file.display());
        let name = file.strip_prefix(workspace)?.to_string_lossy();
        let bytes = fs::read(&file)?;
        hash.update(&(name.len() as u64).to_le_bytes());
        hash.update(name.as_bytes());
        hash.update(&(bytes.len() as u64).to_le_bytes());
        hash.update(&bytes);
    }
    println!(
        "cargo:rustc-env=ADE_OPS_COMPATIBILITY={}",
        hash.finalize().to_hex()
    );
    Ok(())
}
