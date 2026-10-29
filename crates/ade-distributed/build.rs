use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};
fn collect(root: &Path, files: &mut Vec<PathBuf>) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            collect(&path, files)?;
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            files.push(path);
        }
    }
    Ok(())
}
fn main() -> Result<(), Box<dyn Error>> {
    let package =
        PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").ok_or("manifest directory")?);
    let workspace = package
        .parent()
        .and_then(Path::parent)
        .ok_or("workspace root")?;
    let mut files = vec![
        workspace.join("Cargo.lock"),
        workspace.join("Cargo.toml"),
        package.join("build.rs"),
    ];
    for name in ["ade-distributed", "ade-exec", "ade-physical"] {
        let root = workspace.join("crates").join(name);
        println!("cargo:rerun-if-changed={}", root.join("src").display());
        files.push(root.join("Cargo.toml"));
        collect(&root.join("src"), &mut files)?;
    }
    files.sort();
    let mut hash = blake3::Hasher::new();
    hash.update(b"ade-managed-native-source-v1");
    hash.update(&[u8::from(
        std::env::var_os("CARGO_FEATURE_REMOTE_S3").is_some(),
    )]);
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
        "cargo:rustc-env=ADE_MANAGED_RUNTIME={}",
        hash.finalize().to_hex()
    );
    Ok(())
}
