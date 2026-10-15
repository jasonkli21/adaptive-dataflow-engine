use std::process::Command;

fn main() {
    for (key, value) in [
        ("ADE_PROFILE", std::env::var("PROFILE").unwrap_or_default()),
        ("ADE_TARGET", std::env::var("TARGET").unwrap_or_default()),
        (
            "ADE_RUSTC",
            Command::new(std::env::var("RUSTC").unwrap_or_else(|_| "rustc".into()))
                .arg("--version")
                .output()
                .ok()
                .filter(|o| o.status.success())
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
                .unwrap_or_default(),
        ),
    ] {
        println!("cargo:rustc-env={key}={value}");
    }
    // Retain only performance-relevant codegen flags, never linker paths or arbitrary env.
    let flags = std::env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();
    let mut options = Vec::new();
    let mut tokens = flags.split('\u{1f}');
    while let Some(token) = tokens.next() {
        let option = if token == "-C" {
            tokens.next()
        } else {
            token.strip_prefix("-C")
        };
        if let Some((key, value)) = option.and_then(|o| o.split_once('='))
            && [
                "target-cpu",
                "target-feature",
                "lto",
                "codegen-units",
                "opt-level",
                "debug-assertions",
            ]
            .contains(&key)
            && value
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "_+-.,".contains(c))
        {
            options.push(format!("{key}={value}"));
        }
    }
    println!(
        "cargo:rustc-env=ADE_CODEGEN_OVERRIDES={}",
        options.join(";")
    );
    println!("cargo:rerun-if-env-changed=CARGO_ENCODED_RUSTFLAGS");
    println!("cargo:rerun-if-changed=build.rs");
}
