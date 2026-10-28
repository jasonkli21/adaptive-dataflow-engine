mod support;
use std::{
    fs,
    process::{Command, Output},
};
fn invoke(root: &std::path::Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ade-batch"))
        .arg("--state")
        .arg(root)
        .args(args)
        .output()
        .unwrap()
}
fn result(output: &Output) -> serde_json::Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["schema_version"], 1);
    value["result"].clone()
}
#[test]
fn cli_roundtrip_structured_refusal_and_bounded_recipe_read() {
    let (dir, recipe) = support::fixture(false);
    let root = support::state_path(&dir);
    let caps = result(&invoke(&root, &["capabilities"]));
    assert_eq!(caps["native_execution"], true);
    assert!(!root.exists());
    result(&invoke(&root, &["init"]));
    assert!(!invoke(&root, &["init"]).status.success());
    let file = dir.path().join("recipe.json");
    fs::write(&file, serde_json::to_vec(&recipe).unwrap()).unwrap();
    let file = file.to_str().unwrap();
    let job = result(&invoke(
        &root,
        &["submit", "--key", "cli", "--recipe", file],
    ));
    let id = job["job"].as_str().unwrap();
    assert_eq!(
        result(&invoke(
            &root,
            &["submit", "--key", "cli", "--recipe", file]
        ))["job"],
        id
    );
    assert_eq!(result(&invoke(&root, &["status", id]))["state"], "PENDING");
    assert_eq!(
        result(&invoke(&root, &["list", "--limit", "1"]))[0]["job"],
        id
    );
    result(&invoke(&root, &["verify-sources", id]));
    assert_eq!(
        result(&invoke(&root, &["history", id]))
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        result(&invoke(&root, &["cancel", id]))["state"],
        "CANCELLED"
    );
    assert_eq!(
        result(&invoke(&root, &["cancel", id]))["state"],
        "CANCELLED"
    );
    assert_eq!(result(&invoke(&root, &["recover"])), serde_json::json!([]));
    result(&invoke(&root, &["cleanup"]));
    let large = dir.path().join("large.json");
    fs::write(&large, vec![b' '; 512 * 1024 + 1]).unwrap();
    let refused = invoke(
        &root,
        &[
            "submit",
            "--key",
            "large",
            "--recipe",
            large.to_str().unwrap(),
        ],
    );
    assert!(!refused.status.success());
    assert_eq!(refused.stdout, Vec::<u8>::new());
    assert_eq!(
        result(&invoke(&root, &["list"])).as_array().unwrap().len(),
        1
    );
    let invalid = dir.path().join("invalid.json");
    fs::write(&invalid, format!("{{\"{}\":0}}", "unknown".repeat(1000))).unwrap();
    let refused = invoke(
        &root,
        &[
            "submit",
            "--key",
            "invalid",
            "--recipe",
            invalid.to_str().unwrap(),
        ],
    );
    assert!(!refused.status.success());
    assert_eq!(refused.stdout, Vec::<u8>::new());
    let error: serde_json::Value = serde_json::from_slice(&refused.stderr).unwrap();
    assert!(error["error"].as_str().unwrap().len() <= 1024);
    assert!(!root.join("COMMITTED.json").exists());
}

#[test]
fn cli_list_labels_success_as_recorded_and_unverified() {
    let (dir, mut recipe) = support::fixture(false);
    let root = support::state_path(&dir);
    let mut store = ade_ops::Store::init(&root, ade_ops::Limits::default()).unwrap();
    let (_workers, endpoints) =
        support::managed_workers(&dir.path().canonicalize().unwrap(), store.id(), 2);
    recipe.workers = endpoints;
    let job = store.submit("list-verification", recipe).unwrap();
    drop(store);

    let succeeded = ade_ops::runner::run(&root, job.id).unwrap();
    assert_eq!(succeeded.state, ade_ops::JobState::Succeeded);
    let listed = result(&invoke(&root, &["list", "--limit", "1"]));
    assert_eq!(listed[0]["state"], "SUCCEEDED");
    assert_eq!(listed[0]["recorded_state"], "SUCCEEDED");
    assert_eq!(listed[0]["output_verification"], "NOT_CHECKED");

    let id = job.id.to_string();
    let status = result(&invoke(&root, &["status", &id]));
    assert_eq!(status["output_verification"], "VERIFIED");
    assert!(invoke(&root, &["verify-output", &id]).status.success());

    let marker = root
        .join("jobs")
        .join(job.id.to_string())
        .join("COMMITTED.json");
    let valid_marker = fs::read(&marker).unwrap();
    fs::remove_file(&marker).unwrap();
    let missing = result(&invoke(&root, &["list", "--limit", "1"]));
    assert_eq!(missing[0]["recorded_state"], "SUCCEEDED");
    assert_eq!(missing[0]["output_verification"], "NOT_CHECKED");
    assert!(!invoke(&root, &["status", &id]).status.success());
    assert!(!invoke(&root, &["verify-output", &id]).status.success());

    fs::write(&marker, b"corrupt marker").unwrap();
    let corrupt = result(&invoke(&root, &["list", "--limit", "1"]));
    assert_eq!(corrupt[0]["recorded_state"], "SUCCEEDED");
    assert_eq!(corrupt[0]["output_verification"], "NOT_CHECKED");
    assert!(!invoke(&root, &["status", &id]).status.success());
    assert!(!invoke(&root, &["verify-output", &id]).status.success());
    fs::write(marker, valid_marker).unwrap();
}
