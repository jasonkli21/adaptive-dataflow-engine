//! Strict semantic explain/probe; execution stays in native composition APIs.
use std::{fmt::Write as _, io::Read};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 1 && args.len() != 2 {
        return Err("ade-plan PLAN [CONTROLS]".into());
    }
    let mut bytes = Vec::new();
    let f = std::fs::File::open(&args[0])?;
    if !f.metadata()?.is_file() {
        return Err("regular plan".into());
    }
    f.take(ade_logical::MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    let p = ade_logical::LogicalPlanV1::from_json(&bytes)?;
    let controls = if args.len() == 2 {
        let mut bytes = Vec::new();
        std::fs::File::open(&args[1])?
            .take(4097)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 4096 {
            return Err("controls cap".into());
        }
        serde_json::from_slice(&bytes)?
    } else {
        ade_logical::Controls {
            static_choice: if p.properties()?.termination == ade_logical::Termination::Finite {
                ade_logical::Choice::Local
            } else {
                ade_logical::Choice::NativeKafka
            },
            ..ade_logical::Controls::default()
        }
    };
    let d = ade_logical::optimize(&p, &controls)?;
    let mut job = String::with_capacity(32);
    for byte in p.bound_job_bytes()? {
        write!(&mut job, "{byte:02x}")?;
    }
    println!(
        "{}",
        serde_json::json!({"plan":p,"decision":d,"bound_job_hex":job})
    );
    Ok(())
}
