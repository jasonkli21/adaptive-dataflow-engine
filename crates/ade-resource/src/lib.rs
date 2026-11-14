//! Bounded local scheduler above the fenced native operations runtime.
#![forbid(unsafe_code)]

mod contract;
mod native;
mod policy;
mod service;
mod store;

pub use service::{client, initialize, serve};

use std::time::{SystemTime, UNIX_EPOCH};

type Result<T> = std::result::Result<T, Error>;

/// Errors contain a stable code and never echo a request or a native path.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("{0}")]
pub struct Error(&'static str);

fn require(valid: bool, code: &'static str) -> Result<()> {
    if valid { Ok(()) } else { Err(Error(code)) }
}

fn now() -> Result<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|time| u64::try_from(time.as_millis()).ok())
        .ok_or(Error("CLOCK"))?;
    #[cfg(feature = "fault-injection")]
    if let Ok(back_ms) = std::env::var("ADE_RESOURCE_CLOCK_TEST_ROLLBACK_MS")
        && let Ok(back_ms) = back_ms.parse::<u64>()
    {
        return now.checked_sub(back_ms).ok_or(Error("CLOCK"));
    }
    Ok(now)
}

fn hash(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

fn encode(value: &impl serde::Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|_| Error("ENCODING"))
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|_| Error("ENCODING"))
}

// Only in a test build. A witness precedes the pause so the campaign can kill
// the real service and preserve the already synced manager/native boundary.
fn fault(stage: &str) {
    #[cfg(feature = "fault-injection")]
    if std::env::var("ADE_RESOURCE_FAULT").as_deref() == Ok(stage) {
        eprintln!("ADE_RESOURCE_REACHED {stage}");
        let resume = std::env::var_os("ADE_RESOURCE_FAULT_RESUME");
        let until = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while resume
            .as_ref()
            .is_none_or(|path| !std::path::Path::new(path).is_file())
            && std::time::Instant::now() < until
        {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
    let _ = stage;
}
