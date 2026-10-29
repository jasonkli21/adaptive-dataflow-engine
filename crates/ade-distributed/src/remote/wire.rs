//! One-shot worker protocol; legacy network/control frames are untouched.
use super::{Context, Counters, Job, MAX_JSON, TerminalRef, VERSION, json};
use crate::{Result, lineage::TransferManifest, metrics::NetworkMetrics, require};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    io::{Read, Write},
    net::SocketAddr,
};
pub const MAGIC: &[u8; 8] = b"ADEOBJ02";
pub const WIRE_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    Admission,
    Source,
    Credentials,
    Provider,
    Timeout,
    Cancelled,
    Limit,
    Integrity,
    Stage,
    ChildCrash,
}
impl FailureClass {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Admission => "admission",
            Self::Source => "source",
            Self::Credentials => "credentials",
            Self::Provider => "provider",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::Limit => "limit",
            Self::Integrity => "integrity",
            Self::Stage => "stage",
            Self::ChildCrash => "child_crash",
        }
    }
    pub const fn message(self) -> &'static str {
        match self {
            Self::Admission => "remote grant refused",
            Self::Source => "remote source verification failed",
            Self::Credentials => "remote credentials refused",
            Self::Provider => "remote provider request refused",
            Self::Timeout => "remote stage deadline",
            Self::Cancelled => "remote stage cancelled",
            Self::Limit => "remote resource limit reached",
            Self::Integrity => "remote data integrity verification failed",
            Self::Stage => "remote worker stage failed",
            Self::ChildCrash => "remote worker exited without a valid result",
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Boot {
    pub version: u32,
    pub instance: String,
    pub pid: u32,
    pub runtime: String,
    pub scratch: std::path::PathBuf,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Role {
    Produce {
        worker: usize,
    },
    Read {
        worker: usize,
        width: usize,
        epoch: u64,
        manifest_bytes: u64,
        manifest_hash: String,
        gather: SocketAddr,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pub version: u32,
    pub boot: String,
    pub context: Context,
    pub job: Job,
    pub role: Role,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Final {
    pub partition: usize,
    pub sent: NetworkMetrics,
    pub terminal: TransferManifest,
    pub input_rows: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Done {
    pub boot: Boot,
    pub context: Context,
    pub terminals: Vec<TerminalRef>,
    pub finals: Vec<Final>,
    pub counters: Counters,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Reply {
    Ready(Boot),
    Done(Box<Done>),
    Refused { class: FailureClass },
}
/// # Errors
/// Refuses oversized, foreign-version and truncated frames before JSON decoding.
pub fn read<T: DeserializeOwned>(input: &mut impl Read) -> Result<T> {
    let mut magic = [0; 8];
    input.read_exact(&mut magic)?;
    require(&magic == MAGIC, "remote protocol requires ADEOBJ02")?;
    let mut n = [0; 4];
    input.read_exact(&mut n)?;
    let n = u32::from_le_bytes(n) as usize;
    require(n <= MAX_JSON, "remote frame cap")?;
    let mut bytes = vec![0; n];
    input.read_exact(&mut bytes)?;
    Ok(serde_json::from_slice(&bytes)?)
}
/// # Errors
/// Refuses oversized frames or pipe write failures.
pub fn write(output: &mut impl Write, value: &impl Serialize) -> Result<()> {
    let bytes = json(value)?;
    output.write_all(MAGIC)?;
    output.write_all(
        &u32::try_from(bytes.len())
            .map_err(|_| crate::Error::Invalid("remote frame size".into()))?
            .to_le_bytes(),
    )?;
    output.write_all(&bytes)?;
    output.flush()?;
    Ok(())
}
impl Boot {
    /// # Errors
    /// Refuses foreign boot, process and feature-matched runtime identities.
    pub fn validate(&self) -> Result<()> {
        require(
            self.version == VERSION
                && super::uuid(&self.instance)
                && self.pid > 0
                && self.runtime == crate::managed::RUNTIME_ID
                && self.scratch.is_absolute()
                && self.scratch.to_string_lossy().len() <= 4096,
            "remote boot/runtime identity",
        )
    }
}
