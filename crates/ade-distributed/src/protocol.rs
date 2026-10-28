//! Bounded, private JSON control messages; physical execution stays in worker.rs.
use crate::{
    Error, Result, TaskConfig, WorkerEndpoint,
    ids::{AttemptId, JobId, TaskId},
    metrics::TaskMetrics,
    require,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    time::{Duration, Instant},
};
pub const MAX_CONTROL_BYTES: usize = 1024 * 1024;
pub(crate) const MAGIC: &[u8; 8] = b"ADETCP12";
/// Wake a blocking accept during shutdown, including wildcard-bound listeners.
pub(crate) fn wake_listener(mut address: SocketAddr) {
    if address.ip().is_unspecified() {
        address.set_ip(if address.is_ipv4() {
            std::net::Ipv4Addr::LOCALHOST.into()
        } else {
            std::net::Ipv6Addr::LOCALHOST.into()
        });
    }
    let _ = TcpStream::connect_timeout(&address, Duration::from_millis(100));
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope<T> {
    pub request_id: u64,
    pub body: T,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobSpec {
    pub schedule: Option<crate::scheduler::Spec>,
    pub job: JobId,
    /// Unique execution scope for shared object-store keys; excluded from logical transfer IDs.
    pub execution_namespace: String,
    pub workers: Vec<WorkerEndpoint>,
    pub config: TaskConfig,
    pub snapshot: Option<crate::lineage::SourceSnapshot>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(
    clippy::large_enum_variant,
    reason = "One bounded control request per task; no per-segment messages."
)]
pub enum Request {
    Ping,
    PrepareJoin(crate::join::Spec),
    ProbeJoin {
        job: JobId,
    },
    CommitJoin {
        job: JobId,
        decision: crate::join_policy::Decision,
    },
    RunJoinProducer {
        job: JobId,
    },
    RunJoinFinal {
        job: JobId,
        gather: SocketAddr,
    },
    PrepareJob(JobSpec),
    Probe {
        timeout_ms: u64,
        job: JobId,
        files: Vec<PathBuf>,
    },
    CommitLayout {
        job: JobId,
        layout: crate::topology::Layout,
    },
    VerifyInactive {
        job: JobId,
    },
    RunProducerTask {
        job: JobId,
        task: TaskId,
        attempt: AttemptId,
        files: Vec<PathBuf>,
    },
    RunFinalTask {
        job: JobId,
        task: TaskId,
        attempt: AttemptId,
        gather: SocketAddr,
    },
    RecoverFinal {
        plan: crate::recovery::RecoveryPlan,
        gather: SocketAddr,
    },
    #[cfg(feature = "research-hooks")]
    ReleaseFinal {
        job: JobId,
    },
    CleanupMaterializations {
        job: JobId,
        execution_namespace: String,
    },
    CancelJob {
        job: JobId,
    },
    Shutdown,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(
    clippy::large_enum_variant,
    reason = "TaskMetrics are returned as one bounded diagnostic control message."
)]
pub enum Reply {
    Pong(WorkerEndpoint),
    JoinProbe(Vec<crate::join_policy::Observation>),
    JoinTask(crate::join::TaskMetrics),
    Ready,
    Probe(crate::topology::Observation),
    Task(TaskMetrics),
    Recovered {
        final_metrics: TaskMetrics,
        manifests: Vec<crate::lineage::TransferManifest>,
        work: crate::recovery::RecoveryMetrics,
    },
    Error {
        code: String,
        message: String,
    },
}
/// # Errors
/// Rejects oversized encoding or write failures.
pub fn write_message(stream: &mut impl Write, message: &impl Serialize) -> Result<()> {
    // The bounded writer rejects growth before allocation, including caller-created large requests.
    let mut bytes = crate::exchange::BoundedBuffer::new(MAX_CONTROL_BYTES);
    serde_json::to_writer(&mut bytes, message)?;
    let size = u32::try_from(bytes.0.len())
        .map_err(|_| Error::Invalid("control length overflow".into()))?
        .to_le_bytes();
    write_all_vectored(
        stream,
        &mut [
            std::io::IoSlice::new(MAGIC),
            std::io::IoSlice::new(&size),
            std::io::IoSlice::new(&bytes.0),
        ],
    )?;
    Ok(())
}
pub(crate) fn write_all_vectored(
    stream: &mut impl Write,
    mut buffers: &mut [std::io::IoSlice<'_>],
) -> std::io::Result<()> {
    std::io::IoSlice::advance_slices(&mut buffers, 0);
    while !buffers.is_empty() {
        match stream.write_vectored(buffers) {
            Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
            Ok(n) => std::io::IoSlice::advance_slices(&mut buffers, n),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
/// # Errors
/// Rejects bad version, oversized, truncated or invalid messages.
pub fn read_message<T: DeserializeOwned>(stream: &mut impl Read) -> Result<T> {
    let mut magic = [0; 8];
    stream.read_exact(&mut magic)?;
    require(&magic == MAGIC, "protocol magic/version mismatch")?;
    let mut len = [0; 4];
    stream.read_exact(&mut len)?;
    let len = u32::from_le_bytes(len) as usize;
    require(len <= MAX_CONTROL_BYTES, "control frame too large")?;
    let mut bytes = vec![0; len];
    stream.read_exact(&mut bytes)?;
    Ok(serde_json::from_slice(&bytes)?)
}
/// # Errors
/// Returns connection and socket configuration errors.
pub fn connect(address: SocketAddr, timeout: Duration) -> Result<TcpStream> {
    let timeout = timeout.min(crate::IO_TIMEOUT);
    require(!timeout.is_zero(), "positive connect timeout required")?;
    let socket = TcpStream::connect_timeout(&address, timeout)?;
    socket.set_read_timeout(Some(timeout))?;
    socket.set_write_timeout(Some(timeout))?;
    socket.set_nodelay(true)?;
    Ok(socket)
}
/// # Errors
/// Returns transport/deadline, response identity or structured remote errors.
pub fn rpc(endpoint: &WorkerEndpoint, request: Request, timeout: Duration) -> Result<Reply> {
    rpc_until(endpoint, request, Instant::now() + timeout)
}
/// Run one RPC against a deadline shared with its coordinator stage.
pub(crate) fn rpc_until(
    endpoint: &WorkerEndpoint,
    request: Request,
    deadline: Instant,
) -> Result<Reply> {
    if endpoint.managed.is_some() {
        return crate::managed::call(endpoint, request, deadline);
    }
    let mut socket = connect(endpoint.control, remaining_until(deadline)?)?;
    let id = 1;
    let mut socket = DeadlineStream {
        socket: &mut socket,
        deadline,
    };
    write_message(
        &mut socket,
        &Envelope {
            request_id: id,
            body: request,
        },
    )?;
    let response: Envelope<Reply> = read_message(&mut socket)?;
    require(response.request_id == id, "response request ID mismatch")?;
    match response.body {
        Reply::Error { code, message } => Err(Error::Remote { code, message }),
        body => Ok(body),
    }
}
fn remaining_until(deadline: Instant) -> std::io::Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "RPC deadline exceeded",
        ))
    } else {
        Ok(remaining)
    }
}
// Enforce a total RPC deadline even if a peer dribbles bytes within inactivity timeouts.
pub(crate) struct DeadlineStream<'a> {
    socket: &'a mut TcpStream,
    deadline: Instant,
}
impl<'a> DeadlineStream<'a> {
    pub(crate) fn new(socket: &'a mut TcpStream, timeout: Duration) -> Self {
        Self {
            socket,
            deadline: Instant::now() + timeout,
        }
    }
    fn remaining(&self) -> std::io::Result<Duration> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "I/O deadline exceeded",
            ))
        } else {
            Ok(remaining)
        }
    }
}
impl Read for DeadlineStream<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        self.socket.set_read_timeout(Some(self.remaining()?))?;
        self.socket.read(bytes)
    }
}
impl Write for DeadlineStream<'_> {
    fn write_vectored(&mut self, bytes: &[std::io::IoSlice<'_>]) -> std::io::Result<usize> {
        self.socket.set_write_timeout(Some(self.remaining()?))?;
        self.socket.write_vectored(bytes)
    }
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.socket.set_write_timeout(Some(self.remaining()?))?;
        self.socket.write(bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.socket.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn vectored_write_handles_partial_fallback_interruption_and_zero() {
        #[derive(Default)]
        struct Partial {
            bytes: Vec<u8>,
            interrupted: bool,
            zero: bool,
        }
        impl Write for Partial {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if !self.interrupted {
                    self.interrupted = true;
                    return Err(std::io::ErrorKind::Interrupted.into());
                }
                if self.zero {
                    return Ok(0);
                }
                let n = bytes.len().min(2);
                self.bytes.extend_from_slice(&bytes[..n]);
                Ok(n)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut output = Partial::default();
        write_all_vectored(
            &mut output,
            &mut [
                std::io::IoSlice::new(b""),
                std::io::IoSlice::new(b"length"),
                std::io::IoSlice::new(b"sequence"),
                std::io::IoSlice::new(b"data"),
                std::io::IoSlice::new(b""),
            ],
        )
        .unwrap();
        assert_eq!(output.bytes, b"lengthsequencedata");
        write_all_vectored(&mut output, &mut [std::io::IoSlice::new(b"")]).unwrap();
        output.zero = true;
        assert_eq!(
            write_all_vectored(&mut output, &mut [std::io::IoSlice::new(b"x")])
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::WriteZero
        );
    }

    #[test]
    fn vectored_write_retains_total_deadline() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut socket = connect(listener.local_addr().unwrap(), Duration::from_secs(1)).unwrap();
        let (_peer, _) = listener.accept().unwrap();
        let mut bounded = DeadlineStream::new(&mut socket, Duration::from_millis(1));
        std::thread::sleep(Duration::from_millis(3));
        assert_eq!(
            write_all_vectored(&mut bounded, &mut [std::io::IoSlice::new(b"x")])
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::TimedOut
        );
    }

    #[test]
    fn total_deadline_rejects_a_continuously_progressing_peer() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let peer = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            for _ in 0..64 {
                if socket.write_all(&[1]).is_err() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        });
        let mut socket = connect(address, Duration::from_secs(1)).unwrap();
        let mut bounded = DeadlineStream::new(&mut socket, Duration::from_secs(1));
        let error = bounded.read_exact(&mut [0; 64]).unwrap_err();
        assert!(matches!(
            error.kind(),
            std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
        ));
        drop(socket);
        peer.join().unwrap();
    }

    #[test]
    fn rpc_timeout_bounds_a_nonresponsive_peer_below_io_timeout() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let peer = std::thread::spawn(move || {
            let (_socket, _) = listener.accept().unwrap();
            std::thread::sleep(Duration::from_secs(1));
        });
        let endpoint = WorkerEndpoint {
            managed: None,
            id: crate::ids::WorkerId(0),
            control: address,
            exchange: address,
        };
        let timeout = Duration::from_millis(100);
        let start = Instant::now();
        assert!(rpc(&endpoint, Request::Ping, timeout).is_err());
        assert!(start.elapsed() < Duration::from_millis(750));
        peer.join().unwrap();
    }
    #[test]
    fn rejects_version_size_and_truncation() {
        let mut bytes = Vec::new();
        write_message(
            &mut bytes,
            &Envelope {
                request_id: 7,
                body: Request::Ping,
            },
        )
        .unwrap();
        assert!(read_message::<Envelope<Request>>(&mut &bytes[..bytes.len() - 1]).is_err());
        // Reject the prior source-observation shape before decoding its payload.
        bytes[7] = b'7';
        assert!(read_message::<Envelope<Request>>(&mut &bytes[..]).is_err());
        let mut large = MAGIC.to_vec();
        large.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(read_message::<Envelope<Request>>(&mut &large[..]).is_err());
    }
}
