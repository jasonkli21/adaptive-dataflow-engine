use ade_exec::{BatchMap, CancellationToken, Error, Result};
use arrow::{
    ipc::{
        MessageHeader, MetadataVersion, reader::StreamReader, root_as_message, writer::StreamWriter,
    },
    record_batch::RecordBatch,
};
use rustix::{
    fs::{Mode, OFlags, fcntl_getfl, fcntl_setfl, open},
    process::{Pid, Signal, WaitId, WaitIdOptions, kill_process_group, waitid},
};
use std::{
    fs::File,
    io::{self, Read, Write},
    os::{fd::AsFd, unix::process::CommandExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering::Relaxed},
    time::{Duration, Instant},
};

const FRAME: usize = 1024 * 1024;
const HEADER: usize = 88;
const ARTIFACT: usize = 16 * 1024;
const BOOTSTRAP: &str = include_str!("worker_bootstrap.py");
pub const ABI: u32 = 1;

pub struct Registry {
    python: PathBuf,
    script: PathBuf,
    interpreter: blake3::Hash,
    pub digest: String,
}

fn bad(reason: &'static str) -> Error {
    Error::BatchMap(reason)
}

fn read_script(path: &Path, digest: &str) -> Result<String> {
    let file = File::from(
        open(
            path,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(io::Error::from)?,
    );
    if !file.metadata()?.is_file() {
        return Err(bad("artifact type"));
    }
    let mut bytes = Vec::new();
    file.take((ARTIFACT + 1) as u64).read_to_end(&mut bytes)?;
    if bytes.len() > ARTIFACT || ade_physical::fingerprint(&bytes) != digest {
        return Err(bad("artifact drift"));
    }
    String::from_utf8(bytes).map_err(|_| bad("artifact encoding"))
}

fn interpreter(path: &Path) -> Result<blake3::Hash> {
    const LIMIT: u64 = 64 * 1024 * 1024;
    let file = File::from(
        open(
            path,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(io::Error::from)?,
    );
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > LIMIT {
        return Err(bad("interpreter type/size"));
    }
    let mut hash = blake3::Hasher::new();
    let read = io::copy(&mut file.take(LIMIT + 1), &mut hash)?;
    if read != metadata.len() {
        return Err(bad("interpreter changed"));
    }
    Ok(hash.finalize())
}

impl Registry {
    pub fn new(python: PathBuf, script: PathBuf, digest: String) -> Result<Self> {
        if !python.is_absolute() || !python.metadata()?.is_file() || !script.is_absolute() {
            return Err(bad("invalid deployment"));
        }
        let interpreter = interpreter(&python)?;
        read_script(&script, &digest)?;
        Ok(Self {
            python,
            script,
            interpreter,
            digest,
        })
    }

    pub fn bind(&self, identity: String, deadline: Instant) -> Result<Worker> {
        if interpreter(&self.python)? != self.interpreter {
            return Err(bad("interpreter drift"));
        }
        Ok(Worker {
            python: self.python.clone(),
            script: read_script(&self.script, &self.digest)?,
            identity,
            deadline,
            count: AtomicU64::new(0),
            input: AtomicU64::new(0),
            output: AtomicU64::new(0),
            peak_rss: AtomicU64::new(0),
        })
    }
}

pub struct Worker {
    python: PathBuf,
    script: String,
    identity: String,
    deadline: Instant,
    count: AtomicU64,
    input: AtomicU64,
    output: AtomicU64,
    peak_rss: AtomicU64,
}

impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker").finish_non_exhaustive()
    }
}

impl Worker {
    pub fn metrics(&self) -> serde_json::Value {
        serde_json::json!({
            "invocations": self.count.load(Relaxed),
            "input_bytes": self.input.load(Relaxed),
            "output_bytes": self.output.load(Relaxed),
            "worker_reported_peak_rss_bytes": self.peak_rss.load(Relaxed)
        })
    }
}

struct Capped(Vec<u8>);
impl Write for Capped {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if data.len() > FRAME - self.0.len() {
            return Err(io::Error::other("IPC frame cap"));
        }
        self.0.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl BatchMap for Worker {
    fn map(&self, batch: &RecordBatch, token: &CancellationToken) -> Result<RecordBatch> {
        token.check()?;
        if batch.get_array_memory_size() > FRAME || batch.num_rows() > super::query::BATCH_ROWS {
            return Err(bad("input cap"));
        }
        let mut wire = Capped(vec![0; HEADER]);
        let mut writer =
            StreamWriter::try_new(&mut wire, &batch.schema()).map_err(|_| bad("input IPC"))?;
        writer.write(batch).map_err(|_| bad("input IPC cap"))?;
        writer.finish().map_err(|_| bad("input IPC cap"))?;
        let seq = self.count.fetch_add(1, Relaxed);
        let wire_len = u32::try_from(wire.0.len() - HEADER).map_err(|_| bad("frame length"))?;
        let rows = u32::try_from(batch.num_rows()).map_err(|_| bad("row count"))?;
        wire.0[..8].copy_from_slice(b"ADEUDF01");
        wire.0[8..16].copy_from_slice(&seq.to_le_bytes());
        wire.0[16..20].copy_from_slice(&rows.to_le_bytes());
        wire.0[20..84].copy_from_slice(self.identity.as_bytes());
        wire.0[84..HEADER].copy_from_slice(&wire_len.to_le_bytes());
        self.input.fetch_add(wire.0.len() as u64, Relaxed);
        let response = self.exchange(&wire.0, token)?;
        self.output.fetch_add(response.len() as u64, Relaxed);
        let header = response
            .get(..HEADER + 8)
            .ok_or_else(|| bad("truncated header"))?;
        let payload = &response[HEADER + 8..];
        if header[..84] != wire.0[..84]
            || usize::try_from(u32::from_le_bytes(header[84..88].try_into().unwrap())).ok()
                != Some(payload.len())
        {
            return Err(bad("frame identity/length"));
        }
        self.peak_rss.fetch_max(
            u64::from_le_bytes(header[88..96].try_into().unwrap()),
            Relaxed,
        );
        let output = decode(payload, batch)?;
        token.check()?;
        Ok(output)
    }
}

fn nonblocking(pipe: impl AsFd) -> io::Result<()> {
    let flags = fcntl_getfl(&pipe)?;
    fcntl_setfl(pipe, flags | OFlags::NONBLOCK)?;
    Ok(())
}

struct Running(Child, Option<Pid>);
impl Drop for Running {
    fn drop(&mut self) {
        if let Some(group) = self.1 {
            let _ = kill_process_group(group, Signal::KILL);
            let _ = self.0.wait();
        }
    }
}

fn read_available(pipe: &mut impl Read, buf: &mut [u8]) -> io::Result<Option<usize>> {
    match pipe.read(buf) {
        Ok(n) => Ok(Some(n)),
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
            ) =>
        {
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

impl Worker {
    fn exchange(&self, input: &[u8], token: &CancellationToken) -> Result<Vec<u8>> {
        token.check()?;
        if Instant::now() >= self.deadline {
            return Err(Error::Deadline("UDF"));
        }
        let child = Command::new(&self.python)
            .args([
                "-I",
                "-B",
                "-c",
                BOOTSTRAP,
                &self.script,
                &std::process::id().to_string(),
            ])
            .env_clear()
            .env("OPENBLAS_NUM_THREADS", "1")
            .env("OMP_NUM_THREADS", "1")
            .current_dir("/")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .spawn()?;
        let group = Pid::from_child(&child);
        let mut child = Running(child, Some(group));
        let mut stdin = child.0.stdin.take().unwrap();
        let mut stdout = child.0.stdout.take().unwrap();
        let mut stderr = child.0.stderr.take().unwrap();
        nonblocking(&stdin)?;
        nonblocking(&stdout)?;
        nonblocking(&stderr)?;

        // Keep stdin open after the complete frame. A worker watchdog also checks
        // its original parent PID, including after abrupt gateway termination.
        let mut pending = input;
        let mut response = Vec::new();
        let mut buffer = [0u8; 8192];
        loop {
            token.check()?;
            if Instant::now() >= self.deadline {
                return Err(Error::Deadline("UDF"));
            }
            if !pending.is_empty() {
                match stdin.write(pending) {
                    Ok(n) => pending = &pending[n..],
                    Err(e)
                        if matches!(
                            e.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                        ) => {}
                    Err(_) => return Err(bad("input pipe closed")),
                }
            }
            if read_available(&mut stderr, &mut buffer)?.is_some_and(|n| n != 0) {
                return Err(bad("unexpected stderr"));
            }
            let eof = match read_available(&mut stdout, &mut buffer)? {
                Some(0) => true,
                Some(n) => {
                    if response.len() + n > FRAME {
                        return Err(bad("encoded cap"));
                    }
                    response.extend_from_slice(&buffer[..n]);
                    continue;
                }
                None => false,
            };
            // Wait for pipe EOF before reaping so normal cleanup cannot signal
            // a reused process group ID. Descendants that hold a pipe hit the deadline.
            if eof
                && waitid(
                    WaitId::Pid(group),
                    WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT | WaitIdOptions::EXITED,
                )
                .map_err(io::Error::from)?
                .is_some()
            {
                let _ = kill_process_group(group, Signal::KILL);
                let status = child.0.wait()?;
                child.1 = None;
                if !status.success()
                    || !pending.is_empty()
                    || read_available(&mut stderr, &mut buffer)?.is_some_and(|n| n != 0)
                {
                    return Err(bad("worker failure"));
                }
                return Ok(response);
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}

fn message(input: &mut &[u8], expected: MessageHeader) -> Result<(usize, usize)> {
    let bad_ipc = || bad("invalid IPC envelope");
    let header = input.get(..8).ok_or_else(bad_ipc)?;
    if header[..4] != [255; 4] {
        return Err(bad_ipc());
    }
    let len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    let rest = input.get(8..).ok_or_else(bad_ipc)?;
    let metadata = rest.get(..len).ok_or_else(bad_ipc)?;
    let msg = root_as_message(metadata).map_err(|_| bad_ipc())?;
    let body = usize::try_from(msg.bodyLength()).map_err(|_| bad_ipc())?;
    if msg.version() != MetadataVersion::V5 || msg.header_type() != expected {
        return Err(bad_ipc());
    }
    *input = rest
        .get(len.checked_add(body).ok_or_else(bad_ipc)?..)
        .ok_or_else(bad_ipc)?;
    Ok((len, body))
}

fn decode(bytes: &[u8], batch: &RecordBatch) -> Result<RecordBatch> {
    let mut remaining = bytes;
    let (_, schema_body) = message(&mut remaining, MessageHeader::Schema)?;
    let record = remaining;
    let (meta, body) = message(&mut remaining, MessageHeader::RecordBatch)?;
    if schema_body != 0 || remaining != [255, 255, 255, 255, 0, 0, 0, 0] {
        return Err(bad("extra IPC data"));
    }
    let record = root_as_message(&record[8..8 + meta])
        .ok()
        .and_then(|m| m.header_as_record_batch())
        .ok_or_else(|| bad("record batch metadata"))?;
    let nodes = record.nodes().ok_or_else(|| bad("field nodes"))?;
    let buffers = record.buffers().ok_or_else(|| bad("buffers"))?;
    let rows = i64::try_from(batch.num_rows()).map_err(|_| bad("row count"))?;
    if record.compression().is_some()
        || record.length() != rows
        || nodes.len() != batch.num_columns()
        || nodes
            .iter()
            .any(|n| n.length() != rows || n.null_count() != 0)
        || buffers.len() != 2 * batch.num_columns() + 1
    {
        return Err(bad("record shape/compression"));
    }
    let mut previous_end = 0;
    for buffer in buffers {
        let offset = usize::try_from(buffer.offset()).map_err(|_| bad("buffer offset"))?;
        let length = usize::try_from(buffer.length()).map_err(|_| bad("buffer length"))?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| bad("buffer range"))?;
        if offset < previous_end || end > body {
            return Err(bad("buffer range/decoded cap"));
        }
        previous_end = end;
    }
    let mut reader = StreamReader::try_new(bytes, None).map_err(|_| bad("invalid Arrow schema"))?;
    if reader.schema() != batch.schema() {
        return Err(bad("output schema"));
    }
    let output = reader
        .next()
        .ok_or_else(|| bad("missing batch"))?
        .map_err(|_| bad("invalid Arrow arrays"))?;
    let secondary = batch.schema().index_of("secondary")?;
    if output.get_array_memory_size() > FRAME
        || output.num_rows() != batch.num_rows()
        || output
            .columns()
            .iter()
            .zip(batch.columns())
            .enumerate()
            .any(|(i, (new, old))| new.null_count() != 0 || (i != secondary && new != old))
    {
        return Err(bad("output rows/order/values/cap"));
    }
    Ok(output)
}
