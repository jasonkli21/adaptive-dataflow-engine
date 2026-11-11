use ade_iterate::{
    CancellationToken, Command, Error, Guard, REQUEST_BYTES, Request, Result, operate,
};
use rustix::fs::{Mode, OFlags, fcntl_getfl, fcntl_setfl, open};
use serde::Serialize;
use std::{
    fs::File,
    io::{self, Read, Write},
    os::fd::AsFd,
    path::Path,
    time::Duration,
};

struct Nonblocking<F: AsFd> {
    fd: F,
    saved: OFlags,
}
impl<F: AsFd> Nonblocking<F> {
    fn new(fd: F) -> io::Result<Self> {
        let saved = fcntl_getfl(&fd)?;
        fcntl_setfl(&fd, saved | OFlags::NONBLOCK)?;
        Ok(Self { fd, saved })
    }

    fn send(&self, mut bytes: &[u8], guard: &Guard) -> Result<()> {
        while !bytes.is_empty() {
            guard.check()?;
            match rustix::io::write(&self.fd, bytes) {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero).into()),
                Ok(n) => bytes = &bytes[n..],
                Err(rustix::io::Errno::INTR) => {}
                Err(rustix::io::Errno::AGAIN) => std::thread::sleep(Duration::from_millis(2)),
                Err(e) => return Err(io::Error::from(e).into()),
            }
        }
        Ok(())
    }
}

impl<F: AsFd> Drop for Nonblocking<F> {
    fn drop(&mut self) {
        let _ = fcntl_setfl(&self.fd, self.saved);
    }
}

// The library manages frame budgets. Each call is one complete JSON line.
struct TerminalWriter<'a, F: AsFd> {
    fd: &'a Nonblocking<F>,
    cancel: CancellationToken,
    deadline: std::time::Instant,
}
impl<F: AsFd> Write for TerminalWriter<'_, F> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        // Use the same operation-wide cancellation and deadline in a blocked sink.
        while !bytes.is_empty() {
            self.cancel
                .check()
                .map_err(|_| io::Error::other("INCOMPLETE_CANCELLED"))?;
            if std::time::Instant::now() >= self.deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "INCOMPLETE_LIMIT: stalled stdout deadline",
                ));
            }
            match rustix::io::write(&self.fd.fd, bytes) {
                Ok(n) => return Ok(n),
                Err(rustix::io::Errno::INTR) => {}
                Err(rustix::io::Errno::AGAIN) => std::thread::sleep(Duration::from_millis(2)),
                Err(e) => return Err(e.into()),
            }
        }
        Ok(0)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encoded(value: &impl Serialize) -> Result<Vec<u8>> {
    let mut frame = serde_json::to_vec(value)?;
    frame.push(b'\n');
    Ok(frame)
}

fn run() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let command = match args.next().as_deref().and_then(|arg| arg.to_str()) {
        Some("explain") => Command::Explain,
        Some("run") => Command::Run,
        Some("resume") => Command::Resume,
        Some("read") => Command::Read,
        Some("verify") => Command::Verify,
        _ => {
            return Err(Error::Refused(
                "usage: ade-iterate explain|run|resume|read|verify REQUEST.json",
            ));
        }
    };
    let path = args.next().ok_or(Error::Refused("request path required"))?;
    if args.next().is_some() {
        return Err(Error::Refused("extra command arguments"));
    }
    let request_file = File::from(
        open(
            Path::new(&path),
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(io::Error::from)?,
    );
    if !request_file.metadata()?.is_file() {
        return Err(Error::Refused("regular request file required"));
    }
    let mut bytes = Vec::new();
    request_file
        .take((REQUEST_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    let request = Request::decode(&bytes)?;
    let cancel = CancellationToken::default();
    let signal = cancel.clone();
    ctrlc::set_handler(move || signal.cancel()).map_err(io::Error::other)?;
    let stdout = Nonblocking::new(io::stdout())?;
    let stderr = Nonblocking::new(io::stderr())?;
    let deadline = std::time::Instant::now()
        + Duration::from_millis(request.limits.deadline_ms.min(ade_iterate::MAX_DEADLINE_MS));
    let mut output = TerminalWriter {
        fd: &stdout,
        cancel: cancel.clone(),
        deadline,
    };
    let summary = operate(command, &request, cancel, &mut output, |event, guard| {
        stderr.send(&encoded(&event)?, guard)
    })?;
    match command {
        Command::Explain => output.write_all(&encoded(&(request.plan, summary))?)?,
        Command::Run | Command::Resume => output.write_all(&encoded(&summary)?)?,
        Command::Read | Command::Verify => {}
    }
    Ok(())
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            // Diagnostic writes are best effort, including on a stalled pipe.
            if let Ok(stderr) = Nonblocking::new(io::stderr()) {
                let _ = rustix::io::write(&stderr.fd, format!("{e}\n").as_bytes());
            }
            std::process::ExitCode::FAILURE
        }
    }
}
