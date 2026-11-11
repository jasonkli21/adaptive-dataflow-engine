use ade_incremental::{Action, CancellationToken, Error, Request, Result, read, run};
use std::{
    io::{self, BufWriter, Write},
    os::fd::AsFd,
    path::Path,
    time::{Duration, Instant},
};

struct DeadlineOutput<W: Write + AsFd> {
    inner: W,
    original: rustix::fs::OFlags,
    token: CancellationToken,
    until: Instant,
}
impl<W: Write + AsFd> DeadlineOutput<W> {
    fn new(inner: W, token: CancellationToken, timeout_ms: u64) -> io::Result<Self> {
        let original = rustix::fs::fcntl_getfl(&inner)?;
        let mode = rustix::fs::fstat(&inner)?.st_mode;
        let kind = rustix::fs::FileType::from_raw_mode(mode);
        if matches!(
            kind,
            rustix::fs::FileType::Fifo | rustix::fs::FileType::Socket
        ) {
            rustix::fs::fcntl_setfl(&inner, original | rustix::fs::OFlags::NONBLOCK)?;
        }
        Ok(Self {
            inner,
            original,
            token,
            until: Instant::now() + Duration::from_millis(timeout_ms),
        })
    }

    fn ready(&self) -> io::Result<()> {
        self.token.check().map_err(io::Error::other)?;
        if Instant::now() >= self.until {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "maintained CLI output deadline",
            ));
        }
        Ok(())
    }
}
impl<W: Write + AsFd> Write for DeadlineOutput<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        loop {
            self.ready()?;
            match self.inner.write(bytes) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                result => return result,
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.ready()?;
        self.inner.flush()
    }
}
impl<W: Write + AsFd> Drop for DeadlineOutput<W> {
    fn drop(&mut self) {
        let _ = rustix::fs::fcntl_setfl(&self.inner, self.original);
    }
}

fn cli() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let command = args.next().ok_or(Error::Invalid(
        "usage: ade-maintain explain|bootstrap|apply|read|verify|resume REQUEST.json",
    ))?;
    let path = args.next().ok_or(Error::Invalid("missing request file"))?;
    if args.next().is_some() {
        return Err(Error::Invalid("unexpected extra argument"));
    }
    let request = Request::from_path(Path::new(&path))?;
    let token = CancellationToken::default();
    let interrupt = token.clone();
    ctrlc::set_handler(move || interrupt.cancel()).map_err(io::Error::other)?;
    let mut out = BufWriter::new(DeadlineOutput::new(
        io::stdout().lock(),
        token.clone(),
        request.limits.timeout_ms,
    )?);
    match command.to_str() {
        Some("apply" | "bootstrap" | "resume" | "explain") => {
            if command == "bootstrap" && request.epoch != 0 {
                return Err(Error::Invalid("bootstrap requires epoch zero"));
            }
            let action = if command == "explain" {
                Action::Explain
            } else {
                Action::Apply
            };
            let outcome = run(&request, action, token)?;
            serde_json::to_writer(&mut out, &outcome)?;
            writeln!(out)?;
        }
        Some("read" | "verify") => {
            let report = read(&request, token, |row| {
                if command == "read" {
                    serde_json::to_writer(&mut out, &row)?;
                    writeln!(out)?;
                }
                Ok(())
            })?;
            serde_json::to_writer(&mut out, &report)?;
            writeln!(out)?;
        }
        _ => return Err(Error::Invalid("unknown maintained command")),
    }
    out.flush()?;
    Ok(())
}

fn main() {
    if let Err(error) = cli() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
