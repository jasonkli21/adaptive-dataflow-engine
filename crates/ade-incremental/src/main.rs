use ade_incremental::{Action, CancellationToken, Error, Request, Result, read, read_view, run};
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
    byte_cap: Option<u64>,
    bytes_written: u64,
}
impl<W: Write + AsFd> DeadlineOutput<W> {
    fn new(
        inner: W,
        token: CancellationToken,
        timeout_ms: u64,
        byte_cap: Option<u64>,
    ) -> io::Result<Self> {
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
            byte_cap,
            bytes_written: 0,
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
            let bounded = match self.byte_cap {
                Some(cap) => {
                    let remaining = cap.saturating_sub(self.bytes_written);
                    if remaining == 0 {
                        return Err(io::Error::other(
                            "maintained CLI output byte quota exceeded",
                        ));
                    }
                    &bytes[..bytes
                        .len()
                        .min(usize::try_from(remaining).unwrap_or(usize::MAX))]
                }
                None => bytes,
            };
            match self.inner.write(bounded) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Ok(written) => {
                    self.bytes_written = self
                        .bytes_written
                        .checked_add(written as u64)
                        .ok_or_else(|| {
                            io::Error::other("maintained output byte counter overflow")
                        })?;
                    return Ok(written);
                }
                Err(error) => return Err(error),
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
        "usage: ade-maintain explain|bootstrap|apply|read|verify|resume REQUEST.json; read-view REQUEST.json VIEW",
    ))?;
    let path = args.next().ok_or(Error::Invalid("missing request file"))?;
    let requested_view = if command == "read-view" {
        Some(
            args.next()
                .ok_or(Error::Invalid("read-view requires a view name"))?,
        )
    } else {
        None
    };
    if args.next().is_some() {
        return Err(Error::Invalid("unexpected extra argument"));
    }
    let request = Request::from_path(Path::new(&path))?;
    let token = CancellationToken::default();
    let interrupt = token.clone();
    ctrlc::set_handler(move || interrupt.cancel()).map_err(io::Error::other)?;
    let output_cap = if matches!(command.to_str(), Some("read" | "read-view" | "verify")) {
        Some(request.limits.output_bytes)
    } else {
        None
    };
    let mut out = BufWriter::new(DeadlineOutput::new(
        io::stdout().lock(),
        token.clone(),
        request.limits.timeout_ms,
        output_cap,
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
        Some("read" | "read-view" | "verify") => {
            let view = requested_view
                .as_deref()
                .map(|v| v.to_str().ok_or(Error::Invalid("view name must be UTF-8")))
                .transpose()?;
            let emit = |row| {
                if command != "verify" {
                    serde_json::to_writer(&mut out, &row)?;
                    writeln!(out)?;
                }
                Ok(())
            };
            let report = if let Some(view) = view {
                read_view(&request, view, token, emit)?
            } else {
                read(&request, token, emit)?
            };
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
