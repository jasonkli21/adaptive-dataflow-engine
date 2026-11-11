use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};

/// Cap raw bytes before the PostgreSQL driver buffers a complete message.
pub(super) struct Wire {
    socket: TcpStream,
    received_left: usize,
    sent_left: usize,
    exhausted: Arc<AtomicBool>,
}

impl Wire {
    pub(super) fn new(socket: TcpStream, exhausted: Arc<AtomicBool>) -> Self {
        Self {
            socket,
            received_left: 24 * 1024 * 1024,
            sent_left: 1024 * 1024,
            exhausted,
        }
    }

    fn exhausted(&self) -> io::Error {
        self.exhausted.store(true, Ordering::Relaxed);
        io::Error::other("Postgres wire byte cap")
    }
}

impl AsyncRead for Wire {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.received_left == 0 {
            return Poll::Ready(Err(self.exhausted()));
        }
        let mut scratch = [0; 8192];
        let limit = output
            .remaining()
            .min(self.received_left)
            .min(scratch.len());
        let mut read = ReadBuf::new(&mut scratch[..limit]);
        std::task::ready!(Pin::new(&mut self.socket).poll_read(cx, &mut read))?;
        let bytes = read.filled();
        self.received_left -= bytes.len();
        output.put_slice(bytes);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Wire {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if bytes.len() > self.sent_left {
            return Poll::Ready(Err(self.exhausted()));
        }
        let written = std::task::ready!(Pin::new(&mut self.socket).poll_write(cx, bytes))?;
        self.sent_left -= written;
        Poll::Ready(Ok(written))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}
