//! Incremental private frames: a stalled peer cannot occupy the ledger service.
use super::{Error, Instant, Result, protocol, require};
use std::{
    io::{Read, Write},
    net::TcpStream,
    time::Duration,
};

pub(super) struct Connection {
    socket: TcpStream,
    input: Vec<u8>,
    target: usize,
    output: Vec<u8>,
    written: usize,
    deadline: Instant,
}

impl Connection {
    pub(super) fn new(socket: TcpStream) -> Result<Self> {
        socket.set_nonblocking(true)?;
        socket.set_nodelay(true)?;
        Ok(Self {
            socket,
            input: Vec::new(),
            target: 12,
            output: Vec::new(),
            written: 0,
            deadline: Instant::now() + Duration::from_millis(500),
        })
    }
    /// At most one bounded read/write per peer per turn; incomplete peers yield.
    pub(super) fn poll(
        &mut self,
        handle: impl FnOnce(super::Rpc) -> Result<super::Response>,
    ) -> Result<bool> {
        require(
            Instant::now() < self.deadline,
            "scheduler control frame deadline",
        )?;
        if self.output.is_empty() {
            let mut bytes = [0; 8192];
            let size = (self.target - self.input.len()).min(bytes.len());
            match self.socket.read(&mut bytes[..size]) {
                Ok(0) => return Err(Error::Invalid("truncated scheduler control".into())),
                Ok(n) => self.input.extend_from_slice(&bytes[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(false),
                Err(e) => return Err(e.into()),
            }
            if self.input.len() < self.target {
                return Ok(false);
            }
            if self.target == 12 {
                require(
                    &self.input[..8] == protocol::MAGIC,
                    "protocol magic/version mismatch",
                )?;
                let size = u32::from_le_bytes(self.input[8..12].try_into().expect("frame header"))
                    as usize;
                require(
                    size > 0 && size <= protocol::MAX_CONTROL_BYTES,
                    "control frame size",
                )?;
                self.target += size;
                self.input.reserve_exact(size);
                return Ok(false);
            }
            let request = protocol::read_message(&mut self.input.as_slice())?;
            let response = handle(request)?;
            protocol::write_message(&mut self.output, &response)?;
            // Release the potentially large input before retaining the reply.
            self.input = Vec::new();
        }
        match self.socket.write(&self.output[self.written..]) {
            Ok(0) => Err(Error::Invalid("closed scheduler control reply".into())),
            Ok(n) => {
                self.written += n;
                Ok(self.written == self.output.len())
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn oversized_frame_refuses_before_body_allocation() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut socket = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (peer, _) = listener.accept().unwrap();
        let mut connection = Connection::new(peer).unwrap();
        socket.write_all(protocol::MAGIC).unwrap();
        socket.write_all(&u32::MAX.to_le_bytes()).unwrap();
        let until = Instant::now() + Duration::from_secs(1);
        loop {
            match connection.poll(|_| panic!("oversized frame reached ledger")) {
                Err(_) => break,
                Ok(_) => assert!(Instant::now() < until),
            }
        }
        assert!(connection.input.len() <= 12);
        assert!(connection.input.capacity() <= 16);
        assert_eq!(connection.output.len(), 0);
    }
}
