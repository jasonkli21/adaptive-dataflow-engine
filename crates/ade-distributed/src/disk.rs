//! Opt-in cumulative write budgets. Defaults keep the original unrestricted path.
use std::{
    io::{self, Write},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
#[derive(Debug)]
pub struct Budget {
    limit: u64,
    used: AtomicU64,
}
impl Budget {
    #[must_use]
    pub fn new(limit: u64) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicU64::new(0),
        })
    }
    #[must_use]
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }
    /// Reservations are cumulative, including failed/partial writes; no retry can
    /// reclaim an uncertain reservation and exceed the declared write allowance.
    /// # Errors
    /// Refuses cap+1 and arithmetic overflow before the underlying file call.
    pub fn charge(&self, bytes: u64) -> io::Result<()> {
        let mut old = self.used.load(Ordering::Acquire);
        loop {
            let next = old
                .checked_add(bytes)
                .filter(|next| *next <= self.limit)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::StorageFull, "managed disk write quota")
                })?;
            match self
                .used
                .compare_exchange_weak(old, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Ok(()),
                Err(actual) => old = actual,
            }
        }
    }
}
pub(crate) struct Writer<W> {
    pub inner: W,
    pub budget: Option<Arc<Budget>>,
}
impl<W: Write> Write for Writer<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Some(budget) = &self.budget {
            budget.charge(bytes.len() as u64)?;
        }
        self.inner.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_atomic_cap_and_overflow_never_write_one_more_byte() {
        let budget = Budget::new(8);
        let mut a = Writer {
            inner: vec![],
            budget: Some(budget.clone()),
        };
        let mut b = Writer {
            inner: vec![],
            budget: Some(budget.clone()),
        };
        a.write_all(b"1234").unwrap();
        b.write_all(b"5678").unwrap();
        assert!(a.write_all(b"9").is_err());
        assert_eq!(budget.used(), 8);
        assert_eq!(a.inner, b"1234");
        assert!(budget.charge(u64::MAX).is_err());
    }
}
