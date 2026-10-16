//! Execution-local managed reservations. These bytes are not process RSS.
use crate::{Error, OperatorId, Result};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, Default)]
pub struct ManagedMetrics {
    pub limit: usize,
    pub current: usize,
    pub peak: usize,
    pub attempts: u64,
    pub denials: u64,
    pub pressure_events: u64,
    pub owner_peak: BTreeMap<OperatorId, usize>,
    owners: BTreeMap<OperatorId, usize>,
}
#[derive(Debug, Clone)]
pub struct MemoryManager(Arc<Mutex<ManagedMetrics>>);
impl MemoryManager {
    /// # Errors
    /// Rejects a zero managed-memory limit.
    pub fn new(limit: usize) -> Result<Self> {
        if limit == 0 {
            return Err(Error::Invalid("memory limit must be positive".into()));
        }
        Ok(Self(Arc::new(Mutex::new(ManagedMetrics {
            limit,
            ..Default::default()
        }))))
    }
    /// Returns an atomic snapshot of managed counters.
    /// # Panics
    /// Panics if an internal invariant has poisoned the mutex.
    #[must_use]
    pub fn snapshot(&self) -> ManagedMetrics {
        self.0.lock().expect("memory mutex").clone()
    }
    pub(crate) fn pressure(&self) {
        let mut m = self.0.lock().expect("memory mutex");
        m.pressure_events = m.pressure_events.saturating_add(1);
    }
    /// Creates an empty owned reservation.
    #[must_use]
    pub fn reservation(&self, owner: OperatorId) -> Reservation {
        Reservation {
            manager: self.clone(),
            owner,
            bytes: 0,
        }
    }
}
#[derive(Debug)]
pub struct Reservation {
    manager: MemoryManager,
    owner: OperatorId,
    bytes: usize,
}
impl Reservation {
    /// Atomically acquires additional managed bytes.
    /// # Errors
    /// Denies growth beyond the limit or on integer overflow without mutation.
    /// # Panics
    /// Panics if an internal invariant has poisoned the mutex.
    pub fn grow(&mut self, bytes: usize) -> Result<()> {
        let mut m = self.manager.0.lock().expect("memory mutex");
        m.attempts = m.attempts.saturating_add(1);
        let total = m.current.checked_add(bytes);
        let own = self.bytes.checked_add(bytes);
        if total.is_none_or(|v| v > m.limit) || own.is_none() {
            m.denials = m.denials.saturating_add(1);
            return Err(Error::MemoryLimit {
                requested: bytes,
                current: m.current,
                limit: m.limit,
            });
        }
        m.current = total.expect("checked");
        self.bytes = own.expect("checked");
        m.peak = m.peak.max(m.current);
        let owner = m.owners.entry(self.owner).or_default();
        *owner += bytes;
        let current = *owner;
        let peak = m.owner_peak.entry(self.owner).or_default();
        *peak = (*peak).max(current);
        Ok(())
    }
    /// Releases owned bytes.
    /// # Errors
    /// Rejects release beyond this reservation without mutation.
    /// # Panics
    /// Panics if an internal invariant has poisoned the mutex.
    pub fn shrink(&mut self, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        if bytes > self.bytes {
            return Err(Error::Invalid("reservation underflow".into()));
        }
        let mut m = self.manager.0.lock().expect("memory mutex");
        m.current -= bytes;
        *m.owners.get_mut(&self.owner).expect("reserved owner") -= bytes;
        self.bytes -= bytes;
        Ok(())
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if self.bytes != 0 {
            self.shrink(self.bytes).expect("owned reservation");
        }
    }
}
