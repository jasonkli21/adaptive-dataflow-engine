//! Bounded session-local timers. Deadlines inclusive; event progress never wall time.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Processing,
    Event,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registration {
    pub kind: Kind,
    pub deadline_ms: i64,
}
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Fired {
    pub id: u64,
    pub kind: Kind,
    pub deadline_ms: i64,
}
pub struct Timers {
    entries: BTreeMap<u64, Registration>,
    next_id: u64,
    max_count: usize,
    max_bytes: usize,
    max_registrations: u64,
}
impl Timers {
    /// # Errors
    /// Rejects invalid capacity.
    pub fn new(max_count: usize, max_bytes: usize, max_registrations: u64) -> Result<Self> {
        if max_count == 0 || max_bytes < 128 || max_registrations == 0 {
            return Err(Error::Timer("limits"));
        }
        Ok(Self {
            entries: BTreeMap::new(),
            next_id: 0,
            max_count,
            max_bytes,
            max_registrations,
        })
    }
    /// Fixed 128-byte conservative node charge includes tree allocation/registration.
    /// # Errors
    /// Refuses lifetime/count/byte caps or negative processing deadline.
    pub fn register(&mut self, r: Registration) -> Result<u64> {
        if self.entries.len() >= self.max_count
            || self.entries.len() >= self.max_bytes / 128
            || self.next_id >= self.max_registrations
            || (r.kind == Kind::Processing && r.deadline_ms < 0)
        {
            return Err(Error::Timer("registration cap/deadline"));
        }
        let id = self.next_id;
        self.next_id += 1;
        self.entries.insert(id, r);
        Ok(id)
    }
    pub fn cancel(&mut self, id: u64) -> bool {
        self.entries.remove(&id).is_some()
    }
    pub fn clear(&mut self) {
        self.entries.clear();
    }
    #[must_use]
    pub fn charged_bytes(&self) -> usize {
        self.entries.len() * 128
    }
    /// Deterministic (deadline,kind,registration-id) ordering of ready timers.
    pub fn fire(&mut self, processing_ms: u64, frontier: Option<i64>) -> Vec<Fired> {
        let mut ready = self
            .entries
            .iter()
            .filter(|(_, r)| match r.kind {
                Kind::Processing => u64::try_from(r.deadline_ms).is_ok_and(|d| processing_ms >= d),
                Kind::Event => frontier.is_some_and(|f| f >= r.deadline_ms),
            })
            .map(|(id, r)| Fired {
                id: *id,
                kind: r.kind,
                deadline_ms: r.deadline_ms,
            })
            .collect::<Vec<_>>();
        ready.sort_by_key(|f| (f.deadline_ms, f.kind, f.id));
        for fired in &ready {
            self.entries.remove(&fired.id);
        }
        ready
    }
}
