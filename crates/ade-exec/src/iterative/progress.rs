use crate::{Error, Result};
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct Credits {
    pub produced: usize,
    pub queued: usize,
    pub executing: usize,
    pub acknowledged: usize,
    pub peak_queued: usize,
    pub peak_in_flight: usize,
    pub producer_open: bool,
}

#[derive(Debug)]
pub(super) struct Delivery {
    round: u32,
    serial: usize,
    pub vertex: usize,
}

pub(super) struct Feedback {
    round: u32,
    cap: usize,
    queue: VecDeque<Delivery>,
    counts: Credits,
}

impl Feedback {
    pub fn new(round: u32, cap: usize) -> Self {
        Self {
            round,
            cap,
            queue: VecDeque::with_capacity(cap),
            counts: Credits {
                produced: 0,
                queued: 0,
                executing: 0,
                acknowledged: 0,
                peak_queued: 0,
                peak_in_flight: 0,
                producer_open: true,
            },
        }
    }

    fn check(&self) -> Result<()> {
        let c = self.counts;
        if c.queued != self.queue.len()
            || c.queued + c.executing > self.cap
            || c.produced.checked_sub(c.acknowledged) != Some(c.queued + c.executing)
        {
            return Err(Error::Invalid("feedback credit conservation".into()));
        }
        Ok(())
    }

    pub fn full(&self) -> bool {
        self.counts.queued + self.counts.executing == self.cap
    }

    pub fn send(&mut self, vertex: usize) -> Result<()> {
        if !self.counts.producer_open || self.full() {
            return Err(Error::Invalid(
                "feedback producer closed or queue full".into(),
            ));
        }
        let serial = self.counts.produced;
        self.counts.produced = serial
            .checked_add(1)
            .ok_or(Error::Overflow("feedback serial"))?;
        self.queue.push_back(Delivery {
            round: self.round,
            serial,
            vertex,
        });
        self.counts.queued += 1;
        self.counts.peak_queued = self.counts.peak_queued.max(self.counts.queued);
        self.counts.peak_in_flight = self
            .counts
            .peak_in_flight
            .max(self.counts.queued + self.counts.executing);
        self.check()
    }

    pub fn deliver(&mut self) -> Result<Option<Delivery>> {
        if self.counts.executing != 0 {
            return Err(Error::Invalid("feedback receiver still executing".into()));
        }
        let delivery = self.queue.pop_front();
        if delivery.is_some() {
            self.counts.queued -= 1;
            self.counts.executing = 1;
        }
        self.check()?;
        Ok(delivery)
    }

    pub fn ack(&mut self, delivery: &Delivery) -> Result<()> {
        if self.counts.executing != 1
            || delivery.round != self.round
            || delivery.serial != self.counts.acknowledged
        {
            return Err(Error::Invalid(
                "duplicate, missing or wrong-round feedback acknowledgment".into(),
            ));
        }
        self.counts.executing = 0;
        self.counts.acknowledged += 1;
        self.check()
    }

    pub fn finish(mut self) -> Result<Credits> {
        self.counts.producer_open = false;
        self.settled()
    }

    fn settled(&self) -> Result<Credits> {
        self.check()?;
        let c = self.counts;
        if c.producer_open || c.queued != 0 || c.executing != 0 {
            return Err(Error::Invalid(
                "feedback cut has outstanding credits".into(),
            ));
        }
        Ok(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_queue_does_not_prove_quiescence() {
        let mut feedback = Feedback::new(7, 1);
        assert!(feedback.settled().is_err());
        feedback.send(0).unwrap();
        assert!(feedback.send(1).is_err());
        let delivered = feedback.deliver().unwrap().unwrap();
        assert!(feedback.queue.is_empty());
        assert!(feedback.deliver().is_err());
        let wrong = Delivery {
            round: 8,
            serial: delivered.serial,
            vertex: delivered.vertex,
        };
        assert!(feedback.ack(&wrong).is_err());
        feedback.counts.producer_open = false;
        assert!(feedback.settled().is_err());
        feedback.ack(&delivered).unwrap();
        assert!(feedback.ack(&delivered).is_err());
        let cut = feedback.finish().unwrap();
        assert_eq!(cut.produced, cut.acknowledged);
    }

    #[test]
    fn serial_overflow_refuses() {
        let mut feedback = Feedback::new(1, 1);
        feedback.counts.produced = usize::MAX;
        feedback.counts.acknowledged = usize::MAX;
        assert!(matches!(
            feedback.send(0),
            Err(Error::Overflow("feedback serial"))
        ));
    }
}
