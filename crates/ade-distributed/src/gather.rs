//! Internal attempt commit boundary, isolated from external sink transactions.
use crate::{
    Result,
    exchange::{self, Receiver, Store},
    ids::{AttemptId, JobId, transfer},
    metrics::NetworkMetrics,
    require,
};
use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener},
    path::Path,
    sync::Arc,
};

pub(crate) struct Gather {
    stores: BTreeMap<(u64, u64), Arc<Store>>,
    receivers: Vec<Receiver>,
    committed: BTreeMap<u64, AttemptId>,
    job: JobId,
    workers: usize,
}
impl Gather {
    pub(crate) fn new(parent: &Path, job: JobId, workers: usize) -> Result<Self> {
        let mut stores = BTreeMap::new();
        for p in 0..workers as u64 {
            stores.insert(
                (p, 0),
                Arc::new(Store::new(
                    parent,
                    [transfer(job, 1, 0, workers as u64 + p)].into(),
                    ade_exec::CancellationToken::default(),
                )?),
            );
        }
        let routing = stores.clone();
        let receiver = Receiver::start(TcpListener::bind("127.0.0.1:0")?, move |header| {
            let p = header
                .id
                .producer
                .0
                .checked_sub(workers as u64)
                .ok_or_else(|| crate::Error::Invalid("invalid gather producer".into()))?;
            routing
                .get(&(p, header.id.attempt.0))
                .cloned()
                .ok_or_else(|| crate::Error::Invalid("unexpected gather attempt".into()))
        })?;
        Ok(Self {
            stores,
            receivers: vec![receiver],
            committed: BTreeMap::new(),
            job,
            workers,
        })
    }
    pub(crate) fn address(&self) -> SocketAddr {
        self.receivers[0].address
    }
    pub(crate) fn start_recovery(&mut self, parent: &Path, partition: u64) -> Result<SocketAddr> {
        require(
            !self.committed.contains_key(&partition) && self.receivers.len() == 1,
            "duplicate/ambiguous gather recovery",
        )?;
        let old = &self.stores[&(partition, 0)];
        // Cancel before checking the terminal registry: an in-flight terminal cannot
        // be promoted after abandonment. A validated old terminal is ambiguous and fails.
        old.cancellation.cancel();
        require(
            old.metrics()?.transfers == 0,
            "ambiguous gather commit state",
        )?;
        let store =
            Arc::new(Store::new(
                parent,
                [transfer(self.job, 1, 0, self.workers as u64 + partition)
                    .with_attempt(AttemptId(1))]
                .into(),
                ade_exec::CancellationToken::default(),
            )?);
        let receiving = store.clone();
        let receiver = Receiver::start(TcpListener::bind("127.0.0.1:0")?, move |_| {
            Ok(receiving.clone())
        })?;
        let address = receiver.address;
        self.stores.insert((partition, 1), store);
        self.receivers.push(receiver);
        Ok(address)
    }
    #[cfg(feature = "research-hooks")]
    pub(crate) fn wait_for_partial_prefix(
        &self,
        partition: u64,
        timeout: std::time::Duration,
    ) -> Result<()> {
        let id = transfer(self.job, 1, 0, self.workers as u64 + partition);
        let store = &self.stores[&(partition, 0)];
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let batches = store.observed_batches(id)?;
            if batches > 0 {
                require(
                    batches == 1,
                    "research failure did not leave a one-batch prefix",
                )?;
                return Ok(());
            }
            require(
                std::time::Instant::now() < deadline,
                "research failure produced no accepted gather batch",
            )?;
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }
    pub(crate) fn commit(
        &mut self,
        partition: u64,
        attempt: AttemptId,
        sent: &NetworkMetrics,
    ) -> Result<()> {
        require(
            !self.committed.contains_key(&partition),
            "duplicate gather success",
        )?;
        let store = self
            .stores
            .get(&(partition, attempt.0))
            .ok_or_else(|| crate::Error::Invalid("unexpected gather attempt".into()))?;
        let accepted = store.metrics()?;
        require(
            accepted.transfers == 1
                && accepted.transfers == sent.transfers
                && accepted.batches == sent.batches
                && accepted.rows == sent.rows
                && accepted.payload_bytes == sent.payload_bytes,
            "gather terminal conservation failure",
        )?;
        self.committed.insert(partition, attempt);
        Ok(())
    }
    pub(crate) fn attempts(&self) -> BTreeMap<u64, AttemptId> {
        self.committed.clone()
    }
    pub(crate) fn collect(
        &self,
    ) -> Result<(Vec<arrow::record_batch::RecordBatch>, NetworkMetrics)> {
        require(
            self.committed.len() == self.workers,
            "incomplete gather partition set",
        )?;
        let mut batches = Vec::new();
        let mut metrics = NetworkMetrics::default();
        for (p, attempt) in &self.committed {
            let store = &self.stores[&(*p, attempt.0)];
            batches.extend(exchange::collect(store)?);
            metrics.add(&store.metrics()?);
        }
        Ok((batches, metrics))
    }
}
impl Drop for Gather {
    fn drop(&mut self) {
        for store in self.stores.values() {
            store.cancellation.cancel();
        }
        self.receivers.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::{Header, Sender};
    use std::io::Write;
    #[test]
    fn incomplete_original_is_quarantined_and_only_one_attempt_commits() {
        let root = tempfile::tempdir().unwrap();
        let mut gather = Gather::new(root.path(), JobId(7), 2).unwrap();
        let id = transfer(JobId(7), 1, 0, 2);
        let mut incomplete = crate::protocol::connect(gather.address(), crate::IO_TIMEOUT).unwrap();
        crate::protocol::write_message(&mut incomplete, &Header::new(id)).unwrap();
        // Header accepted, but no terminal. Abandon while its connection is open.
        let recovery_address = gather.start_recovery(root.path(), 0).unwrap();
        let sent = Sender::new(
            recovery_address,
            id.with_attempt(AttemptId(1)),
            ade_exec::CancellationToken::default(),
        )
        .unwrap()
        .finish()
        .unwrap();
        gather.commit(0, AttemptId(1), &sent).unwrap();
        assert!(gather.commit(0, AttemptId(1), &sent).is_err());
        let _ = incomplete.write_all(&[0; 12]);
        drop(incomplete);
        assert!(
            Sender::new(gather.address(), id, ade_exec::CancellationToken::default())
                .unwrap()
                .finish()
                .is_err()
        );
        let normal = Sender::new(
            gather.address(),
            transfer(JobId(7), 1, 0, 3),
            ade_exec::CancellationToken::default(),
        )
        .unwrap()
        .finish()
        .unwrap();
        gather.commit(1, AttemptId(0), &normal).unwrap();
        let (batches, metrics) = gather.collect().unwrap();
        assert_eq!(batches.len(), 0);
        assert_eq!(metrics.transfers, 2);
        assert_eq!(
            gather.attempts(),
            [(0, AttemptId(1)), (1, AttemptId(0))].into()
        );
    }
    #[test]
    fn completed_original_cannot_be_replaced_and_incomplete_output_cannot_commit() {
        let root = tempfile::tempdir().unwrap();
        let mut gather = Gather::new(root.path(), JobId(8), 2).unwrap();
        assert!(
            gather
                .commit(0, AttemptId(0), &NetworkMetrics::default())
                .is_err()
        );
        assert!(gather.collect().is_err());
        let sent = Sender::new(
            gather.address(),
            transfer(JobId(8), 1, 0, 2),
            ade_exec::CancellationToken::default(),
        )
        .unwrap()
        .finish()
        .unwrap();
        assert!(gather.start_recovery(root.path(), 0).is_err());
        // A separate normal partition commits exactly once.
        let other = Sender::new(
            gather.address(),
            transfer(JobId(8), 1, 0, 3),
            ade_exec::CancellationToken::default(),
        )
        .unwrap()
        .finish()
        .unwrap();
        gather.commit(1, AttemptId(0), &other).unwrap();
        assert!(gather.commit(1, AttemptId(1), &sent).is_err());
    }
}
