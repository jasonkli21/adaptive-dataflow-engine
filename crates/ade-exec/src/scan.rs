use crate::{
    CancellationToken, Error, OperatorId, Result,
    memory::{MemoryManager, Reservation},
};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use std::{
    collections::VecDeque,
    fs::File,
    path::PathBuf,
    sync::{Arc, Condvar, Mutex},
    thread::JoinHandle,
};

pub(crate) struct SyncScan {
    files: std::vec::IntoIter<PathBuf>,
    reader: Option<ParquetRecordBatchReader>,
    schema: SchemaRef,
    batch_size: usize,
}
impl SyncScan {
    pub fn new(files: Vec<PathBuf>, schema: SchemaRef, batch_size: usize) -> Self {
        Self {
            files: files.into_iter(),
            reader: None,
            schema,
            batch_size,
        }
    }
    pub fn next(&mut self, token: &CancellationToken) -> Result<Option<RecordBatch>> {
        loop {
            token.check()?;
            if let Some(reader) = &mut self.reader
                && let Some(batch) = reader.next()
            {
                return Ok(Some(batch?));
            }
            let Some(file) = self.files.next() else {
                return Ok(None);
            };
            let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(file)?)?;
            if builder.schema().as_ref() != self.schema.as_ref() {
                return Err(Error::Invalid("scan file schemas differ".into()));
            }
            self.reader = Some(builder.with_batch_size(self.batch_size).build()?);
        }
    }

    pub fn shutdown(&mut self) {
        self.reader.take();
        self.files = Vec::new().into_iter();
    }
}
#[derive(Debug, Clone, Default)]
pub struct PrefetchMetrics {
    pub produced: usize,
    pub consumed: usize,
    pub queue_peak: usize,
    pub observed_batch_peak: usize,
}
type Item = Result<(RecordBatch, Reservation)>;
struct Queue {
    items: VecDeque<Item>,
    stop: bool,
    done: bool,
    failed: bool,
    metrics: PrefetchMetrics,
}
struct Shared {
    queue: Mutex<Queue>,
    ready: Condvar,
    space: Condvar,
}
pub(crate) struct Prefetch {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
    metadata: Option<Reservation>,
}
impl Prefetch {
    pub fn start(
        mut scan: SyncScan,
        depth: usize,
        token: CancellationToken,
        memory: MemoryManager,
        id: OperatorId,
    ) -> Result<Self> {
        let mut metadata = memory.reservation(id);
        metadata.grow(
            depth
                .checked_mul(std::mem::size_of::<Item>())
                .ok_or(Error::Overflow("prefetch metadata"))?,
        )?;
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue {
                items: VecDeque::with_capacity(depth),
                stop: false,
                done: false,
                failed: false,
                metrics: PrefetchMetrics::default(),
            }),
            ready: Condvar::new(),
            space: Condvar::new(),
        });
        let producer = shared.clone();
        let thread = std::thread::Builder::new()
            .name("ade-parquet-prefetch".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    loop {
                        let mut q = producer
                            .queue
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        while q.items.len() == depth && !q.stop {
                            q = producer
                                .space
                                .wait(q)
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                        }
                        if q.stop {
                            break;
                        }
                        drop(q);
                        let item = match scan.next(&token) {
                            Ok(Some(batch)) => {
                                let mut reservation = memory.reservation(id);
                                reservation
                                    .grow(batch.get_array_memory_size())
                                    .map(|()| (batch, reservation))
                            }
                            Ok(None) => break,
                            Err(e) => Err(e),
                        };
                        let failed = item.is_err();
                        let mut q = producer
                            .queue
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if q.stop {
                            break;
                        }
                        if let Ok((batch, _)) = &item {
                            q.metrics.produced += 1;
                            q.metrics.observed_batch_peak = q
                                .metrics
                                .observed_batch_peak
                                .max(batch.get_array_memory_size());
                        }
                        q.items.push_back(item);
                        q.metrics.queue_peak = q.metrics.queue_peak.max(q.items.len());
                        producer.ready.notify_one();
                        if failed {
                            break;
                        }
                    }
                }));
                let mut q = producer
                    .queue
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                q.failed = result.is_err();
                q.done = true;
                producer.ready.notify_all();
            })?;
        Ok(Self {
            shared,
            thread: Some(thread),
            metadata: Some(metadata),
        })
    }
    pub fn next(&mut self) -> Result<Option<RecordBatch>> {
        let mut q = self
            .shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while q.items.is_empty() && !q.done {
            q = self
                .shared
                .ready
                .wait(q)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        let item = q.items.pop_front();
        self.shared.space.notify_one();
        match item {
            Some(Ok((batch, _reservation))) => {
                q.metrics.consumed += 1;
                Ok(Some(batch))
            }
            Some(Err(e)) => Err(e),
            None if q.failed => Err(Error::PrefetchFailed),
            None => Ok(None),
        }
    }
    pub fn metrics(&self) -> PrefetchMetrics {
        self.shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .metrics
            .clone()
    }

    /// Stop the producer, release queued batches, and capture counters after join.
    pub fn shutdown(&mut self) -> PrefetchMetrics {
        {
            let mut q = self
                .shared
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            q.stop = true;
            q.items.clear();
        }
        self.shared.space.notify_all();
        self.shared.ready.notify_all();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        {
            let mut q = self
                .shared
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // Drop the bounded queue allocation together with its reservation.
            q.items = VecDeque::new();
        }
        self.metadata.take();
        self.metrics()
    }
}
impl Drop for Prefetch {
    fn drop(&mut self) {
        self.shutdown();
    }
}
pub(crate) enum Scan {
    Sync(SyncScan),
    Prefetch(Prefetch),
}
impl Scan {
    pub fn next(&mut self, token: &CancellationToken) -> Result<Option<RecordBatch>> {
        match self {
            Self::Sync(scan) => scan.next(token),
            Self::Prefetch(scan) => scan.next(),
        }
    }

    pub fn shutdown(&mut self) -> PrefetchMetrics {
        match self {
            Self::Sync(scan) => {
                scan.shutdown();
                PrefetchMetrics::default()
            }
            Self::Prefetch(scan) => scan.shutdown(),
        }
    }
}
