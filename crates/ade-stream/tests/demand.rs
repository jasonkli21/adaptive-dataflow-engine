#![cfg(feature = "kafka")]
use ade_exec::CancellationToken;
use ade_stream::{
    Clock, Engine, EventV1, MonotonicClock, Poll, Progress, RawRecord, Result, Security, Source,
    Start, StreamConfigV1,
    runtime::{Metrics, Sink, Stop, run},
};
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;
struct TestSource {
    records: VecDeque<RawRecord>,
    polls: Arc<AtomicUsize>,
    paused: bool,
}
impl Source for TestSource {
    fn poll(&mut self) -> Result<Poll> {
        assert!(!self.paused);
        self.polls.fetch_add(1, Ordering::SeqCst);
        if let Some(r) = self.records.pop_front() {
            Ok(Poll::Data(r))
        } else {
            thread::sleep(Duration::from_millis(10));
            Ok(Poll::Pending)
        }
    }
    fn pause(&mut self) -> Result<()> {
        self.paused = true;
        Ok(())
    }
    fn resume(&mut self) -> Result<()> {
        self.paused = false;
        Ok(())
    }
    fn verify(&mut self, _: &Progress) -> Result<()> {
        Ok(())
    }
}
struct Bytes(Vec<u8>);
impl Sink for Bytes {
    fn try_write(&mut self, b: &[u8]) -> Result<usize> {
        self.0.extend_from_slice(b);
        Ok(b.len())
    }
}
struct Block {
    polls: Arc<AtomicUsize>,
    started: Instant,
    resume_after: Option<Duration>,
    fail: bool,
}
impl Sink for Block {
    fn try_write(&mut self, b: &[u8]) -> Result<usize> {
        assert_eq!(
            self.polls.load(Ordering::SeqCst),
            1,
            "source demand while blocked"
        );
        if self.fail {
            return Err(ade_stream::Error::Sink);
        }
        if self
            .resume_after
            .is_some_and(|d| self.started.elapsed() >= d)
        {
            Ok(b.len())
        } else {
            Ok(0)
        }
    }
}
fn setup() -> (Engine, TestSource, Arc<AtomicUsize>) {
    let c = StreamConfigV1 {
        version: 1,
        bootstrap: "127.0.0.1:1".into(),
        expected_cluster_id: "test".into(),
        topic: "test".into(),
        starts: vec![Start {
            partition: 0,
            offset: 0,
        }],
        security: Security::LoopbackDev {},
        key_less_than: 10,
        allowance_ms: 0,
        payload_bytes: 4096,
        client_queue_kib: 1,
        sink_stall_ms: 1000,
        timers: vec![],
    };
    let e = Engine::new(c, Uuid::new_v4()).unwrap();
    let r = RawRecord {
        session: e.progress.session,
        partition: 0,
        offset: 0,
        payload: serde_json::to_vec(&EventV1 {
            version: 1,
            key: 1,
            value_i64: -2,
            event_time_ms: 3,
        })
        .unwrap(),
    };
    let polls = Arc::new(AtomicUsize::new(0));
    (
        e,
        TestSource {
            records: VecDeque::from([r]),
            polls: polls.clone(),
            paused: false,
        },
        polls,
    )
}
#[test]
fn blocked_sink_stops_demand_and_resume_acknowledges_once() {
    let (mut e, mut source, polls) = setup();
    let mut sink = Block {
        polls,
        started: Instant::now(),
        resume_after: Some(Duration::from_millis(60)),
        fail: false,
    };
    let mut metrics = Metrics::default();
    let stop = Stop {
        after_records: Some(1),
        after_ms: None,
    };
    run(
        &mut source,
        &mut e,
        &mut sink,
        &mut Bytes(vec![]),
        &MonotonicClock::default(),
        &CancellationToken::default(),
        &stop,
        1000,
        &mut metrics,
    )
    .unwrap();
    assert_eq!(e.progress.partitions[&0].forwarded, Some(0));
    assert!(metrics.sink_blocked_ticks >= 4);
}
#[test]
fn sink_failure_and_stall_never_advance_forwarded() {
    for fail in [true, false] {
        let (mut e, mut source, polls) = setup();
        let mut sink = Block {
            polls,
            started: Instant::now(),
            resume_after: None,
            fail,
        };
        let result = run(
            &mut source,
            &mut e,
            &mut sink,
            &mut Bytes(vec![]),
            &MonotonicClock::default(),
            &CancellationToken::default(),
            &Stop::default(),
            30,
            &mut Metrics::default(),
        );
        assert!(result.is_err());
        assert_eq!(e.progress.partitions[&0].seen, Some(0));
        assert_eq!(e.progress.partitions[&0].forwarded, None);
    }
}
#[test]
fn cancellation_unblocks_stalled_sink_and_pending_poll() {
    for pending in [true, false] {
        let (mut e, mut source, polls) = setup();
        if pending {
            source.records.clear();
        }
        let token = CancellationToken::default();
        let signal = token.clone();
        let cancel = thread::spawn(move || {
            thread::sleep(Duration::from_millis(60));
            signal.cancel();
        });
        let mut sink = Block {
            polls,
            started: Instant::now(),
            resume_after: None,
            fail: false,
        };
        let began = Instant::now();
        let result = run(
            &mut source,
            &mut e,
            &mut sink,
            &mut Bytes(vec![]),
            &MonotonicClock::default(),
            &token,
            &Stop::default(),
            1000,
            &mut Metrics::default(),
        );
        cancel.join().unwrap();
        assert!(matches!(result, Err(ade_stream::Error::Cancelled)));
        assert!(began.elapsed() < Duration::from_millis(500));
        assert_eq!(e.progress.partitions[&0].forwarded, None);
    }
}
#[test]
fn processing_clock_is_monotonic_injected_and_idle_never_completes() {
    struct Fake(std::cell::Cell<u64>);
    impl Clock for Fake {
        fn now_ms(&self) -> u64 {
            let v = self.0.get();
            self.0.set(v + 1);
            v
        }
    }
    let (mut e, mut source, _) = setup();
    source.records.clear();
    let mut metrics = Metrics::default();
    let result = run(
        &mut source,
        &mut e,
        &mut Bytes(vec![]),
        &mut Bytes(vec![]),
        &Fake(std::cell::Cell::new(0)),
        &CancellationToken::default(),
        &Stop {
            after_records: None,
            after_ms: Some(40),
        },
        1000,
        &mut metrics,
    )
    .unwrap();
    assert_eq!(result, "STOPPED_BY_OPERATOR_TEST_LIMIT");
    assert!(metrics.pending_polls > 0);
    assert_eq!(e.progress.frontier, None);
}
