//! Single-owner demand loop; no detached source, writer or timer tasks.
use crate::{Clock, Engine, Error, Poll, Result, Source};
use ade_exec::CancellationToken;
use serde::Serialize;
use std::{io::Write, thread, time::Duration};

/// A sink must preserve a partial frame across `WouldBlock` and must not block.
pub trait Sink {
    /// # Errors
    /// Returns sink failure; Ok(0) means temporary backpressure.
    fn try_write(&mut self, remaining: &[u8]) -> Result<usize>;
}
/// Finite lifetime diagnostics budget for the opt-in adaptive runtime.
pub struct Limited<S: Sink> {
    pub inner: S,
    pub remaining: u64,
}
impl<S: Sink> Sink for Limited<S> {
    fn try_write(&mut self, b: &[u8]) -> Result<usize> {
        if b.len() as u64 > self.remaining {
            return Err(Error::Sink);
        }
        let n = self.inner.try_write(b)?;
        self.remaining = self.remaining.checked_sub(n as u64).ok_or(Error::Sink)?;
        Ok(n)
    }
}
/// Safe nonblocking FD wrapper. Flags restored on teardown.
pub struct Nonblocking<W: Write + std::os::fd::AsFd> {
    writer: W,
    original: rustix::fs::OFlags,
}
impl<W: Write + std::os::fd::AsFd> Nonblocking<W> {
    /// # Errors
    /// Refuses FDs that cannot be made nonblocking.
    pub fn new(writer: W) -> Result<Self> {
        let original = rustix::fs::fcntl_getfl(&writer).map_err(|_| Error::Sink)?;
        rustix::fs::fcntl_setfl(&writer, original | rustix::fs::OFlags::NONBLOCK)
            .map_err(|_| Error::Sink)?;
        Ok(Self { writer, original })
    }
}
impl<W: Write + std::os::fd::AsFd> Sink for Nonblocking<W> {
    fn try_write(&mut self, bytes: &[u8]) -> Result<usize> {
        match self.writer.write(bytes) {
            Ok(0) => Err(Error::Sink),
            Ok(n) => Ok(n),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) =>
            {
                Ok(0)
            }
            Err(_) => Err(Error::Sink),
        }
    }
}
impl<W: Write + std::os::fd::AsFd> Drop for Nonblocking<W> {
    fn drop(&mut self) {
        let _ = rustix::fs::fcntl_setfl(&self.writer, self.original);
    }
}
/// Bounded telemetry writer. A stalled diagnostics consumer is an error.
/// # Errors
/// Refuses oversized diagnostics, sink failure or diagnostic backpressure.
pub fn trace(sink: &mut dyn Sink, value: &impl Serialize) -> Result<()> {
    let mut bytes = serde_json::to_vec(value).map_err(|_| Error::Sink)?;
    if bytes.len() >= 8192 {
        return Err(Error::Sink);
    }
    bytes.push(b'\n');
    let mut written = 0;
    while written < bytes.len() {
        let n = sink.try_write(&bytes[written..])?;
        if n == 0 {
            return Err(Error::Sink);
        }
        written += n;
    }
    Ok(())
}
#[derive(Default, Debug, Serialize)]
pub struct Metrics {
    pub polls: u64,
    pub pending_polls: u64,
    pub sink_blocked_ticks: u64,
    pub sink_frame_peak_bytes: usize,
    pub in_flight_peak_records: usize,
    pub downstream_channel_depth: usize,
}
/// Operator test limits are not source completion and do not advance progress.
#[derive(Default)]
pub struct Stop {
    pub after_records: Option<u64>,
    pub after_ms: Option<u64>,
}
/// # Errors
/// Returns source/sink/deadline/cancel errors with unchanged unacknowledged offsets.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "Single-owner demand/acknowledgment loop keeps cancellation and sink progress together"
)]
pub fn run(
    source: &mut dyn Source,
    engine: &mut Engine,
    sink: &mut dyn Sink,
    diagnostics: &mut dyn Sink,
    clock: &dyn Clock,
    token: &CancellationToken,
    stop: &Stop,
    stall_ms: u64,
    metrics: &mut Metrics,
) -> Result<&'static str> {
    let result = (|| {
        let mut last_health = clock.now_ms();
        let mut last_trace = clock.now_ms();
        loop {
            token.check().map_err(|_| Error::Cancelled)?;
            #[cfg(feature = "stateful")]
            if let Some(state) = &mut engine.state {
                if let Some(output) = state.next_close(engine.progress.frontier)? {
                    let frame = frame(&output)?;
                    let Some(paused) = write_frame(
                        source,
                        &engine.progress,
                        &frame,
                        sink,
                        diagnostics,
                        clock,
                        token,
                        stop,
                        stall_ms,
                        metrics,
                        &mut last_health,
                        &mut last_trace,
                    )?
                    else {
                        return Ok("STOPPED_BY_OPERATOR_TEST_LIMIT");
                    };
                    state.confirm_close()?;
                    trace(
                        diagnostics,
                        &serde_json::json!({"type":"window_confirmed","output_id":output.output_id,"state":state.store.metrics}),
                    )?;
                    if paused {
                        source.resume()?;
                    }
                    continue;
                }
                if state.due(&engine.progress, clock.now_ms()) {
                    source.pause()?;
                    source.verify(&engine.progress)?;
                    let pause = state.test_pause_at.clone();
                    let pointer = state.checkpoint(&engine.progress,clock.now_ms(), &mut |point| {
                    if point=="generation_synced" || point=="before_selector" {source.verify(&engine.progress)?;}
                    trace(diagnostics,&serde_json::json!({"type":"checkpoint_step","point":point,"progress":engine.progress}))?;
                    if pause.as_deref()==Some(point) {
                        let mut last_pause=clock.now_ms();
                        loop {
                            token.check().map_err(|_|Error::Cancelled)?;
                            if clock.now_ms().saturating_sub(last_pause)>=1000 {
                                trace(diagnostics,&serde_json::json!({"type":"checkpoint_blocked","elapsed_ms":clock.now_ms(),"progress":engine.progress,"source":source.snapshot(&engine.progress)}))?;
                                last_pause=clock.now_ms();
                            }
                            thread::sleep(Duration::from_millis(10));
                        }
                    }
                    Ok(())
                })?;
                    #[cfg(feature = "adaptive-checkpoint")]
                    if let Some(a) = &state.adaptive {
                        trace(
                            diagnostics,
                            &serde_json::json!({"type":"adaptive_decision","decision":a.decision,"metrics":a.last_metrics,"certificate":a.current,"certificate_blake3":a.current_hash}),
                        )?;
                        #[cfg(feature = "remote-checkpoint")]
                        if let Some(client) = &a.client {
                            trace(
                                diagnostics,
                                &serde_json::json!({"type":"remote_accounting","ledger":client.ledger,"metrics":client.metrics}),
                            )?;
                        }
                    }
                    trace(
                        diagnostics,
                        &serde_json::json!({"type":"checkpoint_success","pointer":pointer,"progress":engine.progress,"state":state.store.metrics}),
                    )?;
                    source.resume()?;
                }
            }
            if stop
                .after_records
                .is_some_and(|n| engine.progress.forwarded_records >= n)
                || stop.after_ms.is_some_and(|ms| clock.now_ms() >= ms)
            {
                return Ok("STOPPED_BY_OPERATOR_TEST_LIMIT");
            }
            if clock.now_ms().saturating_sub(last_health) >= 1000 {
                source.verify(&engine.progress)?;
                last_health = clock.now_ms();
            }
            for timer in engine.timers.fire(clock.now_ms(), engine.progress.frontier) {
                trace(
                    diagnostics,
                    &serde_json::json!({"type":"timer", "session":engine.progress.session, "processing_ms":clock.now_ms(), "frontier":engine.progress.frontier, "timer":timer}),
                )?;
            }
            if clock.now_ms().saturating_sub(last_trace) >= 1000 {
                trace(
                    diagnostics,
                    &serde_json::json!({"type":"progress", "elapsed_ms":clock.now_ms(), "progress":engine.progress, "metrics":metrics, "source":source.snapshot(&engine.progress)}),
                )?;
                last_trace = clock.now_ms();
            }
            metrics.polls = metrics
                .polls
                .checked_add(1)
                .ok_or(Error::Overflow("polls"))?;
            match source.poll()? {
                Poll::Pending => {
                    metrics.pending_polls = metrics
                        .pending_polls
                        .checked_add(1)
                        .ok_or(Error::Overflow("pending polls"))?;
                }
                Poll::Stopped => return Ok("STOPPED_BY_SOURCE_OPERATOR"),
                Poll::Data(record) => {
                    #[cfg(feature = "adaptive-checkpoint")]
                    let prepare_start = std::time::Instant::now();
                    let receipt = engine.prepare(record)?;
                    #[cfg(feature = "adaptive-checkpoint")]
                    if let Some(a) = engine.state.as_mut().and_then(|s| s.adaptive.as_mut()) {
                        a.observation.replay_ns_per_record =
                            u64::try_from(prepare_start.elapsed().as_nanos())
                                .map_err(|_| Error::Overflow("record sample"))?
                                .max(1);
                    }
                    #[cfg(feature = "stateful")]
                    let frame = if let Some(state) = &engine.state {
                        frame(
                            &serde_json::json!({"type":"input","receipt":receipt,"closed_window_late":state.last_closed_late}),
                        )?
                    } else {
                        frame(&receipt)?
                    };
                    #[cfg(not(feature = "stateful"))]
                    let frame = frame(&receipt)?;
                    metrics.sink_frame_peak_bytes = metrics.sink_frame_peak_bytes.max(frame.len());
                    metrics.in_flight_peak_records = 1;
                    let Some(paused) = write_frame(
                        source,
                        &engine.progress,
                        &frame,
                        sink,
                        diagnostics,
                        clock,
                        token,
                        stop,
                        stall_ms,
                        metrics,
                        &mut last_health,
                        &mut last_trace,
                    )?
                    else {
                        return Ok("STOPPED_BY_OPERATOR_TEST_LIMIT");
                    };
                    engine.acknowledge()?;

                    trace(
                        diagnostics,
                        &serde_json::json!({"type":"forwarded", "elapsed_ms":clock.now_ms(), "progress":engine.progress}),
                    )?;
                    for timer in engine.timers.fire(clock.now_ms(), engine.progress.frontier) {
                        trace(
                            diagnostics,
                            &serde_json::json!({"type":"timer", "session":engine.progress.session, "processing_ms":clock.now_ms(), "frontier":engine.progress.frontier, "timer":timer}),
                        )?;
                    }
                    if paused {
                        source.resume()?;
                    }
                }
            }
        }
    })();
    #[cfg(feature = "stateful")]
    if result.is_err()
        && let Some(state) = &mut engine.state
    {
        state.poison();
    }
    result
}

fn frame(value: &impl Serialize) -> Result<Vec<u8>> {
    let mut b = serde_json::to_vec(value).map_err(|_| Error::Sink)?;
    if b.len() >= 2048 {
        return Err(Error::Sink);
    }
    b.push(b'\n');
    Ok(b)
}
#[allow(
    clippy::too_many_arguments,
    reason = "Shared single-frame pump preserves identical stateless/stateful demand and cancellation"
)]
fn write_frame(
    source: &mut dyn Source,
    progress: &crate::Progress,
    frame: &[u8],
    sink: &mut dyn Sink,
    diagnostics: &mut dyn Sink,
    clock: &dyn Clock,
    token: &CancellationToken,
    stop: &Stop,
    stall_ms: u64,
    metrics: &mut Metrics,
    last_health: &mut u64,
    last_trace: &mut u64,
) -> Result<Option<bool>> {
    metrics.sink_frame_peak_bytes = metrics.sink_frame_peak_bytes.max(frame.len());
    metrics.in_flight_peak_records = 1;
    let began = clock.now_ms();
    let mut written = 0;
    let mut paused = false;
    while written < frame.len() {
        token.check().map_err(|_| Error::Cancelled)?;
        if stop.after_ms.is_some_and(|ms| clock.now_ms() >= ms) {
            return if written == 0 {
                Ok(None)
            } else {
                Err(Error::PartialFrame {
                    written,
                    total: frame.len(),
                })
            };
        }
        if clock.now_ms().saturating_sub(began) >= stall_ms {
            return Err(Error::SinkDeadline);
        }
        if clock.now_ms().saturating_sub(*last_health) >= 1000 {
            source.verify(progress)?;
            *last_health = clock.now_ms();
        }
        let n = sink.try_write(&frame[written..])?;
        if n > frame.len() - written {
            return Err(Error::Sink);
        }
        written += n;
        if n == 0 {
            if !paused {
                source.pause()?;
                paused = true;
            }
            metrics.sink_blocked_ticks = metrics
                .sink_blocked_ticks
                .checked_add(1)
                .ok_or(Error::Overflow("sink ticks"))?;
            if clock.now_ms().saturating_sub(*last_trace) >= 1000 {
                trace(
                    diagnostics,
                    &serde_json::json!({"type":"sink_blocked", "elapsed_ms":clock.now_ms(), "progress":progress, "metrics":metrics, "source":source.snapshot(progress)}),
                )?;
                *last_trace = clock.now_ms();
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    token.check().map_err(|_| Error::Cancelled)?;
    Ok(Some(paused))
}
