//! Explicit producer-local hash-prefix -> raw contribution transition.
use crate::{
    Error, ExecContext, Node, OperatorId, Result,
    aggregate::{Aggregate, RawPartials},
    memory::Reservation,
};
use arrow::{
    array::{Array, Int64Array, UInt64Array},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Instant};
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    FixedHash,
    AdaptiveV1,
    RawPartial,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub mode: Mode,
    pub sample_rows: usize,
    pub min_reduction_percent: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            mode: Mode::FixedHash,
            sample_rows: 1024,
            min_reduction_percent: 50,
        }
    }
}
impl Config {
    /// # Errors
    /// Refuses zero/overlarge prefixes or invalid reduction thresholds.
    pub fn validate(&self) -> Result<()> {
        if !(1..=4096).contains(&self.sample_rows)
            || !(1..=100).contains(&self.min_reduction_percent)
        {
            return Err(Error::Invalid("partial policy bounds".into()));
        }
        Ok(())
    }
}
#[allow(
    clippy::struct_excessive_bools,
    reason = "Independent measured facts, not configurable state flags."
)]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metrics {
    pub policy: Mode,
    pub prefix_rows: usize,
    pub observed_distinct: usize,
    pub distinct_exact: bool,
    pub prefix_pressure: bool,
    pub hash_to_raw: bool,
    pub reason: String,
    pub flushed_count: u64,
    pub raw_rows: u64,
    pub state_released_before_tail: bool,
}
enum State {
    Hash(Option<Box<Aggregate>>),
    Drain(Box<RawPartials>),
    Raw,
}
pub(crate) struct Partial {
    state: State,
    tail: Option<(RecordBatch, usize, bool)>,
    metrics: Metrics,
    _reservation: Reservation,
}
impl Partial {
    pub(crate) fn prime(
        input: &mut Node,
        columns: [usize; 2],
        config: &Config,
        id: OperatorId,
        cx: &mut ExecContext,
    ) -> Result<Self> {
        config.validate()?;
        let mut reservation = cx.memory.reservation(id);
        reservation.grow(8192)?;
        let mut metrics = Metrics {
            policy: config.mode,
            ..Default::default()
        };
        if config.mode == Mode::RawPartial {
            let tail = if let Some(batch) = input.next_batch(cx)? {
                cx.input(id, &batch);
                Some((batch, 0, true))
            } else {
                None
            };
            metrics.reason = "forced_raw".into();
            metrics.state_released_before_tail = true;
            cx.metrics.partial = Some(metrics.clone());
            return Ok(Self {
                state: State::Raw,
                tail,
                metrics,
                _reservation: reservation,
            });
        }
        let mut aggregate = Aggregate::new(id, cx)?;
        let mut tail = None;
        while metrics.prefix_rows < config.sample_rows {
            let Some(batch) = input.next_batch(cx)? else {
                break;
            };
            let rows = (config.sample_rows - metrics.prefix_rows).min(batch.num_rows());
            aggregate.ingest(&batch.slice(0, rows), columns[0], columns[1], None, id, cx)?;
            metrics.prefix_rows += rows;
            if rows < batch.num_rows() {
                tail = Some((batch.slice(rows, batch.num_rows() - rows), 0, false));
                break;
            }
        }
        let (distinct, pressure) = aggregate.observation();
        metrics.observed_distinct = distinct;
        metrics.distinct_exact = !pressure;
        metrics.prefix_pressure = pressure;
        let raw = pressure
            || (metrics.prefix_rows > 0
                && (metrics.prefix_rows - distinct) * 100
                    < metrics.prefix_rows
                        * usize::try_from(config.min_reduction_percent)
                            .map_err(|_| Error::Overflow("partial threshold"))?);
        let state = if raw {
            metrics.hash_to_raw = true;
            metrics.reason = if pressure {
                "prefix_pressure"
            } else {
                "insufficient_reduction"
            }
            .into();
            State::Drain(Box::new(aggregate.into_raw()))
        } else {
            metrics.reason = "reducing_hash".into();
            State::Hash(Some(Box::new(aggregate)))
        };
        cx.metrics.partial = Some(metrics.clone());
        Ok(Self {
            state,
            tail,
            metrics,
            _reservation: reservation,
        })
    }
    pub(crate) fn next(
        &mut self,
        input: &mut Node,
        columns: [usize; 2],
        schema: &SchemaRef,
        id: OperatorId,
        cx: &mut ExecContext,
    ) -> Result<Option<RecordBatch>> {
        loop {
            cx.cancellation.check()?;
            match &mut self.state {
                State::Hash(state) => {
                    if state.as_ref().is_some_and(|a| !a.built) {
                        let mut aggregate = *state
                            .take()
                            .ok_or_else(|| Error::Invalid("missing partial hash state".into()))?;
                        if let Some((batch, _, _)) = self.tail.take() {
                            aggregate.ingest(&batch, columns[0], columns[1], None, id, cx)?;
                        }
                        *state = Some(Box::new(Aggregate::build(
                            input,
                            columns,
                            None,
                            id,
                            cx,
                            Some(aggregate),
                        )?));
                    }
                    let start = Instant::now();
                    let output = state
                        .as_mut()
                        .ok_or_else(|| Error::Invalid("missing partial hash state".into()))?
                        .next(schema, id, cx)?;
                    cx.work(id, start);
                    return Ok(output);
                }
                State::Drain(drain) => {
                    let start = Instant::now();
                    let rows = cx.config.output_batch_rows;
                    let mut reservation = cx.memory.reservation(id);
                    reservation.grow(
                        rows.checked_mul(24)
                            .and_then(|n| n.checked_add(1024))
                            .ok_or(Error::Overflow("partial output"))?,
                    )?;
                    let mut keys = Vec::with_capacity(rows);
                    let mut sums = Vec::with_capacity(rows);
                    let mut counts = Vec::with_capacity(rows);
                    for _ in 0..rows {
                        let Some((key, sum, count)) = drain.next(cx)? else {
                            break;
                        };
                        self.metrics.flushed_count = self
                            .metrics
                            .flushed_count
                            .checked_add(count)
                            .ok_or(Error::Overflow("partial flushed count"))?;
                        keys.push(key);
                        sums.push(sum);
                        counts.push(count);
                    }
                    cx.work(id, start);
                    if keys.is_empty() {
                        if self.metrics.flushed_count != self.metrics.prefix_rows as u64 {
                            return Err(Error::CorruptSpill("partial prefix count conservation"));
                        }
                        self.state = State::Raw;
                        self.metrics.state_released_before_tail = true;
                        cx.metrics.partial = Some(self.metrics.clone());
                        continue;
                    }
                    cx.metrics.partial = Some(self.metrics.clone());
                    return Ok(Some(RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(UInt64Array::from(keys)),
                            Arc::new(Int64Array::from(sums)),
                            Arc::new(UInt64Array::from(counts)),
                        ],
                    )?));
                }
                State::Raw => return self.raw(input, columns, schema, id, cx),
            }
        }
    }
    fn raw(
        &mut self,
        input: &mut Node,
        columns: [usize; 2],
        schema: &SchemaRef,
        id: OperatorId,
        cx: &mut ExecContext,
    ) -> Result<Option<RecordBatch>> {
        if self.tail.is_none() {
            let Some(batch) = input.next_batch(cx)? else {
                input.shutdown(cx)?;
                cx.metrics.partial = Some(self.metrics.clone());
                return Ok(None);
            };
            self.tail = Some((batch, 0, false));
        }
        let (batch, offset, counted) = self
            .tail
            .as_mut()
            .ok_or_else(|| Error::Invalid("missing raw input".into()))?;
        if !*counted {
            cx.input(id, batch);
            *counted = true;
        }
        let start = Instant::now();
        let rows = (batch.num_rows() - *offset).min(cx.config.output_batch_rows);
        let keys = batch.column(columns[0]);
        let values = batch.column(columns[1]);
        if keys.null_count() != 0 || values.null_count() != 0 {
            return Err(Error::Invalid("raw partial nulls".into()));
        }
        let mut reservation = cx.memory.reservation(id);
        reservation.grow(
            rows.checked_mul(8)
                .and_then(|n| n.checked_add(1024))
                .ok_or(Error::Overflow("raw counts"))?,
        )?;
        let output = RecordBatch::try_new(
            schema.clone(),
            vec![
                keys.slice(*offset, rows),
                values.slice(*offset, rows),
                Arc::new(UInt64Array::from(vec![1; rows])),
            ],
        )?;
        *offset += rows;
        self.metrics.raw_rows = self
            .metrics
            .raw_rows
            .checked_add(rows as u64)
            .ok_or(Error::Overflow("raw rows"))?;
        if *offset == batch.num_rows() {
            self.tail.take();
        }
        cx.work(id, start);
        cx.metrics.partial = Some(self.metrics.clone());
        Ok(Some(output))
    }
}
