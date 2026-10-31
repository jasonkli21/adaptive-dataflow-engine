//! Default-off atomic local state/output epochs. Only the committed reader exposes output.
pub mod epoch;
pub mod migration;
pub mod savepoint;
pub mod sink;

use crate::{
    Engine, Error, RawRecord, Result,
    state::{self, check},
};
use ade_exec::CancellationToken;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub root_bytes: u64,
    pub stage_bytes: u64,
    pub output_bytes: u64,
    pub history_bytes: u64,
    pub epochs: usize,
    pub diagnostic_bytes: u64,
    pub savepoint_bytes: u64,
    pub savepoints: usize,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u32,
    /// Internal native-state descriptor; its v1 wire contract is unchanged.
    pub state: state::Config,
    pub codec: u32,
    pub trusted_history: String,
    pub limits: Limits,
}
impl Config {
    /// # Errors
    /// Strict bounded config admission before filesystem/network effects.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        check(bytes.len() <= 32_768, "v3 config cap")?;
        let c: Self = epoch::json(bytes)?;
        c.validate()?;
        Ok(c)
    }
    /// # Errors
    /// Rejects unbounded/contradictory capacities and unsupported formats.
    pub fn validate(&self) -> Result<()> {
        self.state.validate()?;
        check(
            self.version == 3 && [1, 2].contains(&self.codec),
            "v3 version/codec",
        )?;
        check(
            !self.trusted_history.is_empty()
                && self.trusted_history.len() <= 128
                && self
                    .trusted_history
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b)),
            "immutable history attestation",
        )?;
        let l = &self.limits;
        check(
            (131_072..=268_435_456).contains(&l.root_bytes)
                && (65_536..=16_777_216).contains(&l.stage_bytes)
                && (2048..=8_388_608).contains(&l.output_bytes)
                && (2048..=33_554_432).contains(&l.history_bytes)
                && (2..=128).contains(&l.epochs)
                && (8192..=67_108_864).contains(&l.diagnostic_bytes)
                && (131_072..=536_870_912).contains(&l.savepoint_bytes)
                && (1..=8).contains(&l.savepoints)
                && l.stage_bytes <= l.root_bytes
                && l.output_bytes <= l.stage_bytes
                && l.output_bytes <= l.history_bytes,
            "v3 finite quotas",
        )
    }
    /// # Errors
    /// Canonical semantic/source identity includes original starts and attestation.
    pub fn fingerprint(&self) -> Result<String> {
        Ok(epoch::hash(&epoch::encode(&serde_json::json!({
            "version":3,"native":self.state.fingerprint()?,
            "starts":self.state.source.starts,"history":self.trusted_history
        }))?))
    }
}

/// One provisional epoch. Every mutation failure permanently poisons this handle.
pub struct Transaction {
    pub engine: Engine,
    config: Config,
    lease: epoch::Lease,
    selected: Option<epoch::Pointer>,
    outputs: Vec<sink::Output>,
    output_bytes: u64,
    poisoned: bool,
}
impl Transaction {
    /// # Errors
    /// Validates the entire committed closure before rebuilding native owner stores.
    pub fn open(config: Config, resume: bool, token: CancellationToken) -> Result<Self> {
        config.validate()?;
        let lease = epoch::Lease::acquire(&config)?;
        let selected = epoch::selection(&config.state.checkpoint_root)?;
        check(
            resume || selected.is_none(),
            "existing v3 lineage requires resume",
        )?;
        check(!resume || selected.is_some(), "resume requires CURRENT3")?;
        let closure = selected
            .as_ref()
            .map(|p| epoch::verify(&config, p))
            .transpose()?;
        let session = Uuid::new_v4();
        let mut engine = Engine::new(config.state.source.clone(), session)?;
        let state = state::State::transactional(config.state.clone(), session, token)?;
        engine.state = Some(state);
        if let Some(epochs) = closure {
            let last = epochs.last().ok_or(Error::State("empty lineage"))?;
            check(
                last.manifest.codec == config.codec && last.manifest.owners == config.state.owners,
                "explicit upgrade/rescale required",
            )?;
            epoch::hydrate(&config, last, &mut engine)?;
        }
        Ok(Self {
            engine,
            config,
            lease,
            selected,
            outputs: Vec::new(),
            output_bytes: 0,
            poisoned: false,
        })
    }
    /// # Errors
    /// Native admission/Arrow/state errors abort the whole provisional epoch.
    pub fn accept(&mut self, record: RawRecord) -> Result<()> {
        self.live()?;
        self.poisoned = true;
        self.engine.prepare(record)?;
        // This ACK is provisional inside the transaction, never a sink-visible ACK.
        self.engine.acknowledge()?;
        self.close()?;
        self.poisoned = false;
        Ok(())
    }
    fn close(&mut self) -> Result<()> {
        let state = self
            .engine
            .state
            .as_mut()
            .ok_or(Error::State("native state"))?;
        while let Some(window) = state.next_close(self.engine.progress.frontier)? {
            let output = sink::Output::from(window);
            let bytes = epoch::encode(&output)?.len() as u64 + 1;
            check(
                self.output_bytes
                    .checked_add(bytes)
                    .is_some_and(|n| n <= self.config.limits.output_bytes),
                "provisional output quota",
            )?;
            self.outputs.push(output);
            self.output_bytes += bytes;
            state.confirm_close()?;
        }
        Ok(())
    }
    fn live(&self) -> Result<()> {
        check(
            !self.poisoned,
            "poisoned v3 attempt; reopen authoritative CURRENT3",
        )
    }
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }
    /// # Errors
    /// Sole state+output visibility transition. Any error requires a fresh handle.
    pub fn commit(
        &mut self,
        operation: epoch::Operation,
        hook: &mut dyn FnMut(&str) -> Result<()>,
    ) -> Result<epoch::Pointer> {
        self.live()?;
        self.poisoned = true;
        self.close()?;
        self.lease.fence()?;
        check(
            epoch::selection(&self.config.state.checkpoint_root)?.as_ref()
                == self.selected.as_ref(),
            "pause parent/fence mismatch",
        )?;
        if operation == epoch::Operation::Paused
            && self.outputs.is_empty()
            && let Some(selected) = &self.selected
        {
            let closure = epoch::verify(&self.config, selected)?;
            let prior = &closure
                .last()
                .ok_or(Error::State("pause lineage"))?
                .manifest;
            if prior.operation == epoch::Operation::Paused
                && prior.progress.forwarded_records == self.engine.progress.forwarded_records
            {
                self.poisoned = false;
                return Ok(selected.clone());
            }
        }
        let pointer = epoch::publish(
            &self.config,
            &self.lease,
            self.selected.as_ref(),
            &self.engine,
            &self.outputs,
            operation,
            hook,
        )?;
        self.selected = Some(pointer.clone());
        self.outputs.clear();
        self.output_bytes = 0;
        self.poisoned = false;
        Ok(pointer)
    }
    /// # Errors
    /// Only compatible codec1→2 and logical owner changes are admitted, in one epoch.
    pub fn transition(
        config: Config,
        codec: u32,
        owners: usize,
        hook: &mut dyn FnMut(&str) -> Result<()>,
    ) -> Result<epoch::Pointer> {
        let mut txn = Self::open(config, true, CancellationToken::default())?;
        check(
            (codec == txn.config.codec || (txn.config.codec == 1 && codec == 2))
                && [1, 2, 4].contains(&owners)
                && (codec != txn.config.codec || owners != txn.config.state.owners),
            "unsupported/replayed migration; no physical rescale",
        )?;
        txn.config.codec = codec;
        if owners != txn.config.state.owners {
            let old = txn
                .engine
                .state
                .take()
                .ok_or(Error::State("native state"))?;
            txn.config.state.owners = owners;
            txn.config.validate()?;
            // Canonical immutable closure is the transfer source; old lossy tier is dropped.
            let late = old.closed_window_late;
            drop(old);
            let mut next = state::State::transactional(
                txn.config.state.clone(),
                txn.engine.progress.session,
                CancellationToken::default(),
            )?;
            let pointer = txn
                .selected
                .as_ref()
                .ok_or(Error::State("migration pointer"))?;
            let epochs = epoch::verify_codec(&txn.config, pointer, false)?;
            epoch::entries(
                &txn.config,
                epochs.last().ok_or(Error::State("migration epoch"))?,
                |e| next.store.restore_entry(e),
            )?;
            next.closed_window_late = late;
            txn.engine.state = Some(next);
        }
        txn.commit(epoch::Operation::Transition, hook)
    }
}
