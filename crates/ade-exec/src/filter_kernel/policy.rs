//! Pure selection from verified, frozen observations.
use super::{MAX_ROWS, Reason};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

const PAIRS: usize = 7;
const MAX_OBSERVATION: u64 = 1_000_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Shape {
    pub rows: usize,
    pub nulls: usize,
    pub threshold: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Measurements {
    pub filter_ns: [u64; PAIRS],
    pub query_ns: [u64; PAIRS],
    pub process_ns: [u64; PAIRS],
    pub rss_bytes: [u64; PAIRS],
}

impl Measurements {
    fn valid(&self) -> bool {
        (0..PAIRS).all(|i| {
            self.filter_ns[i] > 0
                && self.filter_ns[i] <= self.query_ns[i]
                && self.query_ns[i] <= self.process_ns[i]
                && self.process_ns[i] <= MAX_OBSERVATION
                && (1..=MAX_OBSERVATION).contains(&self.rss_bytes[i])
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Prediction {
    pub shape: Shape,
    pub scalar_filter_ns: u64,
    pub arrow_filter_ns: u64,
    pub neon_filter_ns: u64,
    pub pairs: usize,
    pub min_paired_query_gain_basis_points: i64,
    pub process_headroom_ns: u64,
    pub admission_ns: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum ProfileRefusal {
    Missing,
    Invalid,
    Incompatible,
}

#[derive(Debug, Clone)]
pub struct AutoSelection {
    profile: std::result::Result<(Prediction, bool), ProfileRefusal>,
}

impl AutoSelection {
    #[must_use]
    pub fn unavailable(reason: ProfileRefusal) -> Self {
        Self {
            profile: Err(reason),
        }
    }

    /// Derives a prediction from seven matched observations per arm. The caller
    /// supplies trusted provenance and binds the profile to the current source,
    /// workload, executable and CPU before admitting it.
    /// # Errors
    /// Refuses impossible, out-of-range or incomplete observations and shapes.
    pub fn from_verified_samples(
        shape: Shape,
        full_batches: usize,
        scalar: &Measurements,
        arrow: &Measurements,
        neon: &Measurements,
    ) -> Result<Self> {
        if !(8..=MAX_ROWS).contains(&shape.rows)
            || shape.nulls > shape.rows
            || !(1..=1_000_000).contains(&full_batches)
            || ![scalar, arrow, neon].into_iter().all(Measurements::valid)
        {
            return Err(Error::Invalid(
                "invalid filter calibration observations".into(),
            ));
        }
        let minimum_gain = [scalar, arrow]
            .into_iter()
            .flat_map(|control| {
                (0..PAIRS).map(move |i| gain(control.query_ns[i], neon.query_ns[i]))
            })
            .min()
            .unwrap_or_default();
        let process_headroom_ns = [scalar, arrow]
            .into_iter()
            .flat_map(|control| {
                (0..PAIRS).map(move |i| {
                    (control.process_ns[i] * 105 / 100).saturating_sub(neon.process_ns[i])
                })
            })
            .min()
            .unwrap_or_default();
        let favorable = minimum_gain >= 500
            && [scalar, arrow].into_iter().all(|control| {
                (0..PAIRS).all(|i| {
                    gain(control.filter_ns[i], neon.filter_ns[i]) >= 500
                        && neon.process_ns[i] * 100 <= control.process_ns[i] * 105
                        && neon.rss_bytes[i] * 100 <= control.rss_bytes[i] * 105
                })
            });
        Ok(Self {
            profile: Ok((
                Prediction {
                    shape,
                    scalar_filter_ns: median(scalar.filter_ns) / full_batches as u64,
                    arrow_filter_ns: median(arrow.filter_ns) / full_batches as u64,
                    neon_filter_ns: median(neon.filter_ns) / full_batches as u64,
                    pairs: PAIRS,
                    min_paired_query_gain_basis_points: minimum_gain,
                    process_headroom_ns,
                    admission_ns: 0,
                },
                favorable,
            )),
        })
    }

    /// Include extra profile admission work in the process-wall comparison.
    #[must_use]
    pub fn with_admission_cost(mut self, elapsed_ns: u64) -> Self {
        if let Ok((prediction, eligible)) = &mut self.profile {
            prediction.admission_ns = elapsed_ns;
            *eligible &= elapsed_ns <= prediction.process_headroom_ns;
        }
        self
    }

    pub(super) fn prediction(&self) -> Option<Prediction> {
        self.profile.ok().map(|(prediction, _)| prediction)
    }

    pub(super) fn select(&self, shape: Shape) -> Reason {
        match self.profile {
            Err(ProfileRefusal::Missing) => Reason::MissingProfile,
            Err(ProfileRefusal::Invalid) => Reason::InvalidProfile,
            Err(ProfileRefusal::Incompatible) => Reason::IncompatibleProfile,
            Ok((prediction, _)) if prediction.shape != shape => Reason::UnknownShape,
            Ok((_, true)) => Reason::MeasuredGain,
            Ok(_) => Reason::InsufficientGain,
        }
    }
}

fn gain(control: u64, candidate: u64) -> i64 {
    // The bounds checked at admission keep all products below i64::MAX.
    (control.cast_signed() - candidate.cast_signed()) * 10_000 / control.cast_signed()
}

fn median(mut values: [u64; PAIRS]) -> u64 {
    values.sort_unstable();
    values[PAIRS / 2]
}
