#![cfg(all(unix, feature = "local-v1"))]
//! Finite Float64 vector preparation and fixed-model scoring for the ADE extension SDK.

use ade_ext_sdk::{
    Capability, Error, Extension, ExtensionDescriptorV1, Field, Result, Row, RowKernel, Type, Value,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Normalize,
    Score,
    NormalizeThenScore,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Model {
    weights: Vec<f64>,
    bias: f64,
}

/// The operation and dimension are static for the entire query. Model weights
/// must exactly cover the active leading features, including for preparation.
#[derive(Debug, Clone, Copy)]
pub struct VectorV1 {
    pub operation: Operation,
    pub dimensions: u8,
}

pub struct Kernel {
    operation: Operation,
    weights: [f64; 4],
    bias: f64,
    dimensions: usize,
}

impl Extension for VectorV1 {
    type Kernel = Kernel;

    fn descriptor(&self) -> ExtensionDescriptorV1 {
        let op = match self.operation {
            Operation::Normalize => "l2-normalize",
            Operation::Score => "affine-score",
            Operation::NormalizeThenScore => "l2-normalize-affine-score",
        };
        let mut mutable = Vec::new();
        if self.operation != Operation::Score {
            mutable.extend(1..=usize::from(self.dimensions.min(4)));
        }
        if self.operation != Operation::Normalize {
            mutable.push(5);
        }
        ExtensionDescriptorV1 {
            abi: 1,
            id: format!("ade.vector.{op}.d{}", self.dimensions),
            version: 1,
            capability: Capability::LocalReplayableRowMap,
            schema: [
                Field::new("row_id", Type::U64, false),
                Field::new("f0", Type::F64, false),
                Field::new("f1", Type::F64, false),
                Field::new("f2", Type::F64, false),
                Field::new("f3", Type::F64, false),
                Field::new("score", Type::F64, false),
            ]
            .into(),
            mutable,
        }
    }

    fn prepare(&self, bytes: &[u8]) -> Result<Self::Kernel> {
        if !(1..=4).contains(&self.dimensions) || bytes.len() > ade_ext_sdk::MAX_MODEL_BYTES {
            return Err(Error::Model);
        }
        let model: Model = serde_json::from_slice(bytes).map_err(|_| Error::Model)?;
        let dimensions = usize::from(self.dimensions);
        if model.weights.len() != dimensions
            || model.weights.iter().any(|x| !x.is_finite())
            || !model.bias.is_finite()
        {
            return Err(Error::Model);
        }
        let mut weights = [0.0; 4];
        weights[..dimensions].copy_from_slice(&model.weights);
        Ok(Kernel {
            operation: self.operation,
            weights,
            bias: model.bias,
            dimensions,
        })
    }
}

impl RowKernel for Kernel {
    fn transform(&self, row: &mut Row) -> Result<()> {
        let mut storage = [0.0; 4];
        let features = &mut storage[..self.dimensions];
        for (i, feature) in features.iter_mut().enumerate() {
            let Some(Value::F64(value)) = row.get(i + 1) else {
                return Err(Error::Kernel("expected Float64 feature"));
            };
            *feature = value;
        }
        if self.operation != Operation::Score {
            // Divide first so squaring never overflows or underflows the entire norm.
            let scale = features
                .iter()
                .fold(0.0_f64, |max, value| max.max(value.abs()));
            if scale == 0.0 {
                return Err(Error::Kernel("zero vector"));
            }
            let mut squares = 0.0;
            for value in &mut *features {
                *value /= scale;
                squares += *value * *value;
            }
            let norm = squares.sqrt();
            for (i, value) in features.iter_mut().enumerate() {
                *value /= norm;
                row.set(i + 1, Value::F64(*value))?;
            }
        }
        if self.operation != Operation::Normalize {
            let mut score = self.bias;
            for (&feature, weight) in features.iter().zip(self.weights) {
                let product = feature * weight;
                if !product.is_finite() {
                    return Err(Error::Kernel("nonfinite affine product"));
                }
                score += product;
                if !score.is_finite() {
                    return Err(Error::Kernel("nonfinite affine sum"));
                }
            }
            row.set(5, Value::F64(score))?;
        }
        Ok(())
    }
}
