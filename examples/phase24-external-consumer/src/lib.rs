//! An independently owned calibration map authored against the public SDK.

use ade_ext_sdk::{
    Capability, Error, Extension, ExtensionDescriptorV1, Field, Result, Row, RowKernel, Type, Value,
};

pub struct Calibration;
pub struct Calibrate(i64);

impl Extension for Calibration {
    type Kernel = Calibrate;

    fn descriptor(&self) -> ExtensionDescriptorV1 {
        ExtensionDescriptorV1 {
            abi: 1,
            id: "example.calibration".into(),
            version: 1,
            capability: Capability::LocalReplayableRowMap,
            schema: vec![
                Field::new("row_id", Type::U64, false),
                Field::new("value_i64", Type::I64, false),
                Field::new("sensor", Type::U64, false),
            ],
            mutable: vec![1],
        }
    }

    fn prepare(&self, model: &[u8]) -> Result<Calibrate> {
        let offset = i64::from_le_bytes(model.try_into().map_err(|_| Error::Model)?);
        Ok(Calibrate(offset))
    }
}

impl RowKernel for Calibrate {
    fn transform(&self, row: &mut Row) -> Result<()> {
        let (Some(Value::I64(raw)), Some(Value::U64(sensor))) = (row.get(1), row.get(2)) else {
            return Err(Error::Kernel("calibration fields"));
        };
        let signed_offset = if sensor % 2 == 0 {
            self.0
        } else {
            self.0
                .checked_neg()
                .ok_or(Error::Kernel("offset overflow"))?
        };
        let calibrated = raw
            .checked_mul(2)
            .and_then(|value| value.checked_add(signed_offset))
            .ok_or(Error::Kernel("calibration overflow"))?;
        row.set(1, Value::I64(calibrated.clamp(-100, 100)))
    }
}
