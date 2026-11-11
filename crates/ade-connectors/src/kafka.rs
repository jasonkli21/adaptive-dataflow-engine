//! Stateless Kafka catalog facade. The native source and engine own all progress.
use crate::{
    Error, Result,
    contract::{hash, identifier},
};
use ade_stream::{Engine, StreamConfigV1, kafka::KafkaSource, unified::Admission};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KafkaEntryV1 {
    pub version: u32,
    pub name: String,
    pub native_spec_blake3: String,
}

pub struct BoundKafka {
    admission: Admission,
    config: StreamConfigV1,
}

impl KafkaEntryV1 {
    #[must_use]
    pub fn capabilities(&self) -> crate::SourceCapabilitiesV1 {
        crate::SourceCapabilitiesV1 {
            bounded: false,
            replay: crate::ReplayCapability::TraceRelativeKafka,
            native_offsets: true,
            checkpoint_restore: false,
            pushdown: "none",
            max_splits: None,
        }
    }

    /// Bind a pinned, explicitly unbounded native stream. The config is an
    /// operational compatibility assertion. The compiled native spec owns
    /// cluster, topic, starts, history/live cut and transform semantics.
    ///
    /// # Errors
    /// Refuses wrong identity, plan kind or incompatible runtime configuration.
    pub fn bind(&self, logical: &[u8], config: &StreamConfigV1) -> Result<BoundKafka> {
        if self.version != 1 || !identifier(&self.name) || !hash(&self.native_spec_blake3) {
            return Err(Error::Admission("Kafka catalog entry"));
        }
        let admission =
            Admission::compile(logical).map_err(|_| Error::Admission("native Kafka plan"))?;
        if admission.native_spec_hash() != self.native_spec_blake3 {
            return Err(Error::Identity("native Kafka spec"));
        }
        let config = admission
            .bind_stateless(config)
            .map_err(|_| Error::Admission("native Kafka config"))?;
        Ok(BoundKafka { admission, config })
    }
}

impl BoundKafka {
    /// # Errors
    /// Native Kafka admission can reject broker identity, partitions or offsets.
    pub fn connect(self, session: Uuid) -> Result<(KafkaSource, Engine)> {
        let engine = Engine::new_unified(&self.config, session, &self.admission)
            .map_err(|_| Error::Admission("native Kafka engine"))?;
        let source = KafkaSource::connect_unified(
            self.config,
            session,
            &engine.progress,
            Some(self.admission),
        )
        .map_err(|_| Error::Protocol("native Kafka connection or source admission"))?;
        Ok((source, engine))
    }
}
