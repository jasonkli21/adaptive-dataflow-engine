//! Logical IDs are independent of OS processes, sockets and attempts.
use serde::{Deserialize, Serialize};
macro_rules! id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        pub struct $name(pub u64);
    };
}
id!(JobId);
id!(WorkerId);
id!(TaskId);
id!(AttemptId);
id!(ExchangeId);
id!(PartitionId);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferId {
    pub job: JobId,
    pub exchange: ExchangeId,
    pub partition: PartitionId,
    pub producer: TaskId,
    pub attempt: AttemptId,
}
#[must_use]
pub fn transfer(job: JobId, exchange: u64, partition: u64, producer: u64) -> TransferId {
    TransferId {
        job,
        exchange: ExchangeId(exchange),
        partition: PartitionId(partition),
        producer: TaskId(producer),
        attempt: AttemptId(0),
    }
}
