//! Manual Kafka assignment; public rdkafka 0.39 / librdkafka 2.12 APIs only.
use crate::{Error, Poll, Progress, RawRecord, Result, Security, Source, StreamConfigV1};
use rdkafka::{
    ClientConfig, Message, Offset, TopicPartitionList,
    admin::{AdminClient, AdminOptions, ResourceSpecifier},
    client::ClientContext,
    config::RDKafkaLogLevel,
    consumer::{BaseConsumer, Consumer, ConsumerContext, base_consumer::PartitionQueue},
    error::KafkaError,
};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use uuid::Uuid;
const RPC: Duration = Duration::from_millis(1000);
const RESPONSE_BYTES: u64 = 65536;
#[derive(Default)]
struct Counters {
    failed: AtomicBool,
    queue_bytes_peak: AtomicU64,
    queue_count_peak: AtomicU64,
    #[cfg(feature = "stateful")]
    control_poll_ticks: AtomicU64,
    #[cfg(feature = "stateful")]
    stats_callbacks: AtomicU64,
    #[cfg(feature = "stateful")]
    control_replyq_peak: AtomicU64,
}
struct Context(Arc<Counters>);
impl ClientContext for Context {
    fn log(&self, _: RDKafkaLogLevel, _: &str, _: &str) {} // never print broker/auth details
    fn error(&self, _: KafkaError, _: &str) {
        self.0.failed.store(true, Ordering::Relaxed);
    }
    fn stats_raw(&self, raw: &[u8]) {
        #[cfg(feature = "stateful")]
        self.0.stats_callbacks.fetch_add(1, Ordering::Relaxed);
        if raw.len() > 65536 {
            self.0.failed.store(true, Ordering::Relaxed);
            return;
        }
        // The raw size bounds decoding. Never retain a history or raw payload.
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(raw) else {
            self.0.failed.store(true, Ordering::Relaxed);
            return;
        };
        #[cfg(feature = "stateful")]
        self.0.control_replyq_peak.fetch_max(
            value
                .get("replyq")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0),
            Ordering::Relaxed,
        );
        let mut bytes = 0u64;
        let mut count = 0u64;
        if let Some(topics) = value.get("topics").and_then(serde_json::Value::as_object) {
            for topic in topics.values() {
                if let Some(partitions) = topic
                    .get("partitions")
                    .and_then(serde_json::Value::as_object)
                {
                    for p in partitions.values() {
                        bytes = bytes.saturating_add(
                            p.get("fetchq_size")
                                .and_then(serde_json::Value::as_u64)
                                .unwrap_or(0),
                        );
                        count = count.saturating_add(
                            p.get("fetchq_cnt")
                                .and_then(serde_json::Value::as_u64)
                                .unwrap_or(0),
                        );
                    }
                }
            }
        }
        self.0.queue_bytes_peak.fetch_max(bytes, Ordering::Relaxed);
        self.0.queue_count_peak.fetch_max(count, Ordering::Relaxed);
    }
}
impl ConsumerContext for Context {}
#[derive(Debug, Serialize)]
pub struct BrokerMetrics {
    pub queue_bytes_peak: u64,
    pub queue_count_peak: u64,
    pub queue_target_bytes: u64,
    pub response_cap_bytes: u64,
    pub brokers_cap: usize,
    pub backlog: BTreeMap<i32, i64>,
}
pub struct KafkaSource {
    consumer: Arc<BaseConsumer<Context>>,
    queues: Vec<PartitionQueue<Context>>,
    admin: AdminClient<Context>,
    next_partition: usize,
    counters: Arc<Counters>,
    config: StreamConfigV1,
    session: Uuid,
    assignment: TopicPartitionList,
    partition_inventory: Vec<i32>,
    cluster_id: Option<String>,
    high: BTreeMap<i32, i64>,
    paused: bool,
    #[cfg(feature = "stateful")]
    checkpoint_control: Option<(Arc<AtomicBool>, std::thread::JoinHandle<()>)>,
}
impl KafkaSource {
    /// # Errors
    /// Fails closed on admission, broker, metadata, security or offset errors.
    #[allow(
        clippy::too_many_lines,
        reason = "Explicit client security, fetch and assignment admission in one transition"
    )]
    pub fn connect(config: StreamConfigV1, session: Uuid, progress: &Progress) -> Result<Self> {
        let mut source = Self::unassigned(config, session)?;
        source.verify(progress)?;
        source.assign()?;
        Ok(source)
    }

    /// Check source identity and both recovery offset vectors without assigning
    /// partitions or polling data. `connect` repeats the selected-vector check
    /// immediately before the final assignment.
    /// # Errors
    /// Refuses source identity drift, broker metadata failures or invalid config.
    #[cfg(feature = "adaptive-checkpoint")]
    pub fn preflight_recovery(
        config: StreamConfigV1,
        session: Uuid,
        replay_starts: &[crate::Start],
        restore_starts: &[crate::Start],
    ) -> Result<crate::adaptive::policy::RecoverySources> {
        let mut source = Self::unassigned(config, session)?;
        source.verify_identity()?;
        let mut expected = source
            .config
            .starts
            .iter()
            .map(|s| s.partition)
            .collect::<Vec<_>>();
        expected.sort_unstable();
        let replay = source.candidate_ranges(replay_starts, &expected, "origin_retention");
        let restore = source.candidate_ranges(restore_starts, &expected, "saved_offset_retention");
        source.healthy()?;
        Ok(crate::adaptive::policy::RecoverySources {
            cluster_id: source
                .cluster_id
                .clone()
                .ok_or(Error::Source("missing cluster ID"))?,
            partition_inventory: source.partition_inventory.clone(),
            replay,
            restore,
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Explicit client security and bounded unassigned metadata setup"
    )]
    fn unassigned(config: StreamConfigV1, session: Uuid) -> Result<Self> {
        config.validate()?;
        let counters = Arc::new(Counters::default());
        let mut client = ClientConfig::new();
        client
            .set("bootstrap.servers", &config.bootstrap)
            .set("group.id", format!("ade-manual-{session}"))
            .set("client.id", format!("ade-stream-{session}"))
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("allow.auto.create.topics", "false")
            .set("auto.offset.reset", "error")
            .set("enable.partition.eof", "false")
            .set("isolation.level", "read_committed")
            .set("queued.min.messages", "1")
            .set(
                "queued.max.messages.kbytes",
                config.client_queue_kib.to_string(),
            )
            .set("message.max.bytes", "16384")
            .set("fetch.message.max.bytes", "16384")
            .set("fetch.max.bytes", "32768")
            .set("receive.message.max.bytes", RESPONSE_BYTES.to_string())
            .set("fetch.wait.max.ms", "100")
            .set("fetch.queue.backoff.ms", "100")
            .set("socket.timeout.ms", "1000")
            .set("socket.connection.setup.timeout.ms", "1000")
            .set("topic.metadata.refresh.interval.ms", "1000")
            .set("metadata.max.age.ms", "1000")
            .set("metadata.recovery.strategy", "none")
            .set("statistics.interval.ms", "500")
            .set("log_level", "0");
        match &config.security {
            Security::LoopbackDev {} => {
                client.set("security.protocol", "PLAINTEXT");
            }
            Security::SaslSsl {
                username_env,
                password_env,
                ca_path,
            } => {
                let metadata =
                    std::fs::metadata(ca_path).map_err(|_| Error::Admission("CA regular file"))?;
                crate::require(
                    metadata.is_file() && metadata.len() <= 1024 * 1024,
                    "CA regular file <=1MiB",
                )?;
                let user = std::env::var(username_env)
                    .map_err(|_| Error::Admission("missing credential environment"))?;
                let password = std::env::var(password_env)
                    .map_err(|_| Error::Admission("missing credential environment"))?;
                crate::require(
                    !user.is_empty()
                        && user.len() <= 4096
                        && !password.is_empty()
                        && password.len() <= 4096,
                    "credential length",
                )?;
                client
                    .set("security.protocol", "SASL_SSL")
                    .set("sasl.mechanism", "PLAIN")
                    .set("sasl.username", user)
                    .set("sasl.password", password)
                    .set("ssl.ca.location", ca_path)
                    .set("enable.ssl.certificate.verification", "true")
                    .set("ssl.endpoint.identification.algorithm", "https");
            }
        }
        let mut admin_config = client.clone();
        admin_config.remove("group.id");
        admin_config.set("statistics.interval.ms", "0");
        let admin = admin_config
            .create_with_context(Context(counters.clone()))
            .map_err(|_| Error::Broker)?;
        let consumer: BaseConsumer<Context> = client
            .create_with_context(Context(counters.clone()))
            .map_err(|_| Error::Broker)?;
        let source = Self {
            consumer: Arc::new(consumer),
            queues: Vec::new(),
            admin,
            next_partition: 0,
            counters,
            config,
            session,
            assignment: TopicPartitionList::new(),
            partition_inventory: Vec::new(),
            cluster_id: None,
            high: BTreeMap::new(),
            paused: false,
            #[cfg(feature = "stateful")]
            checkpoint_control: None,
        };
        Ok(source)
    }

    fn assign(&mut self) -> Result<()> {
        for start in &self.config.starts {
            self.assignment
                .add_partition_offset(
                    &self.config.topic,
                    start.partition,
                    Offset::Offset(start.offset),
                )
                .map_err(|_| Error::Broker)?;
        }
        self.consumer
            .assign(&self.assignment)
            .map_err(|_| Error::Broker)?;
        for start in &self.config.starts {
            self.queues.push(
                self.consumer
                    .split_partition_queue(&self.config.topic, start.partition)
                    .ok_or(Error::Source("partition queue"))?,
            );
        }
        Ok(())
    }

    #[cfg(feature = "adaptive-checkpoint")]
    fn candidate_ranges(
        &self,
        starts: &[crate::Start],
        expected: &[i32],
        expired_reason: &'static str,
    ) -> crate::adaptive::policy::CandidateEligibility {
        classify_candidate_ranges(starts, expected, expired_reason, |partition| {
            if !self.partition_inventory.contains(&partition) {
                return Err(());
            }
            self.consumer
                .fetch_watermarks(&self.config.topic, partition, RPC)
                .map_err(|_| ())
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Single bounded source identity and immutable-topic verification"
    )]
    fn verify_identity(&mut self) -> Result<()> {
        if self.assignment.count() > 0 {
            self.control()?;
        } else {
            self.healthy()?;
        }
        let metadata = self
            .consumer
            .fetch_metadata(Some(&self.config.topic), RPC)
            .map_err(|_| Error::Broker)?;
        if metadata.brokers().is_empty() || metadata.brokers().len() > 4 {
            return Err(Error::Source("broker inventory cap"));
        }
        if matches!(self.config.security, Security::LoopbackDev {}) {
            for broker in metadata.brokers() {
                let loopback = broker.host() == "localhost"
                    || broker
                        .host()
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|ip| ip.is_loopback());
                if !loopback {
                    return Err(Error::Source("non-loopback advertised broker"));
                }
            }
        }
        let cluster = self
            .consumer
            .client()
            .fetch_cluster_id(RPC)
            .ok_or(Error::Source("missing cluster ID"))?;
        if cluster != self.config.expected_cluster_id {
            return Err(Error::Source("cluster ID changed"));
        }
        self.cluster_id = Some(cluster);
        let options = AdminOptions::new().request_timeout(Some(RPC));
        let resources = futures_executor::block_on(
            self.admin
                .describe_configs(&[ResourceSpecifier::Topic(&self.config.topic)], &options),
        )
        .map_err(|_| Error::Broker)?;
        if resources.len() != 1 {
            return Err(Error::Source("topic config inventory"));
        }
        let resource = resources[0].as_ref().map_err(|_| Error::Broker)?;
        let compression = resource
            .get("compression.type")
            .and_then(|entry| entry.value.as_deref());
        let message_cap = resource
            .get("max.message.bytes")
            .and_then(|entry| entry.value.as_deref())
            .and_then(|value| value.parse::<u64>().ok());
        if compression != Some("uncompressed")
            || !message_cap.is_some_and(|bytes| bytes <= 32768 && bytes > 0)
        {
            return Err(Error::Source(
                "topic requires immutable uncompressed <=32KiB batches",
            ));
        }
        let topics = metadata.topics();
        if topics.len() != 1 || topics[0].name() != self.config.topic || topics[0].error().is_some()
        {
            return Err(Error::Source("topic metadata"));
        }
        let topic = &topics[0];
        let mut inventory = topic
            .partitions()
            .iter()
            .map(rdkafka::metadata::MetadataPartition::id)
            .collect::<Vec<_>>();
        inventory.sort_unstable();
        if inventory.is_empty()
            || inventory.len() > 4
            || topic
                .partitions()
                .iter()
                .any(|p| p.error().is_some() || p.leader() < 0)
            || (!self.partition_inventory.is_empty() && inventory != self.partition_inventory)
        {
            return Err(Error::Source("partition inventory/leader"));
        }
        self.partition_inventory = inventory;
        Ok(())
    }

    #[cfg(feature = "stateful")]
    #[must_use]
    pub fn partition_inventory(&self) -> &[i32] {
        &self.partition_inventory
    }
    /// Keep native control/statistics drained even during synchronous state I/O.
    /// Partition data queues remain exclusively polled by the demand owner.
    /// # Errors
    /// Refuses duplicate setup or OS thread creation failure.
    #[cfg(feature = "stateful")]
    pub fn enable_checkpoint_control(&mut self) -> Result<()> {
        crate::require(
            self.checkpoint_control.is_none(),
            "checkpoint control already started",
        )?;
        let stop = Arc::new(AtomicBool::new(false));
        let signal = stop.clone();
        let consumer = self.consumer.clone();
        let counters = self.counters.clone();
        let handle = std::thread::Builder::new()
            .name("ade-state-control".into())
            .stack_size(1024 * 1024)
            .spawn(move || {
                while !signal.load(Ordering::Relaxed) {
                    counters.control_poll_ticks.fetch_add(1, Ordering::Relaxed);
                    if consumer.poll(Duration::from_millis(10)).is_some() {
                        counters.failed.store(true, Ordering::Relaxed);
                    }
                }
            })
            .map_err(|_| Error::Source("checkpoint control thread creation"))?;
        self.checkpoint_control = Some((stop, handle));
        Ok(())
    }
    #[cfg(feature = "stateful")]
    fn stop_checkpoint_control(&mut self) -> Result<()> {
        if let Some((stop, handle)) = self.checkpoint_control.take() {
            stop.store(true, Ordering::Relaxed);
            handle
                .join()
                .map_err(|_| Error::Source("checkpoint control thread failed"))?;
        }
        Ok(())
    }
    /// Close explicitly before reporting terminal success; drain control only.
    /// # Errors
    /// Refuses a consumer that does not close within the cooperative shutdown budget.
    pub fn shutdown(&mut self) -> Result<()> {
        #[cfg(feature = "stateful")]
        self.stop_checkpoint_control()?;
        self.queues.clear();
        self.consumer.unassign().map_err(|_| Error::Broker)?;
        self.consumer.close_queue().map_err(|_| Error::Broker)?;
        let began = std::time::Instant::now();
        while !self.consumer.closed() {
            let _ = self.consumer.poll(Duration::from_millis(10));
            if began.elapsed() > Duration::from_secs(2) {
                return Err(Error::Source("consumer close deadline"));
            }
        }
        Ok(())
    }
    #[must_use]
    pub fn metrics(&self, progress: &Progress) -> BrokerMetrics {
        BrokerMetrics {
            queue_bytes_peak: self.counters.queue_bytes_peak.load(Ordering::Relaxed),
            queue_count_peak: self.counters.queue_count_peak.load(Ordering::Relaxed),
            queue_target_bytes: u64::from(self.config.client_queue_kib) * 1024,
            response_cap_bytes: RESPONSE_BYTES,
            brokers_cap: 4,
            backlog: self
                .high
                .iter()
                .map(|(p, high)| {
                    (
                        *p,
                        high.saturating_sub(progress.partitions[p].next_offset_suggestion),
                    )
                })
                .collect(),
        }
    }
    /// Verify the complete original replay range before any output. The trusted
    /// immutable-topic attestation remains necessary; no public topic UUID claim.
    /// # Errors
    /// Refuses original history loss, topic identity drift or inventory mismatch.
    #[cfg(feature = "adaptive-checkpoint")]
    pub fn verify_original_starts(&mut self, starts: &[crate::Start]) -> Result<()> {
        for start in starts {
            let (low, high) = self
                .consumer
                .fetch_watermarks(&self.config.topic, start.partition, RPC)
                .map_err(|_| Error::Broker)?;
            crate::require(
                self.partition_inventory.contains(&start.partition)
                    && low <= start.offset
                    && start.offset <= high,
                "complete replay origin no longer retained",
            )?;
        }
        self.healthy()
    }
    fn control(&self) -> Result<()> {
        #[cfg(feature = "stateful")]
        if let Some((_, handle)) = &self.checkpoint_control {
            if handle.is_finished() {
                return Err(Error::Source("checkpoint control thread stopped"));
            }
            return self.healthy();
        }
        for _ in 0..8 {
            if self.consumer.poll(Duration::ZERO).is_some() {
                return Err(Error::Source("unexpected main-queue data/error"));
            }
        }
        self.healthy()
    }
    fn healthy(&self) -> Result<()> {
        if self.counters.failed.load(Ordering::Relaxed) || self.consumer.assignment_lost() {
            Err(Error::Broker)
        } else {
            Ok(())
        }
    }
}
impl Source for KafkaSource {
    fn poll(&mut self) -> Result<Poll> {
        self.healthy()?;
        if self.paused {
            return Err(Error::Source("poll while paused"));
        }
        self.control()?;
        for turn in 0..self.queues.len() {
            let index = (self.next_partition + turn) % self.queues.len();
            if let Some(message) = self.queues[index].poll(Duration::ZERO) {
                let m = message.map_err(|_| Error::Broker)?;
                if m.topic() != self.config.topic {
                    return Err(Error::Source("topic"));
                }
                let payload = m.payload().unwrap_or_default();
                let payload = if m.key_len() > 4096 || m.headers().is_some() {
                    Vec::new()
                } else {
                    payload[..payload.len().min(self.config.payload_bytes + 1)].to_vec()
                };
                let record = RawRecord {
                    session: self.session,
                    partition: m.partition(),
                    offset: m.offset(),
                    payload,
                };
                self.next_partition = (index + 1) % self.queues.len();
                self.healthy()?;
                return Ok(Poll::Data(record));
            }
        }
        std::thread::sleep(Duration::from_millis(100));
        Ok(Poll::Pending)
    }
    fn pause(&mut self) -> Result<()> {
        // Demand gate only. The finite client prefetch queue is allowed to fill;
        // stop polling partition queues and keep servicing control/statistics.
        self.paused = true;
        Ok(())
    }
    fn resume(&mut self) -> Result<()> {
        self.healthy()?;
        self.paused = false;
        Ok(())
    }
    fn snapshot(&self, progress: &Progress) -> serde_json::Value {
        let value = serde_json::to_value(self.metrics(progress)).unwrap_or(serde_json::Value::Null);
        #[cfg(feature = "stateful")]
        if self.checkpoint_control.is_some() {
            let mut value = value;
            value["checkpoint_control"] = serde_json::json!({"poll_ticks":self.counters.control_poll_ticks.load(Ordering::Relaxed),"stats_callbacks":self.counters.stats_callbacks.load(Ordering::Relaxed),"replyq_peak":self.counters.control_replyq_peak.load(Ordering::Relaxed),"requested_stack_bytes":1024*1024});
            return value;
        }
        value
    }
    #[allow(
        clippy::too_many_lines,
        reason = "Single bounded metadata/config/range verification pass"
    )]
    fn verify(&mut self, progress: &Progress) -> Result<()> {
        self.verify_identity()?;
        for start in &self.config.starts {
            if !self.partition_inventory.contains(&start.partition) {
                return Err(Error::Source("assigned partition missing"));
            }
            let (low, high) = self
                .consumer
                .fetch_watermarks(&self.config.topic, start.partition, RPC)
                .map_err(|_| Error::Broker)?;
            let cursor = progress
                .partitions
                .get(&start.partition)
                .ok_or(Error::Source("progress assignment"))?;
            // Forwarded cursor is the earliest input not yet sink-confirmed. Retention
            // past it invalidates replay truth even if the client prefetched records.
            if low < 0
                || high < low
                || low > cursor.next_offset_suggestion
                || high < cursor.next_offset_suggestion
                || self
                    .high
                    .get(&start.partition)
                    .is_some_and(|old| high < *old)
            {
                return Err(Error::Source("offset range/retention/truncation"));
            }
            self.high.insert(start.partition, high);
        }
        if self.assignment.count() > 0 {
            let assigned = self.consumer.assignment().map_err(|_| Error::Broker)?;
            if assigned.count() != self.config.starts.len()
                || assigned
                    .elements()
                    .iter()
                    .zip(&self.config.starts)
                    .any(|(p, start)| {
                        p.topic() != self.config.topic || p.partition() != start.partition
                    })
            {
                return Err(Error::Source("assignment changed"));
            }
        }
        Ok(())
    }
}

#[cfg(feature = "adaptive-checkpoint")]
fn classify_candidate_ranges(
    starts: &[crate::Start],
    expected: &[i32],
    expired_reason: &'static str,
    mut watermarks: impl FnMut(i32) -> std::result::Result<(i64, i64), ()>,
) -> crate::adaptive::policy::CandidateEligibility {
    use crate::adaptive::policy::{CandidateEligibility, ObservedOffsetRange};

    let mut partitions = starts.iter().map(|s| s.partition).collect::<Vec<_>>();
    partitions.sort_unstable();
    if partitions != expected
        || starts.iter().any(|s| s.offset < 0)
        || partitions.windows(2).any(|w| w[0] == w[1])
    {
        return CandidateEligibility {
            eligible: false,
            reason: Some("partition_inventory"),
            ranges: Vec::new(),
        };
    }
    let mut ranges = Vec::with_capacity(starts.len());
    let mut reason = None;
    for start in starts {
        let Ok((low, high)) = watermarks(start.partition) else {
            reason.get_or_insert("watermark_unavailable");
            continue;
        };
        ranges.push(ObservedOffsetRange {
            partition: start.partition,
            requested_offset: start.offset,
            low,
            high,
        });
        if low < 0 || high < low {
            reason.get_or_insert("invalid_watermark");
        } else if low > start.offset {
            reason.get_or_insert(expired_reason);
        } else if start.offset > high {
            reason.get_or_insert("offset_outside_range");
        }
    }
    CandidateEligibility {
        eligible: reason.is_none() && ranges.len() == starts.len(),
        reason,
        ranges,
    }
}

#[cfg(all(test, feature = "adaptive-checkpoint"))]
mod recovery_preflight_tests {
    use super::classify_candidate_ranges;
    use crate::Start;

    #[test]
    fn retained_checkpoint_vector_survives_expired_replay_origin() {
        let original = [
            Start {
                partition: 0,
                offset: 0,
            },
            Start {
                partition: 1,
                offset: 0,
            },
        ];
        let saved = [
            Start {
                partition: 0,
                offset: 20,
            },
            Start {
                partition: 1,
                offset: 20,
            },
        ];
        let replay = classify_candidate_ranges(&original, &[0, 1], "origin_retention", |p| {
            Ok(if p == 0 { (20, 40) } else { (0, 40) })
        });
        let restore = classify_candidate_ranges(&saved, &[0, 1], "saved_offset_retention", |p| {
            Ok(if p == 0 { (20, 40) } else { (0, 40) })
        });
        assert!(!replay.eligible);
        assert_eq!(replay.reason, Some("origin_retention"));
        assert!(restore.eligible);
        assert_eq!(restore.ranges[0].requested_offset, 20);

        let neither = classify_candidate_ranges(&saved, &[0, 1], "saved_offset_retention", |p| {
            Ok(if p == 0 { (21, 40) } else { (0, 40) })
        });
        assert!(!neither.eligible);
        assert_eq!(neither.reason, Some("saved_offset_retention"));
    }
}

impl Drop for KafkaSource {
    fn drop(&mut self) {
        #[cfg(feature = "stateful")]
        let _ = self.stop_checkpoint_control();
        // PartitionQueue owns an Arc to the consumer. Destroy every queue while
        // our owner Arc is alive, before BaseConsumer closes its queue.
        self.queues.clear();
        if !self.consumer.closed() {
            let _ = self.consumer.unassign();
        }
    }
}
