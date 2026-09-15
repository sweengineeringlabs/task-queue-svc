//! [`TaskQueueFactory`] — public task queue construction surface.
//!
//! All factory methods are associated functions on this zero-size type.
//! Consumers never construct `TaskQueueFactory` directly — they call
//! associated functions like [`TaskQueueFactory::in_memory`]. Each method is
//! independent and directly typed: no shared "which backend" type ties them
//! together, and no config-driven runtime dispatch across them exists here.
//! No `TaskQueue` backend exists for Postgres — `edge-runtime`'s original
//! pilot never had one either.

#[cfg(feature = "inmemory")]
use task_queue_svc_core::InMemoryTaskQueue;
#[cfg(feature = "kafka")]
use task_queue_svc_kafka_spi::KafkaTaskQueue;
#[cfg(feature = "nats")]
use task_queue_svc_nats_spi::NatsTaskQueue;

#[cfg(any(feature = "kafka", feature = "nats"))]
use task_queue_pattern::QueueError;
use task_queue_pattern::TaskQueueFactoryContract;

use crate::AnyTaskQueue;

/// Zero-size factory type for constructing task queue instances.
pub struct TaskQueueFactory;

impl TaskQueueFactory {
    /// Construct an in-memory task queue backed by [`tokio::sync::mpsc`].
    ///
    /// No external service or network connection is required, so this
    /// constructor never fails.
    ///
    /// Requires the `inmemory` feature.
    #[cfg(feature = "inmemory")]
    pub fn in_memory() -> AnyTaskQueue {
        AnyTaskQueue::InMemory(InMemoryTaskQueue::new())
    }

    /// Connect to a NATS server and return a JetStream-backed task queue.
    ///
    /// # Errors
    ///
    /// Returns [`QueueError::Connection`] if `nats_url` is blank or the NATS
    /// server is unreachable.
    ///
    /// Requires the `nats` feature.
    #[cfg(feature = "nats")]
    pub async fn nats(
        nats_url: &str,
        stream_name: String,
        consumer_group: String,
    ) -> Result<AnyTaskQueue, QueueError> {
        Ok(AnyTaskQueue::Nats(Box::new(
            NatsTaskQueue::connect(nats_url, stream_name, consumer_group).await?,
        )))
    }

    /// Create a Kafka-backed competing-consumer task queue.
    ///
    /// # Errors
    ///
    /// Returns [`QueueError::Connection`] if the rdkafka client configuration is
    /// rejected (e.g. invalid broker address format).
    ///
    /// Requires the `kafka` feature.
    #[cfg(feature = "kafka")]
    pub fn kafka(brokers: &str, group_id: &str, topic: &str) -> Result<AnyTaskQueue, QueueError> {
        Ok(AnyTaskQueue::Kafka(KafkaTaskQueue::new(
            brokers, group_id, topic,
        )?))
    }
}

impl TaskQueueFactoryContract for TaskQueueFactory {}
