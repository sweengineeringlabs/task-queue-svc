//! [`AnyTaskQueue`] — zero-cost static dispatch over every backend this repo ships.

use task_queue_pattern::{QueueError, Task, TaskHandle, TaskQueue};

/// One concrete type covering every backend this repo ships, selected by
/// which [`crate::TaskQueueFactory`] constructor was called.
///
/// Zero-cost on every call: no heap allocation, no vtable. Matching on
/// `self` compiles to a jump table, and each variant's own `async` code
/// becomes one branch of a single generated state machine for each trait
/// method, not a boxed future -- unlike the `Box<dyn TaskQueue>` this
/// replaces, which paid a heap allocation and vtable dispatch for the
/// queue's whole lifetime with nothing to show for it once `TaskQueue`
/// itself stopped being object-safe. See `docs/3-design/architecture.md`'s
/// "Why `AnyTaskQueue`, not `Box<dyn TaskQueue>`".
///
/// `Nats` is boxed (unlike `InMemory`/`Kafka`) purely to keep this enum's
/// own stack size small -- `NatsTaskQueue` is the largest variant by a wide
/// margin, so leaving it unboxed would size every `AnyTaskQueue` value to
/// fit it, even an `InMemory` one. This is a one-time allocation at
/// construction, not a per-call cost -- a fundamentally different, much
/// smaller concern than the per-call future-boxing this type exists to
/// avoid.
pub enum AnyTaskQueue {
    /// The in-process reference queue.
    #[cfg(feature = "inmemory")]
    InMemory(task_queue_svc_core::InMemoryTaskQueue),
    /// NATS JetStream-backed queue.
    #[cfg(feature = "nats")]
    Nats(Box<task_queue_svc_nats_spi::NatsTaskQueue>),
    /// Kafka-backed queue.
    #[cfg(feature = "kafka")]
    Kafka(task_queue_svc_kafka_spi::KafkaTaskQueue),
}

impl TaskQueue for AnyTaskQueue {
    #[cfg_attr(
        not(any(feature = "inmemory", feature = "nats", feature = "kafka")),
        allow(unused_variables)
    )]
    async fn enqueue(&self, task: Task) -> Result<(), QueueError> {
        match self {
            #[cfg(feature = "inmemory")]
            Self::InMemory(q) => q.enqueue(task).await,
            #[cfg(feature = "nats")]
            Self::Nats(q) => q.enqueue(task).await,
            #[cfg(feature = "kafka")]
            Self::Kafka(q) => q.enqueue(task).await,
            #[cfg(not(any(feature = "inmemory", feature = "nats", feature = "kafka")))]
            _ => match *self {},
        }
    }

    async fn dequeue(&self) -> Result<Option<TaskHandle>, QueueError> {
        match self {
            #[cfg(feature = "inmemory")]
            Self::InMemory(q) => q.dequeue().await,
            #[cfg(feature = "nats")]
            Self::Nats(q) => q.dequeue().await,
            #[cfg(feature = "kafka")]
            Self::Kafka(q) => q.dequeue().await,
            #[cfg(not(any(feature = "inmemory", feature = "nats", feature = "kafka")))]
            _ => match *self {},
        }
    }

    async fn health_check(&self) -> Result<(), QueueError> {
        match self {
            #[cfg(feature = "inmemory")]
            Self::InMemory(q) => q.health_check().await,
            #[cfg(feature = "nats")]
            Self::Nats(q) => q.health_check().await,
            #[cfg(feature = "kafka")]
            Self::Kafka(q) => q.health_check().await,
            #[cfg(not(any(feature = "inmemory", feature = "nats", feature = "kafka")))]
            _ => match *self {},
        }
    }
}
