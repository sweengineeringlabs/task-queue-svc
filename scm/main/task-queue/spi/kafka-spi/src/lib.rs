//! `task_queue_svc_kafka_spi` — Apache Kafka implementation of
//! `task-queue-pattern`'s `TaskQueue` trait.
//!
//! Consumed only by `task-queue-svc-saf`. Split out of
//! `message-broker-svc-kafka-spi` (SRP).

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod constants;
mod kafka_task_queue;
mod logging_consumer_context;

pub use kafka_task_queue::KafkaTaskQueue;
