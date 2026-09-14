//! `task_queue_svc_nats_spi` — NATS JetStream implementation of
//! `task-queue-pattern`'s `TaskQueue` trait.
//!
//! Consumed only by `task-queue-svc-saf`. Split out of
//! `message-broker-svc-nats-spi` (SRP).

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod constants;
mod nats_task_queue;

pub use nats_task_queue::NatsTaskQueue;
