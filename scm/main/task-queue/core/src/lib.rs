//! `task_queue_svc_core` — the technology-free reference implementation of
//! `task-queue-pattern`'s `TaskQueue` trait.
//!
//! [`InMemoryTaskQueue`] is backed by `tokio::sync::mpsc` only -- no
//! external service, no network connection, zero external-technology
//! dependency.
//!
//! Split out of `message-broker-svc-core` — `TaskQueue`
//! (competing-consumer) and `MessageBroker` (fan-out/broadcast) are
//! separate domains, split for SRP; see `message-broker-pattern`'s own
//! ADR-002 and `../template-engine`'s `pattern_svc_workflow.md`.

mod inmemory_task_queue;

pub use inmemory_task_queue::InMemoryTaskQueue;
