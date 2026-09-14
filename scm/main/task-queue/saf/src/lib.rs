//! `task_queue_svc_saf` — task queue construction/dispatch facade.
//!
//! The `TaskQueue` contract (trait + value types) lives in
//! `task-queue-pattern`; this crate owns the construction factory that
//! selects among `task-queue-svc-core`'s in-memory backend and the
//! `task-queue-svc-{nats,kafka}-spi` crates' backends. No `postgres`
//! constructor and no no-op reference — neither exists for `TaskQueue` in
//! this domain (`edge-runtime`'s original pilot never had a Postgres
//! `TaskQueue` either).
//!
//! Split out of `message-broker-svc-saf` — `TaskQueue`
//! (competing-consumer) and `MessageBroker` (fan-out/broadcast) are
//! separate domains, split for SRP; see `message-broker-pattern`'s own
//! ADR-002.
//!
//! # Security note
//!
//! `InMemoryTaskQueue`/`NatsTaskQueue`/`KafkaTaskQueue` are `pub` within
//! their own crates (required for this crate to construct them across the
//! crate boundary), but this crate's own `lib.rs` re-exports only
//! [`TaskQueueFactory`] — a consumer depending on `task-queue-svc-saf`
//! alone cannot name a concrete backend type without also depending
//! directly on the `core`/`spi` crate itself.

mod task_queue_factory;

pub use task_queue_factory::TaskQueueFactory;
