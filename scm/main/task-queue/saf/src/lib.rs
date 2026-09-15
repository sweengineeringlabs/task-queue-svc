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
//! # Zero-cost, not `Box<dyn TaskQueue>`
//!
//! `TaskQueueFactory`'s constructors all return [`AnyTaskQueue`] — a
//! static-dispatch enum, one variant per backend — instead of
//! `Box<dyn TaskQueue>`. `TaskQueue` itself is no longer object-safe (its
//! methods return `impl Future`, not a boxed one), so this is required to
//! compile at all, not just an optimization. See
//! `docs/3-design/architecture.md`'s "Why `AnyTaskQueue`, not
//! `Box<dyn TaskQueue>`".
//!
//! # Security note
//!
//! `InMemoryTaskQueue`/`NatsTaskQueue`/`KafkaTaskQueue` are `pub` within
//! their own crates (required for this crate to construct them across the
//! crate boundary and for [`AnyTaskQueue`] to name them as variant
//! payloads), but this crate's own `lib.rs` re-exports only
//! [`TaskQueueFactory`] and [`AnyTaskQueue`] — a consumer depending on
//! `task-queue-svc-saf` alone can hold and call methods on the queue
//! `TaskQueueFactory` returns without depending directly on the `core`/`spi`
//! crate itself; only destructuring `AnyTaskQueue`'s variants requires that.

mod any_task_queue;
mod task_queue_factory;

pub use any_task_queue::AnyTaskQueue;
pub use task_queue_factory::TaskQueueFactory;
