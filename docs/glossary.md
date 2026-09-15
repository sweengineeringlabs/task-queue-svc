# Glossary

Alphabetized list of terms used in `task-queue-svc`.

---

**AnyTaskQueue** - Static-dispatch enum in `task-queue-svc-saf`, one variant per backend, returned by every `TaskQueueFactory` constructor instead of `Box<dyn TaskQueue>` (`TaskQueue` is not object-safe). Zero-cost: no heap allocation or vtable on any call.

**InMemoryTaskQueue** - `task-queue-svc-core`'s technology-free reference `TaskQueue`, backed by `tokio::sync::mpsc`.

**KafkaTaskQueue** - `TaskQueue` backed by `rdkafka`, using the caller-supplied `group_id` directly — competing consumption is what a task queue wants.

**NatsTaskQueue** - `TaskQueue` backed by `async-nats` JetStream, using competing-consumer groups.

**TaskQueueFactory** - Construction facade in `task-queue-svc-saf`: `in_memory`/`nats`/`kafka`, all returning `AnyTaskQueue`. No `postgres` constructor and no no-op reference — neither exists for `TaskQueue` in this domain.

[← Docs index](README.md)
