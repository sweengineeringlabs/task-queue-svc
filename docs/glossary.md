# Glossary

Alphabetized list of terms used in `task-queue-svc`.

---

**InMemoryTaskQueue** - `task-queue-svc-core`'s technology-free reference `TaskQueue`, backed by `tokio::sync::mpsc`.

**KafkaTaskQueue** - `TaskQueue` backed by `rdkafka`, using the caller-supplied `group_id` directly — competing consumption is what a task queue wants.

**NatsTaskQueue** - `TaskQueue` backed by `async-nats` JetStream, using competing-consumer groups.

**TaskQueueFactory** - Construction facade in `task-queue-svc-saf`: `in_memory`/`nats`/`kafka`, all returning `Box<dyn TaskQueue>`. No `postgres` constructor and no no-op reference — neither exists for `TaskQueue` in this domain.

[← Docs index](README.md)
