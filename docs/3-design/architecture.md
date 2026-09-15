# task-queue-svc Architecture

**Audience**: Architects, technical leads, contributors.

## Overview

Four crates — one `core` reference implementation, two `spi` providers, and
one `saf` facade:

- **`task-queue-svc-core`** — the technology-free reference implementation:
  `InMemoryTaskQueue` (`tokio::sync::mpsc`).
- **`task-queue-svc-nats-spi`** — `NatsTaskQueue` (`async-nats` JetStream,
  competing-consumer groups).
- **`task-queue-svc-kafka-spi`** — `KafkaTaskQueue` (`rdkafka`).
- **`task-queue-svc-saf`** — `TaskQueueFactory` (`in_memory`/`nats`/`kafka`,
  no `postgres` constructor — that backend never had a `TaskQueue` in
  `edge-runtime`'s original pilot either), returning `AnyTaskQueue` (a
  zero-cost static-dispatch enum — see "Why `AnyTaskQueue`, not `Box<dyn
  TaskQueue>`" below). A consumer depends on `task-queue-pattern` +
  `task-queue-svc-saf` alone and never imports a `spi` crate directly.

Each crate depends on `task-queue-pattern` (the `TaskQueue` trait and value
types) and whatever technology client it wraps. Split out of
`message-broker-svc` — see [ADR-001](adr/ADR-001-split-from-message-broker-svc.md)
for the full reasoning, including what did and didn't need duplicating
across the split.

## Component Diagram

```mermaid
flowchart TD
    subgraph pattern["task-queue-pattern"]
        contract["TaskQueue, Task, TaskHandle,<br/>TaskHandleBuilder, TaskId, QueueError"]
    end

    subgraph svc["task-queue-svc"]
        core["task-queue-svc-core<br/>InMemoryTaskQueue"]
        nats["task-queue-svc-nats-spi<br/>NatsTaskQueue"]
        kafka["task-queue-svc-kafka-spi<br/>KafkaTaskQueue"]
        saf["task-queue-svc-saf<br/>TaskQueueFactory"]

        core -->|implements| contract
        nats -->|implements| contract
        kafka -->|implements| contract
        saf -->|wires, feature-gated| core
        saf -->|wires, feature-gated| nats
        saf -->|wires, feature-gated| kafka
    end
```

## No config-type duplication needed — but constants did need it

Checked before assuming: neither `NatsTaskQueue::connect` nor
`KafkaTaskQueue::new` used a config type (`NatsConfig`/`KafkaConfig`) at
all — both take raw parameters directly. So unlike
`message-broker-svc`'s own `nats-spi`/`kafka-spi` (each with a real
`Validator`-implementing config type), there was no config type to
duplicate across this split.

Tuning constants were a different story — `kafka-spi`'s `constants.rs` mixed
values used by both `KafkaMessageBroker` and `KafkaTaskQueue`
(`KAFKA_MESSAGE_TIMEOUT_MS`, `KAFKA_SESSION_TIMEOUT_MS`,
`KAFKA_HEALTH_CHECK_TIMEOUT_SECS`). Both crates now declare their own copy —
real duplication, not shared, matching this org's own SEA anti-pattern rule
("no cross-module shared utilities") applied at the repo-split level. See
ADR-001 for the full breakdown of what moved, what duplicated, and what
stayed behind.

## Dispatch: independent constructors per backend, no shared selection type

`TaskQueueFactory::in_memory()` / `::nats(nats_url, stream_name,
consumer_group)` / `::kafka(brokers, group_id, topic)` — three independent,
directly-typed, Cargo-feature-gated constructors, all returning
`AnyTaskQueue` uniformly (a caller collecting queues from more than one
constructor into one `Vec` needs them to actually be the same type —
verified by
`kafka_task_queue_int_test.rs::test_kafka_and_in_memory_constructors_return_the_same_queue_type`).

## Why `AnyTaskQueue`, not `Box<dyn TaskQueue>`

Raised as a real, checked zero-cost abstraction question in
[task-queue-svc#1](https://github.com/sweengineeringlabs/task-queue-svc/issues/1):
`TaskQueueFactory` used to return `Box<dyn TaskQueue>` from every
constructor — a heap allocation plus vtable dispatch for the queue's whole
lifetime. Once `task-queue-pattern` v0.2.0 made `TaskQueue`'s methods
return `impl Future` (removing their own per-call boxing), `TaskQueue`
stopped being object-safe at all — `Box<dyn TaskQueue>` no longer compiles,
so this wasn't optional cleanup, it was required.

This repo still has a genuine reason to want one uniform,
runtime-selectable queue type — a deployment picking in-memory vs. NATS vs.
Kafka from config without recompiling — so `saf` introduces `AnyTaskQueue`:
a plain enum, one variant per backend (feature-gated to match each
constructor), implementing `TaskQueue` itself by matching on `self` and
delegating to whichever variant is active. Matching on an enum compiles to
a jump table, not a vtable lookup, and each variant's own `async` code
becomes one branch of a single generated state machine per trait method —
zero heap allocation on any call, same ergonomics as before (every
constructor still returns one nameable type a caller can store and collect
into a `Vec`).

`Nats` is the one variant that's boxed (`Box<NatsTaskQueue>`, not
`NatsTaskQueue` directly) — purely to keep `AnyTaskQueue`'s own stack size
small, since `NatsTaskQueue` is far larger than `InMemoryTaskQueue`/
`KafkaTaskQueue` and an unboxed enum is sized to fit its largest variant.
This is a one-time heap allocation at construction, not a per-call cost —
a different, much smaller concern than the per-call future-boxing
`AnyTaskQueue` exists to avoid.

## Scope boundary

This repo covers exactly `task-queue-pattern`'s `TaskQueue` trait, for three
backends. Not covered:

- **Postgres** — `edge-runtime`'s original pilot never had a `TaskQueue`
  backend for Postgres; `pgmq` has no competing-consumer concept to map
  onto it cleanly.
- **`MessageBroker`** implementations — those stay in `message-broker-svc`.

[← Docs index](../README.md)
