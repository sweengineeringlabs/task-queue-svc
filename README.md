# task-queue-svc

> **TLDR:** In-memory/NATS/Kafka `TaskQueue` implementations, on top of
> [`task-queue-pattern`](https://github.com/sweengineeringlabs/task-queue-pattern)'s
> contract. See [Architecture](docs/3-design/architecture.md) for the full design.

Split out of `message-broker-svc` — see
[ADR-001](docs/3-design/adr/ADR-001-split-from-message-broker-svc.md) for
why `TaskQueue` (competing-consumer) and `MessageBroker` (fan-out/broadcast)
implementations are separate repos, not one.

## Quick Start

Requires the `inmemory` feature (`cargo add task-queue-svc-saf --features inmemory`):

```rust
use task_queue_svc_saf::TaskQueueFactory;

let queue = TaskQueueFactory::in_memory();
```

## Crates

| Crate | What it is |
|-------|------------|
| [`task-queue-svc-core`](scm/main/task-queue/core) | The technology-free reference implementation: `InMemoryTaskQueue` |
| [`task-queue-svc-nats-spi`](scm/main/task-queue/spi/nats-spi) | `NatsTaskQueue` — JetStream competing-consumer groups |
| [`task-queue-svc-kafka-spi`](scm/main/task-queue/spi/kafka-spi) | `KafkaTaskQueue` |
| [`task-queue-svc-saf`](scm/main/task-queue/saf) | `TaskQueueFactory` — construction facade consumers depend on |

No Postgres backend — `edge-runtime`'s original pilot never had a
`TaskQueue` for Postgres either.

## Documentation

| Document | Description |
|----------|--------------|
| [Docs index](docs/README.md) | Full documentation index |
| [Architecture](docs/3-design/architecture.md) | Component diagram, dispatch table |
| [ADR-001](docs/3-design/adr/ADR-001-split-from-message-broker-svc.md) | Why this repo is split from `message-broker-svc` |
| [Developer Guide](docs/4-development/developer_guide.md) | Repo layout, feature flags |

## License

MIT OR Apache-2.0
