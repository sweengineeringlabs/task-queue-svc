# task-queue-svc-nats-spi

`NatsTaskQueue`: NATS JetStream-backed implementation of
`task-queue-pattern`'s `TaskQueue` trait, via `async-nats`
(competing-consumer semantics).

See [Architecture](../../../../../docs/3-design/architecture.md) for backend
details and this repo's own history.
