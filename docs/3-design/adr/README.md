# Architecture Decision Records

**Audience**: Architects, technical leads, contributors.

| ADR | Status | Date | Decision |
|-----|--------|------|----------|
| [ADR-001](ADR-001-split-from-message-broker-svc.md) | Accepted | 2026-09-14 | Split `InMemoryTaskQueue`/`NatsTaskQueue`/`KafkaTaskQueue`/`TaskQueueFactory` out of `message-broker-svc` into this standalone repo (SRP) — moved unchanged, no config type needed splitting since neither `NatsTaskQueue` nor `KafkaTaskQueue` used their sibling `MessageBroker`'s config type to begin with |

[← 3-design index](../README.md)
