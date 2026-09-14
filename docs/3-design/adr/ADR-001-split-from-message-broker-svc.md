# ADR-001: Split from `message-broker-svc`

**Status**: Accepted
**Date**: 2026-09-14

## Context

`message-broker-svc` bundled `TaskQueue` implementations
(`InMemoryTaskQueue`, `NatsTaskQueue`, `KafkaTaskQueue`) alongside
`MessageBroker` implementations per backend. See
`message-broker-pattern`'s own ADR-002 for the contract-level half of this
split (`TaskQueue` moving to `task-queue-pattern`) and the SRP reasoning
behind it: `MessageBroker` (fan-out/broadcast) and `TaskQueue`
(competing-consumer) are different domains, bundled originally for
migration-completeness.

## What made the implementation-level split straightforward

Checked before starting, not assumed: neither `NatsTaskQueue::connect`
nor `KafkaTaskQueue::new` used their sibling `MessageBroker` backend's
config type (`NatsConfig`/`KafkaConfig`) — both took raw parameters
(`nats_url: &str`, `brokers: &str`) directly, with no `Validator`/config-struct
involvement at all. This meant no config-type duplication was needed across
the split; each `TaskQueue` implementation was already fully self-contained
relative to its sibling `MessageBroker`.

Their supporting files were not as clean-cut:

- `nats-spi`'s `constants.rs` (`DEFAULT_VISIBILITY_TIMEOUT_SECS`,
  `DEFAULT_MAX_ACK_PENDING`, `DEFAULT_HEARTBEAT_SECS`) was 100%
  `NatsTaskQueue`-only — moved here whole, nothing left behind in
  `message-broker-svc-nats-spi`.
- `kafka-spi`'s `constants.rs` mixed both domains:
  `KAFKA_MESSAGE_TIMEOUT_MS`/`KAFKA_SESSION_TIMEOUT_MS`/
  `KAFKA_HEALTH_CHECK_TIMEOUT_SECS` are used by both `KafkaMessageBroker`
  and `KafkaTaskQueue`, so both crates now declare their own copy (real
  duplication, not shared — matches this org's own SEA anti-pattern rule,
  "no cross-module shared utilities," applied at the repo-split level, not
  just the module level). `KAFKA_SUBSCRIBE_CHANNEL_CAPACITY`/
  `KAFKA_SUBSCRIBE_IDLE_CHECK_SECS` stayed `MessageBroker`-only;
  `KAFKA_DEQUEUE_POLL_TIMEOUT_MS` moved here as `TaskQueue`-only.
- `kafka-spi`'s `logging_consumer_context.rs` (`LoggingConsumerContext`) was
  used only by `KafkaTaskQueue` — moved here whole.

## Decision

- New crate `task-queue-svc-core`: `InMemoryTaskQueue`, ported unchanged
  from `message-broker-svc-core`.
- New crate `task-queue-svc-nats-spi`: `NatsTaskQueue` + its own
  `constants.rs`, ported unchanged from `message-broker-svc-nats-spi`.
- New crate `task-queue-svc-kafka-spi`: `KafkaTaskQueue` +
  `logging_consumer_context.rs` + its own `constants.rs` (duplicating the
  three constants both domains need), ported unchanged from
  `message-broker-svc-kafka-spi`.
- New crate `task-queue-svc-saf`: `TaskQueueFactory`
  (`in_memory`/`nats`/`kafka`, no `postgres` — that backend never had a
  `TaskQueue`), ported unchanged from `message-broker-svc-saf`.
- Every crate depends on `task-queue-pattern` via `git`+`tag` (not yet
  published to crates.io).

## Consequences

- `message-broker-svc`'s own `core`/`nats-spi`/`kafka-spi`/`saf` crates lose
  their `TaskQueue` exports and the dependencies that existed only to
  support them (`uuid` from `nats-spi`; `bytes`/`uuid`/`tracing` from
  `kafka-spi`) — see that repo's own commit for the mirrored removal.
- Any consumer wanting both `MessageBroker` and `TaskQueue` now depends on
  four crates across two repo pairs instead of two crates in one — the
  correct trade-off per SRP.
