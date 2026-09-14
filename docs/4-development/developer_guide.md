# task-queue-svc Developer Guide

**Audience**: Developers, contributors.

## Repo Structure

```
task-queue-svc/
├── README.md
├── docs/
│   ├── README.md
│   ├── glossary.md
│   ├── 3-design/README.md, architecture.md
│   ├── 3-design/compliance/compliance_checklist.md
│   ├── 3-design/adr/README.md, ADR-001-split-from-message-broker-svc.md
│   └── 4-development/README.md, developer_guide.md   # this file
└── scm/
    ├── Cargo.toml          # workspace: [core, spi/*, saf]
    └── main/task-queue/
        ├── core/              # task-queue-svc-core -- InMemoryTaskQueue
        ├── spi/
        │   ├── nats-spi/        # task-queue-svc-nats-spi -- NatsTaskQueue
        │   └── kafka-spi/       # task-queue-svc-kafka-spi -- KafkaTaskQueue
        └── saf/               # task-queue-svc-saf -- TaskQueueFactory
```

## Branching and Releases

- `dev` is the default branch; all work lands there first.
- `main` gets fast-forwarded to `dev` after a shipped change, not on every commit.
- Not yet published to crates.io. First publish will be v0.1.0 for all four
  crates. Depends on
  [`task-queue-pattern`](https://github.com/sweengineeringlabs/task-queue-pattern)
  by `git`+`tag` (not yet published either) — switch to a version requirement
  once it publishes.

## Working on Any Crate

All four crates are members of `scm/Cargo.toml`, so from `scm/`:

```
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

`TaskQueueFactory` has no no-op reference (unlike `message-broker-svc`'s
`MessageBrokerFactory::noop()`) — no `TaskQueue` equivalent exists for this
domain. Each real backend's feature is off by default:

```
cargo test --manifest-path main/task-queue/saf/Cargo.toml --features inmemory
cargo test --manifest-path main/task-queue/saf/Cargo.toml --features nats
cargo test --manifest-path main/task-queue/saf/Cargo.toml --features kafka
```

## Live-Infra Tests

Every `spi` crate's tests run without any live backend — connection/config error
paths only. Round-trip tests against a real broker are `#[ignore]`d by default:

```
KAFKA_BROKERS=localhost:9092 cargo test --manifest-path main/task-queue/saf/Cargo.toml \
  --features kafka -- --include-ignored --test kafka_task_queue_int_test
```

## See Also

- [Architecture](../3-design/architecture.md)
- [ADR-001](../3-design/adr/ADR-001-split-from-message-broker-svc.md)
- [Pattern/Svc Workflow](https://github.com/sweengineeringlabs/template-engine/blob/main/pattern_svc_workflow.md)
