# Architecture Compliance Checklist

**Audience**: Architects, contributors, reviewers.

Derived from [architecture.md](../architecture.md). Every rule here is enforceable —
re-run the listed command after any change and expect the stated result.

## 1. `core` is technology-free

| # | Rule | Verify |
|---|------|--------|
| 1 | `task-queue-svc-core` names no backend technology | `grep -nE "^\s*(pub )?(struct\|enum\|fn) \w*(Nats\|Kafka\|Postgres\|Redis)" main/task-queue/core/src/*.rs` returns nothing |

## 2. Uniform constructor return types, zero-cost

| # | Rule | Verify |
|---|------|--------|
| 2 | Every `TaskQueueFactory` constructor returns `AnyTaskQueue`, never `Box<dyn TaskQueue>` (`TaskQueue` is not object-safe) | `grep -n "Box<dyn TaskQueue>" main/task-queue/saf/src/*.rs` returns nothing; `grep -n "-> AnyTaskQueue\|Result<AnyTaskQueue" main/task-queue/saf/src/task_queue_factory.rs` shows every constructor's return type |
| 3 | Regression test exists proving constructors from different backends unify into one `Vec` | `cargo test -p task-queue-svc-saf --features inmemory,kafka test_kafka_and_in_memory_constructors_return_the_same_queue_type` passes |

## 3. No concrete backend type leaks through `saf`

| # | Rule | Verify |
|---|------|--------|
| 4 | `saf`'s own `lib.rs` re-exports only `TaskQueueFactory` and `AnyTaskQueue` | `grep -n "^pub use" main/task-queue/saf/src/lib.rs` shows only those two |

## 4. Lint gates

| # | Rule | Verify |
|---|------|--------|
| 5 | `#![deny(unsafe_code)]` enforced across every crate | `cargo build --workspace` fails on any `unsafe` block |
| 6 | `#![warn(missing_docs)]` enforced across every crate | `cargo doc --workspace --no-deps` warns on any undocumented public item |
| 7 | `cargo clippy --workspace --all-targets --features inmemory,nats,kafka -- -D warnings` clean | Run before every commit |
| 8 | `cargo fmt --check` clean across every crate | Run before every commit |

[← 3-design index](../README.md)
