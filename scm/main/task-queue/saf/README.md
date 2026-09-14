# task-queue-svc-saf

`TaskQueueFactory`: construction facade for every backend this repo ships,
selected by Cargo feature (`inmemory`/`nats`/`kafka`). See
[Architecture](../../../../docs/3-design/architecture.md) for how backend
selection works.
