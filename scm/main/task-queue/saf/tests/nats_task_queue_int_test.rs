//! Integration tests for the NATS task queue.

#![allow(clippy::unwrap_used, clippy::expect_used)]

/// @covers: TaskQueueFactory::nats — fails fast for a blank URL, without
/// attempting a connection.
#[cfg(feature = "nats")]
#[tokio::test]
async fn test_nats_task_queue_rejects_blank_url() {
    use task_queue_pattern::QueueError;
    use task_queue_svc_saf::TaskQueueFactory;

    let result = TaskQueueFactory::nats("   ", "tasks".into(), "workers".into()).await;
    assert!(matches!(result, Err(QueueError::Connection(_))));
}

/// @covers: TaskQueueFactory::nats — fails for an unreachable host.
#[cfg(feature = "nats")]
#[tokio::test]
async fn test_nats_task_queue_connect_fails_for_unreachable_host() {
    use task_queue_svc_saf::TaskQueueFactory;

    let result =
        TaskQueueFactory::nats("nats://127.0.0.1:4229", "tasks".into(), "workers".into()).await;
    assert!(result.is_err(), "expected error for unreachable NATS host");
}
