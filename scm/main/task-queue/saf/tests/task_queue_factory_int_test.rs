//! Integration tests for [`TaskQueueFactory`]'s in-memory queue.

#![allow(clippy::unwrap_used, clippy::expect_used)]

/// @covers: TaskQueueFactory::in_memory
///
/// Asserting on `health_check()` (not just the return type) proves the
/// returned value is a live, working `TaskQueue`, not just a value of the
/// right type.
#[cfg(feature = "inmemory")]
#[tokio::test]
async fn test_in_memory_returns_a_working_queue() {
    use task_queue_svc_saf::TaskQueueFactory;

    let queue = TaskQueueFactory::in_memory();
    assert!(
        queue.health_check().await.is_ok(),
        "in_memory factory must return a working, healthy TaskQueue"
    );
}

/// @covers: TaskQueueFactory::in_memory — round-trips a real task, not just a
/// health check.
#[cfg(feature = "inmemory")]
#[tokio::test]
async fn test_in_memory_enqueue_dequeue_round_trip() {
    use task_queue_pattern::Task;
    use task_queue_svc_saf::TaskQueueFactory;

    let queue = TaskQueueFactory::in_memory();
    let task = Task::new(b"work".as_ref());
    let task_id = task.id;

    queue.enqueue(task).await.expect("enqueue must succeed");
    let handle = queue
        .dequeue()
        .await
        .expect("dequeue must succeed")
        .expect("the enqueued task must be available");
    assert_eq!(handle.task_id, task_id);
}
