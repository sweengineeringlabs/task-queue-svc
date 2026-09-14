//! Integration tests for [`InMemoryTaskQueue`]'s concrete behavior.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use task_queue_pattern::{Task, TaskQueue};
use task_queue_svc_core::InMemoryTaskQueue;

#[tokio::test]
async fn test_enqueue_and_dequeue_delivers_task() {
    let queue = InMemoryTaskQueue::new();
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

#[tokio::test]
async fn test_enqueue_multiple_tasks_dequeue_fifo() {
    let queue = InMemoryTaskQueue::new();
    let task1 = Task::new(b"first".as_ref());
    let task2 = Task::new(b"second".as_ref());
    let task1_id = task1.id;
    let task2_id = task2.id;

    queue
        .enqueue(task1)
        .await
        .expect("enqueue task1 must succeed");
    queue
        .enqueue(task2)
        .await
        .expect("enqueue task2 must succeed");

    let h1 = queue
        .dequeue()
        .await
        .expect("dequeue must succeed")
        .expect("task1 must be available");
    let h2 = queue
        .dequeue()
        .await
        .expect("dequeue must succeed")
        .expect("task2 must be available");

    assert_eq!(
        h1.task_id, task1_id,
        "first dequeue must return task1 (FIFO)"
    );
    assert_eq!(
        h2.task_id, task2_id,
        "second dequeue must return task2 (FIFO)"
    );
}

#[tokio::test]
async fn test_ack_completes_future() {
    let queue = InMemoryTaskQueue::new();
    let task = Task::new(b"work".as_ref());
    queue.enqueue(task).await.expect("enqueue must succeed");

    let handle = queue
        .dequeue()
        .await
        .expect("dequeue must succeed")
        .expect("the enqueued task must be available");
    assert!(handle.ack.await.is_ok(), "ack must complete successfully");
}

/// @covers: TaskQueue::nack (in-memory)
///
/// Proves a nacked task is actually redelivered on the next `dequeue`,
/// matching the `TaskQueue` trait's documented nack contract, rather than
/// being silently dropped.
#[tokio::test]
async fn test_nack_requeues_task_for_redelivery() {
    let queue = InMemoryTaskQueue::new();
    let task = Task::new(b"work".as_ref());
    let task_id = task.id;
    queue.enqueue(task).await.expect("enqueue must succeed");

    let handle = queue
        .dequeue()
        .await
        .expect("dequeue must succeed")
        .expect("the enqueued task must be available");
    assert!(handle.nack.await.is_ok(), "nack must complete successfully");

    // The task must reappear for redelivery, not be lost.
    let redelivered = tokio::time::timeout(std::time::Duration::from_secs(1), queue.dequeue())
        .await
        .expect("redelivered task must arrive without blocking indefinitely")
        .expect("dequeue must succeed")
        .expect("nacked task must be redelivered, not dropped");
    assert_eq!(
        redelivered.task_id, task_id,
        "redelivered task must be the same one that was nacked"
    );
}

#[tokio::test]
async fn test_health_check_returns_ok() {
    let queue = InMemoryTaskQueue::new();
    assert!(queue.health_check().await.is_ok());
}

#[tokio::test]
async fn test_enqueue_task_with_headers() {
    use std::collections::HashMap;

    let queue = InMemoryTaskQueue::new();
    let mut headers = HashMap::new();
    headers.insert("correlation-id".into(), "abc123".into());
    let task = Task::with_headers(b"data".as_ref(), headers.clone());
    let task_id = task.id;

    queue.enqueue(task).await.expect("enqueue must succeed");

    let handle = queue
        .dequeue()
        .await
        .expect("dequeue must succeed")
        .expect("the enqueued task must be available");
    assert_eq!(handle.task_id, task_id);
    assert_eq!(
        handle.headers, headers,
        "dequeued headers must match the enqueued headers"
    );
}

#[tokio::test]
async fn test_dequeue_empty_queue_blocks() {
    let queue = InMemoryTaskQueue::new();
    let result = tokio::time::timeout(std::time::Duration::from_millis(100), queue.dequeue()).await;
    assert!(result.is_err(), "dequeue should block on empty queue");
}
