//! Integration tests for the Kafka task queue.
//!
//! These tests run against a synthetic unreachable broker to verify error paths.
//! Tests that require a live Kafka cluster are skipped when none is available.

#![allow(clippy::unwrap_used, clippy::expect_used)]

/// @covers: TaskQueueFactory::kafka — construction succeeds before first IO.
#[cfg(feature = "kafka")]
#[tokio::test]
async fn test_kafka_task_queue_factory_constructs_without_network() {
    use task_queue_svc_saf::TaskQueueFactory;
    // subscribe() is called during construction; rdkafka requires a tokio runtime for this.
    let result = TaskQueueFactory::kafka("127.0.0.1:9999", "test-group", "test-topic");
    assert!(
        result.is_ok(),
        "Kafka task queue factory must succeed before the first IO attempt"
    );
}

/// @covers: TaskQueueFactory::kafka — health_check fails for an unreachable broker.
#[cfg(feature = "kafka")]
#[tokio::test]
async fn test_kafka_task_queue_health_check_fails_for_unreachable_broker() {
    use task_queue_pattern::QueueError;
    use task_queue_svc_saf::TaskQueueFactory;

    let queue = TaskQueueFactory::kafka("127.0.0.1:9999", "test-group", "test-topic")
        .expect("client builds");
    let result = queue.health_check().await;
    assert!(
        matches!(result, Err(QueueError::Connection(_))),
        "health_check must return Connection error for unreachable broker, got: {result:?}"
    );
}

/// @covers: TaskQueueFactory::kafka — enqueue fails for an unreachable broker.
#[cfg(feature = "kafka")]
#[tokio::test]
async fn test_kafka_task_queue_enqueue_fails_for_unreachable_broker() {
    use task_queue_pattern::{QueueError, Task};
    use task_queue_svc_saf::TaskQueueFactory;

    let queue = TaskQueueFactory::kafka("127.0.0.1:9999", "test-group", "test-topic")
        .expect("client builds");
    let result = queue.enqueue(Task::new(b"payload".as_ref())).await;
    assert!(
        matches!(result, Err(QueueError::Enqueue(_))),
        "enqueue must return Enqueue error for unreachable broker, got: {result:?}"
    );
}

/// @covers: TaskQueueFactory::kafka — dequeue returns None when broker is unreachable
/// within the poll timeout.
#[cfg(feature = "kafka")]
#[tokio::test]
async fn test_kafka_task_queue_dequeue_returns_none_when_no_broker() {
    use task_queue_svc_saf::TaskQueueFactory;

    let queue = TaskQueueFactory::kafka("127.0.0.1:9999", "test-group", "test-topic")
        .expect("client builds");
    // With no broker reachable, recv() will time out and dequeue returns None.
    // Either None (timeout) or a Dequeue error is acceptable — the key
    // invariant is that dequeue must not hang indefinitely.
    let result = queue.dequeue().await;
    match result {
        Ok(None) => {}
        Ok(Some(_)) => panic!("unexpected task from unreachable broker"),
        Err(_) => {}
    }
}

/// @covers: kafka, in_memory — both return `Box<dyn TaskQueue>`, so a caller
/// can unify queues picked from different constructors into one `Vec`
/// without manually boxing any of them itself.
///
/// `#[tokio::test]`, not a plain `#[test]`: unlike `KafkaMessageBroker::new`
/// (producer only), `KafkaTaskQueue::new` also builds a `StreamConsumer`,
/// which requires an active Tokio runtime to construct.
#[cfg(all(feature = "kafka", feature = "inmemory"))]
#[tokio::test]
async fn test_kafka_and_in_memory_constructors_return_the_same_boxed_queue_type() {
    use task_queue_pattern::TaskQueue;
    use task_queue_svc_saf::TaskQueueFactory;

    let queues: Vec<Box<dyn TaskQueue>> = vec![
        TaskQueueFactory::in_memory(),
        TaskQueueFactory::kafka("127.0.0.1:9999", "test-group", "test-topic")
            .expect("kafka client construction succeeds before first IO"),
    ];
    assert_eq!(queues.len(), 2);
}

// ── Live-broker tests ────────────────────────────────────────────────────────
//
// Run with a real Kafka cluster:
//
//   KAFKA_BROKERS=localhost:9092 cargo test --features kafka -- \
//     --include-ignored --test kafka_task_queue_int_test
//
// All tests below are ignored unless explicitly included so normal CI (without
// a Kafka sidecar) still passes green.

/// Returns the broker address from KAFKA_BROKERS, or panics with a clear message.
#[cfg(feature = "kafka")]
fn require_kafka_brokers() -> String {
    std::env::var("KAFKA_BROKERS")
        .expect("KAFKA_BROKERS env var must be set to run live-broker tests (e.g. localhost:9092)")
}

/// @covers: enqueue + dequeue + ack — happy-path roundtrip with a live broker.
#[cfg(feature = "kafka")]
#[tokio::test]
#[ignore = "requires-kafka"]
async fn test_enqueue_dequeue_ack_roundtrip_with_live_broker() {
    use bytes::Bytes;
    use task_queue_pattern::Task;
    use task_queue_svc_saf::TaskQueueFactory;

    let brokers = require_kafka_brokers();
    let topic = "swe-edge-test-enqueue-dequeue-ack";
    let payload = b"roundtrip-payload";

    let queue = TaskQueueFactory::kafka(&brokers, "swe-edge-test-group-ack", topic)
        .expect("queue construction must succeed with a live broker");

    queue
        .enqueue(Task::new(payload.as_ref()))
        .await
        .expect("enqueue must succeed with a live broker");

    let handle = {
        let mut handle = None;
        for _ in 0..30 {
            match queue.dequeue().await.expect("dequeue must not error") {
                Some(h) => {
                    handle = Some(h);
                    break;
                }
                None => tokio::time::sleep(std::time::Duration::from_millis(200)).await,
            }
        }
        handle.expect("dequeue must return the enqueued task within 6 s")
    };

    assert_eq!(
        handle.payload,
        Bytes::from_static(payload),
        "dequeued payload must match the enqueued payload"
    );

    handle
        .ack
        .await
        .expect("ack must succeed with a live broker");
}

/// @covers: enqueue + dequeue + nack — nack redelivers the task to the same consumer.
#[cfg(feature = "kafka")]
#[tokio::test]
#[ignore = "requires-kafka"]
async fn test_enqueue_dequeue_nack_redelivers_with_live_broker() {
    use bytes::Bytes;
    use task_queue_pattern::Task;
    use task_queue_svc_saf::TaskQueueFactory;

    let brokers = require_kafka_brokers();
    let topic = "swe-edge-test-enqueue-dequeue-nack";
    let payload = b"nack-payload";

    let queue = TaskQueueFactory::kafka(&brokers, "swe-edge-test-group-nack", topic)
        .expect("queue construction must succeed with a live broker");

    queue
        .enqueue(Task::new(payload.as_ref()))
        .await
        .expect("enqueue must succeed with a live broker");

    let first = {
        let mut handle = None;
        for _ in 0..30 {
            match queue.dequeue().await.expect("dequeue must not error") {
                Some(h) => {
                    handle = Some(h);
                    break;
                }
                None => tokio::time::sleep(std::time::Duration::from_millis(200)).await,
            }
        }
        handle.expect("first dequeue must return the enqueued task within 6 s")
    };

    assert_eq!(
        first.payload,
        Bytes::from_static(payload),
        "first dequeue must return the published payload"
    );

    first
        .nack
        .await
        .expect("nack must succeed with a live broker");

    let second = {
        let mut handle = None;
        for _ in 0..30 {
            match queue.dequeue().await.expect("dequeue must not error") {
                Some(h) => {
                    handle = Some(h);
                    break;
                }
                None => tokio::time::sleep(std::time::Duration::from_millis(200)).await,
            }
        }
        handle.expect("second dequeue must redeliver the nacked task within 6 s")
    };

    assert_eq!(
        second.payload,
        Bytes::from_static(payload),
        "nack must cause the same payload to be redelivered"
    );

    second.ack.await.expect("final ack must succeed");
}

/// @covers: enqueue + dequeue — task ID and headers survive a live-broker round trip.
#[cfg(feature = "kafka")]
#[tokio::test]
#[ignore = "requires-kafka"]
async fn test_enqueue_dequeue_headers_and_task_id_survive_with_live_broker() {
    use bytes::Bytes;
    use std::collections::HashMap;
    use task_queue_pattern::Task;
    use task_queue_svc_saf::TaskQueueFactory;

    let brokers = require_kafka_brokers();
    let topic = "swe-edge-test-headers-task-id";
    let payload = b"headers-task-id-payload";
    let mut headers = HashMap::new();
    headers.insert("correlation-id".to_string(), "live-broker-99".to_string());

    let queue = TaskQueueFactory::kafka(&brokers, "swe-edge-test-group-headers-task-id", topic)
        .expect("queue construction must succeed with a live broker");

    let task = Task::with_headers(payload.as_ref(), headers.clone());
    let enqueued_task_id = task.id;

    queue
        .enqueue(task)
        .await
        .expect("enqueue must succeed with a live broker");

    let handle = {
        let mut handle = None;
        for _ in 0..30 {
            match queue.dequeue().await.expect("dequeue must not error") {
                Some(h) => {
                    handle = Some(h);
                    break;
                }
                None => tokio::time::sleep(std::time::Duration::from_millis(200)).await,
            }
        }
        handle.expect("dequeue must return the enqueued task within 6 s")
    };

    assert_eq!(handle.payload, Bytes::from_static(payload));
    assert_eq!(
        handle.task_id, enqueued_task_id,
        "dequeued task ID must match the ID the producer set, not a freshly minted one"
    );
    assert_eq!(
        handle.headers, headers,
        "dequeued headers must match the enqueued headers exactly"
    );

    handle
        .ack
        .await
        .expect("ack must succeed with a live broker");
}
