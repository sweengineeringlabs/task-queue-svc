//! [`KafkaTaskQueue`] — Apache Kafka backed competing-consumer task queue.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer as _};
use rdkafka::message::{BorrowedHeaders, Header, Headers as _, Message as RdkafkaMessage};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer as _};
use rdkafka::topic_partition_list::Offset;

use crate::logging_consumer_context::{LoggingConsumer, LoggingConsumerContext};

use task_queue_pattern::QueueError;
use task_queue_pattern::Task;
use task_queue_pattern::TaskHandle;
use task_queue_pattern::TaskId;
use task_queue_pattern::TaskQueue;

/// Encode a task's ID and header map as Kafka message headers.
///
/// The task ID is stored under [`task_queue_pattern::TASK_ID_HEADER_KEY`] so
/// [`decode_task_headers`] can recover it on the consuming side; Kafka's wire
/// format otherwise has no notion of task identity separate from the payload.
fn encode_task_headers(
    task_id: TaskId,
    headers: &HashMap<String, String>,
) -> rdkafka::message::OwnedHeaders {
    let id_str = task_id.as_uuid().to_string();
    let mut owned = rdkafka::message::OwnedHeaders::new_with_capacity(headers.len() + 1);
    owned = owned.insert(Header {
        key: task_queue_pattern::TASK_ID_HEADER_KEY,
        value: Some(id_str.as_str()),
    });
    for (key, value) in headers {
        owned = owned.insert(Header {
            key: key.as_str(),
            value: Some(value.as_str()),
        });
    }
    owned
}

/// Decode Kafka message headers back into a task ID (if present) and header map.
///
/// Returns `None` for the ID when the reserved header is absent or unparsable —
/// e.g. a message published by something other than [`KafkaTaskQueue::enqueue`].
/// Callers must mint a fresh [`TaskId`] in that case; there is no original ID to
/// recover.
fn decode_task_headers(
    headers: Option<&BorrowedHeaders>,
) -> (Option<TaskId>, HashMap<String, String>) {
    let Some(headers) = headers else {
        return (None, HashMap::new());
    };
    let mut id = None;
    let mut map = HashMap::with_capacity(headers.count());
    for i in 0..headers.count() {
        let header = headers.get(i);
        if header.key == task_queue_pattern::TASK_ID_HEADER_KEY {
            id = header
                .value
                .and_then(|v| std::str::from_utf8(v).ok())
                .and_then(|s| uuid::Uuid::parse_str(s).ok())
                .map(TaskId::from_uuid);
            continue;
        }
        if let Some(value) = header.value {
            map.insert(
                header.key.to_owned(),
                String::from_utf8_lossy(value).into_owned(),
            );
        }
    }
    (id, map)
}

/// Kafka-backed competing-consumer work queue.
///
/// Each call to [`enqueue`](KafkaTaskQueue::enqueue) publishes a message to
/// the configured Kafka topic, encoding the task's ID and headers into Kafka
/// message headers (see [`encode_task_headers`]) so they survive the trip to
/// [`dequeue`](KafkaTaskQueue::dequeue), which decodes them back
/// (see [`decode_task_headers`]) instead of minting an unrelated new ID.
///
/// - **ack** — commits the message offset so Kafka knows it was processed.
/// - **nack** — seeks back to the message offset so it is redelivered on the
///   next [`dequeue`](KafkaTaskQueue::dequeue) call within the same session.
///
/// If neither is called, the message is re-delivered after the consumer
/// session restarts (no committed offset for that partition).
///
/// # Rebalance contract
///
/// During a consumer-group rebalance (member join/leave/timeout), the broker
/// may revoke partition assignments mid-flight. If a partition is revoked after
/// `dequeue` returns a `TaskHandle` but before `ack`/`nack` is called:
///
/// - `ack` will attempt to commit an offset on a partition this consumer no
///   longer owns — the commit is silently dropped by Kafka.
/// - `nack` will attempt to seek on a revoked partition — rdkafka returns an
///   error which is propagated as [`QueueError::Dequeue`].
/// - The message will be redelivered to whichever consumer is assigned the
///   partition after the rebalance completes.
///
/// **Callers must be idempotent.** At-least-once delivery is guaranteed; exactly-once
/// requires external deduplication (e.g. an idempotency key in the task payload).
///
/// Rebalance events are logged at `INFO` level via [`LoggingConsumerContext`].
pub struct KafkaTaskQueue {
    producer: FutureProducer,
    consumer: Arc<LoggingConsumer>,
    topic: String,
}

impl KafkaTaskQueue {
    /// Initialise the Kafka producer and consumer for the given topic.
    ///
    /// This call does **not** establish a network connection — rdkafka connects
    /// lazily on the first enqueue or dequeue operation.
    ///
    /// # Errors
    ///
    /// Returns [`QueueError::Connection`] if the Kafka client configuration is invalid.
    pub fn new(brokers: &str, group_id: &str, topic: &str) -> Result<Self, QueueError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set(
                "message.timeout.ms",
                crate::constants::KAFKA_MESSAGE_TIMEOUT_MS,
            )
            .create()
            .map_err(|e| QueueError::Connection(e.to_string()))?;

        let consumer: LoggingConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            // Manual commit — ack() and nack() control offset progression.
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set(
                "session.timeout.ms",
                crate::constants::KAFKA_SESSION_TIMEOUT_MS,
            )
            .create_with_context(LoggingConsumerContext)
            .map_err(|e| QueueError::Connection(e.to_string()))?;

        consumer
            .subscribe(&[topic])
            .map_err(|e| QueueError::Connection(e.to_string()))?;

        Ok(Self {
            producer,
            consumer: Arc::new(consumer),
            topic: topic.to_owned(),
        })
    }
}

impl TaskQueue for KafkaTaskQueue {
    fn enqueue(&self, task: Task) -> BoxFuture<'_, Result<(), QueueError>> {
        let topic = self.topic.clone();
        let producer = self.producer.clone();
        Box::pin(async move {
            let headers = encode_task_headers(task.id, &task.headers);
            producer
                .send(
                    // No key: omitting it (rather than passing `""`) lets rdkafka's
                    // default partitioner distribute across all partitions instead
                    // of hashing every message to the same one.
                    FutureRecord::<(), _>::to(&topic)
                        .payload(&task.payload[..])
                        .headers(headers),
                    std::time::Duration::from_secs(5),
                )
                .await
                .map(|_| ())
                .map_err(|(e, _)| QueueError::Enqueue(e.to_string()))
        })
    }

    fn dequeue(&self) -> BoxFuture<'_, Result<Option<TaskHandle>, QueueError>> {
        let consumer = Arc::clone(&self.consumer);
        Box::pin(async move {
            let recv_result = tokio::time::timeout(
                std::time::Duration::from_millis(crate::constants::KAFKA_DEQUEUE_POLL_TIMEOUT_MS),
                consumer.recv(),
            )
            .await;

            let borrowed = match recv_result {
                Err(_elapsed) => return Ok(None), // no message within timeout
                Ok(Err(e)) => return Err(QueueError::Dequeue(e.to_string())),
                Ok(Ok(msg)) => msg,
            };

            // Extract all data from the borrowed message before it is dropped
            // (BorrowedMessage<'_> lifetime is tied to the consumer borrow).
            let payload = Bytes::from(borrowed.payload().unwrap_or_default().to_vec());
            let (decoded_id, headers) = decode_task_headers(borrowed.headers());
            // Fall back to a fresh ID only when the message carried none of our
            // own — e.g. published by something other than `enqueue()`. When we
            // did publish it, this recovers the exact ID the producer set.
            let task_id = decoded_id.unwrap_or_else(TaskId::new);
            let partition = borrowed.partition();
            let offset = borrowed.offset();
            let topic = borrowed.topic().to_owned();
            drop(borrowed);

            let consumer_ack = Arc::clone(&consumer);
            let topic_ack = topic.clone();
            let ack: BoxFuture<'static, Result<(), QueueError>> = Box::pin(async move {
                use rdkafka::topic_partition_list::TopicPartitionList;
                let mut tpl = TopicPartitionList::new();
                tpl.add_partition_offset(
                    &topic_ack,
                    partition,
                    // Commit the offset AFTER this message so Kafka won't redeliver it.
                    Offset::Offset(offset + 1),
                )
                .map_err(|e| QueueError::Dequeue(e.to_string()))?;
                // Sync (not Async): block until the broker confirms the commit so a
                // caller that receives `Ok(())` from `ack` can trust the offset is
                // actually durable, rather than merely enqueued in a local work
                // queue that could still fail silently after this future resolves.
                // The blocking FFI call is offloaded to a blocking-pool thread so it
                // doesn't stall the async runtime.
                tokio::task::spawn_blocking(move || {
                    consumer_ack
                        .commit(&tpl, CommitMode::Sync)
                        .map_err(|e| QueueError::Dequeue(e.to_string()))
                })
                .await
                .map_err(|e| QueueError::Dequeue(format!("commit task failed: {e}")))?
            });

            let consumer_nack = Arc::clone(&consumer);
            let topic_nack = topic;
            let nack: BoxFuture<'static, Result<(), QueueError>> = Box::pin(async move {
                // Seek back to the message offset so this consumer redelivers it
                // on the next dequeue call within the same session.
                consumer_nack
                    .seek(
                        &topic_nack,
                        partition,
                        Offset::Offset(offset),
                        std::time::Duration::ZERO,
                    )
                    .map_err(|e| QueueError::Dequeue(e.to_string()))
            });

            Ok(Some(TaskHandle::new(task_id, payload, headers, ack, nack)))
        })
    }

    fn health_check(&self) -> BoxFuture<'_, Result<(), QueueError>> {
        let producer = self.producer.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                producer
                    .client()
                    .fetch_metadata(
                        None,
                        std::time::Duration::from_secs(
                            crate::constants::KAFKA_HEALTH_CHECK_TIMEOUT_SECS,
                        ),
                    )
                    .map(|_| ())
                    .map_err(|e| QueueError::Connection(e.to_string()))
            })
            .await
            .map_err(|e| QueueError::Connection(format!("health check task failed: {e}")))?
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// @covers: new
    #[test]
    fn test_new_kafka_task_queue_is_send_and_sync() {
        fn _assert<T: Send + Sync>() {}
        _assert::<KafkaTaskQueue>();
        assert!(
            std::hint::black_box(true),
            "KafkaTaskQueue is Send + Sync (checked above at compile time)"
        );
    }

    /// @covers: new
    #[tokio::test]
    async fn test_new_accepts_unreachable_broker_without_panic() {
        // rdkafka connects lazily but the subscribe call requires a tokio runtime.
        let result = KafkaTaskQueue::new("127.0.0.1:9999", "test-group", "test-topic");
        assert!(
            result.is_ok(),
            "KafkaTaskQueue::new must not fail before the first IO attempt"
        );
    }

    /// @covers: health_check
    #[tokio::test]
    async fn test_health_check_fails_for_unreachable_broker() {
        let queue = KafkaTaskQueue::new("127.0.0.1:9999", "test-group", "test-topic")
            .expect("construction succeeds before first IO");
        let result = queue.health_check().await;
        assert!(
            matches!(result, Err(QueueError::Connection(_))),
            "expected Connection error for unreachable broker, got: {result:?}"
        );
    }

    /// @covers: enqueue
    #[tokio::test]
    async fn test_enqueue_fails_for_unreachable_broker() {
        let queue = KafkaTaskQueue::new("127.0.0.1:9999", "test-group", "test-topic")
            .expect("construction succeeds before first IO");
        let result = queue.enqueue(Task::new(b"payload".as_ref())).await;
        assert!(
            matches!(result, Err(QueueError::Enqueue(_))),
            "expected Enqueue error for unreachable broker, got: {result:?}"
        );
    }

    /// @covers: encode_task_headers, decode_task_headers
    ///
    /// Real bug this catches: `enqueue` previously never wrote `task.headers`
    /// or `task.id` onto the Kafka record at all (`FutureRecord` carried only
    /// the payload), and `dequeue` minted a brand new `TaskId` unconditionally.
    /// This proves both survive an encode/decode round trip through Kafka's
    /// native header wire format without a live broker.
    #[test]
    fn test_encode_decode_task_headers_round_trips_id_and_headers() {
        let task_id = TaskId::new();
        let mut headers = HashMap::new();
        headers.insert("correlation-id".to_string(), "req-42".to_string());
        headers.insert("content-type".to_string(), "application/json".to_string());

        let owned = encode_task_headers(task_id, &headers);
        let borrowed = owned.as_borrowed();
        let (decoded_id, decoded_headers) = decode_task_headers(Some(borrowed));

        assert_eq!(
            decoded_id,
            Some(task_id),
            "decoded task ID must match the ID the producer set"
        );
        assert_eq!(
            decoded_headers, headers,
            "decoded headers must match the producer's header map exactly"
        );
    }

    /// @covers: encode_task_headers, decode_task_headers
    #[test]
    fn test_encode_decode_task_headers_round_trips_with_no_user_headers() {
        let task_id = TaskId::new();
        let headers = HashMap::new();

        let owned = encode_task_headers(task_id, &headers);
        let borrowed = owned.as_borrowed();
        let (decoded_id, decoded_headers) = decode_task_headers(Some(borrowed));

        assert_eq!(decoded_id, Some(task_id));
        assert!(decoded_headers.is_empty());
    }

    /// @covers: decode_task_headers
    ///
    /// A message with no headers at all (e.g. published by something other
    /// than this queue's `enqueue`) has no ID to recover — callers must mint
    /// a fresh one rather than panicking or fabricating a stale ID.
    #[test]
    fn test_decode_task_headers_missing_reserved_key_returns_none_id() {
        let (decoded_id, decoded_headers) = decode_task_headers(None);
        assert_eq!(
            decoded_id, None,
            "a message with no headers must decode to no task ID"
        );
        assert!(decoded_headers.is_empty());
    }

    /// @covers: decode_task_headers
    #[test]
    fn test_decode_task_headers_malformed_id_value_returns_none_id() {
        let mut owned = rdkafka::message::OwnedHeaders::new();
        owned = owned.insert(Header {
            key: task_queue_pattern::TASK_ID_HEADER_KEY,
            value: Some("not-a-uuid"),
        });
        let borrowed = owned.as_borrowed();
        let (decoded_id, _) = decode_task_headers(Some(borrowed));
        assert_eq!(
            decoded_id, None,
            "an unparsable task-id header must decode to None, not panic"
        );
    }
}
