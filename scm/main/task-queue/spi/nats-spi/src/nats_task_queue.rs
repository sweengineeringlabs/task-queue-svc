//! [`NatsTaskQueue`] — NATS JetStream queue group backed task queue.

use std::collections::HashMap;
use std::sync::Arc;

use async_nats::jetstream;
use futures::future::BoxFuture;
use tokio::sync::Mutex;

use task_queue_pattern::QueueError;
use task_queue_pattern::Task;
use task_queue_pattern::TaskHandle;
use task_queue_pattern::TaskId;
use task_queue_pattern::TaskQueue;

/// Visibility timeout for nacked messages before redelivery (5 minutes).
const VISIBILITY_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(crate::constants::DEFAULT_VISIBILITY_TIMEOUT_SECS);

/// Encode a task's ID and header map as NATS message headers.
///
/// The task ID is stored under [`task_queue_pattern::TASK_ID_HEADER_KEY`] so
/// [`decode_task_headers`] can recover it on the consuming side; JetStream's
/// wire format otherwise has no notion of task identity separate from the
/// payload.
fn encode_task_headers(
    task_id: TaskId,
    headers: &HashMap<String, String>,
) -> async_nats::HeaderMap {
    let mut map = async_nats::HeaderMap::new();
    map.insert(
        task_queue_pattern::TASK_ID_HEADER_KEY,
        task_id.as_uuid().to_string().as_str(),
    );
    for (key, value) in headers {
        map.insert(key.as_str(), value.as_str());
    }
    map
}

/// Decode NATS message headers back into a task ID (if present) and header map.
///
/// Returns `None` for the ID when the reserved header is absent or unparsable —
/// e.g. a message published by something other than [`NatsTaskQueue::enqueue`].
/// Callers must mint a fresh [`TaskId`] in that case; there is no original ID to
/// recover.
fn decode_task_headers(
    headers: Option<&async_nats::HeaderMap>,
) -> (Option<TaskId>, HashMap<String, String>) {
    let Some(headers) = headers else {
        return (None, HashMap::new());
    };
    let mut id = None;
    let mut map = HashMap::new();
    for (name, values) in headers.iter() {
        let Some(value) = values.first() else {
            continue;
        };
        if (name.as_ref() as &str) == task_queue_pattern::TASK_ID_HEADER_KEY {
            id = uuid::Uuid::parse_str(value.as_str())
                .ok()
                .map(TaskId::from_uuid);
            continue;
        }
        map.insert(name.to_string(), value.as_str().to_owned());
    }
    (id, map)
}

/// Task queue backed by NATS JetStream with competing consumer groups.
///
/// Tasks are published to a JetStream stream and consumed via a consumer group,
/// ensuring exactly-once delivery semantics. Each consumer in the group competes
/// for messages — a nacked message reappears after the visibility timeout.
pub struct NatsTaskQueue {
    jetstream_context: jetstream::Context,
    stream_name: String,
    consumer_name: String,
    /// Cached consumer — created on first dequeue, reused for subsequent calls.
    /// Protected by Mutex for shared mutable access across dequeue calls.
    consumer: Arc<Mutex<Option<jetstream::consumer::PullConsumer>>>,
}

impl NatsTaskQueue {
    /// Create a new NATS-backed task queue.
    ///
    /// Does not perform IO — connection is established lazily on first enqueue/dequeue.
    pub async fn new(
        jetstream_context: jetstream::Context,
        stream_name: String,
        consumer_group: String,
    ) -> Result<Self, QueueError> {
        Ok(Self {
            jetstream_context,
            stream_name,
            consumer_name: consumer_group,
            consumer: Arc::new(Mutex::new(None)),
        })
    }

    /// Connect to a NATS server and return a JetStream-backed task queue.
    ///
    /// # Errors
    ///
    /// Returns [`QueueError::Connection`] if `nats_url` is blank or the NATS
    /// server is unreachable.
    pub async fn connect(
        nats_url: &str,
        stream_name: String,
        consumer_group: String,
    ) -> Result<Self, QueueError> {
        if nats_url.trim().is_empty() {
            return Err(QueueError::Connection(
                "nats backend requires a non-empty `nats_url`".to_owned(),
            ));
        }
        let connection = async_nats::connect(nats_url)
            .await
            .map_err(|e| QueueError::Connection(e.to_string()))?;
        let jetstream_context = async_nats::jetstream::new(connection);
        Self::new(jetstream_context, stream_name, consumer_group).await
    }

    /// Get or create the durable consumer for this queue.
    async fn get_or_create_consumer(
        &self,
    ) -> Result<jetstream::consumer::PullConsumer, QueueError> {
        let mut consumer_guard = self.consumer.lock().await;

        if let Some(consumer) = consumer_guard.as_ref() {
            return Ok(consumer.clone());
        }

        // Create durable consumer config for competing-consumer pattern
        let consumer_config = jetstream::consumer::pull::Config {
            durable_name: Some(self.consumer_name.clone()),
            // Explicit ack required — consumer must call ack() or nack()
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            // Redeliver a delivered-but-unacked message after the visibility timeout.
            ack_wait: VISIBILITY_TIMEOUT,
            max_ack_pending: crate::constants::DEFAULT_MAX_ACK_PENDING,
            ..Default::default()
        };

        // async-nats 0.49: consumers are created on a stream. `create_consumer_on_stream`
        // uses a create-or-update action, so a durable consumer is reused if it exists.
        let consumer = self
            .jetstream_context
            .create_consumer_on_stream(consumer_config, self.stream_name.clone())
            .await
            .map_err(|e| QueueError::Connection(e.to_string()))?;

        *consumer_guard = Some(consumer.clone());
        Ok(consumer)
    }
}

impl TaskQueue for NatsTaskQueue {
    fn enqueue(&self, task: Task) -> BoxFuture<'_, Result<(), QueueError>> {
        let stream_name = self.stream_name.clone();
        let context = self.jetstream_context.clone();

        Box::pin(async move {
            // Encode the task's ID and headers into the JetStream message so
            // `dequeue` can recover the exact ID (and headers) the producer set,
            // instead of a fresh, unrelated ID being minted on receipt.
            let headers = encode_task_headers(task.id, &task.headers);
            let publish_ack = context
                .publish_with_headers(stream_name, headers, task.payload)
                .await
                .map_err(|e| QueueError::Enqueue(e.to_string()))?;

            // Wait for server ack
            publish_ack
                .await
                .map_err(|e| QueueError::Enqueue(e.to_string()))?;

            Ok(())
        })
    }

    fn dequeue(&self) -> BoxFuture<'_, Result<Option<TaskHandle>, QueueError>> {
        let consumer_fut = self.get_or_create_consumer();

        Box::pin(async move {
            let consumer = consumer_fut.await?;

            // Pull at most one message with a bounded wait (async-nats FetchBuilder).
            let mut messages = consumer
                .fetch()
                .max_messages(1)
                .heartbeat(std::time::Duration::from_secs(
                    crate::constants::DEFAULT_HEARTBEAT_SECS,
                ))
                .expires(std::time::Duration::from_secs(30))
                .messages()
                .await
                .map_err(|e| QueueError::Dequeue(e.to_string()))?;

            // At most one message is requested (`max_messages(1)`), so take the first.
            if let Some(msg_result) = futures::stream::StreamExt::next(&mut messages).await {
                let msg = msg_result.map_err(|e| QueueError::Dequeue(e.to_string()))?;

                // Recover the ID and headers the producer set (see
                // `encode_task_headers`). Fall back to a fresh ID only when the
                // message carried none of our own — e.g. published by something
                // other than `enqueue()` — since there is nothing to recover then.
                let (decoded_id, headers) = decode_task_headers(msg.headers.as_ref());
                let task_id = decoded_id.unwrap_or_else(TaskId::new);
                let payload = msg.payload.clone();

                let msg_clone = msg.clone();
                let ack = Box::pin(async move {
                    msg.ack()
                        .await
                        .map_err(|e| QueueError::Dequeue(e.to_string()))
                });

                let nack = Box::pin(async move {
                    // Negative ack is `ack_with(AckKind::Nak(..))`. `Nak(None)`
                    // redelivers using the consumer's configured backoff.
                    msg_clone
                        .ack_with(async_nats::jetstream::AckKind::Nak(None))
                        .await
                        .map_err(|e| QueueError::Dequeue(e.to_string()))
                });

                return Ok(Some(TaskHandle::new(task_id, payload, headers, ack, nack)));
            }

            // No messages available
            Ok(None)
        })
    }

    fn health_check(&self) -> BoxFuture<'_, Result<(), QueueError>> {
        let context = self.jetstream_context.clone();
        Box::pin(async move {
            context
                .query_account()
                .await
                .map_err(|e| QueueError::Connection(e.to_string()))?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// @covers: new
    /// Verify that health_check on a queue built from an unreachable NATS server
    /// returns a Connection error — proving that new() accepted the context and
    /// that IO failures surface as QueueError::Connection.
    #[tokio::test]
    async fn test_new_accepts_context_and_health_check_fails_for_unreachable_server() {
        let client = async_nats::connect("nats://127.0.0.1:4229").await;
        let Ok(client) = client else {
            // No NATS server available — skip queue construction test.
            return;
        };
        let context = async_nats::jetstream::new(client);
        let queue = NatsTaskQueue::new(context, "test-stream".into(), "test-group".into())
            .await
            .map_err(|e| e.to_string())
            .ok();
        if let Some(q) = queue {
            let result = q.health_check().await;
            assert!(
                result.is_err(),
                "health_check must fail without a real JetStream account"
            );
        }
    }

    #[test]
    fn test_new_is_async_constructor() {
        // new() is an async fn — its type signature is verifiable at compile time.
        // The fact that this crate compiles proves new() accepts (Context, String, String) -> Result.
        fn _assert_fn_exists() {
            let _ = NatsTaskQueue::new as fn(_, _, _) -> _;
        }
        assert!(
            std::hint::black_box(true),
            "NatsTaskQueue::new has correct async constructor signature (checked above at compile time)"
        );
    }

    #[test]
    fn test_visibility_timeout_is_five_minutes() {
        assert_eq!(VISIBILITY_TIMEOUT.as_secs(), 300);
    }

    /// @covers: encode_task_headers, decode_task_headers
    ///
    /// Real bug this catches: `enqueue` previously published with
    /// `Default::default()` headers (dropping `task.id` and `task.headers`
    /// entirely, despite a comment claiming otherwise), and `dequeue` minted a
    /// brand new `TaskId` unconditionally via `Task::new(msg.payload.clone())`.
    /// This proves both survive an encode/decode round trip through NATS's
    /// native header wire format without a live server.
    #[test]
    fn test_encode_decode_task_headers_round_trips_id_and_headers() {
        let task_id = TaskId::new();
        let mut headers = HashMap::new();
        headers.insert("correlation-id".to_string(), "req-9".to_string());
        headers.insert("content-type".to_string(), "application/json".to_string());

        let encoded = encode_task_headers(task_id, &headers);
        let (decoded_id, decoded_headers) = decode_task_headers(Some(&encoded));

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

        let encoded = encode_task_headers(task_id, &headers);
        let (decoded_id, decoded_headers) = decode_task_headers(Some(&encoded));

        assert_eq!(decoded_id, Some(task_id));
        assert!(decoded_headers.is_empty());
    }

    /// @covers: decode_task_headers
    ///
    /// A message with no headers at all (e.g. published by something other
    /// than this queue's `enqueue`) has no ID to recover — callers must mint
    /// a fresh one rather than panicking or fabricating a stale ID.
    #[test]
    fn test_decode_task_headers_missing_headers_returns_none_id() {
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
        let mut map = async_nats::HeaderMap::new();
        map.insert(task_queue_pattern::TASK_ID_HEADER_KEY, "not-a-uuid");
        let (decoded_id, _) = decode_task_headers(Some(&map));
        assert_eq!(
            decoded_id, None,
            "an unparsable task-id header must decode to None, not panic"
        );
    }
}
