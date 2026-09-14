//! Kafka-specific tuning constants — this crate's own concern.

/// Kafka producer `message.timeout.ms` — maximum time to wait for delivery acknowledgement.
pub(crate) const KAFKA_MESSAGE_TIMEOUT_MS: &str = "5000";

/// Kafka consumer `session.timeout.ms` — broker considers consumer dead after this interval.
pub(crate) const KAFKA_SESSION_TIMEOUT_MS: &str = "6000";

/// Kafka health-check metadata fetch timeout in seconds.
pub(crate) const KAFKA_HEALTH_CHECK_TIMEOUT_SECS: u64 = 5;

/// Kafka dequeue poll timeout in milliseconds.
///
/// `dequeue()` waits at most this long for a message before returning `None`.
/// Sized to keep queue workers responsive without spinning.
pub(crate) const KAFKA_DEQUEUE_POLL_TIMEOUT_MS: u64 = 100;
