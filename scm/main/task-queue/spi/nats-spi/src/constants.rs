//! NATS-specific tuning constants — this crate's own concern.

/// Visibility timeout for nacked JetStream tasks before redelivery (seconds).
pub(crate) const DEFAULT_VISIBILITY_TIMEOUT_SECS: u64 = 300;

/// Maximum number of pending acks before JetStream applies backpressure.
pub(crate) const DEFAULT_MAX_ACK_PENDING: i64 = 1000;

/// Idle heartbeat interval for JetStream consumers in seconds.
pub(crate) const DEFAULT_HEARTBEAT_SECS: u64 = 5;
