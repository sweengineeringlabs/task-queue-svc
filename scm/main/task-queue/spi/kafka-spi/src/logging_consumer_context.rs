//! [`LoggingConsumerContext`] — rdkafka consumer context that traces rebalance events.

use rdkafka::consumer::{ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::ClientContext;

/// Consumer context that logs partition assignment events for observability.
///
/// Rebalance events are emitted at `INFO` level so operators can correlate
/// unexpected ack/nack failures with partition revocations in their log stream.
pub(crate) struct LoggingConsumerContext;

impl ClientContext for LoggingConsumerContext {}

impl ConsumerContext for LoggingConsumerContext {
    fn pre_rebalance(
        &self,
        base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &Rebalance<'_>,
    ) {
        let _ = base_consumer;
        tracing::info!(event = "kafka.pre_rebalance", ?rebalance);
    }

    fn post_rebalance(
        &self,
        base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &Rebalance<'_>,
    ) {
        let _ = base_consumer;
        tracing::info!(event = "kafka.post_rebalance", ?rebalance);
    }
}

/// [`StreamConsumer`] parameterised with [`LoggingConsumerContext`].
pub(crate) type LoggingConsumer = StreamConsumer<LoggingConsumerContext>;
