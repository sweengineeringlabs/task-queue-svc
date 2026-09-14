//! Direct-dep integration test for rdkafka (arch rule 95 — dep must have test coverage).
#![allow(clippy::expect_used)]
//!
//! This test exercises rdkafka unconditionally (no feature gate) to satisfy the
//! structural audit requirement that every dependency used in src/ has integration
//! test coverage. The same pattern is used for async-nats in nats_int_test.rs.

use rdkafka::config::ClientConfig;
use rdkafka::producer::BaseProducer;

/// @covers: rdkafka
/// Verifies that rdkafka compiles and a ClientConfig can be constructed without a
/// running Kafka cluster (rdkafka connects lazily on first produce/consume call).
#[test]
fn test_rdkafka_client_config_constructs_without_network() {
    let result: Result<BaseProducer, _> = ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:9999")
        .create();
    let producer = result.expect("rdkafka ClientConfig must succeed before IO");
    // Verify the producer was created successfully by checking it's a valid type
    drop(producer);
}
