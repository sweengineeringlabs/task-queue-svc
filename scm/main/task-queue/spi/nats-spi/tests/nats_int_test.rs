//! Integration tests for the async-nats dependency itself.

use async_nats::connect as nats_connect;

/// @covers: async-nats
/// Verify that async-nats returns an error when the NATS server is unreachable.
#[tokio::test]
async fn test_async_nats_connect_fails_for_unreachable_host() {
    let result = nats_connect("nats://127.0.0.1:4229").await;
    assert!(
        result.is_err(),
        "expected connection error for unreachable NATS server"
    );
}
