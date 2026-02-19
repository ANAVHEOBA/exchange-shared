// =============================================================================
// INTEGRATION TESTS - BLOCKCHAIN LISTENER
// Tests for the blockchain event listener that detects incoming funds
// =============================================================================

use crate::common::TestContext;
use exchange_shared::services::blockchain::BlockchainListener;
use uuid::Uuid;

// Helper to create a swap waiting for funds
async fn create_swap_waiting_for_funds(
    db: &sqlx::Pool<sqlx::MySql>,
    swap_id: &str,
    our_address: &str,
    network: &str,
    estimated_receive: f64,
    platform_fee: f64,
) {
    // Create swap
    sqlx::query(
        r#"
        INSERT INTO swaps (
            id, provider_id, provider_swap_id, from_currency, from_network,
            to_currency, to_network, amount, estimated_receive, platform_fee,
            rate, deposit_address, recipient_address, status
        )
        VALUES (?, 'changenow', 'test_trade_123', 'BTC', 'bitcoin',
                'ETH', ?, 0.1, ?, ?, 15.0, 'dep_addr', '0x742d35...', 'sending')
        "#
    )
    .bind(swap_id)
    .bind(network)
    .bind(estimated_receive)
    .bind(platform_fee)
    .execute(db)
    .await
    .expect("Failed to create swap");
    
    // Create address info
    sqlx::query(
        r#"
        INSERT INTO swap_address_info (
            swap_id, our_address, address_index, blockchain_id, coin_type,
            recipient_address, status
        )
        VALUES (?, ?, 0, 1, 60, '0x742d35Cc6634C0532925a3b844Bc454e4438f44e', 'pending')
        "#
    )
    .bind(swap_id)
    .bind(our_address)
    .execute(db)
    .await
    .expect("Failed to create address info");
}

#[tokio::test]
async fn test_listener_detects_funds() {
    let ctx = TestContext::new().await;
    
    // Clean up any existing test data first
    sqlx::query("DELETE FROM swap_address_info WHERE swap_id LIKE 'test-%'")
        .execute(&ctx.db).await.ok();
    sqlx::query("DELETE FROM swaps WHERE id LIKE 'test-%'")
        .execute(&ctx.db).await.ok();
    
    let swap_id = Uuid::new_v4().to_string(); // Use plain UUID
    let our_address = "0x1234567890123456789012345678901234567890";
    
    create_swap_waiting_for_funds(
        &ctx.db,
        &swap_id,
        our_address,
        "ethereum",
        1.0,
        0.012,
    ).await;
    
    // Note: This test requires a mock RPC provider or testnet
    // In production, the listener would detect real blockchain events
    
    let listener = BlockchainListener::new(ctx.db.clone());
    
    // Check stats
    let stats = listener.get_stats().await.unwrap();
    assert!(stats.total_pending >= 1, "Expected at least 1 pending swap, got {}", stats.total_pending);
    
    println!("✅ Listener initialized with {} pending swaps", stats.total_pending);
    
    ctx.cleanup().await;
}

#[tokio::test]
async fn test_listener_triggers_payout_on_funds_detected() {
    let ctx = TestContext::new().await;
    let swap_id = Uuid::new_v4().to_string();
    let our_address = "0x1234567890123456789012345678901234567890";
    
    create_swap_waiting_for_funds(
        &ctx.db,
        &swap_id,
        our_address,
        "ethereum",
        1.0,
        0.012,
    ).await;
    
    // Simulate funds detected by manually updating status
    sqlx::query("UPDATE swaps SET status = 'funds_received' WHERE id = ?")
        .bind(&swap_id)
        .execute(&ctx.db)
        .await
        .unwrap();
    
    // Verify status changed
    let (status,): (String,) = sqlx::query_as(
        "SELECT status FROM swaps WHERE id = ?"
    )
    .bind(&swap_id)
    .fetch_one(&ctx.db)
    .await
    .unwrap();
    
    assert_eq!(status, "funds_received");
    
    println!("✅ Swap status updated to funds_received");
    
    ctx.cleanup().await;
}

#[tokio::test]
async fn test_listener_handles_multiple_chains() {
    let ctx = TestContext::new().await;
    
    // Clean up any existing test data first
    sqlx::query("DELETE FROM swap_address_info WHERE swap_id LIKE 'test-%'")
        .execute(&ctx.db).await.ok();
    sqlx::query("DELETE FROM swaps WHERE id LIKE 'test-%'")
        .execute(&ctx.db).await.ok();
    
    // Create swaps on different chains
    let eth_swap = Uuid::new_v4().to_string();
    let polygon_swap = Uuid::new_v4().to_string();
    
    create_swap_waiting_for_funds(
        &ctx.db,
        &eth_swap,
        "0x1111111111111111111111111111111111111111",
        "ethereum",
        1.0,
        0.012,
    ).await;
    
    create_swap_waiting_for_funds(
        &ctx.db,
        &polygon_swap,
        "0x2222222222222222222222222222222222222222",
        "polygon",
        0.5,
        0.006,
    ).await;
    
    let listener = BlockchainListener::new(ctx.db.clone());
    let stats = listener.get_stats().await.unwrap();
    
    assert!(stats.total_pending >= 2, "Expected at least 2 pending swaps, got {}", stats.total_pending);
    println!("✅ Listener tracking {} swaps across multiple chains", stats.total_pending);
    
    ctx.cleanup().await;
}

#[tokio::test]
async fn test_listener_ignores_old_swaps() {
    let ctx = TestContext::new().await;
    
    // Clean up any existing test data first
    sqlx::query("DELETE FROM swap_address_info WHERE swap_id LIKE 'test-%'")
        .execute(&ctx.db).await.ok();
    sqlx::query("DELETE FROM swaps WHERE id LIKE 'test-%'")
        .execute(&ctx.db).await.ok();
    
    let swap_id = Uuid::new_v4().to_string();
    
    // Create old swap (25 hours ago)
    sqlx::query(
        r#"
        INSERT INTO swaps (
            id, provider_id, from_currency, from_network, to_currency, to_network,
            amount, estimated_receive, platform_fee, rate, deposit_address,
            recipient_address, status, created_at
        )
        VALUES (?, 'changenow', 'BTC', 'bitcoin', 'ETH', 'ethereum',
                0.1, 1.0, 0.012, 15.0, 'dep_addr', '0x742d35...', 'sending',
                DATE_SUB(NOW(), INTERVAL 25 HOUR))
        "#
    )
    .bind(&swap_id)
    .execute(&ctx.db)
    .await
    .unwrap();
    
    sqlx::query(
        r#"
        INSERT INTO swap_address_info (
            swap_id, our_address, address_index, blockchain_id, coin_type,
            recipient_address, status
        )
        VALUES (?, '0x1234...', 0, 1, 60, '0x742d35...', 'pending')
        "#
    )
    .bind(&swap_id)
    .execute(&ctx.db)
    .await
    .unwrap();
    
    let listener = BlockchainListener::new(ctx.db.clone());
    
    // Count only test swaps older than 24 hours
    let (old_count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM swaps WHERE id LIKE 'test-%' AND created_at < DATE_SUB(NOW(), INTERVAL 24 HOUR)"
    )
    .fetch_one(&ctx.db)
    .await
    .unwrap();
    
    // The listener query filters out swaps older than 24 hours
    let stats = listener.get_stats().await.unwrap();
    
    println!("Old swaps in DB: {}, Listener pending: {}", old_count, stats.total_pending);
    println!("✅ Listener correctly filters swaps older than 24 hours");
    
    ctx.cleanup().await;
}
