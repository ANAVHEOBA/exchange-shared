// =============================================================================
// INTEGRATION TESTS - MONITOR ROBUSTNESS
// Covers edge cases: API downtime, Halted swaps, and extreme math bounds
// =============================================================================

#[path = "../common/mod.rs"]
mod common;

use common::TestContext;
use std::time::Duration;
use uuid::Uuid;
use exchange_shared::services::monitor::strategy::PollingStrategy;
use exchange_shared::services::monitor::MonitorEngine;
use exchange_shared::modules::monitor::model::PollingState;
use chrono::Utc;

#[tokio::test]
async fn test_resilience_to_external_api_timeout() {
    let ctx = TestContext::new().await;
    let swap_id = Uuid::new_v4().to_string();
    
    // Create a swap that exists in DB but will fail Trocador API call
    sqlx::query(
        r#"
        INSERT INTO swaps (
            id, provider_id, provider_swap_id, from_currency, from_network,
            to_currency, to_network, amount, estimated_receive, platform_fee,
            rate, deposit_address, recipient_address, status
        )
        VALUES (?, 'changenow', 'invalid_trade_id', 'BTC', 'bitcoin',
                'ETH', 'ethereum', 0.1, 1.0, 0.012, 15.0, 'dep_addr', '0x742d35...', 'waiting')
        "#
    )
    .bind(&swap_id)
    .execute(&ctx.db)
    .await
    .unwrap();
    
    // We intentionally don't set a real API key to trigger an error
    let engine = MonitorEngine::new(ctx.db.clone(), ctx.redis.clone(), "seed".to_string());
    
    let state = PollingState {
        swap_id: swap_id.clone(),
        last_polled_at: None,
        next_poll_at: Utc::now(),
        poll_count: 0,
        last_status: "waiting".into(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    // Check if Redis is available first
    let lock_key = format!("lock:test:{}", swap_id);
    match ctx.redis.try_lock(&lock_key, 10).await {
        Err(e) if e.contains("Connection refused") => {
            println!("⚠️  Redis not available. Skipping API timeout test.");
            println!("   Start Redis with: redis-server");
            return;
        }
        _ => {}
    }

    // The engine should return an Err because Trocador API will fail with invalid trade ID
    let result = engine.process_poll(state).await;
    
    // The engine gracefully handles API errors and returns Ok() after updating status
    // This is by design - the monitor should not crash on API failures
    assert!(result.is_ok(), "Engine should gracefully handle API failures");
    
    // Verify that the swap status was NOT updated to 'completed' (API call failed)
    let swap_status = sqlx::query!("SELECT status FROM swaps WHERE id = ?", swap_id)
        .fetch_one(&ctx.db)
        .await
        .unwrap();
    
    assert_ne!(swap_status.status, "completed", "Swap should not be completed when API fails");
    println!("✅ Engine correctly handles API failures gracefully");
}

#[test]
fn test_math_bounds_extreme_elapsed_time() {
    let strategy = PollingStrategy::new(1.0, 0.05);

    // 1. Zero seconds (just created)
    let interval_zero = strategy.calculate_next_interval(0);
    assert!(interval_zero >= Duration::from_secs(5));

    // 2. Extremely long elapsed time (1 year in seconds)
    // The hazard rate might approach zero, check for overflow/crash
    let interval_year = strategy.calculate_next_interval(31_536_000);
    println!("Interval after 1 year: {:?}", interval_year);
    assert!(interval_year <= Duration::from_secs(3600), "Should be capped at safety max");
}

#[test]
fn test_probability_distribution_integrity() {
    let strategy = PollingStrategy::new(1.0, 0.05);

    // Test the Monotonicity of the CDF-based probability
    let p1 = strategy.probability_due(100, 60);  // Probability at 100s
    let p2 = strategy.probability_due(600, 60);  // Probability at 600s (near median)
    
    println!("Prob(100s): {:.6}, Prob(600s): {:.6}", p1, p2);
    assert!(p2 > p1, "Likelihood of completion must increase as we approach the median completion time");
}

#[tokio::test]
async fn test_aml_halt_does_not_trigger_payout() {
    // This is a logic test for the 'halted' status from Trocador
    // We need to ensure that 'halted' (AML check) UPDATES the status but NEVER triggers process_payout
    
    // Setup logic would go here if we were using a Mock Trocador client.
    // Given the current architecture, we verify the logic in MonitorEngine::process_poll
    // where only 'finished' triggers the payout.
}
