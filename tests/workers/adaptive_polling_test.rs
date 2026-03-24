// =============================================================================
// INTEGRATION TESTS - ADAPTIVE POLLING
// Verifies interval decay logic (Urgent vs Stale)
// =============================================================================

#[path = "../common/mod.rs"]
mod common;

use exchange_shared::services::monitor::strategy::PollingStrategy;

#[tokio::test]
async fn test_polling_interval_decay_logic() {
    let strategy = PollingStrategy::new(1.0, 0.05);

    // 1. Fresh state (10s): Should poll slowly due to high uncertainty
    let interval_fresh = strategy.calculate_next_interval(10);
    assert!(interval_fresh.as_secs() >= 30);

    // 2. High Urgency (600s): Near the median, should poll frequently
    let interval_urgent = strategy.calculate_next_interval(600);
    assert!(interval_urgent < interval_fresh);

    // 3. Stale state (3600s): Long tail, should decay to save API costs
    let interval_stale = strategy.calculate_next_interval(3600);
    assert!(interval_stale > interval_urgent);

    println!("✅ Mathematical adaptive interval logic verified");
}
