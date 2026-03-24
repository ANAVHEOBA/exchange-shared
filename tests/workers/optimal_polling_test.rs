// =============================================================================
// INTEGRATION TESTS - OPTIMAL POLLING STRATEGY
// Verifies the mathematical QCD / Hazard Rate model
// =============================================================================

#[path = "../common/mod.rs"]
mod common;

use exchange_shared::services::monitor::strategy::PollingStrategy;
use std::time::Duration;

#[test]
fn test_optimal_polling_mathematical_behavior() {
    // cost_per_poll = 1.0, cost_per_delay_sec = 0.05
    // This means 20 seconds of delay equals the cost of one API call.
    let strategy = PollingStrategy::new(1.0, 0.05);

    // 1. PHASE: START (t = 10s)
    // Likelihood of finishing in 10s is near zero.
    // The model should be cautious/slow.
    let interval_start = strategy.calculate_next_interval(10);
    println!("Interval at 10s: {:?}", interval_start);
    assert!(
        interval_start >= Duration::from_secs(60),
        "Should poll slowly at start (uncertainty)"
    );

    // 2. PHASE: ANTICIPATION (t = 600s / 10m)
    // We are approaching the median (mu=6.4 -> ~600s).
    // Urgency should peak here.
    let interval_peak = strategy.calculate_next_interval(600);
    println!("Interval at 600s: {:?}", interval_peak);
    assert!(
        interval_peak < interval_start,
        "Urgency should increase as we approach expected completion"
    );
    assert!(
        interval_peak >= Duration::from_secs(5),
        "Should never violate API safety min"
    );

    // 3. PHASE: LONG TAIL (t = 3600s / 1h)
    // If it hasn't finished in 1 hour, it's a "slow" swap.
    // We should decay to save API credits.
    let interval_tail = strategy.calculate_next_interval(3600);
    println!("Interval at 1h: {:?}", interval_tail);
    assert!(
        interval_tail > interval_peak,
        "Should decay frequency in the long tail"
    );
}

#[test]
fn test_hazard_rate_probability_bounds() {
    let strategy = PollingStrategy::new(1.0, 0.1);

    // Probability of finishing in next 30s given we just started
    let prob_early = strategy.probability_due(10, 30);

    // Probability of finishing in next 30s given we are at the median (600s)
    let prob_median = strategy.probability_due(600, 30);

    println!(
        "Prob at 10s: {:.4}, Prob at 600s: {:.4}",
        prob_early, prob_median
    );
    assert!(
        prob_median > prob_early,
        "Completion probability must be higher near the distribution mean"
    );
    assert!(prob_median <= 1.0);
    assert!(prob_early >= 0.0);
}
