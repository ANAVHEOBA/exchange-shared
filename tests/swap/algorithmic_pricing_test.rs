use serial_test::serial;
#[path = "../common/mod.rs"]
mod common;

use exchange_shared::modules::swap::schema::TrocadorQuote;
use exchange_shared::services::pricing::PricingEngine;

#[serial]
#[tokio::test]
async fn test_gas_floor_protection_on_small_trades() {
    let engine = PricingEngine::new();
    let gas_cost_native = 0.002; // Roughly $5 at current prices

    // Mock quotes for a very small trade ($10 equivalent)
    let quotes = vec![TrocadorQuote {
        provider: "provider1".to_string(),
        amount_to: "0.004".to_string(), // User receives 0.004 ETH
        min_amount: Some(0.001),
        max_amount: Some(10.0),
        kycrating: Some("A".to_string()),
        waste: Some("0.0".to_string()),
        fixed: None,
        insurance: None,
        logpolicy: None,
        amount_to_usd: None,
        provider_logo: None,
        rate_id: None,
        amount_from_usd: None,
        unadjusted_amount_to: None,
        usd_total_cost_percentage: None,
        eta: Some(15.0),
    }];

    let results = engine.apply_optimal_markup(&quotes, 0.004, 10.0, "ethereum", gas_cost_native);

    // The gas cost is 0.002. Buffer is 1.5x = 0.003.
    // 1.2% of 0.004 is almost nothing.
    // The platform_fee SHOULD be 0.003 (the gas floor).
    assert!(results[0].platform_fee >= 0.003);
    assert_eq!(results[0].network_fee, 0.002);
    assert_eq!(results[0].estimated_amount, 0.0); // 0.004 - 0.003 - 0.002, floored at 0

    println!(
        "✅ Gas floor protection verified: Fee {} covers gas cost {}",
        results[0].platform_fee, gas_cost_native
    );
}

#[serial]
#[tokio::test]
async fn test_whale_discount_on_large_trades() {
    let engine = PricingEngine::new();
    let gas_cost_native = 0.001;

    // Mock quotes for a large trade ($5000 equivalent)
    let amount_from = 5000.0;
    let quotes = vec![TrocadorQuote {
        provider: "whale_provider".to_string(),
        amount_to: "5000.0".to_string(),
        min_amount: Some(0.1),
        max_amount: Some(10000.0),
        kycrating: Some("A".to_string()),
        waste: Some("0.0".to_string()),
        fixed: None,
        insurance: None,
        logpolicy: None,
        amount_to_usd: None,
        provider_logo: None,
        rate_id: None,
        amount_from_usd: None,
        unadjusted_amount_to: None,
        usd_total_cost_percentage: None,
        eta: Some(10.0),
    }];

    let results =
        engine.apply_optimal_markup(&quotes, amount_from, 5000.0, "ethereum", gas_cost_native);

    // $5000 is in the > $2000 tier (0.4%)
    // 0.4% of 5000 is 20.
    assert_eq!(results[0].platform_fee, 20.0);

    println!("✅ Whale discount verified: Large trade fee is 0.4%");
}

#[serial]
#[tokio::test]
async fn test_volatility_premium_during_market_spread() {
    let engine = PricingEngine::new();
    let gas_cost_native = 0.001;

    // Providers have wildly different prices (high spread)
    let quotes = vec![
        TrocadorQuote {
            provider: "p1".to_string(),
            amount_to: "100.0".to_string(),
            min_amount: None,
            max_amount: None,
            kycrating: None,
            waste: None,
            fixed: None,
            insurance: None,
            logpolicy: None,
            amount_to_usd: None,
            provider_logo: None,
            rate_id: None,
            amount_from_usd: None,
            unadjusted_amount_to: None,
            usd_total_cost_percentage: None,
            eta: None,
        },
        TrocadorQuote {
            provider: "p2".to_string(),
            amount_to: "95.0".to_string(), // 5% spread
            min_amount: None,
            max_amount: None,
            kycrating: None,
            waste: None,
            fixed: None,
            insurance: None,
            logpolicy: None,
            amount_to_usd: None,
            provider_logo: None,
            rate_id: None,
            amount_from_usd: None,
            unadjusted_amount_to: None,
            usd_total_cost_percentage: None,
            eta: None,
        },
    ];

    let results = engine.apply_optimal_markup(&quotes, 100.0, 100.0, "ethereum", gas_cost_native);

    // Spread is > 2%, so 0.5% premium is added to the 1.2% tier (since 100 is small < 200)
    // Total rate should be 1.7% (1.2 + 0.5)
    // 1.7% of 100 is 1.7.
    assert!((results[0].platform_fee - 1.7).abs() < 0.001);

    println!("✅ Volatility premium verified: Fee increased during high spread");
}

#[serial]
#[tokio::test]
async fn test_trocador_quote_metadata_is_preserved_in_rate_response() {
    let engine = PricingEngine::new();

    let quotes = vec![TrocadorQuote {
        provider: "metadata_provider".to_string(),
        amount_to: "25.5".to_string(),
        min_amount: Some(0.01),
        max_amount: Some(10.0),
        kycrating: Some("B".to_string()),
        waste: Some("-0.4".to_string()),
        fixed: Some("True".to_string()),
        insurance: Some(100.0),
        logpolicy: Some("A".to_string()),
        amount_to_usd: Some("510.0".to_string()),
        provider_logo: Some("https://example.com/provider.png".to_string()),
        rate_id: Some("rate-123".to_string()),
        amount_from_usd: Some("512.0".to_string()),
        unadjusted_amount_to: Some(25.8),
        usd_total_cost_percentage: Some("-0.39".to_string()),
        eta: Some(12.0),
    }];

    let results = engine.apply_optimal_markup(&quotes, 1.0, 512.0, "btc", 0.01);
    let rate = &results[0];

    assert_eq!(
        rate.rate_type,
        exchange_shared::modules::swap::schema::RateType::Fixed
    );
    assert!(rate.fixed);
    assert_eq!(rate.kyc_rating.as_deref(), Some("B"));
    assert_eq!(rate.privacy_rating.as_deref(), Some("B"));
    assert_eq!(rate.logpolicy.as_deref(), Some("A"));
    assert_eq!(rate.insurance, Some(100.0));
    assert_eq!(rate.amount_from_usd, Some(512.0));
    assert_eq!(rate.amount_to_usd, Some(510.0));
    assert_eq!(rate.unadjusted_amount_to, Some(25.8));
    assert_eq!(rate.rate_id.as_deref(), Some("rate-123"));
    assert_eq!(
        rate.provider_logo.as_deref(),
        Some("https://example.com/provider.png")
    );
    assert_eq!(rate.spread_percentage, Some(-0.4));
    assert_eq!(rate.usd_total_cost_percentage, Some(-0.39));
}
