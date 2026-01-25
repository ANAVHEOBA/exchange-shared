use serde_json::Value;

#[path = "../common/mod.rs"]
mod common;
use common::{setup_test_server, timed_get};
use std::time::Duration;
use tokio::time::sleep;

// =============================================================================
// INTEGRATION TESTS - RATES ENDPOINT
// These tests call the actual Trocador API via our backend
// =============================================================================

#[tokio::test]
async fn test_get_rates_successful() {
    sleep(Duration::from_secs(1)).await; // Prevent Rate Limit
    let server = setup_test_server().await;

    // BTC -> XMR (Standard Swap)
    let url = "/swap/rates?from=btc&to=xmr&amount=0.01&network_from=Mainnet&network_to=Mainnet";
    
    let response = timed_get(&server, url).await;
    
    // If this fails with 500, it might be because the endpoint isn't implemented yet, 
    // but this is the integration test defining the behavior.
    response.assert_status_ok();

    let json: Value = response.json();

    // 1. Verify Request Echo (from, to, amount)
    assert_eq!(json["from"], "btc");
    assert_eq!(json["to"], "xmr");
    // Handle floating point comparison loosely or exact if preserved
    assert!(json["amount"].as_f64().unwrap() > 0.0);

    // 2. CRUCIAL: Verify trade_id (needed for next step)
    // Trocador returns a "trade_id" (or we might wrap it)
    assert!(
        json.get("trade_id").is_some(), 
        "Response must contain 'trade_id' for creating the swap"
    );
    let trade_id = json["trade_id"].as_str().unwrap();
    assert!(!trade_id.is_empty());

    // 3. Verify Rates List
    let rates = json["rates"].as_array().expect("Should have 'rates' array");
    assert!(!rates.is_empty(), "Should return at least one rate quote");

    // 4. Validate Rate Structure
    let best_rate = &rates[0];
    assert!(best_rate.get("provider").is_some());
    assert!(best_rate.get("provider_name").is_some());
    assert!(best_rate.get("rate").is_some());
    assert!(best_rate.get("estimated_amount").is_some());
    assert!(best_rate.get("min_amount").is_some());
    assert!(best_rate.get("max_amount").is_some());
    
    // Check values are reasonable
    assert!(best_rate["rate"].as_f64().unwrap() > 0.0);
    assert!(best_rate["estimated_amount"].as_f64().unwrap() > 0.0);
}

#[tokio::test]
async fn test_get_rates_sorted_by_best_price() {
    sleep(Duration::from_secs(1)).await; // Prevent Rate Limit
    let server = setup_test_server().await;
    
    // BTC -> ETH (Likely multiple providers)
    let url = "/swap/rates?from=btc&to=eth&amount=0.01&network_from=Mainnet&network_to=ERC20";
    let response = timed_get(&server, url).await;
    response.assert_status_ok();

    let json: Value = response.json();
    let rates = json["rates"].as_array().unwrap();

    if rates.len() >= 2 {
        let first_rate = rates[0]["estimated_amount"].as_f64().unwrap();
        let second_rate = rates[1]["estimated_amount"].as_f64().unwrap();

        // Best rate (highest estimated amount) should be first
        assert!(
            first_rate >= second_rate,
            "Rates should be sorted descending by estimated amount. First: {}, Second: {}",
            first_rate,
            second_rate
        );
    }
}

#[tokio::test]
async fn test_get_rates_minimum_amount_validation() {
    sleep(Duration::from_secs(1)).await; // Prevent Rate Limit
    let server = setup_test_server().await;

    // Extremely small amount that should be below limits
    let url = "/swap/rates?from=btc&to=xmr&amount=0.00000001&network_from=Mainnet&network_to=Mainnet";
    
    let response = timed_get(&server, url).await;
    
    // Expecting either 400 Bad Request or a structured error response
    // Trocador API might return valid response but empty quotes, or an error.
    // Our API should probably return 400 or a specific error code.
    if response.status_code() == 200 {
        let json: Value = response.json();
        // If 200, rates should be empty or explicitly indicate issue
        if let Some(rates) = json.get("rates") {
             // It's possible some provider supports it, but unlikely. 
             // Just ensure we don't crash.
             println!("Received {} rates for tiny amount", rates.as_array().unwrap().len());
        }
    } else {
        // 400 or 422 is acceptable
        assert!(response.status_code().as_u16() >= 400);
    }
}

#[tokio::test]
async fn test_get_rates_invalid_pair() {
    sleep(Duration::from_secs(1)).await; // Prevent Rate Limit
    let server = setup_test_server().await;

    // Invalid ticker
    let url = "/swap/rates?from=INVALIDCOIN&to=xmr&amount=0.1&network_from=Mainnet&network_to=Mainnet";
    
    let response = timed_get(&server, url).await;
    
    // Should fail
    assert!(
        response.status_code().as_u16() >= 400,
        "Should return error for invalid coin ticker"
    );
}

#[tokio::test]
async fn test_get_rates_missing_params() {
    sleep(Duration::from_secs(1)).await; // Prevent Rate Limit
    let server = setup_test_server().await;

    // Missing 'to' and 'amount'
    let url = "/swap/rates?from=btc";
    
    let response = timed_get(&server, url).await;
    
    // Axum should reject this automatically
    assert_eq!(response.status_code(), 400);
}

#[tokio::test]
async fn test_get_rates_includes_network_fees() {
    sleep(Duration::from_secs(1)).await; // Prevent Rate Limit
    let server = setup_test_server().await;

    let url = "/swap/rates?from=btc&to=eth&amount=0.01&network_from=Mainnet&network_to=ERC20";
    let response = timed_get(&server, url).await;
    response.assert_status_ok();

    let json: Value = response.json();
    let rates = json["rates"].as_array().unwrap();

    if let Some(first_rate) = rates.first() {
        // Ensure fee fields are present (even if 0)
        assert!(first_rate.get("network_fee").is_some());
        assert!(first_rate.get("total_fee").is_some());
    }
}