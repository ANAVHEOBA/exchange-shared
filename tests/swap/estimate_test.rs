use serial_test::serial;
use serde_json::Value;

#[path = "../common/mod.rs"]
mod common;
use common::{setup_test_server, timed_get};
use std::time::Duration;
use tokio::time::sleep;

// =============================================================================
// INTEGRATION TESTS - ESTIMATE ENDPOINT
// Quick rate preview without creating swap
// =============================================================================

#[serial]
#[tokio::test]
async fn test_estimate_successful() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // BTC -> XMR estimate
    let url = "/swap/estimate?from=btc&to=xmr&amount=0.01&network_from=Mainnet&network_to=Mainnet";
    
    let response = timed_get(&server, url).await;
    response.assert_status_ok();

    let json: Value = response.json();

    // Verify request echo
    assert_eq!(json["from"], "btc");
    assert_eq!(json["to"], "xmr");
    assert!(json["amount"].as_f64().unwrap() > 0.0);

    // Verify rate data
    assert!(json["best_rate"].as_f64().unwrap() > 0.0);
    assert!(json["estimated_receive"].as_f64().unwrap() > 0.0);
    assert!(json["estimated_receive_min"].as_f64().unwrap() > 0.0);
    assert!(json["estimated_receive_max"].as_f64().unwrap() > 0.0);

    // Verify fee breakdown
    assert!(json.get("platform_fee").is_some());
    assert!(json.get("total_fee").is_some());

    // Verify slippage info
    assert!(json.get("slippage_percentage").is_some());
    assert!(json.get("price_impact").is_some());

    // Verify provider info
    assert!(json.get("best_provider").is_some());
    assert!(json["provider_count"].as_u64().unwrap() > 0);

    // Verify metadata
    assert!(json.get("cached").is_some());
    assert!(json.get("cache_age_seconds").is_some());
    assert!(json.get("expires_in_seconds").is_some());

    // Verify warnings array exists
    assert!(json.get("warnings").is_some());
    
    println!("✅ Estimate successful: {} {} -> {} {}", 
        json["amount"], json["from"], json["estimated_receive"], json["to"]);
}

#[serial]
#[tokio::test]
async fn test_estimate_caching() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let url = "/swap/estimate?from=btc&to=eth&amount=0.1&network_from=Mainnet&network_to=ERC20";
    
    // First request (cache miss)
    let response1 = timed_get(&server, url).await;
    response1.assert_status_ok();
    let json1: Value = response1.json();
    
    // Second request (should be cached)
    let response2 = timed_get(&server, url).await;
    response2.assert_status_ok();
    let json2: Value = response2.json();
    
    // Verify second request was cached
    assert_eq!(json2["cached"].as_bool().unwrap(), true);
    assert!(json2["cache_age_seconds"].as_i64().unwrap() >= 0);
    
    // Verify rates are consistent
    assert_eq!(
        json1["best_rate"].as_f64().unwrap(),
        json2["best_rate"].as_f64().unwrap()
    );
    
    println!("✅ Cache working: cached={}, age={}s", 
        json2["cached"], json2["cache_age_seconds"]);
}

#[serial]
#[tokio::test]
async fn test_estimate_bucketed_caching() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Request with 0.1 BTC
    let url1 = "/swap/estimate?from=btc&to=eth&amount=0.1&network_from=Mainnet&network_to=ERC20";
    let response1 = timed_get(&server, url1).await;
    response1.assert_status_ok();
    
    sleep(Duration::from_millis(100)).await;
    
    // Request with 0.101 BTC (should hit bucketed cache)
    let url2 = "/swap/estimate?from=btc&to=eth&amount=0.101&network_from=Mainnet&network_to=ERC20";
    let response2 = timed_get(&server, url2).await;
    response2.assert_status_ok();
    let json2: Value = response2.json();
    
    // Should be cached (bucketed to 0.1)
    assert_eq!(json2["cached"].as_bool().unwrap(), true);
    
    println!("✅ Bucketed cache working: 0.101 BTC bucketed to 0.1 BTC");
}

#[serial]
#[tokio::test]
async fn test_estimate_slippage_calculation() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let url = "/swap/estimate?from=btc&to=eth&amount=0.1&network_from=Mainnet&network_to=ERC20";
    
    let response = timed_get(&server, url).await;
    response.assert_status_ok();
    let json: Value = response.json();
    
    let estimated = json["estimated_receive"].as_f64().unwrap();
    let min = json["estimated_receive_min"].as_f64().unwrap();
    let max = json["estimated_receive_max"].as_f64().unwrap();
    let slippage_pct = json["slippage_percentage"].as_f64().unwrap();
    
    // Verify slippage bounds
    assert!(min < estimated, "Min should be less than estimated");
    assert!(max > estimated, "Max should be greater than estimated");
    assert!(slippage_pct > 0.0, "Slippage should be positive");
    assert!(slippage_pct < 10.0, "Slippage should be reasonable (<10%)");
    
    println!("✅ Slippage: {:.2}%, Range: {:.4} - {:.4} ETH", 
        slippage_pct, min, max);
}

#[serial]
#[tokio::test]
async fn test_estimate_large_trade_warning() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Large trade (1 BTC ~= $60k)
    let url = "/swap/estimate?from=btc&to=eth&amount=1.0&network_from=Mainnet&network_to=ERC20";
    
    let response = timed_get(&server, url).await;
    response.assert_status_ok();
    let json: Value = response.json();
    
    let warnings = json["warnings"].as_array().unwrap();
    
    // Should have warning about large trade
    let has_large_trade_warning = warnings.iter().any(|w| {
        w.as_str().unwrap().contains("Large trade")
    });
    
    assert!(has_large_trade_warning, "Should warn about large trades");
    
    println!("✅ Large trade warning: {:?}", warnings);
}

#[serial]
#[tokio::test]
async fn test_estimate_invalid_amount() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Negative amount
    let url = "/swap/estimate?from=btc&to=eth&amount=-0.1&network_from=Mainnet&network_to=ERC20";
    
    let response = timed_get(&server, url).await;
    response.assert_status_bad_request();
    
    println!("✅ Negative amount rejected");
}

#[serial]
#[tokio::test]
async fn test_estimate_missing_parameters() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Missing 'to' parameter
    let url = "/swap/estimate?from=btc&amount=0.1&network_from=Mainnet&network_to=ERC20";
    
    let response = timed_get(&server, url).await;
    response.assert_status_bad_request();
    
    println!("✅ Missing parameters rejected");
}

#[serial]
#[tokio::test]
async fn test_estimate_vs_rates_consistency() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Get estimate
    let estimate_url = "/swap/estimate?from=btc&to=eth&amount=0.1&network_from=Mainnet&network_to=ERC20";
    let estimate_response = timed_get(&server, estimate_url).await;
    estimate_response.assert_status_ok();
    let estimate_json: Value = estimate_response.json();
    
    sleep(Duration::from_millis(500)).await;
    
    // Get actual rates
    let rates_url = "/swap/rates?from=btc&to=eth&amount=0.1&network_from=Mainnet&network_to=ERC20";
    let rates_response = timed_get(&server, rates_url).await;
    rates_response.assert_status_ok();
    let rates_json: Value = rates_response.json();
    
    let estimate_amount = estimate_json["estimated_receive"].as_f64().unwrap();
    let rates_amount = rates_json["rates"][0]["estimated_amount"].as_f64().unwrap();
    
    // Should be within 5% (accounting for cache staleness)
    let diff_pct = ((estimate_amount - rates_amount).abs() / rates_amount) * 100.0;
    assert!(diff_pct < 5.0, "Estimate and rates should be consistent (diff: {:.2}%)", diff_pct);
    
    println!("✅ Estimate vs Rates: {:.4} vs {:.4} ETH (diff: {:.2}%)", 
        estimate_amount, rates_amount, diff_pct);
}

#[serial]
#[tokio::test]
async fn test_estimate_performance() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let url = "/swap/estimate?from=btc&to=eth&amount=0.1&network_from=Mainnet&network_to=ERC20";
    
    // First request
    let response1 = timed_get(&server, url).await;
    response1.assert_status_ok();
    let json1: Value = response1.json();
    
    sleep(Duration::from_millis(100)).await;
    
    // Second request (should be cached)
    let response2 = timed_get(&server, url).await;
    response2.assert_status_ok();
    let json2: Value = response2.json();
    
    // Verify second request was cached
    assert_eq!(json2["cached"].as_bool().unwrap(), true, "Second request should be cached");
    
    // Verify cache age is reasonable (allow up to 60s since cache TTL is 60s)
    let cache_age = json2["cache_age_seconds"].as_i64().unwrap();
    assert!(cache_age >= 0 && cache_age <= 60, "Cache age should be 0-60 seconds, got {}", cache_age);
    
    // Verify expires_in is positive
    let expires_in = json2["expires_in_seconds"].as_i64().unwrap();
    assert!(expires_in >= 0, "Expires in should be non-negative, got {}", expires_in);
    
    println!("✅ Performance: First cached={}, Second cached={}, age={}s, expires_in={}s", 
        json1["cached"], json2["cached"], cache_age, expires_in);
}

#[serial]
#[tokio::test]
async fn test_estimate_multiple_pairs() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Use correct network names for each currency
    let pairs = vec![
        ("btc", "eth", 0.1, "Mainnet", "ERC20"),
        ("btc", "xmr", 0.1, "Mainnet", "Mainnet"),
        ("eth", "usdt", 1.0, "ERC20", "ERC20"),
    ];
    
    for (from, to, amount, network_from, network_to) in pairs {
        let url = format!(
            "/swap/estimate?from={}&to={}&amount={}&network_from={}&network_to={}",
            from, to, amount, network_from, network_to
        );
        
        let response = timed_get(&server, &url).await;
        response.assert_status_ok();
        let json: Value = response.json();
        
        assert!(json["estimated_receive"].as_f64().unwrap() > 0.0);
        println!("✅ {} -> {}: {} {}", from, to, json["estimated_receive"], to);
        
        sleep(Duration::from_millis(500)).await;
    }
}
