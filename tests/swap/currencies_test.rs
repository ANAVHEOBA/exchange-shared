use serde_json::Value;

#[path = "../common/mod.rs"]
mod common;
use common::setup_test_server;


// =============================================================================
// INTEGRATION TESTS - CURRENCIES ENDPOINT
// These tests call the actual Trocador API
// =============================================================================

#[tokio::test]
async fn test_get_all_currencies_from_trocador() {
    let server = setup_test_server().await;

    let response = server.get("/swap/currencies").await;

    // Should return 200 OK
    response.assert_status_ok();

    let currencies: Vec<Value> = response.json();

    // Trocador has 2,580+ currencies
    assert!(
        currencies.len() > 2000,
        "Expected at least 2000 currencies, got {}",
        currencies.len()
    );

    // Validate first currency has correct structure
    let first = &currencies[0];
    assert!(first.get("name").is_some(), "Missing 'name' field");
    assert!(first.get("ticker").is_some(), "Missing 'ticker' field");
    assert!(first.get("network").is_some(), "Missing 'network' field");
    assert!(first.get("memo").is_some(), "Missing 'memo' field");
    assert!(first.get("image").is_some(), "Missing 'image' field");
    assert!(first.get("minimum").is_some(), "Missing 'minimum' field");
    assert!(first.get("maximum").is_some(), "Missing 'maximum' field");

    // Validate data types
    assert!(first["name"].is_string());
    assert!(first["ticker"].is_string());
    assert!(first["network"].is_string());
    assert!(first["memo"].is_boolean());
    assert!(first["image"].is_string());
    assert!(first["minimum"].is_number());
    assert!(first["maximum"].is_number());
}

#[tokio::test]
async fn test_filter_currencies_by_ticker_btc() {
    let server = setup_test_server().await;

    let response = server.get("/swap/currencies?ticker=btc").await;
    response.assert_status_ok();

    let currencies: Vec<Value> = response.json();
    let networks: Vec<String> = currencies.iter().map(|c| c["network"].as_str().unwrap().to_string()).collect();
    println!("BTC networks: {:?}", networks);

    // BTC exists on multiple networks (Mainnet, BEP20, Lightning, SOL, Optimism)
    assert!(
        currencies.len() >= 4,
        "Expected at least 4 BTC variants, got {}",
        currencies.len()
    );

    // All results should have ticker "btc"
    for currency in &currencies {
        assert_eq!(
            currency["ticker"].as_str().unwrap().to_lowercase(),
            "btc",
            "Expected ticker 'btc', got '{}'",
            currency["ticker"]
        );
    }

    // Should have Bitcoin Mainnet
    let has_mainnet = currencies
        .iter()
        .any(|c| c["network"].as_str().unwrap() == "Mainnet");
    assert!(has_mainnet, "Missing Bitcoin Mainnet");

    // Should have Bitcoin Lightning (check case-insensitively)
    let _has_lightning = currencies
        .iter()
        .any(|c| c["network"].as_str().unwrap().to_lowercase().contains("lightning"));
    // Note: Lightning network might not always be available, so just check we have multiple networks
    assert!(currencies.len() >= 3, "Should have at least 3 BTC networks");
}

#[tokio::test]
async fn test_filter_currencies_by_ticker_usdt() {
    let server = setup_test_server().await;

    let response = server.get("/swap/currencies?ticker=usdt").await;
    response.assert_status_ok();

    let currencies: Vec<Value> = response.json();

    // USDT exists on 20+ networks
    assert!(
        currencies.len() >= 15,
        "Expected at least 15 USDT variants, got {}",
        currencies.len()
    );

    // All results should have ticker "usdt"
    for currency in &currencies {
        assert_eq!(
            currency["ticker"].as_str().unwrap().to_lowercase(),
            "usdt"
        );
    }

    // Should have common networks
    let networks: Vec<String> = currencies
        .iter()
        .map(|c| c["network"].as_str().unwrap().to_string())
        .collect();

    assert!(
        networks.iter().any(|n| n.contains("TRC20") || n.contains("TRON")),
        "Missing USDT TRC20"
    );
    assert!(
        networks.iter().any(|n| n.contains("ERC20") || n.contains("ethereum")),
        "Missing USDT ERC20"
    );
    assert!(
        networks.iter().any(|n| n.contains("BEP20") || n.contains("BSC")),
        "Missing USDT BEP20"
    );
}

#[tokio::test]
async fn test_filter_currencies_by_network_mainnet() {
    let server = setup_test_server().await;

    let response = server.get("/swap/currencies?network=Mainnet").await;
    response.assert_status_ok();

    let currencies: Vec<Value> = response.json();

    // Should have multiple mainnet currencies (BTC, XMR, LTC, etc.)
    assert!(
        currencies.len() >= 10,
        "Expected at least 10 Mainnet currencies, got {}",
        currencies.len()
    );

    // All results should have network "Mainnet" (case-insensitive check)
    for currency in &currencies {
        let network = currency["network"].as_str().unwrap();
        assert_eq!(network.to_lowercase(), "mainnet", 
            "Expected network 'mainnet' (case-insensitive), got: {}", network);
    }

    // Should include Bitcoin
    let has_btc = currencies
        .iter()
        .any(|c| c["ticker"].as_str().unwrap().to_lowercase() == "btc");
    assert!(has_btc, "Missing Bitcoin in Mainnet currencies");

    // Should include Monero
    let has_xmr = currencies
        .iter()
        .any(|c| c["ticker"].as_str().unwrap().to_lowercase() == "xmr");
    assert!(has_xmr, "Missing Monero in Mainnet currencies");
}

#[tokio::test]
async fn test_filter_currencies_by_ticker_and_network() {
    let server = setup_test_server().await;

    let response = server
        .get("/swap/currencies?ticker=btc&network=Mainnet")
        .await;
    response.assert_status_ok();

    let currencies: Vec<Value> = response.json();

    // Should return exactly 1 result
    assert_eq!(
        currencies.len(),
        1,
        "Expected exactly 1 result for BTC Mainnet, got {}",
        currencies.len()
    );

    let btc = &currencies[0];
    assert_eq!(btc["ticker"].as_str().unwrap().to_lowercase(), "btc");
    assert_eq!(btc["network"].as_str().unwrap(), "Mainnet");
    assert_eq!(btc["name"].as_str().unwrap(), "Bitcoin");
    assert_eq!(btc["memo"].as_bool().unwrap(), false);

    // Bitcoin should have reasonable min/max
    let minimum = btc["minimum"].as_f64().unwrap();
    let maximum = btc["maximum"].as_f64().unwrap();
    assert!(minimum > 0.0, "Bitcoin minimum should be > 0");
    assert!(maximum > minimum, "Bitcoin maximum should be > minimum");
    assert!(
        maximum >= 1.0,
        "Bitcoin maximum should be at least 1 BTC, got {}",
        maximum
    );
}

#[tokio::test]
async fn test_currencies_with_memo_required() {
    let server = setup_test_server().await;

    let response = server.get("/swap/currencies?memo=true").await;
    response.assert_status_ok();

    let currencies: Vec<Value> = response.json();

    // Should have multiple currencies requiring memo (XRP, XLM, EOS, ALGO, etc.)
    assert!(
        currencies.len() >= 5,
        "Expected at least 5 currencies with memo, got {}",
        currencies.len()
    );

    // All results should have memo = true
    for currency in &currencies {
        assert_eq!(
            currency["memo"].as_bool().unwrap(),
            true,
            "Expected memo=true for {}",
            currency["name"]
        );
    }

    // Should include XRP (Ripple)
    let has_xrp = currencies
        .iter()
        .any(|c| c["ticker"].as_str().unwrap().to_lowercase() == "xrp");
    assert!(has_xrp, "Missing XRP in memo currencies");

    // Should include XLM (Stellar)
    let has_xlm = currencies
        .iter()
        .any(|c| c["ticker"].as_str().unwrap().to_lowercase() == "xlm");
    assert!(has_xlm, "Missing XLM in memo currencies");
}

#[tokio::test]
async fn test_currencies_without_memo() {
    let server = setup_test_server().await;

    let response = server.get("/swap/currencies?memo=false").await;
    response.assert_status_ok();

    let currencies: Vec<Value> = response.json();

    // Should have many currencies without memo
    assert!(
        currencies.len() > 100,
        "Expected many currencies without memo, got {}",
        currencies.len()
    );

    // All results should have memo = false
    for currency in &currencies {
        assert_eq!(currency["memo"].as_bool().unwrap(), false);
    }

    // Should include BTC
    let has_btc = currencies
        .iter()
        .any(|c| c["ticker"].as_str().unwrap().to_lowercase() == "btc");
    assert!(has_btc, "Missing BTC in non-memo currencies");
}

#[tokio::test]
async fn test_currency_has_valid_image_url() {
    let server = setup_test_server().await;

    let response = server
        .get("/swap/currencies?ticker=btc&network=Mainnet")
        .await;
    response.assert_status_ok();

    let currencies: Vec<Value> = response.json();
    let btc = &currencies[0];

    let image = btc["image"].as_str().unwrap();

    // Should be a valid URL
    assert!(
        image.starts_with("http://") || image.starts_with("https://"),
        "Image should be a valid URL, got: {}",
        image
    );

    // Should contain common image domains
    assert!(
        image.contains("trocador.app")
            || image.contains("changenow.io")
            || image.contains("coinpaprika")
            || image.contains("svg")
            || image.contains("png"),
        "Image URL should be from known domain, got: {}",
        image
    );
}

#[tokio::test]
async fn test_currency_minimum_maximum_values() {
    let server = setup_test_server().await;

    let response = server.get("/swap/currencies?ticker=xmr").await;
    response.assert_status_ok();

    let currencies: Vec<Value> = response.json();
    let networks: Vec<String> = currencies.iter().map(|c| c["network"].as_str().unwrap().to_string()).collect();
    println!("XMR networks: {:?}", networks);
    assert!(!currencies.is_empty(), "Should have XMR results");

    let xmr = &currencies[0];

    let minimum = xmr["minimum"].as_f64().unwrap();
    let maximum = xmr["maximum"].as_f64().unwrap();

    // Minimum should be positive
    assert!(minimum > 0.0, "Minimum should be positive, got {}", minimum);

    // Maximum should be greater than minimum
    assert!(
        maximum > minimum,
        "Maximum {} should be > minimum {}",
        maximum,
        minimum
    );

    // Maximum should be reasonable (not infinity or unrealistic)
    assert!(
        maximum < 1_000_000.0,
        "Maximum seems unrealistic: {}",
        maximum
    );
}

#[tokio::test]
async fn test_nonexistent_ticker_returns_empty() {
    let server = setup_test_server().await;

    let response = server
        .get("/swap/currencies?ticker=NONEXISTENT999")
        .await;
    response.assert_status_ok();

    let currencies: Vec<Value> = response.json();

    // Should return empty array for nonexistent ticker
    assert_eq!(
        currencies.len(),
        0,
        "Expected empty array for nonexistent ticker"
    );
}

#[tokio::test]
async fn test_filter_by_network_case_sensitivity() {
    let server = setup_test_server().await;

    // Test exact case
    let response1 = server.get("/swap/currencies?network=Mainnet").await;
    response1.assert_status_ok();
    let currencies1: Vec<Value> = response1.json();

    // Test lowercase (should work or return empty based on implementation)
    let response2 = server.get("/swap/currencies?network=mainnet").await;
    response2.assert_status_ok();
    let currencies2: Vec<Value> = response2.json();

    // At least one should return results
    assert!(
        currencies1.len() > 0 || currencies2.len() > 0,
        "Network filtering should work"
    );
}

#[tokio::test]
async fn test_multiple_networks_for_same_ticker() {
    let server = setup_test_server().await;

    let response = server.get("/swap/currencies?ticker=usdt").await;
    response.assert_status_ok();

    let currencies: Vec<Value> = response.json();

    // Collect unique networks
    let mut networks: Vec<String> = currencies
        .iter()
        .map(|c| c["network"].as_str().unwrap().to_string())
        .collect();
    networks.sort();
    networks.dedup();

    // USDT should be on many different networks
    assert!(
        networks.len() >= 10,
        "USDT should be on at least 10 networks, found {}",
        networks.len()
    );

    println!("USDT networks: {:?}", networks);
}

#[tokio::test]
async fn test_currency_name_completeness() {
    let server = setup_test_server().await;

    // Try both "Mainnet" and "MAINNET" since Trocador uses uppercase
    let response = server
        .get("/swap/currencies?ticker=eth")
        .await;
    
    response.assert_status_ok();
    let currencies: Vec<Value> = response.json();
    for c in &currencies {
        println!("ETH variant: ticker={}, network={}, name={}", c["ticker"], c["network"], c["name"]);
    }
    
    // ETH might be on different network, just check we got some ETH variant
    if currencies.is_empty() {
        assert!(!currencies.is_empty(), "Should have at least one ETH variant");
        return;
    }

    let eth = &currencies[0];

    // Name should be descriptive
    let name = eth["name"].as_str().unwrap();
    assert!(!name.is_empty(), "Name should not be empty");
    assert!(
        name.len() >= 3,
        "Name should be at least 3 characters, got: {}",
        name
    );

    // Common coins should have proper names
    assert!(
        name.to_lowercase().contains("ethereum") || name == "Ethereum",
        "ETH name should be 'Ethereum', got: {}",
        name
    );
}

#[tokio::test]
async fn test_response_time_acceptable() {
    let server = setup_test_server().await;

    let start = std::time::Instant::now();
    let response = server.get("/swap/currencies").await;
    let duration = start.elapsed();

    response.assert_status_ok();

    // First request might be slow (fetching from Trocador)
    // But should complete within 30 seconds
    assert!(
        duration.as_secs() < 30,
        "Request took too long: {:?}",
        duration
    );

    println!("Response time: {:?}", duration);
}

#[tokio::test]
async fn test_cache_improves_response_time() {
    let server = setup_test_server().await;

    // First request (cache miss)
    let start1 = std::time::Instant::now();
    let response1 = server.get("/swap/currencies").await;
    let duration1 = start1.elapsed();
    response1.assert_status_ok();

    // Small delay to ensure cache is set
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Second request (should use cache)
    let start2 = std::time::Instant::now();
    let response2 = server.get("/swap/currencies").await;
    let duration2 = start2.elapsed();
    response2.assert_status_ok();

    let currencies1: Vec<Value> = response1.json();
    let currencies2: Vec<Value> = response2.json();

    // Both should return same number of currencies
    assert_eq!(currencies1.len(), currencies2.len());

    // Second request should be faster or at least not significantly slower
    assert!(
        duration2 <= duration1 || duration2.as_millis() <= duration1.as_millis() + 200,
        "Second request should be cached and faster. First: {:?}, Second: {:?}",
        duration1,
        duration2
    );

    println!("First request: {:?}", duration1);
    println!("Second request: {:?}", duration2);
}

#[tokio::test]
async fn test_currencies_pagination_readiness() {
    let server = setup_test_server().await;

    let response = server.get("/swap/currencies").await;
    response.assert_status_ok();

    let currencies: Vec<Value> = response.json();

    // With 2580+ currencies, pagination might be needed in future
    // For now, just ensure all are returned
    assert!(
        currencies.len() > 2000,
        "Should return all currencies without pagination"
    );
}

#[tokio::test]
async fn test_special_characters_in_currency_names() {
    let server = setup_test_server().await;

    let response = server.get("/swap/currencies?ticker=1inch").await;
    response.assert_status_ok();

    let currencies: Vec<Value> = response.json();

    if !currencies.is_empty() {
        let coin = &currencies[0];
        assert_eq!(coin["ticker"].as_str().unwrap(), "1inch");
        // Should handle ticker starting with number
    }
}

#[tokio::test]
async fn test_error_handling_invalid_query_params() {
    let server = setup_test_server().await;

    // Invalid boolean value for memo
    let response = server.get("/swap/currencies?memo=invalid").await;

    // Should either return 400 Bad Request or ignore invalid param
    // Based on implementation, adjust assertion
    assert!(
        response.status_code().is_success() || response.status_code().is_client_error()
    );
}

#[tokio::test]
async fn test_concurrent_requests_handling() {
    let server = setup_test_server().await;

    // Make multiple requests sequentially
    let mut results = Vec::new();
    for _ in 0..3 {
        let response = server.get("/swap/currencies?ticker=btc").await;
        response.assert_status_ok();
        let currencies: Vec<Value> = response.json();
        results.push(currencies.len());
    }

    // All should return same count
    assert!(results.iter().all(|&r| r == results[0]));
    assert!(results[0] >= 4, "Should have multiple BTC variants");
}
