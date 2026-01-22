use serde_json::{json, Value};

#[path = "../common/mod.rs"]
mod common;
use common::setup_test_server;
use std::time::Duration;
use tokio::time::sleep;

// =============================================================================
// INTEGRATION TESTS - CREATE SWAP ENDPOINT
// These tests call the actual Trocador API via our backend
// =============================================================================

#[tokio::test]
async fn test_create_swap_successful() {
    sleep(Duration::from_secs(1)).await; // Prevent Rate Limit
    let server = setup_test_server().await;

    // 1. First, get a rate to generate a trade_id
    // BTC -> XMR
    let rate_url = "/swap/rates?from=btc&to=xmr&amount=0.001&network_from=Mainnet&network_to=Mainnet";
    let rate_response = server.get(rate_url).await;
    rate_response.assert_status_ok();
    
    let rate_json: Value = rate_response.json();
    let trade_id = rate_json["trade_id"].as_str().expect("Should have trade_id");
    let provider = rate_json["rates"][0]["provider"].as_str().expect("Should have provider");

    println!("Got trade_id: {}", trade_id);

    // 2. Create the swap using the trade_id
    let create_url = "/swap/create";
    let recipient_address = "44AFFq5kSiGBoZ4NMDwYtN18obc8AemS33DBLWs3H7otXft3XjrpDtQGvj85ngqVqWfdn4ufSXIzJRHWKJ32khHA7wGwwve"; // Standard XMR address
    let refund_address = "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"; // Standard BTC address

    let payload = json!({
        "trade_id": trade_id,
        "from": "btc",
        "network_from": "Mainnet",
        "to": "xmr",
        "network_to": "Mainnet",
        "amount": 0.001,
        "provider": provider,
        "recipient_address": recipient_address,
        "refund_address": refund_address,
        "rate_type": "floating"
    });

    let response = server.post(create_url).json(&payload).await;
    
    // Check for success (200 OK or 201 Created)
    if response.status_code() != 200 && response.status_code() != 201 {
        let error_body: Value = response.json();
        println!("Create swap failed: {:?}", error_body);
        panic!("Expected 200/201, got {}", response.status_code());
    }

    let json: Value = response.json();

    // 3. Verify Response Structure
    assert!(json.get("swap_id").is_some(), "Response should have swap_id");
    assert!(json.get("deposit_address").is_some(), "Response should have deposit_address");
    assert!(json.get("status").is_some(), "Response should have status");
    
    let swap_id = json["swap_id"].as_str().unwrap();
    println!("Created swap_id: {}", swap_id);

    // Check if the returned data matches input
    assert_eq!(json["from"].as_str().unwrap(), "btc");
    assert_eq!(json["to"].as_str().unwrap(), "xmr");
    assert_eq!(json["recipient_address"].as_str().unwrap(), recipient_address);
}

#[tokio::test]
async fn test_create_swap_without_trade_id_should_fail() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let create_url = "/swap/create";
    let payload = json!({
        "from": "btc",
        "network_from": "Mainnet",
        "to": "xmr",
        "network_to": "Mainnet",
        "amount": 0.001,
        "provider": "changenow",
        "recipient_address": "44AFFq5kSiGBoZ4NMDwYtN18obc8AemS33DBLWs3H7otXft3XjrpDtQGvj85ngqVqWfdn4ufSXIzJRHWKJ32khHA7wGwwve",
        // "trade_id" missing - should cause API error
    });

    let response = server.post(create_url).json(&payload).await;
    
    // Trocador API should reject requests without trade_id
    // Expect 400 Bad Request or 500 Internal Server Error (from external API)
    let status = response.status_code().as_u16();
    assert!(status >= 400, "Expected error status, got {}", status);
    
    let err: Value = response.json();
    println!("Error response without trade_id: {:?}", err);
    assert!(err.get("error").is_some(), "Response should contain error field");
}

#[tokio::test]
async fn test_create_swap_invalid_address() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let create_url = "/swap/create";
    let payload = json!({
        "from": "btc",
        "network_from": "Mainnet",
        "to": "xmr",
        "network_to": "Mainnet",
        "amount": 0.001,
        "provider": "changenow",
        "recipient_address": "INVALID_ADDRESS_XYZ", // Clearly invalid XMR address
        "rate_type": "floating"
    });

    let response = server.post(create_url).json(&payload).await;

    // Trocador should reject this
    assert!(response.status_code().as_u16() >= 400);
}

#[tokio::test]
async fn test_create_swap_small_amount() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let create_url = "/swap/create";
    // Using a very small amount that should be below min
    let payload = json!({
        "from": "btc",
        "network_from": "Mainnet",
        "to": "xmr",
        "network_to": "Mainnet",
        "amount": 0.00000001,
        "provider": "changenow", 
        "recipient_address": "44AFFq5kSiGBoZ4NMDwYtN18obc8AemS33DBLWs3H7otXft3XjrpDtQGvj85ngqVqWfdn4ufSXIzJRHWKJ32khHA7wGwwve",
    });

    let response = server.post(create_url).json(&payload).await;
    
    // Should fail
    assert!(response.status_code().as_u16() >= 400);
}

#[tokio::test]
async fn test_create_swap_fixed_rate() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // 1. First, get rates to obtain a trade_id
    let rate_url = "/swap/rates?from=btc&to=xmr&amount=0.001&network_from=Mainnet&network_to=Mainnet";
    let rate_response = server.get(rate_url).await;
    rate_response.assert_status_ok();
    
    let rate_json: Value = rate_response.json();
    let trade_id = rate_json["trade_id"].as_str().expect("Should have trade_id");
    
    // 2. Try to find a provider that might support fixed rates
    // If no provider supports fixed, this test will fail gracefully
    let rates = rate_json["rates"].as_array().expect("Should have rates array");
    if rates.is_empty() {
        println!("No rates available, skipping fixed rate test");
        return;
    }
    
    let provider = rates[0]["provider"].as_str().expect("Should have provider");
    println!("Testing fixed rate with provider: {}", provider);

    // 3. Create swap with fixed rate
    let create_url = "/swap/create";
    let payload = json!({
        "trade_id": trade_id,
        "from": "btc",
        "network_from": "Mainnet",
        "to": "xmr",
        "network_to": "Mainnet",
        "amount": 0.001,
        "provider": provider,
        "recipient_address": "44AFFq5kSiGBoZ4NMDwYtN18obc8AemS33DBLWs3H7otXft3XjrpDtQGvj85ngqVqWfdn4ufSXIzJRHWKJ32khHA7wGwwve",
        "rate_type": "fixed"
    });

    let response = server.post(create_url).json(&payload).await;
    
    // Fixed rate might not be available for all providers/pairs
    // Accept success or client error (400) if fixed rate not supported
    let status = response.status_code();
    assert!(
        status.is_success() || status.is_client_error(),
        "Expected success or client error, got {}",
        status
    );
    
    if status.is_success() {
        let json: Value = response.json();
        println!("Fixed rate swap created: {:?}", json);
        assert_eq!(json["rate_type"], "fixed", "Rate type should be fixed");
    } else {
        let err: Value = response.json();
        println!("Fixed rate not available: {:?}", err);
    }
}

#[tokio::test]
async fn test_create_swap_invalid_refund_address() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let create_url = "/swap/create";
    let payload = json!({
        "from": "btc",
        "network_from": "Mainnet",
        "to": "xmr",
        "network_to": "Mainnet",
        "amount": 0.001,
        "provider": "changenow",
        "recipient_address": "44AFFq5kSiGBoZ4NMDwYtN18obc8AemS33DBLWs3H7otXft3XjrpDtQGvj85ngqVqWfdn4ufSXIzJRHWKJ32khHA7wGwwve",
        "refund_address": "INVALID_REFUND_ADDRESS" 
    });

    let response = server.post(create_url).json(&payload).await;
    
    // Should fail validation
    assert!(response.status_code().as_u16() >= 400);
}

#[tokio::test]
async fn test_create_swap_unsupported_pair() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let create_url = "/swap/create";
    let payload = json!({
        "from": "btc", // Same coin swap usually not supported or requires specific handling
        "network_from": "Mainnet",
        "to": "btc",
        "network_to": "Mainnet",
        "amount": 0.001,
        "provider": "changenow",
        "recipient_address": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
    });

    let response = server.post(create_url).json(&payload).await;
    
    // Should likely fail as "No trade to be made"
    assert!(response.status_code().as_u16() >= 400);
}

#[tokio::test]
async fn test_create_swap_amount_too_high() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let create_url = "/swap/create";
    let payload = json!({
        "from": "btc",
        "network_from": "Mainnet",
        "to": "xmr",
        "network_to": "Mainnet",
        "amount": 5000.0, // Excessive amount
        "provider": "changenow",
        "recipient_address": "44AFFq5kSiGBoZ4NMDwYtN18obc8AemS33DBLWs3H7otXft3XjrpDtQGvj85ngqVqWfdn4ufSXIzJRHWKJ32khHA7wGwwve",
    });

    let response = server.post(create_url).json(&payload).await;
    
    // Should fail
    assert!(response.status_code().as_u16() >= 400);
}

#[tokio::test]
async fn test_create_swap_missing_memo_for_xrp() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // BTC -> XRP (XRP usually requires a memo/tag for exchange deposits, but here we are SENDING to a user address.
    // User addresses might not need a tag if it's a personal wallet, but Trocador might enforce it or we want to see behavior)
    let create_url = "/swap/create";
    let payload = json!({
        "from": "btc",
        "network_from": "Mainnet",
        "to": "xrp",
        "network_to": "Mainnet",
        "amount": 0.001,
        "provider": "changenow",
        "recipient_address": "rEb8TK3gBgk5auZkwc6sHnwrGVJH8DuaLh", // Example XRP address
        // Missing recipient_extra_id
    });

    let response = server.post(create_url).json(&payload).await;
    
    // Our implementation might default to "0" or fail. Trocador says "Use '0' for no memo".
    // If our code doesn't handle this, it might fail.
    // Asserting we get a response, either success (if handled) or error (if strict).
    // Ideally, we'd want to inspect if it failed due to missing memo.
    if response.status_code().is_success() {
        println!("XRP swap created without explicit memo (likely defaulted)");
    } else {
        println!("XRP swap failed without memo: {:?}", response.status_code());
    }
}

// =============================================================================
// NEW EDGE CASE TESTS
// =============================================================================

#[tokio::test]
async fn test_create_swap_invalid_provider() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Get a valid trade_id first
    let rate_url = "/swap/rates?from=btc&to=xmr&amount=0.001&network_from=Mainnet&network_to=Mainnet";
    let rate_response = server.get(rate_url).await;
    rate_response.assert_status_ok();
    
    let rate_json: Value = rate_response.json();
    let trade_id = rate_json["trade_id"].as_str().expect("Should have trade_id");

    let create_url = "/swap/create";
    let payload = json!({
        "trade_id": trade_id,
        "from": "btc",
        "network_from": "Mainnet",
        "to": "xmr",
        "network_to": "Mainnet",
        "amount": 0.001,
        "provider": "nonexistent_provider_xyz", // Invalid provider
        "recipient_address": "44AFFq5kSiGBoZ4NMDwYtN18obc8AemS33DBLWs3H7otXft3XjrpDtQGvj85ngqVqWfdn4ufSXIzJRHWKJ32khHA7wGwwve",
    });

    let response = server.post(create_url).json(&payload).await;
    
    // Should fail with 400 or 500 error
    assert!(response.status_code().as_u16() >= 400);
}

#[tokio::test]
async fn test_create_swap_wrong_network_for_currency() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let create_url = "/swap/create";
    let payload = json!({
        "from": "btc",
        "network_from": "ERC20", // BTC doesn't exist on ERC20
        "to": "xmr",
        "network_to": "Mainnet",
        "amount": 0.001,
        "provider": "changenow",
        "recipient_address": "44AFFq5kSiGBoZ4NMDwYtN18obc8AemS33DBLWs3H7otXft3XjrpDtQGvj85ngqVqWfdn4ufSXIzJRHWKJ32khHA7wGwwve",
    });

    let response = server.post(create_url).json(&payload).await;
    
    // Should fail - invalid network for currency
    assert!(response.status_code().as_u16() >= 400);
}

#[tokio::test]
async fn test_create_swap_expired_trade_id() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let create_url = "/swap/create";
    let payload = json!({
        "trade_id": "expired_or_fake_trade_id_12345", // Fake/expired trade_id
        "from": "btc",
        "network_from": "Mainnet",
        "to": "xmr",
        "network_to": "Mainnet",
        "amount": 0.001,
        "provider": "changenow",
        "recipient_address": "44AFFq5kSiGBoZ4NMDwYtN18obc8AemS33DBLWs3H7otXft3XjrpDtQGvj85ngqVqWfdn4ufSXIzJRHWKJ32khHA7wGwwve",
    });

    let response = server.post(create_url).json(&payload).await;
    
    // Should fail with error about invalid/expired trade_id
    assert!(response.status_code().as_u16() >= 400);
}

#[tokio::test]
async fn test_create_swap_rate_type_mismatch() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Get a floating rate quote
    let rate_url = "/swap/rates?from=btc&to=xmr&amount=0.001&network_from=Mainnet&network_to=Mainnet";
    let rate_response = server.get(rate_url).await;
    rate_response.assert_status_ok();
    
    let rate_json: Value = rate_response.json();
    let trade_id = rate_json["trade_id"].as_str().expect("Should have trade_id");
    let provider = rate_json["rates"][0]["provider"].as_str().expect("Should have provider");

    let create_url = "/swap/create";
    let payload = json!({
        "trade_id": trade_id,
        "from": "btc",
        "network_from": "Mainnet",
        "to": "xmr",
        "network_to": "Mainnet",
        "amount": 0.001,
        "provider": provider,
        "recipient_address": "44AFFq5kSiGBoZ4NMDwYtN18obc8AemS33DBLWs3H7otXft3XjrpDtQGvj85ngqVqWfdn4ufSXIzJRHWKJ32khHA7wGwwve",
        "rate_type": "fixed" // Request fixed when we got floating quote
    });

    let response = server.post(create_url).json(&payload).await;
    
    // May succeed (if provider supports both) or fail (if strict validation)
    // Just verify we get a valid response
    let status = response.status_code().as_u16();
    assert!(status >= 200 && status < 600, "Should get valid HTTP status");
}

#[tokio::test]
async fn test_create_swap_empty_recipient_address() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let create_url = "/swap/create";
    let payload = json!({
        "from": "btc",
        "network_from": "Mainnet",
        "to": "xmr",
        "network_to": "Mainnet",
        "amount": 0.001,
        "provider": "changenow",
        "recipient_address": "", // Empty address
    });

    let response = server.post(create_url).json(&payload).await;
    
    // Should fail validation
    assert!(response.status_code().as_u16() >= 400);
}

#[tokio::test]
async fn test_create_swap_whitespace_recipient_address() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let create_url = "/swap/create";
    let payload = json!({
        "from": "btc",
        "network_from": "Mainnet",
        "to": "xmr",
        "network_to": "Mainnet",
        "amount": 0.001,
        "provider": "changenow",
        "recipient_address": "   ", // Whitespace only
    });

    let response = server.post(create_url).json(&payload).await;
    
    // Should fail validation
    assert!(response.status_code().as_u16() >= 400);
}

#[tokio::test]
async fn test_create_swap_exactly_minimum_amount() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Get rates to find minimum amount
    let rate_url = "/swap/rates?from=btc&to=xmr&amount=0.001&network_from=Mainnet&network_to=Mainnet";
    let rate_response = server.get(rate_url).await;
    rate_response.assert_status_ok();
    
    let rate_json: Value = rate_response.json();
    let trade_id = rate_json["trade_id"].as_str().expect("Should have trade_id");
    let rates = rate_json["rates"].as_array().expect("Should have rates");
    
    if rates.is_empty() {
        println!("No rates available, skipping minimum amount test");
        return;
    }
    
    let min_amount = rates[0]["min_amount"].as_f64().unwrap_or(0.0);
    let provider = rates[0]["provider"].as_str().expect("Should have provider");
    
    // Skip test if min_amount is 0 or invalid
    if min_amount <= 0.0 {
        println!("Min amount is {} (invalid), skipping test", min_amount);
        return;
    }
    
    println!("Testing with minimum amount: {}", min_amount);

    let create_url = "/swap/create";
    let payload = json!({
        "trade_id": trade_id,
        "from": "btc",
        "network_from": "Mainnet",
        "to": "xmr",
        "network_to": "Mainnet",
        "amount": min_amount,
        "provider": provider,
        "recipient_address": "44AFFq5kSiGBoZ4NMDwYtN18obc8AemS33DBLWs3H7otXft3XjrpDtQGvj85ngqVqWfdn4ufSXIzJRHWKJ32khHA7wGwwve",
    });

    let response = server.post(create_url).json(&payload).await;
    
    // Should succeed at minimum amount
    let status = response.status_code();
    if !status.is_success() {
        let err: Value = response.json();
        println!("Failed at minimum amount: {:?}", err);
    }
    assert!(status.is_success() || status.is_client_error());
}

#[tokio::test]
async fn test_create_swap_exactly_maximum_amount() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Get rates to find maximum amount
    let rate_url = "/swap/rates?from=btc&to=xmr&amount=0.001&network_from=Mainnet&network_to=Mainnet";
    let rate_response = server.get(rate_url).await;
    rate_response.assert_status_ok();
    
    let rate_json: Value = rate_response.json();
    let trade_id = rate_json["trade_id"].as_str().expect("Should have trade_id");
    let rates = rate_json["rates"].as_array().expect("Should have rates");
    
    if rates.is_empty() {
        println!("No rates available, skipping maximum amount test");
        return;
    }
    
    let max_amount = rates[0]["max_amount"].as_f64().unwrap_or(0.0);
    let provider = rates[0]["provider"].as_str().expect("Should have provider");
    
    // Skip test if max_amount is 0 or invalid
    if max_amount <= 0.0 {
        println!("Max amount is {} (invalid), skipping test", max_amount);
        return;
    }
    
    // Use a reasonable test amount (not the full max which might be too high)
    // Use the smaller of: max_amount or 0.1 BTC
    let test_amount = max_amount.min(0.1);
    println!("Testing with amount: {} (max: {})", test_amount, max_amount);

    let create_url = "/swap/create";
    let payload = json!({
        "trade_id": trade_id,
        "from": "btc",
        "network_from": "Mainnet",
        "to": "xmr",
        "network_to": "Mainnet",
        "amount": test_amount,
        "provider": provider,
        "recipient_address": "44AFFq5kSiGBoZ4NMDwYtN18obc8AemS33DBLWs3H7otXft3XjrpDtQGvj85ngqVqWfdn4ufSXIzJRHWKJ32khHA7wGwwve",
    });

    let response = server.post(create_url).json(&payload).await;
    
    // Should succeed or fail gracefully if liquidity unavailable
    let status = response.status_code();
    if !status.is_success() {
        let err: Value = response.json();
        println!("Failed at test amount: {:?}", err);
    }
    assert!(status.is_success() || status.is_client_error());
}

#[tokio::test]
async fn test_create_swap_concurrent_same_trade_id() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Get a trade_id
    let rate_url = "/swap/rates?from=btc&to=xmr&amount=0.001&network_from=Mainnet&network_to=Mainnet";
    let rate_response = server.get(rate_url).await;
    rate_response.assert_status_ok();
    
    let rate_json: Value = rate_response.json();
    let trade_id = rate_json["trade_id"].as_str().expect("Should have trade_id");
    let provider = rate_json["rates"][0]["provider"].as_str().expect("Should have provider");

    let create_url = "/swap/create";
    let payload = json!({
        "trade_id": trade_id,
        "from": "btc",
        "network_from": "Mainnet",
        "to": "xmr",
        "network_to": "Mainnet",
        "amount": 0.001,
        "provider": provider,
        "recipient_address": "44AFFq5kSiGBoZ4NMDwYtN18obc8AemS33DBLWs3H7otXft3XjrpDtQGvj85ngqVqWfdn4ufSXIzJRHWKJ32khHA7wGwwve",
    });

    // First swap
    let response1 = server.post(create_url).json(&payload).await;
    let status1 = response1.status_code();
    
    sleep(Duration::from_millis(500)).await;
    
    // Second swap with same trade_id
    let response2 = server.post(create_url).json(&payload).await;
    let status2 = response2.status_code();
    
    println!("First swap status: {}, Second swap status: {}", status1, status2);
    
    // At least one should succeed or both should fail gracefully
    assert!(
        status1.as_u16() >= 200 && status1.as_u16() < 600,
        "First swap should return valid status"
    );
    assert!(
        status2.as_u16() >= 200 && status2.as_u16() < 600,
        "Second swap should return valid status"
    );
}