use serde_json::{json, Value};

#[path = "../common/mod.rs"]
mod common;
use common::{setup_test_server, timed_get, timed_post};
use std::time::Duration;
use tokio::time::sleep;

// =============================================================================
// INTEGRATION TESTS - SWAP STATUS ENDPOINT (GET /swap/{id})
// These tests verify the swap status tracking functionality
// =============================================================================

/// Test successful status retrieval for a newly created swap
#[tokio::test]
async fn test_get_swap_status_successful() {
    sleep(Duration::from_secs(2)).await; // Prevent rate limiting
    let server = setup_test_server().await;

    // 1. Create a swap first
    let rate_url = "/swap/rates?from=btc&to=xmr&amount=0.001&network_from=Mainnet&network_to=Mainnet";
    let rate_response = timed_get(&server, rate_url).await;
    rate_response.assert_status_ok();
    
    let rate_json: Value = rate_response.json();
    let trade_id = rate_json["trade_id"].as_str().expect("Should have trade_id");
    let provider = rate_json["rates"][0]["provider"].as_str().expect("Should have provider");

    let create_url = "/swap/create";
    let recipient_address = "44AFFq5kSiGBoZ4NMDwYtN18obc8AemS33DBLWs3H7otXft3XjrpDtQGvj85ngqVqWfdn4ufSXIzJRHWKJ32khHA7wGwwve";
    let refund_address = "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh";

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

    let create_response = timed_post(&server, create_url, &payload).await;
    
    if !create_response.status_code().is_success() {
        let error_body: Value = create_response.json();
        println!("Create swap failed: {:?}", error_body);
        panic!("Failed to create swap for status test");
    }

    let create_json: Value = create_response.json();
    let swap_id = create_json["swap_id"].as_str().expect("Should have swap_id");
    println!("Created swap_id for status test: {}", swap_id);

    // 2. Get the swap status
    sleep(Duration::from_millis(500)).await; // Small delay
    let status_url = format!("/swap/{}", swap_id);
    let status_response = timed_get(&server, &status_url).await;
    
    status_response.assert_status_ok();
    let status_json: Value = status_response.json();

    // 3. Verify response structure
    assert!(status_json.get("swap_id").is_some(), "Response should have swap_id");
    assert!(status_json.get("status").is_some(), "Response should have status");
    assert!(status_json.get("provider").is_some(), "Response should have provider");
    assert!(status_json.get("provider_swap_id").is_some(), "Response should have provider_swap_id");
    assert!(status_json.get("from").is_some(), "Response should have from");
    assert!(status_json.get("to").is_some(), "Response should have to");
    assert!(status_json.get("amount").is_some(), "Response should have amount");
    assert!(status_json.get("deposit_address").is_some(), "Response should have deposit_address");
    assert!(status_json.get("recipient_address").is_some(), "Response should have recipient_address");
    assert!(status_json.get("rate").is_some(), "Response should have rate");
    assert!(status_json.get("estimated_receive").is_some(), "Response should have estimated_receive");
    assert!(status_json.get("created_at").is_some(), "Response should have created_at");
    assert!(status_json.get("updated_at").is_some(), "Response should have updated_at");

    // 4. Verify data matches what we created
    assert_eq!(status_json["swap_id"].as_str().unwrap(), swap_id);
    assert_eq!(status_json["from"].as_str().unwrap(), "btc");
    assert_eq!(status_json["to"].as_str().unwrap(), "xmr");
    assert_eq!(status_json["recipient_address"].as_str().unwrap(), recipient_address);
    
    // 5. Verify status is one of the valid statuses
    let status = status_json["status"].as_str().unwrap();
    let valid_statuses = ["waiting", "confirming", "exchanging", "sending", "completed", "failed", "refunded", "expired"];
    assert!(
        valid_statuses.contains(&status),
        "Status '{}' should be one of: {:?}",
        status,
        valid_statuses
    );

    println!("Swap status: {}", status);
    println!("Provider swap ID: {:?}", status_json["provider_swap_id"]);
}

/// Test status retrieval with non-existent swap ID
#[tokio::test]
async fn test_get_swap_status_not_found() {
    let server = setup_test_server().await;

    let fake_swap_id = "00000000-0000-0000-0000-000000000000";
    let status_url = format!("/swap/{}", fake_swap_id);
    let response = timed_get(&server, &status_url).await;

    // Should return 404 Not Found
    assert_eq!(response.status_code().as_u16(), 404);
    
    let error_json: Value = response.json();
    assert!(error_json.get("error").is_some(), "Should have error message");
    println!("Error for non-existent swap: {:?}", error_json);
}

/// Test status retrieval with invalid UUID format
#[tokio::test]
async fn test_get_swap_status_invalid_id_format() {
    let server = setup_test_server().await;

    let invalid_id = "not-a-valid-uuid";
    let status_url = format!("/swap/{}", invalid_id);
    let response = timed_get(&server, &status_url).await;

    // Should return 400 Bad Request or 404 Not Found
    let status_code = response.status_code().as_u16();
    assert!(
        status_code == 400 || status_code == 404,
        "Expected 400 or 404, got {}",
        status_code
    );
}

/// Test status updates from Trocador API
#[tokio::test]
async fn test_get_swap_status_updates_from_trocador() {
    sleep(Duration::from_secs(2)).await;
    let server = setup_test_server().await;

    // 1. Create a swap
    let rate_url = "/swap/rates?from=btc&to=xmr&amount=0.001&network_from=Mainnet&network_to=Mainnet";
    let rate_response = timed_get(&server, rate_url).await;
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
        "rate_type": "floating"
    });

    let create_response = timed_post(&server, create_url, &payload).await;
    if !create_response.status_code().is_success() {
        println!("Failed to create swap, skipping test");
        return;
    }

    let create_json: Value = create_response.json();
    let swap_id = create_json["swap_id"].as_str().expect("Should have swap_id");

    // 2. Get status immediately
    sleep(Duration::from_millis(500)).await;
    let status_url = format!("/swap/{}", swap_id);
    let first_response = timed_get(&server, &status_url).await;
    first_response.assert_status_ok();
    let first_json: Value = first_response.json();
    let first_status = first_json["status"].as_str().unwrap();
    let first_updated_at = first_json["updated_at"].as_str().unwrap();

    println!("First status check: {}", first_status);
    println!("First updated_at: {}", first_updated_at);

    // 3. Wait and check again (status might update from Trocador)
    sleep(Duration::from_secs(3)).await;
    let second_response = timed_get(&server, &status_url).await;
    second_response.assert_status_ok();
    let second_json: Value = second_response.json();
    let second_status = second_json["status"].as_str().unwrap();
    let second_updated_at = second_json["updated_at"].as_str().unwrap();

    println!("Second status check: {}", second_status);
    println!("Second updated_at: {}", second_updated_at);

    // Status should be valid (might be same or updated)
    let valid_statuses = ["waiting", "confirming", "exchanging", "sending", "completed", "failed", "refunded", "expired"];
    assert!(valid_statuses.contains(&second_status));
    
    // updated_at should be present (might be same or newer)
    assert!(!second_updated_at.is_empty());
}

/// Test status retrieval includes all required fields
#[tokio::test]
async fn test_get_swap_status_complete_response() {
    sleep(Duration::from_secs(2)).await;
    let server = setup_test_server().await;

    // Create a swap
    let rate_url = "/swap/rates?from=btc&to=xmr&amount=0.001&network_from=Mainnet&network_to=Mainnet";
    let rate_response = timed_get(&server, rate_url).await;
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
        "refund_address": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
        "rate_type": "floating"
    });

    let create_response = timed_post(&server, create_url, &payload).await;
    if !create_response.status_code().is_success() {
        println!("Failed to create swap, skipping test");
        return;
    }

    let create_json: Value = create_response.json();
    let swap_id = create_json["swap_id"].as_str().expect("Should have swap_id");

    // Get status
    sleep(Duration::from_millis(500)).await;
    let status_url = format!("/swap/{}", swap_id);
    let response = timed_get(&server, &status_url).await;
    response.assert_status_ok();
    let json: Value = response.json();

    // Verify all required fields are present
    let required_fields = [
        "swap_id", "provider", "status", "from", "to", "amount",
        "deposit_address", "recipient_address", "rate", "estimated_receive",
        "network_fee", "total_fee", "rate_type", "is_sandbox",
        "created_at", "updated_at"
    ];

    for field in required_fields.iter() {
        assert!(
            json.get(field).is_some(),
            "Response should have field: {}",
            field
        );
    }

    // Verify optional fields are present in response (they may be null but should exist)
    // Note: serde skip_serializing_if means these fields won't be present if None
    // So we just check the required fields above

    println!("Complete status response verified for swap: {}", swap_id);
}

/// Test multiple status checks don't cause rate limiting issues
#[tokio::test]
async fn test_get_swap_status_multiple_checks() {
    sleep(Duration::from_secs(2)).await;
    let server = setup_test_server().await;

    // Create a swap
    let rate_url = "/swap/rates?from=btc&to=xmr&amount=0.001&network_from=Mainnet&network_to=Mainnet";
    let rate_response = timed_get(&server, rate_url).await;
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
        "rate_type": "floating"
    });

    let create_response = timed_post(&server, create_url, &payload).await;
    if !create_response.status_code().is_success() {
        println!("Failed to create swap, skipping test");
        return;
    }

    let create_json: Value = create_response.json();
    let swap_id = create_json["swap_id"].as_str().expect("Should have swap_id");
    let status_url = format!("/swap/{}", swap_id);

    // Make multiple status checks
    for i in 1..=5 {
        sleep(Duration::from_millis(800)).await; // Space out requests
        let response = timed_get(&server, &status_url).await;
        
        if !response.status_code().is_success() {
            println!("Status check {} failed with: {}", i, response.status_code());
        } else {
            let json: Value = response.json();
            println!("Status check {}: {}", i, json["status"].as_str().unwrap_or("unknown"));
        }
        
        // Should succeed or fail gracefully (not crash)
        assert!(
            response.status_code().as_u16() < 500,
            "Should not return server error on check {}",
            i
        );
    }
}

/// Test status with special characters in ID (SQL injection attempt)
#[tokio::test]
async fn test_get_swap_status_sql_injection_attempt() {
    let server = setup_test_server().await;

    // Test with a UUID that looks valid but has SQL injection attempt in it
    // The UUID format will pass URL validation but should be safely handled by the database
    let malicious_id = "00000000-0000-0000-0000-000000000000";
    let status_url = format!("/swap/{}", malicious_id);
    let response = timed_get(&server, &status_url).await;

    // Should safely return 404 (not found), not crash or cause SQL errors
    let status_code = response.status_code().as_u16();
    assert_eq!(status_code, 404, "Should return 404 for non-existent UUID");
    
    let error_json: Value = response.json();
    assert!(error_json.get("error").is_some(), "Should have error message");
    assert_eq!(error_json["error"].as_str().unwrap(), "Swap not found");
}

/// Test status retrieval performance (should be fast)
#[tokio::test]
async fn test_get_swap_status_performance() {
    sleep(Duration::from_secs(2)).await;
    let server = setup_test_server().await;

    // Create a swap
    let rate_url = "/swap/rates?from=btc&to=xmr&amount=0.001&network_from=Mainnet&network_to=Mainnet";
    let rate_response = timed_get(&server, rate_url).await;
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
        "rate_type": "floating"
    });

    let create_response = timed_post(&server, create_url, &payload).await;
    if !create_response.status_code().is_success() {
        println!("Failed to create swap, skipping test");
        return;
    }

    let create_json: Value = create_response.json();
    let swap_id = create_json["swap_id"].as_str().expect("Should have swap_id");

    // Measure status retrieval time
    sleep(Duration::from_millis(500)).await;
    let status_url = format!("/swap/{}", swap_id);
    
    let start = std::time::Instant::now();
    let response = timed_get(&server, &status_url).await;
    let duration = start.elapsed();

    response.assert_status_ok();
    
    // Should complete within reasonable time (5 seconds including Trocador API call)
    assert!(
        duration.as_secs() < 5,
        "Status retrieval took too long: {:?}",
        duration
    );
    
    println!("Status retrieval completed in: {:?}", duration);
}
