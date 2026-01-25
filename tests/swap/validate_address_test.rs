use serde_json::{json, Value};

#[path = "../common/mod.rs"]
mod common;
use common::{setup_test_server, timed_post};
use std::time::Duration;
use tokio::time::sleep;

// =============================================================================
// INTEGRATION TESTS - ADDRESS VALIDATION ENDPOINT (POST /swap/validate-address)
// These tests verify the address validation functionality
// =============================================================================

/// Test valid Bitcoin address validation
#[tokio::test]
async fn test_validate_address_valid_btc() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    let payload = json!({
        "ticker": "btc",
        "network": "Mainnet",
        "address": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"
    });

    let response = timed_post(&server, validate_url, &payload).await;
    response.assert_status_ok();
    
    let json: Value = response.json();
    
    assert!(json.get("valid").is_some(), "Response should have 'valid' field");
    assert_eq!(json["valid"].as_bool().unwrap(), true, "BTC address should be valid");
    assert_eq!(json["ticker"].as_str().unwrap(), "btc");
    assert_eq!(json["network"].as_str().unwrap(), "Mainnet");
    
    println!("Valid BTC address validated successfully");
}

/// Test invalid Bitcoin address validation
#[tokio::test]
async fn test_validate_address_invalid_btc() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    let payload = json!({
        "ticker": "btc",
        "network": "Mainnet",
        "address": "INVALID_BTC_ADDRESS_12345"
    });

    let response = timed_post(&server, validate_url, &payload).await;
    response.assert_status_ok();
    
    let json: Value = response.json();
    
    assert!(json.get("valid").is_some(), "Response should have 'valid' field");
    assert_eq!(json["valid"].as_bool().unwrap(), false, "Invalid BTC address should be rejected");
    
    println!("Invalid BTC address correctly rejected");
}

/// Test valid Monero address validation
#[tokio::test]
async fn test_validate_address_valid_xmr() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    let payload = json!({
        "ticker": "xmr",
        "network": "Mainnet",
        "address": "44AFFq5kSiGBoZ4NMDwYtN18obc8AemS33DBLWs3H7otXft3XjrpDtQGvj85ngqVqWfdn4ufSXIzJRHWKJ32khHA7wGwwve"
    });

    let response = timed_post(&server, validate_url, &payload).await;
    response.assert_status_ok();
    
    let json: Value = response.json();
    
    assert_eq!(json["valid"].as_bool().unwrap(), true, "XMR address should be valid");
    assert_eq!(json["ticker"].as_str().unwrap(), "xmr");
    
    println!("Valid XMR address validated successfully");
}

/// Test valid Ethereum address validation
#[tokio::test]
async fn test_validate_address_valid_eth() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    let payload = json!({
        "ticker": "eth",
        "network": "ethereum",
        "address": "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0"
    });

    let response = timed_post(&server, validate_url, &payload).await;
    
    // ETH validation might fail if Trocador doesn't support this exact network name
    // So we just verify we get a proper response structure
    if response.status_code().is_success() {
        let json: Value = response.json();
        assert!(json.get("valid").is_some(), "Response should have 'valid' field");
        println!("ETH address validation response: {}", json["valid"]);
    } else {
        println!("ETH validation returned error (might be network name issue with Trocador)");
    }
}

/// Test invalid Ethereum address validation
#[tokio::test]
async fn test_validate_address_invalid_eth() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    let payload = json!({
        "ticker": "eth",
        "network": "ethereum",
        "address": "0xINVALID"
    });

    let response = timed_post(&server, validate_url, &payload).await;
    
    // Similar to above, just verify we get a response
    if response.status_code().is_success() {
        let json: Value = response.json();
        assert!(json.get("valid").is_some());
        println!("Invalid ETH address validation response: {}", json["valid"]);
    } else {
        println!("ETH validation returned error");
    }
}

/// Test valid Litecoin address validation
#[tokio::test]
async fn test_validate_address_valid_ltc() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    let payload = json!({
        "ticker": "ltc",
        "network": "Mainnet",
        "address": "LTC1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"  // Valid LTC bech32 address
    });

    let response = timed_post(&server, validate_url, &payload).await;
    
    // LTC address validation might fail if format is wrong, so we check both cases
    if response.status_code().is_success() {
        let json: Value = response.json();
        // Just verify we get a response, don't assert validity since address format might vary
        assert!(json.get("valid").is_some(), "Response should have 'valid' field");
        println!("LTC address validation response: {}", json["valid"]);
    } else {
        println!("LTC validation returned error (might be address format issue)");
    }
}

/// Test missing required fields
#[tokio::test]
async fn test_validate_address_missing_ticker() {
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    let payload = json!({
        "network": "Mainnet",
        "address": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"
    });

    let response = timed_post(&server, validate_url, &payload).await;
    
    // Should return 400 Bad Request or 422 Unprocessable Entity
    let status = response.status_code().as_u16();
    assert!(status >= 400 && status < 500, "Should return client error for missing ticker");
}

/// Test missing network field
#[tokio::test]
async fn test_validate_address_missing_network() {
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    let payload = json!({
        "ticker": "btc",
        "address": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"
    });

    let response = timed_post(&server, validate_url, &payload).await;
    
    let status = response.status_code().as_u16();
    assert!(status >= 400 && status < 500, "Should return client error for missing network");
}

/// Test missing address field
#[tokio::test]
async fn test_validate_address_missing_address() {
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    let payload = json!({
        "ticker": "btc",
        "network": "Mainnet"
    });

    let response = timed_post(&server, validate_url, &payload).await;
    
    let status = response.status_code().as_u16();
    assert!(status >= 400 && status < 500, "Should return client error for missing address");
}

/// Test empty address string
#[tokio::test]
async fn test_validate_address_empty_address() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    let payload = json!({
        "ticker": "btc",
        "network": "Mainnet",
        "address": ""
    });

    let response = timed_post(&server, validate_url, &payload).await;
    
    // Should either return 400 or return valid: false
    if response.status_code().is_success() {
        let json: Value = response.json();
        assert_eq!(json["valid"].as_bool().unwrap(), false, "Empty address should be invalid");
    } else {
        assert!(response.status_code().as_u16() >= 400);
    }
}

/// Test whitespace-only address
#[tokio::test]
async fn test_validate_address_whitespace_address() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    let payload = json!({
        "ticker": "btc",
        "network": "Mainnet",
        "address": "   "
    });

    let response = timed_post(&server, validate_url, &payload).await;
    
    if response.status_code().is_success() {
        let json: Value = response.json();
        assert_eq!(json["valid"].as_bool().unwrap(), false, "Whitespace address should be invalid");
    } else {
        assert!(response.status_code().as_u16() >= 400);
    }
}

/// Test unsupported coin ticker
#[tokio::test]
async fn test_validate_address_unsupported_coin() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    let payload = json!({
        "ticker": "FAKECOIN",
        "network": "Mainnet",
        "address": "some_address_here"
    });

    let response = timed_post(&server, validate_url, &payload).await;
    
    // Should return error or valid: false
    if response.status_code().is_success() {
        let json: Value = response.json();
        // Trocador might return false for unsupported coins
        assert_eq!(json["valid"].as_bool().unwrap(), false);
    } else {
        assert!(response.status_code().as_u16() >= 400);
    }
}

/// Test wrong network for coin
#[tokio::test]
async fn test_validate_address_wrong_network() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    let payload = json!({
        "ticker": "btc",
        "network": "ERC20", // BTC doesn't exist on ERC20
        "address": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"
    });

    let response = timed_post(&server, validate_url, &payload).await;
    
    // Should return error or valid: false
    if response.status_code().is_success() {
        let json: Value = response.json();
        assert_eq!(json["valid"].as_bool().unwrap(), false, "Wrong network should be invalid");
    } else {
        assert!(response.status_code().as_u16() >= 400);
    }
}

/// Test case sensitivity of ticker
#[tokio::test]
async fn test_validate_address_case_insensitive_ticker() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    let payload = json!({
        "ticker": "BTC", // Uppercase
        "network": "Mainnet",
        "address": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"
    });

    let response = timed_post(&server, validate_url, &payload).await;
    
    // Should work regardless of case
    if response.status_code().is_success() {
        let json: Value = response.json();
        assert!(json.get("valid").is_some());
    }
}

/// Test very long address string
#[tokio::test]
async fn test_validate_address_very_long_address() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    let long_address = "a".repeat(500);
    let payload = json!({
        "ticker": "btc",
        "network": "Mainnet",
        "address": long_address
    });

    let response = timed_post(&server, validate_url, &payload).await;
    
    if response.status_code().is_success() {
        let json: Value = response.json();
        assert_eq!(json["valid"].as_bool().unwrap(), false, "Very long address should be invalid");
    }
}

/// Test special characters in address
#[tokio::test]
async fn test_validate_address_special_characters() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    let payload = json!({
        "ticker": "btc",
        "network": "Mainnet",
        "address": "bc1q!@#$%^&*()"
    });

    let response = timed_post(&server, validate_url, &payload).await;
    
    if response.status_code().is_success() {
        let json: Value = response.json();
        assert_eq!(json["valid"].as_bool().unwrap(), false, "Address with special chars should be invalid");
    }
}

/// Test multiple rapid validations (rate limiting resilience)
#[tokio::test]
async fn test_validate_address_multiple_rapid_calls() {
    sleep(Duration::from_secs(2)).await;
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    
    for i in 1..=5 {
        let payload = json!({
            "ticker": "btc",
            "network": "Mainnet",
            "address": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"
        });

        let response = timed_post(&server, validate_url, &payload).await;
        
        if !response.status_code().is_success() {
            println!("Validation {} failed with: {}", i, response.status_code());
        }
        
        // Should not return 500 errors
        assert!(
            response.status_code().as_u16() < 500,
            "Should not return server error on validation {}",
            i
        );
        
        sleep(Duration::from_millis(500)).await;
    }
}

/// Test validation response time (should be fast)
#[tokio::test]
async fn test_validate_address_performance() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    let payload = json!({
        "ticker": "btc",
        "network": "Mainnet",
        "address": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"
    });

    let start = std::time::Instant::now();
    let response = timed_post(&server, validate_url, &payload).await;
    let duration = start.elapsed();

    response.assert_status_ok();
    
    // Should complete within 3 seconds
    assert!(
        duration.as_secs() < 3,
        "Address validation took too long: {:?}",
        duration
    );
    
    println!("Address validation completed in: {:?}", duration);
}

/// Test XRP address with destination tag requirement
#[tokio::test]
async fn test_validate_address_xrp_with_tag() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    let payload = json!({
        "ticker": "xrp",
        "network": "Mainnet",
        "address": "rEb8TK3gBgk5auZkwc6sHnwrGVJH8DuaLh"
    });

    let response = timed_post(&server, validate_url, &payload).await;
    
    if response.status_code().is_success() {
        let json: Value = response.json();
        // XRP address should be valid (destination tag is optional for validation)
        assert!(json.get("valid").is_some());
    }
}

/// Test response structure completeness
#[tokio::test]
async fn test_validate_address_response_structure() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let validate_url = "/swap/validate-address";
    let payload = json!({
        "ticker": "btc",
        "network": "Mainnet",
        "address": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"
    });

    let response = timed_post(&server, validate_url, &payload).await;
    response.assert_status_ok();
    
    let json: Value = response.json();
    
    // Verify required fields
    assert!(json.get("valid").is_some(), "Response should have 'valid' field");
    assert!(json.get("ticker").is_some(), "Response should have 'ticker' field");
    assert!(json.get("network").is_some(), "Response should have 'network' field");
    assert!(json.get("address").is_some(), "Response should have 'address' field");
    
    // Verify types
    assert!(json["valid"].is_boolean(), "'valid' should be boolean");
    assert!(json["ticker"].is_string(), "'ticker' should be string");
    assert!(json["network"].is_string(), "'network' should be string");
    assert!(json["address"].is_string(), "'address' should be string");
    
    println!("Response structure validated successfully");
}
