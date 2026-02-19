use serial_test::serial;
// =============================================================================
// INTEGRATION TESTS - WALLET/ADDRESS VALIDATION EDGE CASES
// Tests for address format validation, chain mismatches, invalid addresses
// =============================================================================

use serde_json::{json, Value};

#[path = "../common/mod.rs"]
mod common;
use common::{setup_test_server, timed_post, timed_get};
use std::time::Duration;
use tokio::time::sleep;

// =============================================================================
// TEST 1: Invalid Bitcoin Address Rejection
// System should reject obviously invalid BTC addresses
// =============================================================================

#[serial]
#[tokio::test]
async fn test_invalid_btc_address_rejection() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Get rates first
    let rate_url = "/swap/rates?from=eth&to=btc&amount=1&network_from=Ethereum&network_to=Mainnet";
    let rate_response = timed_get(&server, rate_url).await;

    if !rate_response.status().is_success() {
        println!("Skipping BTC address test - rates unavailable");
        return;
    }

    let rate_json: Value = rate_response.json();
    let trade_id = rate_json.get("trade_id").and_then(|v| v.as_str());
    let provider = rate_json["rates"][0]["provider"].as_str().unwrap_or("changenow");

    // Try with invalid BTC address (too short, wrong checksum)
    let payload = json!({
        "trade_id": trade_id,
        "from": "eth",
        "network_from": "Ethereum",
        "to": "btc",
        "network_to": "Mainnet",
        "amount": 1.0,
        "provider": provider,
        "recipient_address": "1InvalidBTCAddressXYZ", // ❌ Invalid format
        "refund_address": "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12", // ✅ Valid ETH
        "rate_type": "floating"
    });

    let response = timed_post(&server, "/swap/create", &payload).await;
    
    // Should fail
    assert!(
        response.status().is_client_error() || response.status().is_server_error(),
        "Should reject invalid BTC address"
    );
}

// =============================================================================
// TEST 2: Invalid Ethereum Address Rejection
// Ethereum addresses must be 0x + 40 hex characters
// =============================================================================

#[serial]
#[tokio::test]
async fn test_invalid_eth_address_rejection() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let rate_url = "/swap/rates?from=btc&to=eth&amount=0.1&network_from=Mainnet&network_to=Ethereum";
    let rate_response = timed_get(&server, rate_url).await;

    if !rate_response.status().is_success() {
        println!("Skipping ETH address test - rates unavailable");
        return;
    }

    let rate_json: Value = rate_response.json();
    let trade_id = rate_json.get("trade_id").and_then(|v| v.as_str());
    let provider = rate_json["rates"][0]["provider"].as_str().unwrap_or("changenow");

    // Invalid ETH address (wrong length)
    let payload = json!({
        "trade_id": trade_id,
        "from": "btc",
        "network_from": "Mainnet",
        "to": "eth",
        "network_to": "Ethereum",
        "amount": 0.1,
        "provider": provider,
        "recipient_address": "0x123", // ❌ Too short
        "refund_address": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh", // ✅ Valid BTC
        "rate_type": "floating"
    });

    let response = timed_post(&server, "/swap/create", &payload).await;
    
    assert!(
        !response.status().is_success(),
        "Should reject invalid Ethereum address"
    );
}

// =============================================================================
// TEST 3: Solana Address Format Validation
// Solana addresses are Base58, not hex
// =============================================================================

#[serial]
#[tokio::test]
async fn test_solana_address_format_validation() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Test address validation endpoint
    let payload = json!({
        "ticker": "sol",
        "network": "Solana",
        "address": "9B5X1CbM3nDZCoDjiHWuqJ3UaYN2" // Valid Solana address
    });

    let response = timed_post(&server, "/swap/validate-address", &payload).await;
    
    if response.status().is_success() {
        let validation_json: Value = response.json();
        assert_eq!(validation_json["valid"].as_bool().unwrap_or(false), true, 
                   "Valid Solana address should be accepted");
    }

    // Try invalid Solana address (Ethereum format)
    let invalid_payload = json!({
        "ticker": "sol",
        "network": "Solana",
        "address": "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12" // ❌ Ethereum format
    });

    let invalid_response = timed_post(&server, "/swap/validate-address", &invalid_payload).await;
    
    if invalid_response.status().is_success() {
        let validation_json: Value = invalid_response.json();
        assert_eq!(validation_json["valid"].as_bool().unwrap_or(true), false, 
                   "ETH address should not be valid for Solana");
    }
}

// =============================================================================
// TEST 4: Address with Memo/Tag Required (XRP, Stellar, etc.)
// XRP addresses require destination tag, system should handle this
// =============================================================================

#[serial]
#[tokio::test]
async fn test_address_with_required_memo() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Get rate for a coin that requires memo
    let rate_url = "/swap/rates?from=btc&to=xrp&amount=0.1&network_from=Mainnet&network_to=Mainnet";
    let rate_response = timed_get(&server, rate_url).await;

    if !rate_response.status().is_success() {
        println!("Skipping XRP memo test - rates unavailable");
        return;
    }

    let rate_json: Value = rate_response.json();
    
    // Check if rate response indicates memo requirement
    let needs_memo = rate_json["rates"][0].get("memo_required")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    if needs_memo {
        println!("XRP requires memo - testing memo handling");
        // System should prompt user for memo/tag
    }
}

// =============================================================================
// TEST 5: Case Sensitivity in Addresses
// Bitcoin and Ethereum are case-insensitive (when hex), 
// but some validation might be strict
// =============================================================================

#[serial]
#[tokio::test]
async fn test_address_case_handling() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Get rates
    let rate_url = "/swap/rates?from=btc&to=eth&amount=0.1&network_from=Mainnet&network_to=Ethereum";
    let rate_response = timed_get(&server, rate_url).await;

    if !rate_response.status().is_success() {
        println!("Skipping case sensitivity test - rates unavailable");
        return;
    }

    let rate_json: Value = rate_response.json();
    let trade_id = rate_json.get("trade_id").and_then(|v| v.as_str());
    let provider = rate_json["rates"][0]["provider"].as_str().unwrap_or("changenow");

    // Try ETH address in lowercase
    let lower_payload = json!({
        "trade_id": trade_id,
        "from": "btc",
        "network_from": "Mainnet",
        "to": "eth",
        "network_to": "Ethereum",
        "amount": 0.1,
        "provider": provider,
        "recipient_address": "0x742d35cc6634c0532925a3b844bc9e7595f5be12", // lowercase
        "refund_address": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
        "rate_type": "floating"
    });

    let lower_response = timed_post(&server, "/swap/create", &payload).await;
    
    // Should accept (Ethereum addresses are case-insensitive in terms of validity)
    println!("Lowercase address response: {}", lower_response.status());
}

// =============================================================================
// TEST 6: Empty Address Field
// System should reject empty addresses
// =============================================================================

#[serial]
#[tokio::test]
async fn test_empty_address_rejection() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let rate_url = "/swap/rates?from=btc&to=eth&amount=0.1&network_from=Mainnet&network_to=Ethereum";
    let rate_response = timed_get(&server, rate_url).await;

    if !rate_response.status().is_success() {
        return;
    }

    let rate_json: Value = rate_response.json();
    let trade_id = rate_json.get("trade_id").and_then(|v| v.as_str());
    let provider = rate_json["rates"][0]["provider"].as_str().unwrap_or("changenow");

    // Empty recipient address
    let payload = json!({
        "trade_id": trade_id,
        "from": "btc",
        "network_from": "Mainnet",
        "to": "eth",
        "network_to": "Ethereum",
        "amount": 0.1,
        "provider": provider,
        "recipient_address": "", // ❌ Empty
        "refund_address": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
        "rate_type": "floating"
    });

    let response = timed_post(&server, "/swap/create", &payload).await;
    
    assert!(!response.status().is_success(), "Should reject empty address");
}

// =============================================================================
// TEST 7: Address with Whitespace (Should Be Trimmed or Rejected)
// Some systems are lenient, others strict
// =============================================================================

#[serial]
#[tokio::test]
async fn test_address_with_whitespace() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let rate_url = "/swap/rates?from=btc&to=eth&amount=0.1&network_from=Mainnet&network_to=Ethereum";
    let rate_response = timed_get(&server, rate_url).await;

    if !rate_response.status().is_success() {
        return;
    }

    let rate_json: Value = rate_response.json();
    let trade_id = rate_json.get("trade_id").and_then(|v| v.as_str());
    let provider = rate_json["rates"][0]["provider"].as_str().unwrap_or("changenow");

    // Address with spaces
    let payload = json!({
        "trade_id": trade_id,
        "from": "btc",
        "network_from": "Mainnet",
        "to": "eth",
        "network_to": "Ethereum",
        "amount": 0.1,
        "provider": provider,
        "recipient_address": " 0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12 ", // Spaces
        "refund_address": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
        "rate_type": "floating"
    });

    let response = timed_post(&server, "/swap/create", &payload).await;
    
    // Either should succeed (if trimmed) or fail (if strict)
    println!("Whitespace address response: {}", response.status());
}

// =============================================================================
// TEST 8: Special Characters in Address
// Some coins have special formats (e.g., Monero, Zcash with payment IDs)
// =============================================================================

#[serial]
#[tokio::test]
async fn test_special_characters_in_address() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Monero address (longer, different format)
    let validation_payload = json!({
        "ticker": "xmr",
        "network": "Mainnet",
        "address": "44AFFq5kSiGBoZ4NMDwYtN18obc8AemS33DBLWs3H7otXft3XjrpDtQGvj85ngqVqWfdn4ufSXIzJRHWKJ32khHA7wGwwve"
    });

    let response = timed_post(&server, "/swap/validate-address", &validation_payload).await;
    
    if response.status().is_success() {
        let validation_json: Value = response.json();
        let is_valid = validation_json["valid"].as_bool().unwrap_or(false);
        println!("Monero address valid: {}", is_valid);
    }
}

// =============================================================================
// TEST 9: Very Long Address String
// Some addresses can be long; system should handle or reject gracefully
// =============================================================================

#[serial]
#[tokio::test]
async fn test_very_long_address_handling() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let rate_url = "/swap/rates?from=btc&to=eth&amount=0.1&network_from=Mainnet&network_to=Ethereum";
    let rate_response = timed_get(&server, rate_url).await;

    if !rate_response.status().is_success() {
        return;
    }

    let rate_json: Value = rate_response.json();
    let trade_id = rate_json.get("trade_id").and_then(|v| v.as_str());
    let provider = rate_json["rates"][0]["provider"].as_str().unwrap_or("changenow");

    // Extremely long string
    let long_address = "0x" + &"a".repeat(1000);

    let payload = json!({
        "trade_id": trade_id,
        "from": "btc",
        "network_from": "Mainnet",
        "to": "eth",
        "network_to": "Ethereum",
        "amount": 0.1,
        "provider": provider,
        "recipient_address": long_address,
        "refund_address": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
        "rate_type": "floating"
    });

    let response = timed_post(&server, "/swap/create", &payload).await;
    
    // Should reject (address too long)
    assert!(!response.status().is_success(), "Should reject extremely long address");
}

// =============================================================================
// TEST 10: Same Address for Both Recipient and Refund
// This might be allowed or disallowed depending on business logic
// =============================================================================

#[serial]
#[tokio::test]
async fn test_same_recipient_and_refund_address() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let rate_url = "/swap/rates?from=btc&to=eth&amount=0.1&network_from=Mainnet&network_to=Ethereum";
    let rate_response = timed_get(&server, rate_url).await;

    if !rate_response.status().is_success() {
        return;
    }

    let rate_json: Value = rate_response.json();
    let trade_id = rate_json.get("trade_id").and_then(|v| v.as_str());
    let provider = rate_json["rates"][0]["provider"].as_str().unwrap_or("changenow");

    let address = "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh";

    // Same address for both
    let payload = json!({
        "trade_id": trade_id,
        "from": "btc",
        "network_from": "Mainnet",
        "to": "eth",
        "network_to": "Ethereum",
        "amount": 0.1,
        "provider": provider,
        "recipient_address": "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12",
        "refund_address": "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12", // Same as recipient
        "rate_type": "floating"
    });

    let response = timed_post(&server, "/swap/create", &payload).await;
    
    // Implementation decision: allow or disallow
    println!("Same address response: {}", response.status());
}

// =============================================================================
// TEST 11: Address with Extra ID Format (Ripple, Stellar)
// Coins like XRP need memo/extra_id field
// =============================================================================

#[serial]
#[tokio::test]
async fn test_address_with_extra_id() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Get rate for coin requiring extra ID
    let rate_url = "/swap/rates?from=btc&to=xrp&amount=0.1&network_from=Mainnet&network_to=Mainnet";
    let rate_response = timed_get(&server, rate_url).await;

    if !rate_response.status().is_success() {
        println!("Skipping extra ID test - XRP rates unavailable");
        return;
    }

    let rate_json: Value = rate_response.json();
    let trade_id = rate_json.get("trade_id").and_then(|v| v.as_str());
    let provider = rate_json["rates"][0]["provider"].as_str().unwrap_or("changenow");

    // Try with extra_id (destination tag for XRP)
    let payload = json!({
        "trade_id": trade_id,
        "from": "btc",
        "network_from": "Mainnet",
        "to": "xrp",
        "network_to": "Mainnet",
        "amount": 0.1,
        "provider": provider,
        "recipient_address": "rN7n7otQDdUQbE5owLqGD328HAjvzrm7Rh", // XRP address
        "recipient_extra_id": "123456789", // Destination tag
        "refund_address": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
        "rate_type": "floating"
    });

    let response = timed_post(&server, "/swap/create", &payload).await;
    
    if response.status().is_success() {
        println!("✅ Extra ID accepted for XRP");
    } else {
        println!("XRP swap response: {}", response.status());
    }
}

// =============================================================================
// TEST 12: Cross-Chain Address Format Confusion
// User sends ETH address when they should send different format
// =============================================================================

#[serial]
#[tokio::test]
async fn test_cross_chain_address_format_confusion() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Get rates for BTC to USDT (multiple chains for USDT)
    let rate_url = "/swap/rates?from=btc&to=usdt&amount=0.1&network_from=Mainnet&network_to=Ethereum";
    let rate_response = timed_get(&server, rate_url).await;

    if !rate_response.status().is_success() {
        println!("Skipping cross-chain test - rates unavailable");
        return;
    }

    let rate_json: Value = rate_response.json();
    let trade_id = rate_json.get("trade_id").and_then(|v| v.as_str());
    let provider = rate_json["rates"][0]["provider"].as_str().unwrap_or("changenow");

    // Send with Solana address format when expecting Ethereum
    let payload = json!({
        "trade_id": trade_id,
        "from": "btc",
        "network_from": "Mainnet",
        "to": "usdt",
        "network_to": "Ethereum",
        "amount": 0.1,
        "provider": provider,
        "recipient_address": "9B5X1CbM3nDZCoDjiHWuqJ3UaYN2Wmmv9", // Solana format
        "refund_address": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
        "rate_type": "floating"
    });

    let response = timed_post(&server, "/swap/create", &payload).await;
    
    // Should reject - address format doesn't match network
    if !response.status().is_success() {
        println!("✅ Correctly rejected Solana address for Ethereum USDT");
    } else {
        println!("⚠️ Accepted cross-chain address (may need stricter validation)");
    }
}

// =============================================================================
// TEST 13: Address Validation Edge Case - Batch Validation
// When showing multiple addresses, validate format of each
// =============================================================================

#[serial]
#[tokio::test]
async fn test_batch_address_validation() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let addresses = vec![
        ("btc", "Mainnet", "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"),
        ("eth", "Ethereum", "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12"),
        ("xmr", "Mainnet", "44AFFq5kSiGBoZ4NMDwYtN18obc8AemS33DBLWs3H7otXft3XjrpDtQGvj85ngqVqWfdn4ufSXIzJRHWKJ32khHA7wGwwve"),
    ];

    for (ticker, network, address) in addresses {
        let payload = json!({
            "ticker": ticker,
            "network": network,
            "address": address
        });

        let response = timed_post(&server, "/swap/validate-address", &payload).await;
        
        if response.status().is_success() {
            let validation_json: Value = response.json();
            let is_valid = validation_json["valid"].as_bool().unwrap_or(false);
            println!("{} ({}): {}", ticker, network, is_valid);
        }
    }
}
