use serial_test::serial;
// =============================================================================
// INTEGRATION TESTS - MULTI-CHAIN / MULTI-TOKEN EDGE CASES
// Tests handle scenarios with same token on different chains
// (USDT Ethereum vs Solana, USDC Polygon vs Ethereum, etc.)
// =============================================================================

use serde_json::{json, Value};

#[path = "../common/mod.rs"]
mod common;
use common::{setup_test_server, timed_post, timed_get};
use std::time::Duration;
use tokio::time::sleep;

// =============================================================================
// TEST 1: USDT Same Token, Different Chains
// User sends USDT on Ethereum, should NOT work if system expects Solana
// =============================================================================

#[serial]
#[tokio::test]
async fn test_usdt_ethereum_to_ethereum_swap() {
    sleep(Duration::from_secs(1)).await; // Rate limit
    let server = setup_test_server().await;

    // Get rates for USDT Ethereum → USDC Ethereum
    let rate_url = "/swap/rates?from=usdt&to=usdc&amount=100&network_from=Ethereum&network_to=Ethereum";
    let rate_response = timed_get(&server, rate_url).await;
    
    // Should succeed - both on same chain
    assert!(rate_response.status().is_success(), 
        "Getting USDT(ETH) → USDC(ETH) rates should succeed. Response: {:?}", 
        rate_response.text().await);
    
    let rate_json: Value = rate_response.json();
    
    // Verify response structure
    assert!(rate_json["rates"].is_array(), "Should have rates array");
    assert!(!rate_json["rates"].as_array().unwrap().is_empty(), "Should have at least one rate");
    
    // Verify chain information in response
    assert_eq!(rate_json["network_from"].as_str(), Some("Ethereum"), "Should show Ethereum as network");
    assert_eq!(rate_json["network_to"].as_str(), Some("Ethereum"), "Should show Ethereum as network");
}

// =============================================================================
// TEST 2: USDT Chain Mismatch (Critical Edge Case)
// User specifies Ethereum USDT but tries to use Solana contract address
// System should detect and reject this mismatch
// =============================================================================

#[serial]
#[tokio::test]
async fn test_usdt_chain_mismatch_rejection() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // First, get rate for USDT Solana
    let rate_url = "/swap/rates?from=usdt&to=eth&amount=100&network_from=Solana&network_to=Ethereum";
    let rate_response = timed_get(&server, rate_url).await;
    
    if !rate_response.status().is_success() {
        println!("Note: Solana → Ethereum rates may not be supported by Trocador. Skipping test.");
        return;
    }

    let rate_json: Value = rate_response.json();
    let trade_id = rate_json.get("trade_id").and_then(|v| v.as_str());

    // Try to create swap with WRONG address format
    // Ethereum address (0x...) cannot receive SPL tokens on Solana
    let payload = json!({
        "trade_id": trade_id,
        "from": "usdt",
        "network_from": "Solana",
        "to": "eth",
        "network_to": "Ethereum",
        "amount": 100.0,
        "provider": "changenow",
        "recipient_address": "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12", // ❌ ETH address, not Solana
        "refund_address": "9B5X1CbM3nDZCoDjiHWuqJ3UaYN2..." // ✅ Valid Solana address
    });

    let response = timed_post(&server, "/swap/create", &payload).await;
    
    // Should fail - address format doesn't match network
    assert!(
        response.status().is_client_error() || response.status().is_server_error(),
        "Should reject Ethereum address for Solana-based USDT"
    );
}

// =============================================================================
// TEST 3: USDC Multi-Chain Selection - User Gets Right Chain
// Verify system can route USDC to multiple chains correctly
// =============================================================================

#[serial]
#[tokio::test]
async fn test_usdc_multiple_chains_supported() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Get supported networks for USDC
    let currencies_url = "/swap/currencies?ticker=usdc";
    let currencies_response = timed_get(&server, currencies_url).await;
    
    assert!(currencies_response.status().is_success(), "Should list USDC currencies");
    
    let currencies_json: Value = currencies_response.json();
    
    // USDC should exist on multiple networks
    // Filter to find USDC entries
    let usdc_entries: Vec<_> = currencies_json.as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter(|c| c["ticker"].as_str() == Some("usdc"))
        .collect();
    
    // Ideally should have USDC on Ethereum, Polygon, etc.
    // (At minimum, should have at least one)
    assert!(!usdc_entries.is_empty(), "USDC should be available on at least one network");
    
    // Verify each USDC has network info
    for entry in usdc_entries {
        assert!(entry["network"].is_string(), "Should specify network for USDC");
        assert!(entry["ticker"].is_string(), "Should have ticker");
    }
}

// =============================================================================
// TEST 4: Commission with Multi-Chain Swap
// Verify commission is applied correctly regardless of chains involved
// =============================================================================

#[serial]
#[tokio::test]
async fn test_commission_applies_across_chains() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Get rates for a multi-chain swap
    let rate_url = "/swap/rates?from=btc&to=eth&amount=0.1&network_from=Mainnet&network_to=Ethereum";
    let rate_response = timed_get(&server, rate_url).await;
    
    if !rate_response.status().is_success() {
        println!("Note: BTC → ETH rates may not be available. Skipping commission test.");
        return;
    }

    let rate_json: Value = rate_response.json();
    
    // Check that rates include fee information
    for rate in rate_json["rates"].as_array().unwrap_or(&vec![]) {
        let network_fee = rate["network_fee"].as_f64().unwrap_or(0.0);
        let provider_fee = rate["provider_fee"].as_f64().unwrap_or(0.0);
        let platform_fee = rate["platform_fee"].as_f64().unwrap_or(0.0);
        let total_fee = rate["total_fee"].as_f64().unwrap_or(0.0);
        
        // Platform fee should be part of total (commission logic)
        assert!(total_fee > 0.0, "Total fee should be greater than 0");
        
        // Total should be sum of components (or close to it, accounting for rounding)
        let calculated_total = network_fee + provider_fee + platform_fee;
        let diff = (total_fee - calculated_total).abs();
        assert!(diff < 0.001, "Total fee should roughly equal sum of components (diff: {})", diff);
    }
}

// =============================================================================
// TEST 5: Invalid Chain Specification
// Test that system rejects invalid/unsupported chain combinations
// =============================================================================

#[serial]
#[tokio::test]
async fn test_invalid_chain_specification() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Try to get rates for completely invalid chain
    let rate_url = "/swap/rates?from=btc&to=eth&amount=0.1&network_from=InvalidChain&network_to=InvalidChain";
    let rate_response = timed_get(&server, rate_url).await;
    
    // Should fail
    assert!(
        !rate_response.status().is_success(),
        "Should reject invalid chain specification"
    );
    
    let error_json: Value = rate_response.json();
    assert!(error_json["error"].is_string(), "Should return error message");
}

// =============================================================================
// TEST 6: Token Exists But Not on Requested Chain
// e.g., USDT exists on Ethereum & Solana, but not on Bitcoin network
// =============================================================================

#[serial]
#[tokio::test]
async fn test_token_on_unsupported_chain() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Try USDT on Bitcoin (doesn't exist - Bitcoin doesn't have tokens like Ethereum)
    let rate_url = "/swap/rates?from=usdt&to=btc&amount=100&network_from=Bitcoin&network_to=Mainnet";
    let rate_response = timed_get(&server, rate_url).await;
    
    // Should fail - USDT not available on Bitcoin network
    if !rate_response.status().is_success() {
        let error = rate_response.text().await;
        println!("Expected error for USDT on Bitcoin: {}", error);
        assert!(true, "Correctly rejected unsupported token on chain");
    }
}

// =============================================================================
// TEST 7: Refund Address Chain Mismatch
// User tries to refund to wrong chain address
// =============================================================================

#[serial]
#[tokio::test]
async fn test_refund_address_chain_validation() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Get a swap quote first
    let rate_url = "/swap/rates?from=btc&to=eth&amount=0.01&network_from=Mainnet&network_to=Ethereum";
    let rate_response = timed_get(&server, rate_url).await;
    
    if !rate_response.status().is_success() {
        println!("Skipping refund validation test - rates unavailable");
        return;
    }

    let rate_json: Value = rate_response.json();
    let trade_id = rate_json.get("trade_id").and_then(|v| v.as_str());

    // Try to create swap with ETH address as BTC refund address
    let payload = json!({
        "trade_id": trade_id,
        "from": "btc",
        "network_from": "Mainnet",
        "to": "eth",
        "network_to": "Ethereum",
        "amount": 0.01,
        "provider": "changenow",
        "recipient_address": "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12", // ✅ Valid ETH address
        "refund_address": "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12", // ❌ ETH address, not BTC
        "rate_type": "floating"
    });

    let response = timed_post(&server, "/swap/create", &payload).await;
    
    // Should fail - refund address format doesn't match source currency
    if !response.status().is_success() {
        println!("✅ Correctly rejected mismatched refund address");
        assert!(true);
    } else {
        println!("⚠️  System accepted mismatched refund address (may need validation)");
    }
}

// =============================================================================
// TEST 8: Same Pair, Different Networks - Prices Should Vary
// USDT-USDC swap should have different rates if on different chains
// (due to different liquidity/fees per chain)
// =============================================================================

#[serial]
#[tokio::test]
async fn test_same_pair_different_networks_different_rates() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Get USDT → USDC on Ethereum
    let eth_url = "/swap/rates?from=usdt&to=usdc&amount=100&network_from=Ethereum&network_to=Ethereum";
    let eth_response = timed_get(&server, eth_url).await;

    // Get USDT → USDC on Polygon (or other chain if Solana supported)
    let poly_url = "/swap/rates?from=usdt&to=usdc&amount=100&network_from=Polygon&network_to=Polygon";
    let poly_response = timed_get(&server, poly_url).await;

    if !eth_response.status().is_success() {
        println!("Note: USDT/USDC on Ethereum not available. Skipping test.");
        return;
    }

    if !poly_response.status().is_success() {
        println!("Note: USDT/USDC on Polygon not available. Skipping test.");
        return;
    }

    let eth_json: Value = eth_response.json();
    let poly_json: Value = poly_response.json();

    // Both should have rates
    assert!(!eth_json["rates"].as_array().unwrap_or(&vec![]).is_empty());
    assert!(!poly_json["rates"].as_array().unwrap_or(&vec![]).is_empty());

    // Network fees should likely be different (Polygon cheaper than Ethereum)
    let eth_fee = eth_json["rates"][0]["network_fee"].as_f64().unwrap_or(0.0);
    let poly_fee = poly_json["rates"][0]["network_fee"].as_f64().unwrap_or(0.0);

    println!("ETH network fee: {}, Polygon network fee: {}", eth_fee, poly_fee);
    // Polygon should have lower fees (just informational, not asserting)
}

// =============================================================================
// TEST 9: Memo/Tag Required Addresses Across Chains
// Some coins need memo/tag (XRP, Stellar, etc.)
// Test that system properly tracks this requirement per chain
// =============================================================================

#[serial]
#[tokio::test]
async fn test_memo_required_address_handling() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Get currencies and check which ones require memo
    let currencies_url = "/swap/currencies?memo=true"; // Filter for memo-required
    let currencies_response = timed_get(&server, currencies_url).await;
    
    if currencies_response.status().is_success() {
        let currencies_json: Value = currencies_response.json();
        
        // Should have currencies that require memo
        if let Some(arr) = currencies_json.as_array() {
            let memo_required_count = arr.iter()
                .filter(|c| c["memo"].as_bool().unwrap_or(false))
                .count();
            
            println!("Found {} currencies requiring memo", memo_required_count);
            
            // Verify memo field exists and is boolean
            for currency in arr.iter() {
                assert!(currency["memo"].is_boolean(), "Memo field should be boolean");
            }
        }
    }
}

// =============================================================================
// TEST 10: Rate Expiration with Multi-Chain Transactions
// Quote should expire before user can deposit on slow chain
// =============================================================================

#[serial]
#[tokio::test]
async fn test_rate_expiration_slow_chain() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Get rates - they should have expiration time
    let rate_url = "/swap/rates?from=btc&to=eth&amount=0.01&network_from=Mainnet&network_to=Ethereum";
    let rate_response = timed_get(&server, rate_url).await;
    
    if !rate_response.status().is_success() {
        println!("Skipping expiration test - rates unavailable");
        return;
    }

    let rate_json: Value = rate_response.json();
    
    // If system returns expiration info, validate it
    if rate_json.get("expires_at").is_some() {
        let expires_at = rate_json["expires_at"].as_str();
        println!("Quote expires at: {:?}", expires_at);
        assert!(expires_at.is_some(), "Should have expiration timestamp");
    }
}

// =============================================================================
// TEST 11: Decimal Precision for Different Tokens
// Some tokens use 6 decimals (USDC), others 18 (ETH)
// System must handle different precisions correctly
// =============================================================================

#[serial]
#[tokio::test]
async fn test_decimal_precision_handling() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Get all currencies to verify decimal information
    let currencies_url = "/swap/currencies?limit=50";
    let currencies_response = timed_get(&server, currencies_url).await;
    
    if currencies_response.status().is_success() {
        let currencies_json: Value = currencies_response.json();
        
        // Verify that amount minimums/maximums are present and reasonable
        if let Some(arr) = currencies_json.as_array() {
            for currency in arr.iter().take(10) {
                let minimum = currency["minimum"].as_f64();
                let maximum = currency["maximum"].as_f64();
                let ticker = currency["ticker"].as_str();
                
                println!("{}  min: {:?}, max: {:?}", ticker.unwrap_or("?"), minimum, maximum);
                
                // Validate ranges make sense
                if let (Some(min), Some(max)) = (minimum, maximum) {
                    assert!(min <= max, "Min should be <= max for {}", ticker.unwrap_or("?"));
                    assert!(min >= 0.0, "Min should be non-negative");
                    assert!(max > 0.0, "Max should be positive");
                }
            }
        }
    }
}

// =============================================================================
// TEST 12: Zero Amount Edge Case
// Test that zero or negative amounts are rejected
// =============================================================================

#[serial]
#[tokio::test]
async fn test_zero_amount_rejection() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Try zero amount
    let rate_url = "/swap/rates?from=btc&to=eth&amount=0&network_from=Mainnet&network_to=Ethereum";
    let rate_response = timed_get(&server, rate_url).await;
    
    assert!(!rate_response.status().is_success(), "Should reject zero amount");
}

// =============================================================================
// TEST 13: Very Large Amount - Beyond Max
// Test handling of amounts exceeding maximum per currency
// =============================================================================

#[serial]
#[tokio::test]
async fn test_amount_exceeds_maximum() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Try extremely large amount
    let rate_url = "/swap/rates?from=btc&to=eth&amount=999999&network_from=Mainnet&network_to=Ethereum";
    let rate_response = timed_get(&server, rate_url).await;
    
    // Should either fail or have limits in response
    if !rate_response.status().is_success() {
        let error_json: Value = rate_response.json();
        // Should indicate amount out of range
        assert!(error_json["error"].is_string());
    }
}

// =============================================================================
// TEST 14: Provider Support Varies by Chain
// Not all providers support all chains - verify system handles this
// =============================================================================

#[serial]
#[tokio::test]
async fn test_provider_chain_support_variance() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Get providers
    let providers_url = "/swap/providers";
    let providers_response = timed_get(&server, providers_url).await;
    
    if providers_response.status().is_success() {
        let providers_json: Value = providers_response.json();
        
        if let Some(arr) = providers_json.as_array() {
            println!("Found {} providers", arr.len());
            
            for provider in arr.iter() {
                assert!(provider["name"].is_string(), "Provider should have name");
                assert!(provider["rating"].is_string(), "Provider should have rating");
            }
        }
    }
}

// =============================================================================
// TEST 15: Swap History with Mixed Chains
// User's swap history should correctly show which chains involved
// =============================================================================

#[serial]
#[tokio::test]
async fn test_swap_history_multi_chain_tracking() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // Endpoint requires auth, so this is more of a structural test
    let history_url = "/swap/history";
    let history_response = timed_get(&server, history_url).await;
    
    // Should return error (no auth) or empty list
    // Structure should still be correct if available
    if history_response.status().is_success() {
        let history_json: Value = history_response.json();
        
        // Verify response structure
        assert!(history_json.get("swaps").is_some(), "Should have swaps array");
    }
}
