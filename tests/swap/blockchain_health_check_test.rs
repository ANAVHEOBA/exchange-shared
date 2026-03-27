use serde_json::Value;
use serial_test::serial;

#[path = "../common/mod.rs"]
mod common;
use common::{setup_test_server, timed_get};

// =============================================================================
// BLOCKCHAIN HEALTH CHECK TEST
// =============================================================================
// Ultra-fast deployment probe that validates:
// - Core infrastructure is working
// - Top 5 networks are accessible
// - Basic swap operations work
//
// Target runtime: <30 seconds
// Purpose: Run on every deploy to catch catastrophic failures early
// =============================================================================

#[serial]
#[tokio::test]
async fn test_blockchain_health_check_deployment() {
    let server = setup_test_server().await;

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║        BLOCKCHAIN HEALTH CHECK (Pre-Deploy)               ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // Test 1: Can fetch currency list
    println!("1. Checking currency list endpoint...");
    let response = timed_get(&server, "/swap/currencies").await;
    assert!(
        response.status_code().as_u16() == 200,
        "Currency endpoint is down!"
    );
    let currencies: Vec<Value> = response.json();
    assert!(!currencies.is_empty(), "No currencies returned!");
    println!("   ✓ Currency list OK ({} currencies)", currencies.len());

    // Test 2: Major networks exist (using actual Trocador network names)
    println!("2. Checking major networks...");
    let mut networks = std::collections::HashSet::new();
    for currency in &currencies {
        if let Some(net) = currency.get("network").and_then(|n| n.as_str()) {
            networks.insert(net.to_string());
        }
    }

    let critical_networks = vec!["ERC20", "Mainnet", "BEP20", "Arbitrum", "SOL"];
    for net in critical_networks {
        let found = networks.iter().any(|n| n == net || n.contains(net));
        assert!(found, "Critical network '{}' not found!", net);
        println!("   ✓ {} available", net);
    }

    // Test 3: ERC20 swap rates work
    println!("3. Testing ERC20 swap rates...");
    let url = "/swap/rates?from=eth&to=usdt&amount=1&network_from=ERC20&network_to=ERC20";
    let response = timed_get(&server, url).await;
    assert!(
        response.status_code().as_u16() == 200,
        "ERC20 swap rates endpoint failed!"
    );
    let rates: Value = response.json();
    assert!(
        rates.get("rates").is_some(),
        "No rates returned for ERC20 swap!"
    );
    println!("   ✓ ERC20 swap rates OK");

    // Test 4: Mainnet swap rates work (Bitcoin, Cardano, etc)
    println!("4. Testing Mainnet swap rates...");
    let url = "/swap/rates?from=btc&to=ada&amount=0.1&network_from=Mainnet&network_to=Mainnet";
    let response = timed_get(&server, url).await;
    if response.status_code().as_u16() == 200 {
        println!("   ✓ Mainnet swap rates OK");
    } else {
        println!("   ⚠ Mainnet rates unavailable (non-critical)");
    }

    // Test 5: Address validation works
    println!("5. Testing address validation...");
    let url = "/swap/validate-address?ticker=eth&network=ERC20&address=0x742d35Cc6634C0532925a3b844Bc454e4438f44e";
    let response = timed_get(&server, url).await;
    if response.status_code().as_u16() == 200 {
        println!("   ✓ Address validation OK");
    } else {
        println!("   ⚠ Address validation unavailable (non-critical)");
    }

    println!("\n✅ Health check passed! Backend is ready for deployment.\n");
}

// =============================================================================
// Core functionality quick test - verify swap creation works
// =============================================================================

#[serial]
#[tokio::test]
async fn test_blockchain_health_check_swap_creation() {
    let server = setup_test_server().await;

    println!("Testing core swap creation...");

    // Just verify the endpoint responds, don't require successful swap
    let payload = serde_json::json!({
        "from": "eth",
        "to": "usdt",
        "network_from": "Ethereum",
        "network_to": "Ethereum",
        "amount": 1.0,
        "provider": "changenow",
        "recipient_address": "0x742d35Cc6634C0532925a3b844Bc454e4438f44e",
        "refund_address": "0x742d35Cc6634C0532925a3b844Bc454e4438f44e"
    });

    // Note: This will likely fail due to missing trade_id, but endpoint should respond
    let response = common::timed_post(&server, "/swap/create", &payload).await;

    // Just verify endpoint is not returning 500
    if response.status_code().as_u16() >= 500 {
        panic!(
            "Swap creation endpoint returned 500 error: {}",
            response.text()
        );
    }

    println!("✓ Swap creation endpoint is responsive");
}
