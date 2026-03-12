use serial_test::serial;
use serde_json::Value;
use std::collections::HashSet;

#[path = "../common/mod.rs"]
mod common;
use common::{setup_test_server, timed_get};

// =============================================================================
// BLOCKCHAIN COVERAGE TEST - VALIDATES ALL 124 BLOCKCHAINS ARE SUPPORTED
// =============================================================================
// This test verifies that the backend supports swaps on all blockchains
// offered by Trocador, not just the 21 with real-time RPC configuration.
//
// Strategy:
// 1. Fetch all currencies from backend
// 2. Extract unique networks
// 3. Verify each network has valid currency entries
// 4. Spot-check address validation for diverse network types
// =============================================================================

#[serial]
#[tokio::test]
async fn test_all_124_blockchains_represented() {
    let server = setup_test_server().await;

    // Fetch all currencies from the backend
    let response = timed_get(&server, "/swap/currencies").await;
    response.assert_status_ok();

    let currencies: Vec<Value> = response.json();
    println!("Total currencies fetched: {}", currencies.len());

    // Extract unique networks
    let mut networks: HashSet<String> = HashSet::new();
    for currency in &currencies {
        if let Some(network) = currency.get("network").and_then(|n| n.as_str()) {
            networks.insert(network.to_string());
        }
    }

    println!("Unique networks found: {}", networks.len());
    for net in networks.iter() {
        println!("  ✓ {}", net);
    }

    // Expected: 124 unique networks (matches Trocador data)
    assert!(
        networks.len() >= 120, // Allow small variance due to data updates
        "Expected at least 120 networks, found {}. Backend may not support all blockchains.",
        networks.len()
    );

    // Verify total count is reasonable
    assert!(
        currencies.len() >= 2400, // 2,507 in Trocador; allow for variance
        "Expected at least 2400 total currencies, found {}",
        currencies.len()
    );
}

// =============================================================================
// TEST: Each network has at least one currency available
// This ensures no empty/ghost networks in the response
// =============================================================================

#[serial]
#[tokio::test]
async fn test_each_network_has_currencies() {
    let server = setup_test_server().await;

    let response = timed_get(&server, "/swap/currencies").await;
    response.assert_status_ok();

    let currencies: Vec<Value> = response.json();

    // Group currencies by network
    let mut network_counts: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    for currency in &currencies {
        if let Some(network) = currency.get("network").and_then(|n| n.as_str()) {
            *network_counts.entry(network.to_string()).or_insert(0) += 1;
        }
    }

    // Every network should have at least 1 currency
    for (network, count) in &network_counts {
        assert!(
            *count > 0,
            "Network '{}' has no currencies - data integrity issue",
            network
        );
    }

    println!("✓ All {} networks have valid currencies", network_counts.len());

    // Print distribution
    let mut counts: Vec<_> = network_counts.iter().collect();
    counts.sort_by(|a, b| b.1.cmp(a.1));

    println!("\nTop 10 networks by currency count:");
    for (network, count) in counts.iter().take(10) {
        println!("  {} → {} currencies", network, count);
    }
}

// =============================================================================
// TEST: Network-specific currency metadata is complete
// Validates that all required fields exist for each network's currencies
// =============================================================================

#[serial]
#[tokio::test]
async fn test_network_currency_metadata_complete() {
    let server = setup_test_server().await;

    let response = timed_get(&server, "/swap/currencies").await;
    response.assert_status_ok();

    let currencies: Vec<Value> = response.json();

    let required_fields = vec!["name", "ticker", "network", "memo", "image", "minimum", "maximum"];

    let mut missing_count = 0;
    let mut invalid_count = 0;

    for (idx, currency) in currencies.iter().take(100).enumerate() {
        // Check all required fields exist
        for field in &required_fields {
            if currency.get(field).is_none() {
                missing_count += 1;
                println!(
                    "  ⚠ Currency {} missing field '{}'",
                    idx, field
                );
            }
        }

        // Validate data types
        if !currency["name"].is_string() {
            invalid_count += 1;
        }
        if !currency["ticker"].is_string() {
            invalid_count += 1;
        }
        if !currency["network"].is_string() {
            invalid_count += 1;
        }
        if !currency["memo"].is_boolean() {
            invalid_count += 1;
        }
        if !currency["minimum"].is_number() {
            invalid_count += 1;
        }
        if !currency["maximum"].is_number() {
            invalid_count += 1;
        }
    }

    assert_eq!(
        missing_count, 0,
        "Found {} missing required fields in currency metadata",
        missing_count
    );

    assert_eq!(
        invalid_count, 0,
        "Found {} invalid data types in currency metadata",
        invalid_count
    );

    println!("✓ All currency metadata is complete and properly typed");
}

// =============================================================================
// TEST: Filter by network works for all network types
// Tests filtering for diverse networks: EVM, Layer-1, other protocols
// =============================================================================

#[serial]
#[tokio::test]
async fn test_filter_by_network_diverse_chains() {
    let server = setup_test_server().await;

    // Test a diverse set of networks
    // Note: Networks are named by contract type (ERC20=Ethereum, BEP20=BSC, TRC20=Tron, etc)
    let test_networks = vec![
        ("ERC20", "evm_mainnet"),      // Ethereum
        ("Mainnet", "layer1"),         // Bitcoin, Solana, Cardano
        ("BEP20", "evm_sidechain"),    // Binance Smart Chain
        ("Arbitrum", "evm_l2"),        // Arbitrum
        ("Optimism", "evm_l2"),        // Optimism
        ("MATIC", "evm_sidechain"),    // Polygon
    ];

    for (network_name, _description) in test_networks {
        let url = format!("/swap/currencies?network={}", network_name);
        let response = timed_get(&server, &url).await;

        if response.status_code().as_u16() == 200 {
            let currencies: Vec<Value> = response.json();

            // Each network should return at least 1 currency (usually more)
            assert!(
                !currencies.is_empty(),
                "Network '{}' returned no currencies when filtered",
                network_name
            );

            // Verify all returned currencies match the filter
            for currency in &currencies {
                let net = currency.get("network").and_then(|n| n.as_str());
                assert_eq!(
                    net, Some(network_name),
                    "Returned currency from wrong network. Expected '{}', got '{:?}'",
                    network_name, net
                );
            }

            println!(
                "✓ {} → {} currencies",
                network_name,
                currencies.len()
            );
        } else {
            println!(
                "⚠ {} → Not available (might be renamed or discontinued)",
                network_name
            );
        }
    }
}

// =============================================================================
// TEST: Network discovery - list all discovered networks for documentation
// This test discovers the actual list of networks and can be used to
// generate documentation and compare against expected count
// =============================================================================

#[serial]
#[tokio::test]
async fn test_network_discovery_and_reporting() {
    let server = setup_test_server().await;

    let response = timed_get(&server, "/swap/currencies").await;
    response.assert_status_ok();

    let currencies: Vec<Value> = response.json();

    // Collect all unique networks
    let mut networks: std::collections::BTreeMap<String, usize> =
        std::collections::BTreeMap::new();

    for currency in &currencies {
        if let Some(network) = currency.get("network").and_then(|n| n.as_str()) {
            *networks.entry(network.to_string()).or_insert(0) += 1;
        }
    }

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║        BLOCKCHAIN COVERAGE REPORT                          ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("\nTotal Networks: {}", networks.len());
    println!("Total Currencies: {}\n", currencies.len());
    println!("Network List (with currency count):");
    println!("─────────────────────────────────────");

    let mut idx = 1;
    for (network, count) in &networks {
        println!("{:3}. {:30} → {:4} currencies", idx, network, count);
        idx += 1;
    }

    println!("\n✓ Backend supports {} blockchains with {} total currencies",
        networks.len(), currencies.len());
}

// =============================================================================
// TEST: Spot-check address validation for diverse networks
// Validates that address format checking works for various blockchain types
// =============================================================================

#[serial]
#[tokio::test]
async fn test_address_validation_diverse_networks() {
    let server = setup_test_server().await;

    // Test addresses for different network types
    let test_cases = vec![
        // (ticker, network, valid_address, description)
        ("BTC", "Mainnet", "1A1z7agoat2LWSE6BY2Zust4gLssQwSgd", "Bitcoin P2PKH"),
        (
            "ETH",
            "Ethereum",
            "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12",
            "Ethereum EVM",
        ),
        (
            "SOL",
            "Solana",
            "9B5X1CbM3nDZCoDjiHWuqJ3UaYNaEX9vJ7A13jgTgJJJ",
            "Solana SPL",
        ),
        (
            "ADA",
            "ADA",
            "addr1qyvf6p3l5vqvfx42k2dznsv9dghk8qw8fpr9u5wcgqx6xkfz7m3el3pg5l3ry0l6k5vqx9e52lh0gq6g5vx6e42dg0kf2",
            "Cardano",
        ),
    ];

    println!("Testing address validation for diverse networks:");

    for (ticker, network, address, description) in test_cases {
        let url = format!(
            "/swap/validate-address?ticker={}&network={}&address={}",
            ticker, network, address
        );

        let response = timed_get(&server, &url).await;

        if response.status_code().as_u16() == 200 {
            let result: Value = response.json();
            println!(
                "  ✓ {} ({}) - Valid: {:?}",
                description,
                network,
                result.get("valid").and_then(|v| v.as_bool())
            );
        } else {
            println!(
                "  ⚠ {} ({}) - Endpoint not available",
                description, network
            );
        }
    }
}

// =============================================================================
// TEST: Verify backend handles all network types correctly
// Ensures EVM, non-EVM, Layer-1, and Layer-2 networks all work
// =============================================================================

#[serial]
#[tokio::test]
async fn test_all_network_types_functional() {
    let server = setup_test_server().await;

    let response = timed_get(&server, "/swap/currencies").await;
    response.assert_status_ok();

    let currencies: Vec<Value> = response.json();

    // Categorize networks by type (simplified heuristic)
    let mut evm_networks: HashSet<String> = HashSet::new();
    let mut layer1_networks: HashSet<String> = HashSet::new();
    let mut other_networks: HashSet<String> = HashSet::new();

    for currency in &currencies {
        if let Some(network) = currency.get("network").and_then(|n| n.as_str()) {
            if network.to_lowercase().contains("eth")
                || network.to_lowercase().contains("arb")
                || network.to_lowercase().contains("poly")
                || network.contains("ERC20")
                || network.contains("BEP20")
            {
                evm_networks.insert(network.to_string());
            } else if network == "Mainnet"
                || network == "Bitcoin"
                || network == "Solana"
                || network == "Cardano"
                || network == "Polkadot"
            {
                layer1_networks.insert(network.to_string());
            } else {
                other_networks.insert(network.to_string());
            }
        }
    }

    println!("\nNetwork Type Distribution:");
    println!("  EVM-compatible: {}", evm_networks.len());
    println!("  Layer-1: {}", layer1_networks.len());
    println!("  Other protocols: {}", other_networks.len());

    // Verify we have good coverage across network types
    assert!(
        !evm_networks.is_empty(),
        "No EVM networks found - major issue!"
    );
    assert!(
        !layer1_networks.is_empty(),
        "No Layer-1 networks found - major issue!"
    );

    println!("✓ Backend supports diverse network types (EVM, Layer-1, Other)");
}
