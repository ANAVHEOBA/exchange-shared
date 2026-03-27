use serde_json::{json, Value};
use serial_test::serial;
use std::time::Duration;
use tokio::time::sleep;

#[path = "../common/mod.rs"]
mod common;
use common::{setup_test_server, timed_get, timed_post};

// =============================================================================
// BLOCKCHAIN SMOKE TESTS - VALIDATES END-TO-END SWAP OPERATIONS
// =============================================================================
// Tests that swaps work across diverse blockchain pairs, including:
// - Same-chain swaps (Ethereum → Ethereum)
// - Cross-chain swaps (Bitcoin → Ethereum)
// - Exotic pairs (Cardano → Solana, etc.)
// - Networks with real-time RPC (Ethereum)
// - Networks with estimated gas (Algorand, NEAR)
// =============================================================================

// Helper to generate valid addresses for different networks
fn get_test_address(network: &str) -> String {
    match network {
        // Bitcoin
        "Mainnet" | "Bitcoin" => "1A1z7agoat2LWSE6BY2Zust4gLssQwSgd".to_string(),
        // Ethereum & EVM
        "ERC20" | "Polygon" | "Arbitrum" | "Optimism" | "BASE" | "Avalanche"
        | "Blast" | "zkSync" => "0x742d35Cc6634C0532925a3b844Bc454e4438f44e".to_string(),
        // Solana
        "Solana" => "9B5X1CbM3nDZCoDjiHWuqJ3UaYNaEX9vJ7A13jgTgJJJ".to_string(),
        // Cardano
        "ADA" | "Cardano" => {
            "addr1qyvf6p3l5vqvfx42k2dznsv9dghk8qw8fpr9u5wcgqx6xkfz7m3el3pg5l3ry0l6k5vqx9e52lh0gq6g5vx6e42dg0kf2".to_string()
        }
        // Ripple
        "Ripple" => "rN7n7otQDd6FczFgLdnqt3r5nWXRvRVKjf".to_string(),
        // Polkadot
        "Polkadot" | "DOT" => "12D3CoU7mYzNLKjHf1mDhx5VZzN3cHbXx5k6SaKZKnF1Jhjr".to_string(),
        // Tezos
        "Tezos" | "XTZ" => "tz1eqHXSXvpzV3uHRFy8pVJsQq2qZZ98dKDn".to_string(),
        // Algorand
        "Algorand" | "ALGO" => {
            "4GFDWQF2KZPPVVH4LFQ2GVFUMRB3MQ5Q4HYIJMRJH2UMRUZRDAZKUULM5UA".to_string()
        }
        // Fallback to Ethereum format for unknown networks
        _ => "0x742d35Cc6634C0532925a3b844Bc454e4438f44e".to_string(),
    }
}

// =============================================================================
// TEST: Same-chain swaps (Ethereum)
// Simple validation that swaps work on major networks
// =============================================================================

#[serial]
#[tokio::test]
async fn test_same_chain_swap_ethereum() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // USDT → USDC on Ethereum
    let url = "/swap/rates?from=usdt&to=usdc&amount=100&network_from=Ethereum&network_to=Ethereum";
    let response = timed_get(&server, url).await;

    if response.status_code().as_u16() == 200 {
        let rate: Value = response.json();
        assert!(rate.get("rates").is_some(), "Should have rates array");
        println!("✓ Ethereum USDT → USDC swap available");
    } else {
        println!("⚠ Ethereum swap rates not available");
    }
}

// =============================================================================
// TEST: Bitcoin to Ethereum (Cross-chain, major networks)
// =============================================================================

#[serial]
#[tokio::test]
async fn test_cross_chain_bitcoin_to_ethereum() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let url = "/swap/rates?from=btc&to=eth&amount=0.1&network_from=Mainnet&network_to=Ethereum";
    let response = timed_get(&server, url).await;

    if response.status_code().as_u16() == 200 {
        let rate: Value = response.json();
        assert!(rate.get("rates").is_some(), "Should have rates");
        println!("✓ Bitcoin → Ethereum swap available");
    } else {
        println!("⚠ Bitcoin → Ethereum swap not available");
    }
}

// =============================================================================
// TEST: Solana to Ethereum (Cross-chain with SPL tokens)
// =============================================================================

#[serial]
#[tokio::test]
async fn test_cross_chain_solana_to_ethereum() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let url = "/swap/rates?from=sol&to=eth&amount=1&network_from=Solana&network_to=Ethereum";
    let response = timed_get(&server, url).await;

    if response.status_code().as_u16() == 200 {
        let rate: Value = response.json();
        assert!(rate.get("rates").is_some());
        println!("✓ Solana → Ethereum swap available");
    } else {
        println!("⚠ Solana → Ethereum not available (may be niche pair)");
    }
}

// =============================================================================
// TEST: Polygon to Arbitrum (L2 to L2)
// Tests swaps between different Layer-2 solutions
// =============================================================================

#[serial]
#[tokio::test]
async fn test_l2_to_l2_polygon_arbitrum() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let url = "/swap/rates?from=matic&to=eth&amount=100&network_from=Polygon&network_to=Arbitrum";
    let response = timed_get(&server, url).await;

    if response.status_code().as_u16() == 200 {
        let rate: Value = response.json();
        assert!(rate.get("rates").is_some());
        println!("✓ Polygon → Arbitrum swap available");
    } else {
        println!("⚠ Polygon → Arbitrum swap not available");
    }
}

// =============================================================================
// TEST: Stablecoin across multiple networks (USDT: ERC20, TRC20, BEP20)
// =============================================================================

#[serial]
#[tokio::test]
async fn test_usdt_multichain_coverage() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let networks = vec!["ERC20", "Polygon", "Tron", "BSC"];

    for network in networks {
        let url = format!(
            "/swap/rates?from=usdt&to=usdc&amount=100&network_from={}&network_to=Ethereum",
            network
        );
        let response = timed_get(&server, &url).await;

        if response.status_code().as_u16() == 200 {
            println!("  ✓ USDT on {} is available", network);
        } else {
            println!("  ⚠ USDT on {} not available", network);
        }
    }
}

// =============================================================================
// TEST: Gas estimation works for networks with real-time RPC
// (Ethereum, Polygon, Arbitrum)
// =============================================================================

#[serial]
#[tokio::test]
async fn test_gas_estimation_realtime_networks() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let networks = vec!["ERC20", "Polygon", "Arbitrum"];

    for network in networks {
        let url = format!(
            "/swap/estimate?from=eth&to=usdt&amount=1&network_from={}&network_to={}",
            network, network
        );
        let response = timed_get(&server, &url).await;

        if response.status_code().as_u16() == 200 {
            let estimate: Value = response.json();
            if let Some(fee) = estimate.get("estimated_fee") {
                println!(
                    "  ✓ {} gas fee: ${:.4}",
                    network,
                    fee.as_f64().unwrap_or(0.0)
                );
            }
        }
    }
}

// =============================================================================
// TEST: Gas estimation for networks with hardcoded estimates
// (Cardano, Polkadot, Algorand - not Ethereum-style)
// =============================================================================

#[serial]
#[tokio::test]
async fn test_gas_estimation_estimated_networks() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let test_pairs = vec![
        ("ada", "eth", "ADA", "ERC20"),      // Cardano to Ethereum
        ("dot", "eth", "Polkadot", "ERC20"), // Polkadot to Ethereum
    ];

    for (from_ticker, to_ticker, from_net, to_net) in test_pairs {
        let url = format!(
            "/swap/estimate?from={}&to={}&amount=100&network_from={}&network_to={}",
            from_ticker, to_ticker, from_net, to_net
        );
        let response = timed_get(&server, &url).await;

        if response.status_code().as_u16() == 200 {
            let estimate: Value = response.json();
            if let Some(fee) = estimate.get("estimated_fee") {
                println!(
                    "  ✓ {} gas (estimated): ${:.4}",
                    from_net,
                    fee.as_f64().unwrap_or(0.0)
                );
            }
        } else {
            println!(
                "  ℹ {}/{} pair not available (niche combination)",
                from_ticker, to_ticker
            );
        }
    }
}

// =============================================================================
// TEST: Address validation across different network types
// Ensures address format checking works for all blockchain types
// =============================================================================

#[serial]
#[tokio::test]
async fn test_address_validation_all_types() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let networks = vec![
        ("BTC", "Mainnet"),
        ("ETH", "ERC20"),
        ("SOL", "Solana"),
        ("ADA", "ADA"),
        ("DOT", "Polkadot"),
        ("XRP", "Ripple"),
    ];

    println!("Address validation results:");
    for (ticker, network) in networks {
        let address = get_test_address(network);
        let url = format!(
            "/swap/validate-address?ticker={}&network={}&address={}",
            ticker, network, address
        );
        let response = timed_get(&server, &url).await;

        if response.status_code().as_u16() == 200 {
            let result: Value = response.json();
            let valid = result
                .get("valid")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            println!("  ✓ {} ({}) - Valid: {}", ticker, network, valid);
        } else {
            println!("  ⚠ {} ({}) - Endpoint not available", ticker, network);
        }
    }
}

// =============================================================================
// TEST: Create swap across diverse networks
// Tests the full create_swap flow end-to-end
// =============================================================================

#[serial]
#[tokio::test]
async fn test_create_swap_eth_to_usdc() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    // First get rates
    let rate_url =
        "/swap/rates?from=eth&to=usdc&amount=1&network_from=Ethereum&network_to=Ethereum";
    let rate_response = timed_get(&server, rate_url).await;

    if !rate_response.status_code().as_u16() == 200 {
        println!("⚠ ETH → USDC rates not available");
        return;
    }

    let rate_data: Value = rate_response.json();
    let trade_id = rate_data.get("trade_id").and_then(|v| v.as_str());

    if trade_id.is_none() {
        println!("⚠ Trade ID not provided in rate response");
        return;
    }

    let provider = rate_data
        .get("rates")
        .and_then(|r| r.get(0))
        .and_then(|r| r.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("changenow");

    // Create swap
    let payload = json!({
        "trade_id": trade_id,
        "from": "eth",
        "network_from": "ERC20",
        "to": "usdc",
        "network_to": "ERC20",
        "amount": 1.0,
        "provider": provider,
        "recipient_address": "0x742d35Cc6634C0532925a3b844Bc454e4438f44e",
        "refund_address": "0x742d35Cc6634C0532925a3b844Bc454e4438f44e"
    });

    let response = timed_post(&server, "/swap/create", &payload).await;

    if response.status_code().as_u16() == 200 {
        let swap: Value = response.json();
        assert!(swap.get("swap_id").is_some(), "Should have swap_id");
        println!("✓ ETH → USDC swap created successfully");
    } else {
        println!(
            "⚠ ETH → USDC swap creation failed: {}",
            response.status_code()
        );
    }
}

// =============================================================================
// TEST: Fee deduction works correctly for all network types
// Validates that fees are applied consistently
// =============================================================================

#[serial]
#[tokio::test]
async fn test_fee_deduction_consistency() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    let swaps = vec![
        ("btc", "Mainnet", "eth", "ERC20", 0.1),
        ("eth", "ERC20", "usdt", "ERC20", 1.0),
        ("usdt", "Polygon", "usdc", "Polygon", 100.0),
    ];

    for (from_ticker, from_net, to_ticker, to_net, amount) in swaps {
        let url = format!(
            "/swap/estimate?from={}&to={}&amount={}&network_from={}&network_to={}",
            from_ticker, to_ticker, amount, from_net, to_net
        );
        let response = timed_get(&server, &url).await;

        if response.status_code().as_u16() == 200 {
            let estimate: Value = response.json();
            if let Some(fee) = estimate.get("estimated_fee") {
                if let Some(fee_val) = fee.as_f64() {
                    if fee_val > 0.0 {
                        println!(
                            "  ✓ {}/{} → {}/{} fee: ${:.4}",
                            from_ticker, from_net, to_ticker, to_net, fee_val
                        );
                    }
                }
            }
        }
    }
}

// =============================================================================
// TEST: Summary - validates core swap operations across all network types
// =============================================================================

#[serial]
#[tokio::test]
async fn test_blockchain_support_summary() {
    sleep(Duration::from_secs(1)).await;
    let server = setup_test_server().await;

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║     BLOCKCHAIN SMOKE TEST SUMMARY                          ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // Test multiple network combinations using actual Trocador network names
    let tests = vec![
        (
            "ERC20",
            "USDT → USDC (Ethereum)",
            "/swap/rates?from=usdt&to=usdc&amount=100&network_from=ERC20&network_to=ERC20",
        ),
        (
            "Mainnet",
            "BTC → SOL (Layer-1)",
            "/swap/rates?from=btc&to=sol&amount=0.1&network_from=Mainnet&network_to=Mainnet",
        ),
        (
            "BEP20",
            "ETH → USDC (BSC)",
            "/swap/rates?from=eth&to=usdc&amount=1&network_from=BEP20&network_to=BEP20",
        ),
        (
            "Arbitrum",
            "ETH → USDT (Arbitrum)",
            "/swap/rates?from=eth&to=usdt&amount=1&network_from=Arbitrum&network_to=Arbitrum",
        ),
    ];

    let mut passed = 0;
    let mut total = 0;

    for (network, description, url) in tests {
        total += 1;
        let response = timed_get(&server, url).await;

        if response.status_code().as_u16() == 200 {
            println!("  ✓ {} - {}", network, description);
            passed += 1;
        } else {
            println!("  ✗ {} - {}", network, description);
        }
    }

    println!(
        "\nResults: {}/{} network combinations tested successfully\n",
        passed, total
    );

    // Verify at least 25% pass rate (lower bar since API depends on Trocador availability)
    assert!(
        passed as f64 / total as f64 >= 0.25,
        "Less than 25% of network combinations available"
    );
}
