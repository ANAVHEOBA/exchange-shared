// =============================================================================
// REAL RPC CONNECTIVITY TEST FOR ALL 133 BLOCKCHAINS
// Tests actual RPC connectivity using Ankr, Alchemy, Infura, and public endpoints
// =============================================================================

use exchange_shared::config::rpc_config::{get_rpc_config, load_rpc_config};
use exchange_shared::services::wallet::rpc::{BlockchainProvider, HttpRpcClient};
use std::sync::Arc;

#[tokio::test]
async fn test_all_133_blockchains_real_rpc_connectivity() {
    println!("\n=== TESTING REAL RPC CONNECTIVITY FOR 133 BLOCKCHAINS ===\n");

    let config = load_rpc_config();

    let mut total = 0;
    let mut connected = 0;
    let mut failed = 0;
    let mut provider_stats = std::collections::HashMap::new();

    for (chain_name, endpoint) in config.iter() {
        total += 1;

        let client = Arc::new(HttpRpcClient::new(endpoint.primary.clone()));

        // Determine provider type
        let provider_type = if endpoint.primary.contains("ankr.com") {
            "Ankr"
        } else if endpoint.primary.contains("alchemy.com") {
            "Alchemy"
        } else if endpoint.primary.contains("infura.io") {
            "Infura"
        } else {
            "Public"
        };

        // Try a simple RPC call based on protocol
        let result = match endpoint.protocol {
            exchange_shared::config::rpc_config::BlockchainProtocol::EVM => {
                client.get_gas_price().await.map(|_| 0)
            }
            exchange_shared::config::rpc_config::BlockchainProtocol::Bitcoin => {
                client.get_balance("test").await.map(|_| 0)
            }
            exchange_shared::config::rpc_config::BlockchainProtocol::Solana => {
                client.get_recent_blockhash().await.map(|_| 0)
            }
            _ => client.get_balance("test").await.map(|_| 0),
        };

        match result {
            Ok(_) => {
                connected += 1;
                *provider_stats.entry(provider_type).or_insert(0) += 1;
                println!(
                    "✅ {:<25} | {:<10} | {}",
                    chain_name,
                    provider_type,
                    &endpoint.primary[..60.min(endpoint.primary.len())]
                );
            }
            Err(e) => {
                failed += 1;
                println!(
                    "❌ {:<25} | {:<10} | Error: {}",
                    chain_name,
                    provider_type,
                    e.to_string().chars().take(50).collect::<String>()
                );
            }
        }
    }

    println!("\n=== RPC CONNECTIVITY SUMMARY ===");
    println!("Total Blockchains: {}", total);
    println!("✅ Connected: {}", connected);
    println!("❌ Failed: {}", failed);
    println!(
        "📈 Success Rate: {:.1}%",
        (connected as f64 / total as f64) * 100.0
    );

    println!("\n=== PROVIDER DISTRIBUTION ===");
    for (provider, count) in provider_stats.iter() {
        println!(
            "{:<10}: {} chains ({:.1}%)",
            provider,
            count,
            (*count as f64 / connected as f64) * 100.0
        );
    }

    println!("\n✅ Real RPC connectivity test complete!");
    println!("   This test uses actual blockchain RPC endpoints");
    println!("   Ankr: {}", provider_stats.get("Ankr").unwrap_or(&0));
    println!(
        "   Alchemy: {}",
        provider_stats.get("Alchemy").unwrap_or(&0)
    );
    println!("   Infura: {}", provider_stats.get("Infura").unwrap_or(&0));
    println!("   Public: {}", provider_stats.get("Public").unwrap_or(&0));
}

#[tokio::test]
async fn test_evm_chains_gas_price_fetch() {
    println!("\n=== TESTING EVM CHAINS GAS PRICE (REAL RPC) ===\n");

    let config = load_rpc_config();
    let evm_chains: Vec<_> = config
        .iter()
        .filter(|(_, endpoint)| {
            matches!(
                endpoint.protocol,
                exchange_shared::config::rpc_config::BlockchainProtocol::EVM
            )
        })
        .collect();

    let mut success = 0;
    let mut failed = 0;

    for (chain_name, endpoint) in evm_chains.iter() {
        let client = HttpRpcClient::new(endpoint.primary.clone());

        match client.get_gas_price().await {
            Ok(gas_price) => {
                success += 1;
                let gwei = gas_price as f64 / 1_000_000_000.0;
                println!("✅ {:<25} | Gas: {:.2} gwei", chain_name, gwei);
            }
            Err(e) => {
                failed += 1;
                println!("❌ {:<25} | Error: {}", chain_name, e);
            }
        }
    }

    println!("\n=== EVM GAS PRICE SUMMARY ===");
    println!("Total EVM Chains: {}", evm_chains.len());
    println!("✅ Success: {}", success);
    println!("❌ Failed: {}", failed);
    println!(
        "📈 Success Rate: {:.1}%",
        (success as f64 / evm_chains.len() as f64) * 100.0
    );
}

#[tokio::test]
async fn test_fallback_rpc_endpoints() {
    println!("\n=== TESTING FALLBACK RPC ENDPOINTS ===\n");

    let chains_with_fallbacks = vec!["ethereum", "polygon", "arbitrum", "optimism", "base"];

    for chain_name in chains_with_fallbacks {
        if let Some(config) = get_rpc_config(chain_name) {
            println!(
                "\n{} ({})",
                chain_name.to_uppercase(),
                config.fallbacks.len()
            );
            println!(
                "  Primary: {}",
                &config.primary[..60.min(config.primary.len())]
            );

            for (i, fallback) in config.fallbacks.iter().enumerate() {
                let client = HttpRpcClient::new(fallback.clone());
                match client.get_gas_price().await {
                    Ok(_) => println!(
                        "  ✅ Fallback #{}: {}",
                        i + 1,
                        &fallback[..60.min(fallback.len())]
                    ),
                    Err(_) => println!(
                        "  ❌ Fallback #{}: {}",
                        i + 1,
                        &fallback[..60.min(fallback.len())]
                    ),
                }
            }
        }
    }

    println!("\n✅ Fallback endpoint test complete!");
}
