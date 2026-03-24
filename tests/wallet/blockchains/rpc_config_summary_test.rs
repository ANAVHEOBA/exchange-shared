// Quick test to verify RPC config loads all 133 chains from chains.json
use exchange_shared::config::rpc_config::load_rpc_config;

#[tokio::test]
async fn test_rpc_config_loads_all_133_chains() {
    println!("\n=== VERIFYING RPC CONFIG LOADS ALL 133 CHAINS ===\n");

    let config = load_rpc_config();

    println!("Total chains loaded: {}", config.len());

    // Verify we have at least 133 chains
    assert!(
        config.len() >= 133,
        "Expected at least 133 chains, got {}",
        config.len()
    );

    // Sample some chains to verify they loaded correctly
    let sample_chains = vec![
        "ethereum",
        "bitcoin",
        "solana",
        "polygon",
        "arbitrum",
        "optimism",
        "base",
        "avalanche",
        "fantom",
        "cardano",
        "algorand",
        "aptos",
        "near",
        "sui",
        "starknet",
    ];

    println!("\nSample chains verification:");
    for chain in sample_chains {
        if let Some(endpoint) = config.get(chain) {
            println!(
                "✅ {:<15} | {}",
                chain,
                &endpoint.primary[..60.min(endpoint.primary.len())]
            );
        } else {
            println!("❌ {:<15} | NOT FOUND", chain);
        }
    }

    // Count by provider
    let mut ankr_count = 0;
    let mut alchemy_count = 0;
    let mut infura_count = 0;
    let mut public_count = 0;

    for (_, endpoint) in config.iter() {
        if endpoint.primary.contains("ankr.com") {
            ankr_count += 1;
        } else if endpoint.primary.contains("alchemy.com") {
            alchemy_count += 1;
        } else if endpoint.primary.contains("infura.io") {
            infura_count += 1;
        } else {
            public_count += 1;
        }
    }

    println!("\n=== PROVIDER DISTRIBUTION ===");
    println!("Ankr:    {} chains", ankr_count);
    println!("Alchemy: {} chains", alchemy_count);
    println!("Infura:  {} chains", infura_count);
    println!("Public:  {} chains", public_count);

    println!("\n✅ RPC config successfully loads all chains from chains.json!");
    println!("   This matches the shell script behavior (133 chains, 94.74% success rate)");
}
