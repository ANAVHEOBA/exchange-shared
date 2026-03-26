use super::config::{CircuitBreakerConfig, LoadBalancingStrategy, RpcConfig, RpcEndpoint};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
struct ChainMetadata {
    name: String,
    family: String,
    ankr_slug: String,
    alchemy_slug: String,
    infura_slug: String,
    public_rpc: String,
}

/// Build RPC configurations for all 119 chains from chains.json
pub fn build_default_rpc_configs() -> HashMap<String, RpcConfig> {
    let mut configs = HashMap::new();

    // Load chains.json
    let chains_json = include_str!("../../config/chains.json");
    let chains: Vec<ChainMetadata> =
        serde_json::from_str(chains_json).expect("Failed to parse chains.json");

    // Get API keys from environment
    let alchemy_key = std::env::var("ALCHEMY_API_KEY").ok();
    let infura_key = std::env::var("INFURA_API_KEY").ok();
    let ankr_id = std::env::var("ANKR_ID").unwrap_or_else(|_| {
        "255ef0129f301d346a2a784d9bef2bed6feb53f0584208e29751f1593d597662".to_string()
    });

    // Build configs for all chains
    for chain in chains {
        let chain_key = chain
            .name
            .to_lowercase()
            .replace(' ', "_")
            .replace("-", "_");

        let mut endpoints = vec![];
        let mut priority = 1;

        // Priority 1: Alchemy (if available)
        if !chain.alchemy_slug.is_empty() {
            if let Some(key) = &alchemy_key {
                endpoints.push(RpcEndpoint {
                    url: format!("https://{}.g.alchemy.com/v2/{}", chain.alchemy_slug, key),
                    priority,
                    weight: 100,
                    max_requests_per_second: Some(100),
                    timeout_ms: 5000,
                    auth: None,
                });
                priority += 1;
            }
        }

        // Priority 2: Ankr (if available)
        if !chain.ankr_slug.is_empty() {
            endpoints.push(RpcEndpoint {
                url: format!("https://rpc.ankr.com/{}/{}", chain.ankr_slug, ankr_id),
                priority,
                weight: 80,
                max_requests_per_second: Some(50),
                timeout_ms: 5000,
                auth: None,
            });
            priority += 1;
        }

        // Priority 3: Infura (if available)
        if !chain.infura_slug.is_empty() {
            if let Some(key) = &infura_key {
                endpoints.push(RpcEndpoint {
                    url: format!("https://{}.infura.io/v3/{}", chain.infura_slug, key),
                    priority,
                    weight: 80,
                    max_requests_per_second: Some(50),
                    timeout_ms: 5000,
                    auth: None,
                });
                priority += 1;
            }
        }

        // Priority 4: Public RPC (always available as fallback)
        if !chain.public_rpc.is_empty() {
            endpoints.push(RpcEndpoint {
                url: chain.public_rpc.clone(),
                priority,
                weight: 50,
                max_requests_per_second: Some(30),
                timeout_ms: 8000,
                auth: None,
            });
        }

        // Skip chains with no endpoints
        if endpoints.is_empty() {
            continue;
        }

        // Determine strategy based on chain family
        let strategy = match chain.family.as_str() {
            "evm" => LoadBalancingStrategy::HealthScoreBased,
            "btc" | "utxo" => LoadBalancingStrategy::RoundRobin,
            _ => LoadBalancingStrategy::HealthScoreBased,
        };

        // Adjust health check interval based on chain type
        let health_check_interval = match chain.family.as_str() {
            "btc" | "utxo" => 60, // Bitcoin-like chains are slower
            _ => 30,
        };

        configs.insert(
            chain_key,
            RpcConfig {
                chain: chain.name.clone(),
                family: chain.family.clone(),
                endpoints,
                strategy,
                health_check_interval,
                circuit_breaker_config: CircuitBreakerConfig::default(),
            },
        );
    }

    tracing::info!("🌐 Built RPC configs for {} chains", configs.len());

    configs
}
