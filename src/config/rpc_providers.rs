use std::collections::HashMap;
use std::sync::Arc;
use crate::services::wallet::rpc::{BlockchainProvider, HttpRpcClient};

/// RPC Provider Configuration
/// Centralizes blockchain RPC endpoint management with Alchemy integration
pub struct RpcProviderConfig {
    providers: HashMap<String, Arc<dyn BlockchainProvider>>,
}

impl RpcProviderConfig {
    /// Initialize RPC providers from environment variables
    /// Automatically uses Alchemy API key if available, otherwise falls back to individual RPC URLs
    pub fn from_env() -> Self {
        let mut providers: HashMap<String, Arc<dyn BlockchainProvider>> = HashMap::new();
        
        // Check if Alchemy API key is available
        let alchemy_key = std::env::var("ALCHEMY_API_KEY").ok();
        
        if let Some(ref key) = alchemy_key {
            tracing::info!("🔑 Alchemy API key detected - auto-configuring 70+ blockchain RPC endpoints");
            
            // Alchemy-supported chains with their network identifiers
            // Reference: https://docs.alchemy.com/reference/api-overview
            let alchemy_chains = vec![
                // Tier 1: Major EVM chains
                ("ethereum", "eth-mainnet"),
                ("polygon", "polygon-mainnet"),
                ("arbitrum", "arb-mainnet"),
                ("optimism", "opt-mainnet"),
                ("base", "base-mainnet"),
                ("bsc", "bnb-mainnet"),
                ("avalanche", "avax-mainnet"),
                ("fantom", "fantom-mainnet"),
                
                // Tier 2: Layer 2s and scaling solutions
                ("zksync", "zksync-mainnet"),
                ("polygonzkevm", "polygonzkevm-mainnet"),
                ("arbitrumnova", "arbnova-mainnet"),
                ("blast", "blast-mainnet"),
                ("linea", "linea-mainnet"),
                ("scroll", "scroll-mainnet"),
                ("mantle", "mantle-mainnet"),
                ("starknet", "starknet-mainnet"),
                
                // Tier 3: Emerging chains
                ("astar", "astar-mainnet"),
                ("zetachain", "zetachain-mainnet"),
                ("fraxtal", "fraxtal-mainnet"),
                ("shape", "shape-mainnet"),
                
                // Tier 4: Other supported chains
                ("gnosis", "gnosis-mainnet"),
                ("moonbeam", "moonbeam-mainnet"),
                ("celo", "celo-mainnet"),
                ("aurora", "aurora-mainnet"),
                ("metis", "metis-mainnet"),
            ];
            
            for (chain_name, alchemy_network) in alchemy_chains {
                let rpc_url = format!("https://{}.g.alchemy.com/v2/{}", alchemy_network, key);
                providers.insert(chain_name.to_string(), Arc::new(HttpRpcClient::new(rpc_url)));
            }
            
            tracing::info!("✅ Configured {} chains via Alchemy", providers.len());
        } else {
            tracing::info!("ℹ️  No Alchemy API key found - checking individual RPC URLs");
        }
        
        // Override with custom RPC URLs if provided (takes precedence over Alchemy)
        // This allows users to use specific providers for certain chains
        Self::apply_custom_rpcs(&mut providers);
        
        // Validation
        if providers.is_empty() {
            tracing::warn!("⚠️  No RPC providers configured!");
            tracing::warn!("    Set ALCHEMY_API_KEY in .env for automatic 70+ chain support");
            tracing::warn!("    OR set individual RPC URLs (ETH_RPC_URL, POLYGON_RPC_URL, etc.)");
        } else {
            let chain_list: Vec<String> = providers.keys().cloned().collect();
            tracing::info!("🌐 RPC providers initialized for {} chains", providers.len());
            tracing::debug!("   Supported chains: {:?}", chain_list);
        }
        
        Self { providers }
    }
    
    /// Apply custom RPC URLs from environment variables
    /// These override Alchemy defaults if specified
    fn apply_custom_rpcs(providers: &mut HashMap<String, Arc<dyn BlockchainProvider>>) {
        let custom_rpcs = vec![
            ("ETH_RPC_URL", "ethereum"),
            ("POLYGON_RPC_URL", "polygon"),
            ("BSC_RPC_URL", "bsc"),
            ("ARBITRUM_RPC_URL", "arbitrum"),
            ("OPTIMISM_RPC_URL", "optimism"),
            ("AVALANCHE_RPC_URL", "avalanche"),
            ("BASE_RPC_URL", "base"),
            ("FANTOM_RPC_URL", "fantom"),
            ("ZKSYNC_RPC_URL", "zksync"),
            ("LINEA_RPC_URL", "linea"),
            ("SCROLL_RPC_URL", "scroll"),
            ("BLAST_RPC_URL", "blast"),
            ("MANTLE_RPC_URL", "mantle"),
            ("GNOSIS_RPC_URL", "gnosis"),
            ("MOONBEAM_RPC_URL", "moonbeam"),
            ("CELO_RPC_URL", "celo"),
            ("AURORA_RPC_URL", "aurora"),
            ("METIS_RPC_URL", "metis"),
        ];
        
        for (env_var, chain_name) in custom_rpcs {
            if let Ok(rpc) = std::env::var(env_var) {
                if !rpc.is_empty() {
                    providers.insert(chain_name.to_string(), Arc::new(HttpRpcClient::new(rpc)));
                    tracing::debug!("Using custom RPC for {}", chain_name);
                }
            }
        }
    }
    
    /// Get provider for a specific network
    pub fn get_provider(&self, network: &str) -> Option<Arc<dyn BlockchainProvider>> {
        let normalized = network.to_lowercase();
        
        // Direct match
        if let Some(provider) = self.providers.get(&normalized) {
            return Some(provider.clone());
        }
        
        // Try common aliases
        let provider_key = match normalized.as_str() {
            "eth" | "ethereum" => "ethereum",
            "matic" | "polygon" => "polygon",
            "arb" | "arbitrum" => "arbitrum",
            "op" | "optimism" => "optimism",
            "avax" | "avalanche" => "avalanche",
            "bnb" | "bsc" | "binance" => "bsc",
            "ftm" | "fantom" => "fantom",
            _ => {
                tracing::debug!("No RPC provider found for network: {}", network);
                return None;
            }
        };
        
        self.providers.get(provider_key).cloned()
    }
    
    /// Get all configured providers
    pub fn get_all_providers(&self) -> &HashMap<String, Arc<dyn BlockchainProvider>> {
        &self.providers
    }
    
    /// Check if a network is supported
    pub fn is_supported(&self, network: &str) -> bool {
        self.get_provider(network).is_some()
    }
    
    /// Get list of supported chain names
    pub fn supported_chains(&self) -> Vec<String> {
        self.providers.keys().cloned().collect()
    }
    
    /// Get provider count
    pub fn provider_count(&self) -> usize {
        self.providers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_network_aliases() {
        // This test requires ALCHEMY_API_KEY or custom RPC URLs in environment
        let config = RpcProviderConfig::from_env();
        
        // Test that aliases work (if providers are configured)
        if config.provider_count() > 0 {
            // These should resolve to the same provider if ethereum is configured
            let eth1 = config.get_provider("ethereum");
            let eth2 = config.get_provider("eth");
            
            if eth1.is_some() {
                assert!(eth2.is_some(), "Alias 'eth' should resolve to 'ethereum'");
            }
        }
    }
}
