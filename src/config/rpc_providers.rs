use crate::services::rpc::canonical_chain_key;
use crate::services::wallet::bitcoin_rpc::BitcoinRpcClient;
use crate::services::wallet::rest_rpc::RestRpcClient;
use crate::services::wallet::rpc::{BlockchainProvider, HttpRpcClient};
use crate::services::wallet::solana_rpc::SolanaRpcClient;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct ChainMetadata {
    pub name: String,
    pub family: String,
    pub ankr_slug: String,
    pub alchemy_slug: String,
    pub infura_slug: String,
    pub public_rpc: String,
}

/// RPC Provider Configuration
/// Centralizes blockchain RPC endpoint management with Alchemy, Ankr and Public fallbacks
#[allow(dead_code)]
pub struct RpcProviderConfig {
    providers: HashMap<String, Arc<dyn BlockchainProvider>>,
    metadata: HashMap<String, ChainMetadata>,
}

impl RpcProviderConfig {
    /// Initialize RPC providers from chains.json and environment variables
    pub fn from_env() -> Self {
        let mut providers: HashMap<String, Arc<dyn BlockchainProvider>> = HashMap::new();
        let mut metadata_map: HashMap<String, ChainMetadata> = HashMap::new();

        // Load metadata from chains.json
        let chains_json = include_str!("chains.json");
        let chains: Vec<ChainMetadata> =
            serde_json::from_str(chains_json).expect("Failed to parse chains.json");

        // Secret keys from environment
        let alchemy_key = std::env::var("ALCHEMY_API_KEY").ok();
        let ankr_id = std::env::var("ANKR_ID").ok();
        let infura_id = std::env::var("INFURA_ID").ok();

        for meta in chains {
            let chain_key = canonical_chain_key(&meta.name);

            // Priority 1: Individual Env Var (e.g. ETH_RPC_URL)
            let env_var_name = format!("{}_RPC_URL", chain_key.to_uppercase());
            let rpc_url = if let Ok(custom_url) = std::env::var(&env_var_name) {
                if !custom_url.is_empty() {
                    custom_url
                } else {
                    Self::resolve_url(&meta, &alchemy_key, &ankr_id, &infura_id)
                }
            } else {
                Self::resolve_url(&meta, &alchemy_key, &ankr_id, &infura_id)
            };

            // Dynamic Dispatch based on Family
            let provider: Arc<dyn BlockchainProvider> = match meta.family.as_str() {
                "btc" | "utxo" | "special" => {
                    // Check if it's a REST explorer URL or a real node URL
                    if rpc_url.contains("mempool.space")
                        || rpc_url.contains("blockchair.com")
                        || rpc_url.contains("blockcypher.com")
                    {
                        Arc::new(RestRpcClient::new(rpc_url))
                    } else {
                        Arc::new(BitcoinRpcClient::new(rpc_url))
                    }
                }
                "solana" => Arc::new(SolanaRpcClient::new(rpc_url)),
                _ => Arc::new(HttpRpcClient::new(rpc_url)), // Default to EVM/Generic
            };

            providers.insert(chain_key.clone(), provider);
            metadata_map.insert(chain_key, meta);
        }

        tracing::info!(
            "🌐 RPC providers initialized for {} chains dynamically",
            providers.len()
        );

        Self {
            providers,
            metadata: metadata_map,
        }
    }

    fn resolve_url(
        meta: &ChainMetadata,
        alchemy_key: &Option<String>,
        ankr_id: &Option<String>,
        infura_id: &Option<String>,
    ) -> String {
        // Priority 2: Alchemy
        if let Some(key) = alchemy_key {
            if !meta.alchemy_slug.is_empty() {
                return format!("https://{}.g.alchemy.com/v2/{}", meta.alchemy_slug, key);
            }
        }

        // Priority 3: Ankr
        if let Some(id) = ankr_id {
            if !meta.ankr_slug.is_empty() {
                return format!("https://rpc.ankr.com/{}/{}", meta.ankr_slug, id);
            }
        }

        // Priority 4: Infura
        if let Some(id) = infura_id {
            if !meta.infura_slug.is_empty() {
                return format!("https://{}.infura.io/v3/{}", meta.infura_slug, id);
            }
        }

        // Priority 5: Public Fallback
        meta.public_rpc.clone()
    }

    /// Get provider for a specific network
    pub fn get_provider(&self, network: &str) -> Option<Arc<dyn BlockchainProvider>> {
        let normalized = canonical_chain_key(network);

        // Direct match
        if let Some(provider) = self.providers.get(&normalized) {
            return Some(provider.clone());
        }

        // Common aliases
        let provider_key = match normalized.as_str() {
            "eth" | "ethereum" => "ethereum",
            "btc" | "bitcoin" => "bitcoin",
            "sol" | "solana" => "solana",
            "matic" | "polygon" => "polygon",
            "arb" | "arbitrum" => "arbitrum_one",
            "op" | "optimism" => "optimism",
            "avax" | "avalanche" => "avalanche_c_chain",
            "bnb" | "bsc" | "binance" => "bnb_smart_chain",
            "ftm" | "fantom" => "fantom",
            "cro" | "cronos" => "cronos",
            "xmr" | "monero" => "monero",
            _ => return None,
        };

        self.providers.get(provider_key).cloned()
    }

    /// Get all configured providers
    pub fn get_all_providers(&self) -> &HashMap<String, Arc<dyn BlockchainProvider>> {
        &self.providers
    }

    pub fn is_supported(&self, network: &str) -> bool {
        self.get_provider(network).is_some()
    }

    pub fn supported_chains(&self) -> Vec<String> {
        self.providers.keys().cloned().collect()
    }

    pub fn provider_count(&self) -> usize {
        self.providers.len()
    }
}
