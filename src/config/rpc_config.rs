use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// RPC endpoint configuration for a blockchain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcEndpoint {
    /// Primary RPC endpoint (Alchemy/QuickNode/Infura)
    pub primary: String,
    
    /// Fallback RPC endpoints (free public endpoints)
    pub fallbacks: Vec<String>,
    
    /// Timeout for RPC calls
    pub timeout: Duration,
    
    /// Maximum retries on failure
    pub max_retries: u32,
    
    /// Blockchain protocol type
    pub protocol: BlockchainProtocol,
    
    /// Chain ID (for EVM chains)
    pub chain_id: Option<String>,
}

/// Blockchain protocol types
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum BlockchainProtocol {
    /// Ethereum-compatible chains (JSON-RPC over HTTP)
    EVM,
    
    /// Solana (JSON-RPC but different library)
    Solana,
    
    /// Bitcoin (REST API or other)
    Bitcoin,
    
    /// Polkadot (WebSocket JSON-RPC)
    Polkadot,
    
    /// Cardano (REST API via Blockfrost)
    Cardano,
    
    /// XRP Ledger (JSON-RPC)
    Ripple,
    
    /// Tezos (REST API)
    Tezos,
}

/// Load RPC configuration from environment variables
pub fn load_rpc_config() -> HashMap<String, RpcEndpoint> {
    let mut config = HashMap::new();
    
    // Get Alchemy API key from environment (optional)
    let alchemy_key = std::env::var("ALCHEMY_API_KEY").ok();
    
    // Helper function to build Alchemy URL
    let alchemy_url = |network: &str| -> Option<String> {
        alchemy_key.as_ref().map(|key| {
            format!("https://{}.g.alchemy.com/v2/{}", network, key)
        })
    };
    
    // Helper function to build fallback list with Alchemy as backup
    let build_fallbacks = |network: &str, env_fallback_1: &str, env_fallback_2: &str, default_1: &str, default_2: &str| -> Vec<String> {
        let mut fallbacks = vec![
            std::env::var(env_fallback_1).unwrap_or(default_1.to_string()),
            std::env::var(env_fallback_2).unwrap_or(default_2.to_string()),
        ];
        
        // Add Alchemy as final fallback if available and not already primary
        if let Some(alchemy) = alchemy_url(network) {
            if !fallbacks.contains(&alchemy) {
                fallbacks.push(alchemy);
            }
        }
        
        fallbacks
    };
    
    // ═══════════════════════════════════════════════════════════════════════
    // TIER 1: MAJOR EVM CHAINS
    // ═══════════════════════════════════════════════════════════════════════
    
    config.insert(
        "ethereum".to_string(),
        RpcEndpoint {
            primary: std::env::var("ETH_PRIMARY_RPC")
                .or_else(|_| alchemy_url("eth-mainnet").ok_or(""))
                .unwrap_or("https://eth.llamarpc.com".to_string()),
            fallbacks: build_fallbacks(
                "eth-mainnet",
                "ETH_FALLBACK_1_RPC",
                "ETH_FALLBACK_2_RPC",
                "https://eth-pokt.nodies.app",
                "https://rpc.ankr.com/eth"
            ),
            timeout: Duration::from_secs(10),
            max_retries: 3,
            protocol: BlockchainProtocol::EVM,
            chain_id: Some("0x1".to_string()),
        },
    );
    
    config.insert(
        "polygon".to_string(),
        RpcEndpoint {
            primary: std::env::var("POLYGON_PRIMARY_RPC")
                .or_else(|_| alchemy_url("polygon-mainnet").ok_or(""))
                .unwrap_or("https://polygon-rpc.com".to_string()),
            fallbacks: build_fallbacks(
                "polygon-mainnet",
                "POLYGON_FALLBACK_1_RPC",
                "POLYGON_FALLBACK_2_RPC",
                "https://polygon-pokt.nodies.app",
                "https://rpc.ankr.com/polygon"
            ),
            timeout: Duration::from_secs(10),
            max_retries: 3,
            protocol: BlockchainProtocol::EVM,
            chain_id: Some("0x89".to_string()),
        },
    );
    
    config.insert(
        "bsc".to_string(),
        RpcEndpoint {
            primary: std::env::var("BSC_PRIMARY_RPC")
                .or_else(|_| alchemy_url("bnb-mainnet").ok_or(""))
                .unwrap_or("https://bsc-dataseed.binance.org".to_string()),
            fallbacks: build_fallbacks(
                "bnb-mainnet",
                "BSC_FALLBACK_1_RPC",
                "BSC_FALLBACK_2_RPC",
                "https://bsc-pokt.nodies.app",
                "https://rpc.ankr.com/bsc"
            ),
            timeout: Duration::from_secs(10),
            max_retries: 3,
            protocol: BlockchainProtocol::EVM,
            chain_id: Some("0x38".to_string()),
        },
    );
    
    config.insert(
        "arbitrum".to_string(),
        RpcEndpoint {
            primary: std::env::var("ARBITRUM_PRIMARY_RPC")
                .or_else(|_| alchemy_url("arb-mainnet").ok_or(""))
                .unwrap_or("https://arb1.arbitrum.io/rpc".to_string()),
            fallbacks: build_fallbacks(
                "arb-mainnet",
                "ARBITRUM_FALLBACK_1_RPC",
                "ARBITRUM_FALLBACK_2_RPC",
                "https://arbitrum-pokt.nodies.app",
                "https://rpc.ankr.com/arbitrum"
            ),
            timeout: Duration::from_secs(10),
            max_retries: 3,
            protocol: BlockchainProtocol::EVM,
            chain_id: Some("0xa4b1".to_string()),
        },
    );
    
    config.insert(
        "optimism".to_string(),
        RpcEndpoint {
            primary: std::env::var("OPTIMISM_PRIMARY_RPC")
                .or_else(|_| alchemy_url("opt-mainnet").ok_or(""))
                .unwrap_or("https://mainnet.optimism.io".to_string()),
            fallbacks: build_fallbacks(
                "opt-mainnet",
                "OPTIMISM_FALLBACK_1_RPC",
                "OPTIMISM_FALLBACK_2_RPC",
                "https://optimism-pokt.nodies.app",
                "https://rpc.ankr.com/optimism"
            ),
            timeout: Duration::from_secs(10),
            max_retries: 3,
            protocol: BlockchainProtocol::EVM,
            chain_id: Some("0xa".to_string()),
        },
    );
    
    config.insert(
        "avalanche".to_string(),
        RpcEndpoint {
            primary: std::env::var("AVALANCHE_PRIMARY_RPC")
                .or_else(|_| alchemy_url("avax-mainnet").ok_or(""))
                .unwrap_or("https://api.avax.network/ext/bc/C/rpc".to_string()),
            fallbacks: build_fallbacks(
                "avax-mainnet",
                "AVALANCHE_FALLBACK_1_RPC",
                "AVALANCHE_FALLBACK_2_RPC",
                "https://avax-pokt.nodies.app",
                "https://rpc.ankr.com/avalanche"
            ),
            timeout: Duration::from_secs(10),
            max_retries: 3,
            protocol: BlockchainProtocol::EVM,
            chain_id: Some("0xa86a".to_string()),
        },
    );
    
    config.insert(
        "fantom".to_string(),
        RpcEndpoint {
            primary: std::env::var("FANTOM_PRIMARY_RPC")
                .or_else(|_| alchemy_url("fantom-mainnet").ok_or(""))
                .unwrap_or("https://rpc.ftm.tools".to_string()),
            fallbacks: build_fallbacks(
                "fantom-mainnet",
                "FANTOM_FALLBACK_1_RPC",
                "FANTOM_FALLBACK_2_RPC",
                "https://fantom-pokt.nodies.app",
                "https://rpc.ankr.com/fantom"
            ),
            timeout: Duration::from_secs(10),
            max_retries: 3,
            protocol: BlockchainProtocol::EVM,
            chain_id: Some("0xfa".to_string()),
        },
    );
    
    // ═══════════════════════════════════════════════════════════════════════
    // TIER 2: LAYER 2 & NEW CHAINS
    // ═══════════════════════════════════════════════════════════════════════
    
    config.insert(
        "base".to_string(),
        RpcEndpoint {
            primary: std::env::var("BASE_PRIMARY_RPC")
                .or_else(|_| alchemy_url("base-mainnet").ok_or(""))
                .unwrap_or("https://mainnet.base.org".to_string()),
            fallbacks: build_fallbacks(
                "base-mainnet",
                "BASE_FALLBACK_1_RPC",
                "BASE_FALLBACK_2_RPC",
                "https://base-pokt.nodies.app",
                "https://base-mainnet.blockpi.network/v1/rpc/public"
            ),
            timeout: Duration::from_secs(10),
            max_retries: 3,
            protocol: BlockchainProtocol::EVM,
            chain_id: Some("0x2105".to_string()),
        },
    );
    
    config.insert(
        "linea".to_string(),
        RpcEndpoint {
            primary: std::env::var("LINEA_PRIMARY_RPC")
                .unwrap_or("https://rpc.linea.build".to_string()),
            fallbacks: vec![
                std::env::var("LINEA_FALLBACK_1_RPC")
                    .unwrap_or("https://linea-mainnet.blockpi.network/v1/rpc/public".to_string()),
            ],
            timeout: Duration::from_secs(10),
            max_retries: 3,
            protocol: BlockchainProtocol::EVM,
            chain_id: Some("0xe708".to_string()),
        },
    );
    
    config.insert(
        "scroll".to_string(),
        RpcEndpoint {
            primary: std::env::var("SCROLL_PRIMARY_RPC")
                .unwrap_or("https://rpc.scroll.io".to_string()),
            fallbacks: vec![
                std::env::var("SCROLL_FALLBACK_1_RPC")
                    .unwrap_or("https://scroll-mainnet.blockpi.network/v1/rpc/public".to_string()),
            ],
            timeout: Duration::from_secs(10),
            max_retries: 3,
            protocol: BlockchainProtocol::EVM,
            chain_id: Some("0x82f".to_string()),
        },
    );
    
    config.insert(
        "mantle".to_string(),
        RpcEndpoint {
            primary: std::env::var("MANTLE_PRIMARY_RPC")
                .unwrap_or("https://rpc.mantle.xyz".to_string()),
            fallbacks: vec![
                std::env::var("MANTLE_FALLBACK_1_RPC")
                    .unwrap_or("https://mantle-mainnet.blockpi.network/v1/rpc/public".to_string()),
            ],
            timeout: Duration::from_secs(10),
            max_retries: 3,
            protocol: BlockchainProtocol::EVM,
            chain_id: Some("0x1f0a".to_string()),
        },
    );
    
    config.insert(
        "blast".to_string(),
        RpcEndpoint {
            primary: std::env::var("BLAST_PRIMARY_RPC")
                .unwrap_or("https://rpc.blast.io".to_string()),
            fallbacks: vec![
                std::env::var("BLAST_FALLBACK_1_RPC")
                    .unwrap_or("https://blast-rpc.blockpi.network/v1/rpc/public".to_string()),
            ],
            timeout: Duration::from_secs(10),
            max_retries: 3,
            protocol: BlockchainProtocol::EVM,
            chain_id: Some("0x81457".to_string()),
        },
    );
    
    config.insert(
        "zksync".to_string(),
        RpcEndpoint {
            primary: std::env::var("ZKSYNC_PRIMARY_RPC")
                .or_else(|_| alchemy_url("zksync-mainnet").ok_or(""))
                .unwrap_or("https://mainnet.era.zksync.io".to_string()),
            fallbacks: build_fallbacks(
                "zksync-mainnet",
                "ZKSYNC_FALLBACK_1_RPC",
                "ZKSYNC_FALLBACK_2_RPC",
                "https://zksync-era-mainnet.blockpi.network/v1/rpc/public",
                "https://zksync-era.blockpi.network/v1/rpc/public"
            ),
            timeout: Duration::from_secs(10),
            max_retries: 3,
            protocol: BlockchainProtocol::EVM,
            chain_id: Some("0x144".to_string()),
        },
    );
    
    // ═══════════════════════════════════════════════════════════════════════
    // TIER 3: OTHER EVM CHAINS
    // ═══════════════════════════════════════════════════════════════════════
    
    config.insert(
        "gnosis".to_string(),
        RpcEndpoint {
            primary: std::env::var("GNOSIS_PRIMARY_RPC")
                .unwrap_or("https://rpc.gnosischain.com".to_string()),
            fallbacks: vec![std::env::var("GNOSIS_FALLBACK_1_RPC")
                .unwrap_or("https://rpc.ankr.com/gnosis".to_string())],
            timeout: Duration::from_secs(10),
            max_retries: 3,
            protocol: BlockchainProtocol::EVM,
            chain_id: Some("0x64".to_string()),
        },
    );
    
    config.insert(
        "cronos".to_string(),
        RpcEndpoint {
            primary: std::env::var("CRONOS_PRIMARY_RPC")
                .unwrap_or("https://evm.cronos.org".to_string()),
            fallbacks: vec![std::env::var("CRONOS_FALLBACK_1_RPC")
                .unwrap_or("https://rpc.vvs.finance".to_string())],
            timeout: Duration::from_secs(10),
            max_retries: 3,
            protocol: BlockchainProtocol::EVM,
            chain_id: Some("0x19".to_string()),
        },
    );
    
    // ═══════════════════════════════════════════════════════════════════════
    // TIER 4: NON-EVM CHAINS
    // ═══════════════════════════════════════════════════════════════════════
    
    config.insert(
        "solana".to_string(),
        RpcEndpoint {
            primary: std::env::var("SOLANA_PRIMARY_RPC")
                .or_else(|_| alchemy_url("solana-mainnet").ok_or(""))
                .unwrap_or("https://api.mainnet-beta.solana.com".to_string()),
            fallbacks: vec![
                std::env::var("SOLANA_FALLBACK_1_RPC")
                    .unwrap_or("https://api.rpc.solana.com".to_string()),
                std::env::var("SOLANA_FALLBACK_2_RPC")
                    .unwrap_or("https://solana-api.projectserum.com".to_string()),
            ],
            timeout: Duration::from_secs(15),
            max_retries: 3,
            protocol: BlockchainProtocol::Solana,
            chain_id: None,
        },
    );
    
    config.insert(
        "bitcoin".to_string(),
        RpcEndpoint {
            primary: std::env::var("BITCOIN_BLOCK_EXPLORER")
                .unwrap_or("https://blockchair.com/api/v1".to_string()),
            fallbacks: vec![std::env::var("BITCOIN_FALLBACK")
                .unwrap_or("https://mempool.space/api".to_string())],
            timeout: Duration::from_secs(15),
            max_retries: 3,
            protocol: BlockchainProtocol::Bitcoin,
            chain_id: None,
        },
    );
    
    config.insert(
        "polkadot".to_string(),
        RpcEndpoint {
            primary: std::env::var("POLKADOT_PRIMARY_RPC")
                .unwrap_or("wss://rpc.polkadot.io".to_string()),
            fallbacks: vec![std::env::var("POLKADOT_FALLBACK_1_RPC")
                .unwrap_or("wss://polkadot-rpc.dwellir.com".to_string())],
            timeout: Duration::from_secs(15),
            max_retries: 3,
            protocol: BlockchainProtocol::Polkadot,
            chain_id: None,
        },
    );
    
    config.insert(
        "cardano".to_string(),
        RpcEndpoint {
            primary: std::env::var("CARDANO_RPC")
                .unwrap_or("https://cardano-mainnet.blockfrost.io/api/v0".to_string()),
            fallbacks: vec![],
            timeout: Duration::from_secs(15),
            max_retries: 3,
            protocol: BlockchainProtocol::Cardano,
            chain_id: None,
        },
    );
    
    config.insert(
        "ripple".to_string(),
        RpcEndpoint {
            primary: std::env::var("RIPPLE_PRIMARY_RPC")
                .unwrap_or("https://xrpl.nodies.app".to_string()),
            fallbacks: vec![std::env::var("RIPPLE_FALLBACK_1_RPC")
                .unwrap_or("https://s1.ripple.com:51234".to_string())],
            timeout: Duration::from_secs(15),
            max_retries: 3,
            protocol: BlockchainProtocol::Ripple,
            chain_id: None,
        },
    );
    
    config.insert(
        "tezos".to_string(),
        RpcEndpoint {
            primary: std::env::var("TEZOS_PRIMARY_RPC")
                .unwrap_or("https://mainnet-tezos.giganode.io".to_string()),
            fallbacks: vec![std::env::var("TEZOS_FALLBACK_1_RPC")
                .unwrap_or("https://rpc.tzbeta.com".to_string())],
            timeout: Duration::from_secs(15),
            max_retries: 3,
            protocol: BlockchainProtocol::Tezos,
            chain_id: None,
        },
    );
    
    config
}

/// Get configuration for a specific blockchain
pub fn get_rpc_config(blockchain: &str) -> Option<RpcEndpoint> {
    load_rpc_config().get(blockchain).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_load_rpc_config() {
        let config = load_rpc_config();
        assert!(config.contains_key("ethereum"));
        assert!(config.contains_key("polygon"));
        assert!(config.contains_key("solana"));
        assert!(config.contains_key("bitcoin"));
    }
    
    #[test]
    fn test_ethereum_config() {
        let config = get_rpc_config("ethereum").unwrap();
        assert_eq!(config.protocol, BlockchainProtocol::EVM);
        assert_eq!(config.chain_id, Some("0x1".to_string()));
        assert!(!config.fallbacks.is_empty());
    }
    
    #[test]
    fn test_all_configs_have_primary() {
        let config = load_rpc_config();
        for (name, endpoint) in config.iter() {
            assert!(
                !endpoint.primary.is_empty(),
                "Blockchain {} has no primary RPC",
                name
            );
        }
    }
}
