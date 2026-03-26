use super::{normalize_chain_key, RpcManager, RpcManagerAdapter};
use crate::services::wallet::bitcoin_rpc::BitcoinRpcClient;
use crate::services::wallet::rest_rpc::RestRpcClient;
use crate::services::wallet::rpc::BlockchainProvider;
use crate::services::wallet::solana_rpc::SolanaRpcClient;
use std::sync::Arc;

pub async fn build_provider_for_network(
    manager: Arc<RpcManager>,
    network: &str,
) -> Result<Arc<dyn BlockchainProvider>, String> {
    let chain_key = normalize_chain_key(network);
    let family = manager
        .chain_family(&chain_key)
        .ok_or_else(|| format!("No RPC provider configured for network: {}", network))?
        .to_string();

    if chain_key == "tron" {
        return Ok(Arc::new(RpcManagerAdapter::new(manager, chain_key)));
    }

    match family.as_str() {
        "evm" => Ok(Arc::new(RpcManagerAdapter::new(manager, chain_key))),
        "solana" => {
            let endpoint = manager.select_endpoint(&chain_key).await.map_err(|e| {
                format!(
                    "Failed to select Solana RPC endpoint for {}: {}",
                    network, e
                )
            })?;
            Ok(Arc::new(SolanaRpcClient::new(endpoint)))
        }
        "btc" | "utxo" => {
            let endpoint = manager.select_endpoint(&chain_key).await.map_err(|e| {
                format!("Failed to select UTXO RPC endpoint for {}: {}", network, e)
            })?;

            if is_rest_explorer_url(&endpoint) {
                Ok(Arc::new(RestRpcClient::new(endpoint)))
            } else {
                Ok(Arc::new(BitcoinRpcClient::new(endpoint)))
            }
        }
        other => Err(format!(
            "No chain-specific wallet provider is implemented for network '{}' (family '{}').",
            network, other
        )),
    }
}

fn is_rest_explorer_url(url: &str) -> bool {
    url.contains("mempool.space")
        || url.contains("blockstream.info")
        || url.contains("blockchair.com")
        || url.contains("blockcypher.com")
}

#[cfg(test)]
mod tests {
    use super::build_provider_for_network;
    use crate::services::rpc::{
        CircuitBreakerConfig, LoadBalancingStrategy, RpcConfig, RpcEndpoint, RpcManager,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    fn rpc_manager_for(chain: &str, family: &str) -> Arc<RpcManager> {
        let mut configs = HashMap::new();
        configs.insert(
            chain.to_string(),
            RpcConfig {
                chain: chain.to_string(),
                family: family.to_string(),
                endpoints: vec![RpcEndpoint {
                    url: "http://127.0.0.1:8545".to_string(),
                    priority: 1,
                    weight: 100,
                    max_requests_per_second: None,
                    timeout_ms: 1000,
                    auth: None,
                }],
                strategy: LoadBalancingStrategy::RoundRobin,
                health_check_interval: 30,
                circuit_breaker_config: CircuitBreakerConfig::default(),
            },
        );

        Arc::new(RpcManager::new(configs))
    }

    #[tokio::test]
    async fn supports_evm_networks() {
        let provider =
            build_provider_for_network(rpc_manager_for("ethereum", "evm"), "ethereum").await;
        assert!(provider.is_ok());
    }

    #[tokio::test]
    async fn supports_tron_networks() {
        let provider =
            build_provider_for_network(rpc_manager_for("tron", "special"), "TRC20").await;
        assert!(provider.is_ok());
    }

    #[tokio::test]
    async fn rejects_special_family_without_real_wallet_provider() {
        let err = match build_provider_for_network(rpc_manager_for("cardano", "cardano"), "cardano")
            .await
        {
            Ok(_) => panic!("cardano should not silently fall back to a generic wallet provider"),
            Err(err) => err,
        };
        assert!(err.contains("chain-specific wallet provider"));
    }
}
