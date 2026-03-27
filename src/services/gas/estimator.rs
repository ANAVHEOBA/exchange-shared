use super::types::{GasError, GasEstimate, TxType};
use crate::config::rpc_config::{get_rpc_config, BlockchainProtocol};
use crate::services::redis_cache::RedisService;
use crate::services::wallet::bitcoin_rpc::BitcoinRpcClient;
use crate::services::wallet::rest_rpc::RestRpcClient;
use crate::services::wallet::rpc::{BlockchainProvider, HttpRpcClient};
use chrono::Utc;

/// Gas price estimator with multi-tier caching and EMA smoothing
pub struct GasEstimator {
    redis_service: Option<RedisService>,
    /// EMA alpha parameter (0.125 per EIP-1559 spec)
    ema_alpha: f64,
}

impl GasEstimator {
    pub fn new(redis_service: Option<RedisService>) -> Self {
        Self {
            redis_service,
            ema_alpha: 0.125, // EIP-1559 standard
        }
    }

    /// Normalize API-facing ticker/network labels to canonical gas-estimation keys.
    pub fn normalize_payout_network(ticker: &str, network: &str) -> String {
        let ticker_lower = ticker.to_lowercase();
        let network_lower = network.to_lowercase().replace([' ', '-'], "_");

        match network_lower.as_str() {
            "eth" | "erc20" | "ethereum" => "ethereum".to_string(),
            "matic" | "polygon" => "polygon".to_string(),
            "bep20" | "bsc" | "binance_smart_chain" => "bsc".to_string(),
            "arb" | "arbitrum" | "arbitrum_one" => "arbitrum".to_string(),
            "op" | "optimism" => "optimism".to_string(),
            "avax" | "avalanche" | "avalanche_c_chain" => "avalanche".to_string(),
            "btc" | "bitcoin" => "bitcoin".to_string(),
            "sol" | "solana" => "solana".to_string(),
            "ada" | "cardano" => "cardano".to_string(),
            "xrp" | "ripple" | "xrpl" => "ripple".to_string(),
            "xtz" | "tezos" => "tezos".to_string(),
            "mainnet" => match ticker_lower.as_str() {
                "btc" | "bitcoin" => "bitcoin".to_string(),
                "eth" => "ethereum".to_string(),
                "sol" => "solana".to_string(),
                "ada" => "cardano".to_string(),
                "xrp" => "ripple".to_string(),
                "xtz" => "tezos".to_string(),
                "matic" => "polygon".to_string(),
                "bnb" => "bsc".to_string(),
                "arb" => "arbitrum".to_string(),
                "op" => "optimism".to_string(),
                other => other.to_string(),
            },
            _ => network_lower,
        }
    }

    /// Get gas estimate for a network and transaction type
    /// Uses multi-tier caching with Probabilistic Early Recomputation (PER)
    pub async fn estimate_gas(
        &self,
        network: &str,
        tx_type: TxType,
    ) -> Result<GasEstimate, GasError> {
        let network_lower = network.to_lowercase();

        // 1. Try cache first (Tier 1: 10s TTL)
        if let Some(cached) = self.get_cached_estimate(&network_lower, tx_type).await {
            return Ok(cached);
        }

        // 2. Fetch real gas price based on blockchain type
        let rpc_config = get_rpc_config(&network_lower)
            .ok_or_else(|| GasError::UnsupportedNetwork(network.to_string()))?;

        let estimate = match rpc_config.protocol {
            BlockchainProtocol::EVM => {
                self.estimate_evm_gas(&network_lower, tx_type, &rpc_config.primary)
                    .await?
            }
            BlockchainProtocol::Bitcoin => {
                self.estimate_bitcoin_gas(
                    &network_lower,
                    tx_type,
                    &rpc_config.primary,
                    &rpc_config.fallbacks,
                )
                .await?
            }
            BlockchainProtocol::Solana => self.estimate_solana_gas(&network_lower, tx_type).await?,
            _ => {
                // Fallback to hardcoded for unsupported protocols
                self.get_fallback_estimate(&network_lower, tx_type)
            }
        };

        // 3. Cache the result
        self.cache_estimate(&estimate).await;

        Ok(estimate)
    }

    /// Estimate gas for EVM-compatible chains (Ethereum, Polygon, BSC, etc.)
    async fn estimate_evm_gas(
        &self,
        network: &str,
        tx_type: TxType,
        rpc_url: &str,
    ) -> Result<GasEstimate, GasError> {
        let client = HttpRpcClient::new(rpc_url.to_string());

        // Fetch current gas price from RPC
        let gas_price_wei = match client.get_gas_price().await {
            Ok(price) => price,
            Err(e) => {
                tracing::warn!(
                    "RPC gas price fetch failed for {}: {}, using fallback",
                    network,
                    e
                );
                return Ok(self.get_fallback_estimate(network, tx_type));
            }
        };

        // Apply EMA smoothing to reduce volatility
        let smoothed_gas_price = self.apply_ema_smoothing(network, gas_price_wei).await;

        // Get gas limit for transaction type
        let gas_limit = tx_type.evm_gas_limit();

        // Calculate total cost in native token (ETH, MATIC, BNB, etc.)
        // Formula: cost = gasLimit × gasPrice / 1e18
        let total_cost_native =
            (gas_limit as f64 * smoothed_gas_price as f64) / 1_000_000_000_000_000_000.0;

        Ok(GasEstimate {
            network: network.to_string(),
            tx_type,
            gas_price_wei: smoothed_gas_price,
            gas_limit,
            total_cost_native,
            cached: false,
            timestamp: Utc::now(),
        })
    }

    /// Estimate gas for Bitcoin (UTXO-based)
    async fn estimate_bitcoin_gas(
        &self,
        network: &str,
        tx_type: TxType,
        primary_url: &str,
        fallback_urls: &[String],
    ) -> Result<GasEstimate, GasError> {
        let fee_rate_sat_per_vbyte = self
            .fetch_bitcoin_fee_rate_sat_per_vbyte(primary_url, fallback_urls, 6)
            .await?;
        let smoothed_fee_rate = self
            .apply_ema_smoothing(network, fee_rate_sat_per_vbyte.round() as u64)
            .await;
        let tx_size_vbytes = self.bitcoin_tx_size_vbytes(tx_type);
        let total_fee_sats = smoothed_fee_rate as f64 * tx_size_vbytes as f64;
        let total_cost_btc = total_fee_sats / 100_000_000.0;

        Ok(GasEstimate {
            network: network.to_string(),
            tx_type,
            gas_price_wei: smoothed_fee_rate,
            gas_limit: tx_size_vbytes as u64,
            total_cost_native: total_cost_btc,
            cached: false,
            timestamp: Utc::now(),
        })
    }

    /// Estimate gas for Solana
    async fn estimate_solana_gas(
        &self,
        network: &str,
        _tx_type: TxType,
    ) -> Result<GasEstimate, GasError> {
        // Solana uses fixed fee structure: 5,000 lamports base + priority fees
        // 1 SOL = 1,000,000,000 lamports
        let base_fee_lamports = 5_000;
        let priority_fee_lamports = 1_000; // Conservative priority fee
        let total_fee_lamports = base_fee_lamports + priority_fee_lamports;
        let total_cost_sol = total_fee_lamports as f64 / 1_000_000_000.0;

        Ok(GasEstimate {
            network: network.to_string(),
            tx_type: TxType::NativeTransfer,
            gas_price_wei: total_fee_lamports as u64,
            gas_limit: 1,
            total_cost_native: total_cost_sol,
            cached: false,
            timestamp: Utc::now(),
        })
    }

    /// Apply Exponential Moving Average (EMA) smoothing to gas prices
    /// Uses α = 0.125 (1/8) as per EIP-1559 specification
    /// Formula: EMA_new = α × current + (1 - α) × EMA_old
    async fn apply_ema_smoothing(&self, network: &str, current_price: u64) -> u64 {
        let ema_key = format!("gas:{}:ema", network);

        // Get previous EMA from cache
        let previous_ema = if let Some(redis) = &self.redis_service {
            redis
                .get_string(&ema_key)
                .await
                .ok()
                .flatten()
                .and_then(|s| s.parse::<u64>().ok())
        } else {
            None
        };

        let smoothed = if let Some(prev) = previous_ema {
            // Apply EMA formula
            let alpha = self.ema_alpha;
            let new_ema = (alpha * current_price as f64) + ((1.0 - alpha) * prev as f64);
            new_ema as u64
        } else {
            // First time, use current price as EMA
            current_price
        };

        // Store new EMA in cache (60s TTL for smoothing window)
        if let Some(redis) = &self.redis_service {
            let _ = redis.set_string(&ema_key, &smoothed.to_string(), 60).await;
        }

        smoothed
    }

    /// Get cached gas estimate (Tier 1 cache: 10s TTL)
    async fn get_cached_estimate(&self, network: &str, tx_type: TxType) -> Option<GasEstimate> {
        let redis = self.redis_service.as_ref()?;
        let cache_key = format!("gas:{}:{:?}:estimate", network, tx_type);

        let cached_json = redis.get_string(&cache_key).await.ok()??;
        serde_json::from_str(&cached_json).ok()
    }

    /// Cache gas estimate (10s TTL)
    async fn cache_estimate(&self, estimate: &GasEstimate) {
        if let Some(redis) = &self.redis_service {
            let cache_key = format!("gas:{}:{:?}:estimate", estimate.network, estimate.tx_type);
            if let Ok(json) = serde_json::to_string(estimate) {
                let _ = redis.set_string(&cache_key, &json, 10).await;
            }
        }
    }

    /// Fallback to hardcoded estimates when RPC fails
    fn get_fallback_estimate(&self, network: &str, tx_type: TxType) -> GasEstimate {
        let total_cost_native = match network {
            "ethereum" | "erc20" => 0.002,
            "polygon" | "bsc" | "arbitrum" | "optimism" => 0.001,
            "bitcoin" => 0.0001,
            "solana" | "sol" => 0.00001,
            _ => 0.001,
        };

        GasEstimate {
            network: network.to_string(),
            tx_type,
            gas_price_wei: 0,
            gas_limit: tx_type.evm_gas_limit(),
            total_cost_native,
            cached: false,
            timestamp: Utc::now(),
        }
    }

    /// Get simple gas cost for backward compatibility
    pub async fn try_get_gas_cost_for_network(&self, network: &str) -> Result<f64, GasError> {
        Ok(self
            .estimate_gas(network, TxType::NativeTransfer)
            .await?
            .total_cost_native)
    }

    /// Get simple gas cost for backward compatibility
    pub async fn get_gas_cost_for_network(&self, network: &str) -> f64 {
        match self.try_get_gas_cost_for_network(network).await {
            Ok(cost) => cost,
            Err(e) => {
                tracing::warn!(
                    "Gas estimation failed for {}: {}, using fallback",
                    network,
                    e
                );
                self.get_fallback_estimate(network, TxType::NativeTransfer)
                    .total_cost_native
            }
        }
    }

    async fn fetch_bitcoin_fee_rate_sat_per_vbyte(
        &self,
        primary_url: &str,
        fallback_urls: &[String],
        blocks: u32,
    ) -> Result<f64, GasError> {
        let mut last_error = None;

        for url in std::iter::once(primary_url).chain(fallback_urls.iter().map(String::as_str)) {
            if url.trim().is_empty() {
                continue;
            }

            let result = if self.is_bitcoin_rest_endpoint(url) {
                RestRpcClient::new(url.to_string())
                    .estimate_fee(blocks)
                    .await
            } else {
                BitcoinRpcClient::new(url.to_string())
                    .estimate_fee(blocks)
                    .await
            };

            match result {
                Ok(fee_rate) if fee_rate.is_finite() && fee_rate > 0.0 => return Ok(fee_rate),
                Ok(_) => {
                    last_error = Some(format!("Endpoint {} returned a non-positive fee rate", url))
                }
                Err(e) => last_error = Some(format!("{}: {}", url, e)),
            }
        }

        Err(GasError::Rpc(last_error.unwrap_or_else(|| {
            "No live Bitcoin fee endpoint was available".to_string()
        })))
    }

    fn is_bitcoin_rest_endpoint(&self, url: &str) -> bool {
        url.contains("mempool.space")
            || url.contains("blockstream.info")
            || url.contains("blockchair.com")
            || url.contains("blockcypher.com")
            || url.contains("/api/")
    }

    fn bitcoin_tx_size_vbytes(&self, tx_type: TxType) -> u64 {
        match tx_type {
            TxType::NativeTransfer => 250,
            TxType::TokenTransfer | TxType::TokenApprove | TxType::ComplexContract => 250,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fallback_estimates() {
        let estimator = GasEstimator::new(None);

        let eth_cost = estimator.get_gas_cost_for_network("ethereum").await;
        assert!(eth_cost > 0.0);

        let btc_cost = estimator.get_gas_cost_for_network("bitcoin").await;
        assert!(btc_cost > 0.0);

        let sol_cost = estimator.get_gas_cost_for_network("solana").await;
        assert!(sol_cost > 0.0);
    }

    #[tokio::test]
    async fn test_tx_type_gas_limits() {
        assert_eq!(TxType::NativeTransfer.evm_gas_limit(), 21_000);
        assert_eq!(TxType::TokenTransfer.evm_gas_limit(), 65_000);
        assert_eq!(TxType::TokenApprove.evm_gas_limit(), 45_000);
    }

    #[test]
    fn test_ema_alpha() {
        let estimator = GasEstimator::new(None);
        assert_eq!(estimator.ema_alpha, 0.125); // EIP-1559 standard
    }

    #[test]
    fn normalize_payout_network_handles_api_aliases() {
        assert_eq!(
            GasEstimator::normalize_payout_network("ETH", "ERC20"),
            "ethereum"
        );
        assert_eq!(
            GasEstimator::normalize_payout_network("BTC", "Mainnet"),
            "bitcoin"
        );
        assert_eq!(
            GasEstimator::normalize_payout_network("BNB", "BEP20"),
            "bsc"
        );
    }

    #[test]
    fn recognizes_bitcoin_rest_fee_endpoints() {
        let estimator = GasEstimator::new(None);

        assert!(estimator.is_bitcoin_rest_endpoint("https://mempool.space/api"));
        assert!(estimator.is_bitcoin_rest_endpoint("https://blockstream.info/testnet/api"));
        assert!(!estimator.is_bitcoin_rest_endpoint("http://127.0.0.1:8332"));
    }
}
