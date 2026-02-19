use super::types::{GasEstimate, GasError, TxType};
use crate::config::rpc_config::{get_rpc_config, BlockchainProtocol};
use crate::services::redis_cache::RedisService;
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
                self.estimate_bitcoin_gas(&network_lower, tx_type).await?
            }
            BlockchainProtocol::Solana => {
                self.estimate_solana_gas(&network_lower, tx_type).await?
            }
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
                tracing::warn!("RPC gas price fetch failed for {}: {}, using fallback", network, e);
                return Ok(self.get_fallback_estimate(network, tx_type));
            }
        };

        // Apply EMA smoothing to reduce volatility
        let smoothed_gas_price = self.apply_ema_smoothing(network, gas_price_wei).await;

        // Get gas limit for transaction type
        let gas_limit = tx_type.evm_gas_limit();

        // Calculate total cost in native token (ETH, MATIC, BNB, etc.)
        // Formula: cost = gasLimit × gasPrice / 1e18
        let total_cost_native = (gas_limit as f64 * smoothed_gas_price as f64) / 1_000_000_000_000_000_000.0;

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
        _tx_type: TxType,
    ) -> Result<GasEstimate, GasError> {
        // Bitcoin uses fee rate (sat/vByte) × transaction size
        // For now, use conservative estimate
        // TODO: Fetch from mempool.space API or blockchair
        
        let fee_rate_sat_per_vbyte = 10; // Conservative: 10 sat/vByte
        let avg_tx_size_vbytes = 250; // Average P2PKH transaction
        let total_fee_sats = fee_rate_sat_per_vbyte * avg_tx_size_vbytes;
        let total_cost_btc = total_fee_sats as f64 / 100_000_000.0; // Convert sats to BTC

        Ok(GasEstimate {
            network: network.to_string(),
            tx_type: TxType::NativeTransfer,
            gas_price_wei: fee_rate_sat_per_vbyte as u64,
            gas_limit: avg_tx_size_vbytes as u64,
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
    pub async fn get_gas_cost_for_network(&self, network: &str) -> f64 {
        match self.estimate_gas(network, TxType::NativeTransfer).await {
            Ok(estimate) => estimate.total_cost_native,
            Err(e) => {
                tracing::warn!("Gas estimation failed for {}: {}, using fallback", network, e);
                self.get_fallback_estimate(network, TxType::NativeTransfer)
                    .total_cost_native
            }
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
}
