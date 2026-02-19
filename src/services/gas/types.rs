use serde::{Deserialize, Serialize};

/// Transaction types with different gas requirements
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TxType {
    /// Native token transfer (21,000 gas for EVM)
    NativeTransfer,
    /// ERC20/Token transfer (65,000 gas avg)
    TokenTransfer,
    /// ERC20 approve operation (45,000 gas avg)
    TokenApprove,
    /// Complex contract interaction (use eth_estimateGas)
    ComplexContract,
}

impl TxType {
    /// Get estimated gas limit for EVM chains
    pub fn evm_gas_limit(&self) -> u64 {
        match self {
            TxType::NativeTransfer => 21_000,
            TxType::TokenTransfer => 65_000,
            TxType::TokenApprove => 45_000,
            TxType::ComplexContract => 150_000, // Conservative estimate
        }
    }
}

/// Gas price estimate result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GasEstimate {
    /// Network name
    pub network: String,
    /// Transaction type
    pub tx_type: TxType,
    /// Gas price in wei (for EVM) or smallest unit
    pub gas_price_wei: u64,
    /// Gas limit (for EVM chains)
    pub gas_limit: u64,
    /// Total cost in native token (ETH, BTC, SOL, etc.)
    pub total_cost_native: f64,
    /// Whether this is from cache
    pub cached: bool,
    /// Timestamp of estimate
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, thiserror::Error)]
pub enum GasError {
    #[error("RPC error: {0}")]
    Rpc(String),
    #[error("Network not supported: {0}")]
    UnsupportedNetwork(String),
    #[error("Cache error: {0}")]
    Cache(String),
    #[error("Parse error: {0}")]
    Parse(String),
}
