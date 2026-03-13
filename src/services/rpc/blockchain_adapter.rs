use async_trait::async_trait;
use std::sync::Arc;
use serde_json::json;
use crate::services::wallet::rpc::{BlockchainProvider, RpcError as WalletRpcError};
use crate::services::wallet::bitcoin_rpc::BitcoinUtxo;
use super::manager::RpcManager;

/// Adapter that makes RpcManager compatible with BlockchainProvider trait
pub struct RpcManagerAdapter {
    manager: Arc<RpcManager>,
    chain: String,
}

impl RpcManagerAdapter {
    pub fn new(manager: Arc<RpcManager>, chain: String) -> Self {
        Self { manager, chain }
    }
}

#[async_trait]
impl BlockchainProvider for RpcManagerAdapter {
    async fn get_transaction_count(&self, address: &str) -> Result<u64, WalletRpcError> {
        let hex_count: String = self.manager
            .call(&self.chain, "eth_getTransactionCount", json!([address, "latest"]))
            .await
            .map_err(|e| WalletRpcError::Network(e.to_string()))?;
        
        u64::from_str_radix(hex_count.trim_start_matches("0x"), 16)
            .map_err(|e| WalletRpcError::Parse(format!("Invalid nonce hex: {}", e)))
    }

    async fn get_gas_price(&self) -> Result<u64, WalletRpcError> {
        let hex_price: String = self.manager
            .call(&self.chain, "eth_gasPrice", json!([]))
            .await
            .map_err(|e| WalletRpcError::Network(e.to_string()))?;
        
        u64::from_str_radix(hex_price.trim_start_matches("0x"), 16)
            .map_err(|e| WalletRpcError::Parse(format!("Invalid gas price hex: {}", e)))
    }

    async fn send_raw_transaction(&self, signed_hex: &str) -> Result<String, WalletRpcError> {
        self.manager
            .call(&self.chain, "eth_sendRawTransaction", json!([signed_hex]))
            .await
            .map_err(|e| WalletRpcError::Network(e.to_string()))
    }

    async fn get_balance(&self, address: &str) -> Result<f64, WalletRpcError> {
        let hex_balance: String = self.manager
            .call(&self.chain, "eth_getBalance", json!([address, "latest"]))
            .await
            .map_err(|e| WalletRpcError::Network(e.to_string()))?;
        
        let wei = u128::from_str_radix(hex_balance.trim_start_matches("0x"), 16)
            .map_err(|e| WalletRpcError::Parse(format!("Invalid balance hex: {}", e)))?;
        
        Ok(wei as f64 / 1_000_000_000_000_000_000.0)
    }

    async fn get_utxos(&self, address: &str) -> Result<Vec<BitcoinUtxo>, WalletRpcError> {
        // Bitcoin UTXO fetching via RPC
        let utxos: Vec<serde_json::Value> = self.manager
            .call(&self.chain, "listunspent", json!([0, 9999999, [address]]))
            .await
            .map_err(|e| WalletRpcError::Network(e.to_string()))?;
        
        let mut result = Vec::new();
        for utxo in utxos {
            result.push(BitcoinUtxo {
                txid: utxo["txid"].as_str().unwrap_or("").to_string(),
                vout: utxo["vout"].as_u64().unwrap_or(0) as u32,
                amount: utxo["amount"].as_f64().unwrap_or(0.0),
                confirmations: utxo["confirmations"].as_u64().unwrap_or(0) as u32,
            });
        }
        
        Ok(result)
    }

    async fn estimate_fee(&self, blocks: u32) -> Result<f64, WalletRpcError> {
        let result: serde_json::Value = self.manager
            .call(&self.chain, "estimatesmartfee", json!([blocks]))
            .await
            .map_err(|e| WalletRpcError::Network(e.to_string()))?;
        
        result["feerate"]
            .as_f64()
            .ok_or_else(|| WalletRpcError::Parse("Missing feerate".to_string()))
    }

    async fn get_recent_blockhash(&self) -> Result<String, WalletRpcError> {
        let result: serde_json::Value = self.manager
            .call(&self.chain, "getRecentBlockhash", json!([]))
            .await
            .map_err(|e| WalletRpcError::Network(e.to_string()))?;
        
        result["value"]["blockhash"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| WalletRpcError::Parse("Missing blockhash".to_string()))
    }
}
