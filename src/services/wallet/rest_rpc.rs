use async_trait::async_trait;
use serde::Deserialize;
use super::rpc::{BlockchainProvider, RpcError};
use super::bitcoin_rpc::BitcoinUtxo;
use std::time::Duration;

pub struct RestRpcClient {
    client: reqwest::Client,
    url: String,
}

impl RestRpcClient {
    pub fn new(url: String) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
            url,
        }
    }

    async fn get_json<T: for<'de> Deserialize<'de>>(&self, url: &str) -> Result<T, RpcError> {
        self.client.get(url)
            .send()
            .await
            .map_err(|e| RpcError::Network(e.to_string()))?
            .json()
            .await
            .map_err(|e| RpcError::Parse(e.to_string()))
    }
}

#[async_trait]
impl BlockchainProvider for RestRpcClient {
    async fn get_balance(&self, address: &str) -> Result<f64, RpcError> {
        if self.url.contains("mempool.space") {
            // Bitcoin Mempool logic
            let res: serde_json::Value = self.get_json(&format!("{}/address/{}", self.url.replace("/api/blocks/tip/height", "/api"), address)).await?;
            let chain_stats = res.get("chain_stats").ok_or(RpcError::Parse("Missing stats".into()))?;
            let funded = chain_stats.get("funded_txo_sum").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let spent = chain_stats.get("spent_txo_sum").and_then(|v| v.as_f64()).unwrap_or(0.0);
            return Ok((funded - spent) / 100_000_000.0);
        } 
        
        if self.url.contains("blockchair.com") {
            // Generic Blockchair logic (BTC, DASH, ZCASH)
            let res: serde_json::Value = self.get_json(&format!("{}/dashboards/address/{}", self.url.replace("/stats", ""), address)).await?;
            let balance = res["data"][address]["address"]["balance"].as_f64().unwrap_or(0.0);
            return Ok(balance / 100_000_000.0);
        }

        if self.url.contains("blockcypher.com") {
            // Dogecoin / LTC logic
            let res: serde_json::Value = self.get_json(&format!("{}/addrs/{}/balance", self.url, address)).await?;
            let balance = res.get("balance").and_then(|v| v.as_f64()).unwrap_or(0.0);
            return Ok(balance / 100_000_000.0);
        }

        // Default fallback for generic REST explorers that return balance directly or in a "balance" field
        let res: serde_json::Value = self.get_json(&format!("{}/address/{}", self.url, address)).await?;
        if let Some(bal) = res.get("balance").and_then(|v| v.as_f64()) {
            Ok(bal)
        } else if let Some(bal) = res.as_f64() {
            Ok(bal)
        } else {
            Err(RpcError::Unsupported)
        }
    }

    async fn get_transaction_count(&self, _address: &str) -> Result<u64, RpcError> { Err(RpcError::Unsupported) }
    async fn get_gas_price(&self) -> Result<u64, RpcError> { Ok(0) }
    async fn send_raw_transaction(&self, _signed_hex: &str) -> Result<String, RpcError> { Err(RpcError::Unsupported) }
    async fn get_utxos(&self, _address: &str) -> Result<Vec<BitcoinUtxo>, RpcError> { Err(RpcError::Unsupported) }
    async fn estimate_fee(&self, _blocks: u32) -> Result<f64, RpcError> { Ok(0.00001) }
    async fn get_recent_blockhash(&self) -> Result<String, RpcError> { Err(RpcError::Unsupported) }
}
