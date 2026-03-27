use async_trait::async_trait;
use serde::Deserialize;
use std::time::Duration;

use super::rpc::{BlockchainProvider, RpcError};

#[derive(Debug, Deserialize)]
struct AlgorandAccountResponse {
    amount: u64,
}

#[derive(Debug, Deserialize)]
struct AlgorandBroadcastResponse {
    #[serde(rename = "txId")]
    tx_id: String,
}

pub struct AlgorandRpcClient {
    client: reqwest::Client,
    url: String,
}

impl AlgorandRpcClient {
    pub fn new(url: String) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
            url,
        }
    }

    fn endpoint(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.url.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }
}

#[async_trait]
impl BlockchainProvider for AlgorandRpcClient {
    async fn get_balance(&self, address: &str) -> Result<f64, RpcError> {
        let response: AlgorandAccountResponse = self
            .client
            .get(self.endpoint(&format!("/v2/accounts/{address}")))
            .send()
            .await
            .map_err(|e| RpcError::Network(e.to_string()))?
            .json()
            .await
            .map_err(|e| RpcError::Parse(e.to_string()))?;

        Ok(response.amount as f64 / 1_000_000.0)
    }

    async fn send_raw_transaction(&self, signed_hex: &str) -> Result<String, RpcError> {
        let signed_bytes = hex::decode(signed_hex.trim_start_matches("0x"))
            .map_err(|e| RpcError::Parse(format!("Invalid Algorand tx hex: {}", e)))?;

        let response: AlgorandBroadcastResponse = self
            .client
            .post(self.endpoint("/v2/transactions"))
            .header("Content-Type", "application/x-binary")
            .body(signed_bytes)
            .send()
            .await
            .map_err(|e| RpcError::Network(e.to_string()))?
            .json()
            .await
            .map_err(|e| RpcError::Parse(e.to_string()))?;

        Ok(response.tx_id)
    }
}
