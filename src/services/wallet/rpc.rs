use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("Network error: {0}")]
    Network(String),
    #[error("RPC error: {0}")]
    Rpc(String),
    #[error("Parse error: {0}")]
    Parse(String),
    #[error("Unsupported operation for this chain")]
    Unsupported,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TronPreparedTransaction {
    #[serde(rename = "txID")]
    pub tx_id: String,
    pub raw_data: serde_json::Value,
    #[serde(default)]
    pub raw_data_hex: Option<String>,
    #[serde(default)]
    pub signature: Vec<String>,
    #[serde(default)]
    pub visible: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TronContractTriggerResult {
    pub result: bool,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TronContractCallResponse {
    #[serde(default)]
    pub result: Option<TronContractTriggerResult>,
    #[serde(default)]
    pub constant_result: Vec<String>,
    #[serde(default)]
    pub transaction: Option<TronPreparedTransaction>,
    #[serde(default)]
    pub energy_used: Option<u64>,
    #[serde(default)]
    pub energy_penalty: Option<u64>,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TronAccountResponse {
    #[serde(default)]
    pub balance: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TronBroadcastResponse {
    pub result: bool,
    #[serde(default)]
    pub txid: Option<String>,
    #[serde(default, rename = "txID")]
    pub tx_id: Option<String>,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CosmosAccountState {
    pub account_number: u64,
    pub sequence: u64,
    pub chain_id: String,
}

#[async_trait]
pub trait BlockchainProvider: Send + Sync {
    // EVM / Common methods
    async fn get_transaction_count(&self, _address: &str) -> Result<u64, RpcError> {
        Err(RpcError::Unsupported)
    }
    async fn get_gas_price(&self) -> Result<u64, RpcError> {
        Err(RpcError::Unsupported)
    }
    async fn send_raw_transaction(&self, _signed_hex: &str) -> Result<String, RpcError> {
        Err(RpcError::Unsupported)
    }
    async fn evm_call(&self, _to_address: &str, _data: &str) -> Result<String, RpcError> {
        Err(RpcError::Unsupported)
    }
    async fn get_balance(&self, address: &str) -> Result<f64, RpcError>;

    // Bitcoin specific methods (added to unified trait)
    async fn get_utxos(
        &self,
        _address: &str,
    ) -> Result<Vec<crate::services::wallet::bitcoin_rpc::BitcoinUtxo>, RpcError> {
        Err(RpcError::Unsupported)
    }
    async fn estimate_fee(&self, _blocks: u32) -> Result<f64, RpcError> {
        Err(RpcError::Unsupported)
    }

    // Solana specific methods (added to unified trait)
    async fn get_recent_blockhash(&self) -> Result<String, RpcError> {
        Err(RpcError::Unsupported)
    }

    // Tron-specific methods
    async fn tron_create_transaction(
        &self,
        _owner_address_hex: &str,
        _to_address_hex: &str,
        _amount_sun: u64,
    ) -> Result<TronPreparedTransaction, RpcError> {
        Err(RpcError::Unsupported)
    }

    async fn tron_trigger_constant_contract(
        &self,
        _owner_address_hex: &str,
        _contract_address_hex: &str,
        _function_selector: &str,
        _parameter_hex: &str,
    ) -> Result<TronContractCallResponse, RpcError> {
        Err(RpcError::Unsupported)
    }

    async fn tron_trigger_smart_contract(
        &self,
        _owner_address_hex: &str,
        _contract_address_hex: &str,
        _function_selector: &str,
        _parameter_hex: &str,
        _fee_limit_sun: u64,
    ) -> Result<TronContractCallResponse, RpcError> {
        Err(RpcError::Unsupported)
    }

    async fn tron_broadcast_transaction(
        &self,
        _transaction: &TronPreparedTransaction,
    ) -> Result<String, RpcError> {
        Err(RpcError::Unsupported)
    }

    async fn cosmos_get_account_state(
        &self,
        _address: &str,
    ) -> Result<CosmosAccountState, RpcError> {
        Err(RpcError::Unsupported)
    }
}

pub struct HttpRpcClient {
    client: reqwest::Client,
    url: String,
}

impl HttpRpcClient {
    pub fn new(url: String) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
            url,
        }
    }

    async fn call_rpc<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, RpcError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        });

        let response = self
            .client
            .post(&self.url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| RpcError::Network(e.to_string()))?;

        let rpc_response: RpcResponse<T> = response
            .json()
            .await
            .map_err(|e| RpcError::Parse(e.to_string()))?;

        if let Some(err) = rpc_response.error {
            return Err(RpcError::Rpc(err.message));
        }

        rpc_response
            .result
            .ok_or_else(|| RpcError::Parse("Missing result".to_string()))
    }

    async fn post_path<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        payload: serde_json::Value,
    ) -> Result<T, RpcError> {
        let url = format!(
            "{}/{}",
            self.url.trim_end_matches('/'),
            path.trim_start_matches('/')
        );

        self.client
            .post(url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| RpcError::Network(e.to_string()))?
            .json()
            .await
            .map_err(|e| RpcError::Parse(e.to_string()))
    }
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcErrorObj>,
}

#[derive(Deserialize)]
struct RpcErrorObj {
    message: String,
}

#[async_trait]
impl BlockchainProvider for HttpRpcClient {
    async fn get_transaction_count(&self, address: &str) -> Result<u64, RpcError> {
        let hex_count: String = self
            .call_rpc("eth_getTransactionCount", json!([address, "latest"]))
            .await?;
        u64::from_str_radix(hex_count.trim_start_matches("0x"), 16)
            .map_err(|e| RpcError::Parse(format!("Invalid nonce hex: {}", e)))
    }

    async fn get_gas_price(&self) -> Result<u64, RpcError> {
        let hex_price: String = self.call_rpc("eth_gasPrice", json!([])).await?;
        u64::from_str_radix(hex_price.trim_start_matches("0x"), 16)
            .map_err(|e| RpcError::Parse(format!("Invalid gas price hex: {}", e)))
    }

    async fn send_raw_transaction(&self, signed_hex: &str) -> Result<String, RpcError> {
        self.call_rpc("eth_sendRawTransaction", json!([signed_hex]))
            .await
    }

    async fn evm_call(&self, to_address: &str, data: &str) -> Result<String, RpcError> {
        self.call_rpc(
            "eth_call",
            json!([
                {
                    "to": to_address,
                    "data": data
                },
                "latest"
            ]),
        )
        .await
    }

    async fn get_balance(&self, address: &str) -> Result<f64, RpcError> {
        let hex_balance: String = self
            .call_rpc("eth_getBalance", json!([address, "latest"]))
            .await?;
        let wei = u128::from_str_radix(hex_balance.trim_start_matches("0x"), 16)
            .map_err(|e| RpcError::Parse(format!("Invalid balance hex: {}", e)))?;
        Ok(wei as f64 / 1_000_000_000_000_000_000.0)
    }

    async fn tron_create_transaction(
        &self,
        owner_address_hex: &str,
        to_address_hex: &str,
        amount_sun: u64,
    ) -> Result<TronPreparedTransaction, RpcError> {
        self.post_path(
            "/wallet/createtransaction",
            json!({
                "owner_address": owner_address_hex,
                "to_address": to_address_hex,
                "amount": amount_sun
            }),
        )
        .await
    }

    async fn tron_trigger_constant_contract(
        &self,
        owner_address_hex: &str,
        contract_address_hex: &str,
        function_selector: &str,
        parameter_hex: &str,
    ) -> Result<TronContractCallResponse, RpcError> {
        self.post_path(
            "/wallet/triggerconstantcontract",
            json!({
                "owner_address": owner_address_hex,
                "contract_address": contract_address_hex,
                "function_selector": function_selector,
                "parameter": parameter_hex
            }),
        )
        .await
    }

    async fn tron_trigger_smart_contract(
        &self,
        owner_address_hex: &str,
        contract_address_hex: &str,
        function_selector: &str,
        parameter_hex: &str,
        fee_limit_sun: u64,
    ) -> Result<TronContractCallResponse, RpcError> {
        self.post_path(
            "/wallet/triggersmartcontract",
            json!({
                "owner_address": owner_address_hex,
                "contract_address": contract_address_hex,
                "function_selector": function_selector,
                "parameter": parameter_hex,
                "fee_limit": fee_limit_sun,
                "call_value": 0
            }),
        )
        .await
    }

    async fn tron_broadcast_transaction(
        &self,
        transaction: &TronPreparedTransaction,
    ) -> Result<String, RpcError> {
        let response: TronBroadcastResponse = self
            .post_path(
                "/wallet/broadcasttransaction",
                serde_json::to_value(transaction).unwrap_or_default(),
            )
            .await?;

        if response.result {
            return response
                .txid
                .or(response.tx_id)
                .or_else(|| Some(transaction.tx_id.clone()))
                .ok_or_else(|| {
                    RpcError::Parse("Missing Tron txid in broadcast response".to_string())
                });
        }

        Err(RpcError::Rpc(
            response
                .message
                .or(response.code)
                .unwrap_or_else(|| "Tron broadcast failed".to_string()),
        ))
    }
}
