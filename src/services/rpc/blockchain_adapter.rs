use super::manager::RpcManager;
use crate::services::wallet::bitcoin_rpc::BitcoinUtxo;
use crate::services::wallet::blockchains::encoding::tron_address_to_hex;
use crate::services::wallet::rpc::{
    BlockchainProvider, RpcError as WalletRpcError, TronAccountResponse, TronBroadcastResponse,
    TronContractCallResponse, TronPreparedTransaction,
};
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;

/// Adapter that makes RpcManager compatible with BlockchainProvider trait
pub struct RpcManagerAdapter {
    manager: Arc<RpcManager>,
    chain: String,
}

impl RpcManagerAdapter {
    pub fn new(manager: Arc<RpcManager>, chain: String) -> Self {
        Self {
            manager,
            chain: normalize_chain_key(&chain),
        }
    }

    fn chain_family(&self) -> &str {
        self.manager.chain_family(&self.chain).unwrap_or("unknown")
    }

    fn is_evm(&self) -> bool {
        self.chain_family() == "evm"
    }

    fn is_solana(&self) -> bool {
        self.chain_family() == "solana" || self.chain == "solana"
    }

    fn is_utxo_family(&self) -> bool {
        matches!(self.chain_family(), "btc" | "utxo")
    }

    fn is_tron(&self) -> bool {
        self.chain == "tron"
    }

    fn btc_per_kvb_to_sat_per_vbyte(feerate_btc_per_kvb: f64) -> f64 {
        feerate_btc_per_kvb * 100_000.0
    }

    fn unsupported_operation(&self, operation: &str) -> WalletRpcError {
        WalletRpcError::Rpc(format!(
            "{} is not supported for chain '{}' (family '{}')",
            operation,
            self.chain,
            self.chain_family()
        ))
    }
}

pub(crate) fn normalize_chain_key(chain: &str) -> String {
    match chain
        .to_ascii_lowercase()
        .replace(' ', "_")
        .replace('-', "_")
        .as_str()
    {
        "eth" | "erc20" => "ethereum".to_string(),
        "smartchain" | "bep20" => "bsc".to_string(),
        "trx" | "trc20" => "tron".to_string(),
        "sol" | "spl" => "solana".to_string(),
        other => other.to_string(),
    }
}

#[async_trait]
impl BlockchainProvider for RpcManagerAdapter {
    async fn get_transaction_count(&self, address: &str) -> Result<u64, WalletRpcError> {
        if !self.is_evm() {
            return Err(self.unsupported_operation("get_transaction_count"));
        }

        let hex_count: String = self
            .manager
            .call(
                &self.chain,
                "eth_getTransactionCount",
                json!([address, "latest"]),
            )
            .await
            .map_err(|e| WalletRpcError::Network(e.to_string()))?;

        u64::from_str_radix(hex_count.trim_start_matches("0x"), 16)
            .map_err(|e| WalletRpcError::Parse(format!("Invalid nonce hex: {}", e)))
    }

    async fn get_gas_price(&self) -> Result<u64, WalletRpcError> {
        if !self.is_evm() {
            return Err(self.unsupported_operation("get_gas_price"));
        }

        let hex_price: String = self
            .manager
            .call(&self.chain, "eth_gasPrice", json!([]))
            .await
            .map_err(|e| WalletRpcError::Network(e.to_string()))?;

        u64::from_str_radix(hex_price.trim_start_matches("0x"), 16)
            .map_err(|e| WalletRpcError::Parse(format!("Invalid gas price hex: {}", e)))
    }

    async fn send_raw_transaction(&self, signed_hex: &str) -> Result<String, WalletRpcError> {
        if self.is_evm() {
            return self
                .manager
                .call(&self.chain, "eth_sendRawTransaction", json!([signed_hex]))
                .await
                .map_err(|e| WalletRpcError::Network(e.to_string()));
        }

        if self.is_solana() {
            return self
                .manager
                .call(
                    &self.chain,
                    "sendTransaction",
                    json!([signed_hex, {"encoding": "base64", "skipPreflight": false}]),
                )
                .await
                .map_err(|e| WalletRpcError::Network(e.to_string()));
        }

        if self.is_utxo_family() {
            return self
                .manager
                .call(&self.chain, "sendrawtransaction", json!([signed_hex]))
                .await
                .map_err(|e| WalletRpcError::Network(e.to_string()));
        }

        Err(self.unsupported_operation("send_raw_transaction"))
    }

    async fn evm_call(&self, to_address: &str, data: &str) -> Result<String, WalletRpcError> {
        if !self.is_evm() {
            return Err(self.unsupported_operation("evm_call"));
        }

        self.manager
            .call(
                &self.chain,
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
            .map_err(|e| WalletRpcError::Network(e.to_string()))
    }

    async fn get_balance(&self, address: &str) -> Result<f64, WalletRpcError> {
        if self.is_tron() {
            let address_hex = tron_address_to_hex(address).map_err(WalletRpcError::Parse)?;
            let account: TronAccountResponse = self
                .manager
                .post_json(
                    &self.chain,
                    "/wallet/getaccount",
                    json!({ "address": address_hex }),
                )
                .await
                .map_err(|e| WalletRpcError::Network(e.to_string()))?;

            return Ok(account.balance as f64 / 1_000_000.0);
        }

        if self.is_solana() {
            let result: serde_json::Value = self
                .manager
                .call(
                    &self.chain,
                    "getBalance",
                    json!([address, {"commitment": "confirmed"}]),
                )
                .await
                .map_err(|e| WalletRpcError::Network(e.to_string()))?;

            let lamports = result["value"]
                .as_u64()
                .ok_or_else(|| WalletRpcError::Parse("Missing lamport balance".to_string()))?;

            return Ok(lamports as f64 / 1_000_000_000.0);
        }

        if self.is_utxo_family() {
            let utxos = self.get_utxos(address).await?;
            return Ok(utxos.iter().map(|utxo| utxo.amount).sum());
        }

        if !self.is_evm() {
            return Err(self.unsupported_operation("get_balance"));
        }

        let hex_balance: String = self
            .manager
            .call(&self.chain, "eth_getBalance", json!([address, "latest"]))
            .await
            .map_err(|e| WalletRpcError::Network(e.to_string()))?;

        let wei = u128::from_str_radix(hex_balance.trim_start_matches("0x"), 16)
            .map_err(|e| WalletRpcError::Parse(format!("Invalid balance hex: {}", e)))?;

        Ok(wei as f64 / 1_000_000_000_000_000_000.0)
    }

    async fn get_utxos(&self, address: &str) -> Result<Vec<BitcoinUtxo>, WalletRpcError> {
        if !self.is_utxo_family() {
            return Err(self.unsupported_operation("get_utxos"));
        }

        // Bitcoin UTXO fetching via RPC
        let utxos: Vec<serde_json::Value> = self
            .manager
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
        if !self.is_utxo_family() {
            return Err(self.unsupported_operation("estimate_fee"));
        }

        let result: serde_json::Value = self
            .manager
            .call(&self.chain, "estimatesmartfee", json!([blocks]))
            .await
            .map_err(|e| WalletRpcError::Network(e.to_string()))?;

        let feerate_btc_per_kvb = result["feerate"]
            .as_f64()
            .ok_or_else(|| WalletRpcError::Parse("Missing feerate".to_string()))?;

        Ok(Self::btc_per_kvb_to_sat_per_vbyte(feerate_btc_per_kvb))
    }

    async fn get_recent_blockhash(&self) -> Result<String, WalletRpcError> {
        if !self.is_solana() {
            return Err(self.unsupported_operation("get_recent_blockhash"));
        }

        let result: serde_json::Value = self
            .manager
            .call(
                &self.chain,
                "getLatestBlockhash",
                json!([{"commitment": "finalized"}]),
            )
            .await
            .map_err(|e| WalletRpcError::Network(e.to_string()))?;

        result["value"]["blockhash"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| WalletRpcError::Parse("Missing blockhash".to_string()))
    }

    async fn tron_create_transaction(
        &self,
        owner_address_hex: &str,
        to_address_hex: &str,
        amount_sun: u64,
    ) -> Result<TronPreparedTransaction, WalletRpcError> {
        if !self.is_tron() {
            return Err(self.unsupported_operation("tron_create_transaction"));
        }

        self.manager
            .post_json(
                &self.chain,
                "/wallet/createtransaction",
                json!({
                    "owner_address": owner_address_hex,
                    "to_address": to_address_hex,
                    "amount": amount_sun
                }),
            )
            .await
            .map_err(|e| WalletRpcError::Network(e.to_string()))
    }

    async fn tron_trigger_constant_contract(
        &self,
        owner_address_hex: &str,
        contract_address_hex: &str,
        function_selector: &str,
        parameter_hex: &str,
    ) -> Result<TronContractCallResponse, WalletRpcError> {
        if !self.is_tron() {
            return Err(self.unsupported_operation("tron_trigger_constant_contract"));
        }

        self.manager
            .post_json(
                &self.chain,
                "/wallet/triggerconstantcontract",
                json!({
                    "owner_address": owner_address_hex,
                    "contract_address": contract_address_hex,
                    "function_selector": function_selector,
                    "parameter": parameter_hex
                }),
            )
            .await
            .map_err(|e| WalletRpcError::Network(e.to_string()))
    }

    async fn tron_trigger_smart_contract(
        &self,
        owner_address_hex: &str,
        contract_address_hex: &str,
        function_selector: &str,
        parameter_hex: &str,
        fee_limit_sun: u64,
    ) -> Result<TronContractCallResponse, WalletRpcError> {
        if !self.is_tron() {
            return Err(self.unsupported_operation("tron_trigger_smart_contract"));
        }

        self.manager
            .post_json(
                &self.chain,
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
            .map_err(|e| WalletRpcError::Network(e.to_string()))
    }

    async fn tron_broadcast_transaction(
        &self,
        transaction: &TronPreparedTransaction,
    ) -> Result<String, WalletRpcError> {
        if !self.is_tron() {
            return Err(self.unsupported_operation("tron_broadcast_transaction"));
        }

        let response: TronBroadcastResponse = self
            .manager
            .post_json(
                &self.chain,
                "/wallet/broadcasttransaction",
                serde_json::to_value(transaction).unwrap_or_default(),
            )
            .await
            .map_err(|e| WalletRpcError::Network(e.to_string()))?;

        if response.result {
            return response
                .txid
                .or(response.tx_id)
                .or_else(|| Some(transaction.tx_id.clone()))
                .ok_or_else(|| {
                    WalletRpcError::Parse("Missing Tron txid in broadcast response".to_string())
                });
        }

        Err(WalletRpcError::Rpc(
            response
                .message
                .or(response.code)
                .unwrap_or_else(|| "Tron broadcast failed".to_string()),
        ))
    }
}
