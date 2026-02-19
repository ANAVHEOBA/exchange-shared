use async_trait::async_trait;
use bitcoin::{
    Address, Amount, Network, OutPoint, ScriptBuf, Transaction, TxIn, TxOut, Witness,
    absolute::LockTime, transaction::Version, Sequence,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::str::FromStr;
use std::time::Duration;

use super::rpc::RpcError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitcoinUtxo {
    pub txid: String,
    pub vout: u32,
    pub amount: f64,
    pub confirmations: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitcoinFeeEstimate {
    pub feerate: f64, // BTC per KB
}

#[async_trait]
pub trait BitcoinProvider: Send + Sync {
    async fn get_utxos(&self, address: &str) -> Result<Vec<BitcoinUtxo>, RpcError>;
    async fn get_balance(&self, address: &str) -> Result<f64, RpcError>;
    async fn estimate_fee(&self, blocks: u32) -> Result<f64, RpcError>;
    async fn broadcast_transaction(&self, tx_hex: &str) -> Result<String, RpcError>;
}

pub struct BitcoinRpcClient {
    client: reqwest::Client,
    url: String,
}

impl BitcoinRpcClient {
    pub fn new(url: String) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
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
            "jsonrpc": "1.0",
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
impl BitcoinProvider for BitcoinRpcClient {
    async fn get_utxos(&self, address: &str) -> Result<Vec<BitcoinUtxo>, RpcError> {
        // Use listunspent RPC call
        let result: Vec<serde_json::Value> = self
            .call_rpc("listunspent", json!([0, 9999999, [address]]))
            .await?;

        let utxos = result
            .into_iter()
            .filter_map(|v| {
                Some(BitcoinUtxo {
                    txid: v.get("txid")?.as_str()?.to_string(),
                    vout: v.get("vout")?.as_u64()? as u32,
                    amount: v.get("amount")?.as_f64()?,
                    confirmations: v.get("confirmations")?.as_u64()? as u32,
                })
            })
            .collect();

        Ok(utxos)
    }

    async fn get_balance(&self, address: &str) -> Result<f64, RpcError> {
        let utxos = self.get_utxos(address).await?;
        Ok(utxos.iter().map(|u| u.amount).sum())
    }

    async fn estimate_fee(&self, blocks: u32) -> Result<f64, RpcError> {
        let result: BitcoinFeeEstimate = self
            .call_rpc("estimatesmartfee", json!([blocks]))
            .await?;
        Ok(result.feerate)
    }

    async fn broadcast_transaction(&self, tx_hex: &str) -> Result<String, RpcError> {
        self.call_rpc("sendrawtransaction", json!([tx_hex]))
            .await
    }
}

/// Build a Bitcoin transaction from UTXOs
pub fn build_bitcoin_transaction(
    utxos: Vec<BitcoinUtxo>,
    to_address: &str,
    amount: f64,
    fee_rate: f64,
    change_address: &str,
) -> Result<Transaction, String> {
    let network = Network::Bitcoin;
    
    let to_addr = Address::from_str(to_address)
        .map_err(|e| format!("Invalid to address: {}", e))?
        .require_network(network)
        .map_err(|e| format!("Address network mismatch: {}", e))?;

    let change_addr = Address::from_str(change_address)
        .map_err(|e| format!("Invalid change address: {}", e))?
        .require_network(network)
        .map_err(|e| format!("Address network mismatch: {}", e))?;

    // Convert BTC to satoshis
    let amount_sats = (amount * 100_000_000.0) as u64;
    
    // Select UTXOs
    let mut selected_utxos = Vec::new();
    let mut total_input = 0u64;
    
    for utxo in utxos {
        selected_utxos.push(utxo.clone());
        total_input += (utxo.amount * 100_000_000.0) as u64;
        
        // Estimate tx size: inputs * 148 + outputs * 34 + 10
        let estimated_size = selected_utxos.len() * 148 + 2 * 34 + 10;
        let estimated_fee = ((fee_rate * estimated_size as f64) / 1000.0) as u64;
        
        if total_input >= amount_sats + estimated_fee {
            break;
        }
    }

    // Calculate final fee
    let tx_size = selected_utxos.len() * 148 + 2 * 34 + 10;
    let fee = ((fee_rate * tx_size as f64) / 1000.0) as u64;
    
    if total_input < amount_sats + fee {
        return Err(format!(
            "Insufficient funds: have {} sats, need {} sats",
            total_input,
            amount_sats + fee
        ));
    }

    let change = total_input - amount_sats - fee;

    // Build transaction inputs
    let inputs: Vec<TxIn> = selected_utxos
        .iter()
        .map(|utxo| TxIn {
            previous_output: OutPoint {
                txid: utxo.txid.parse().unwrap(),
                vout: utxo.vout,
            },
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        })
        .collect();

    // Build transaction outputs
    let mut outputs = vec![TxOut {
        value: Amount::from_sat(amount_sats),
        script_pubkey: to_addr.script_pubkey(),
    }];

    // Add change output if significant
    if change > 546 {
        // 546 sats is dust limit
        outputs.push(TxOut {
            value: Amount::from_sat(change),
            script_pubkey: change_addr.script_pubkey(),
        });
    }

    Ok(Transaction {
        version: Version::TWO,
        lock_time: LockTime::ZERO,
        input: inputs,
        output: outputs,
    })
}
