use async_trait::async_trait;
use bitcoin::{
    absolute::LockTime, transaction::Version, Address, Amount, Network, OutPoint, ScriptBuf,
    Sequence, Transaction, TxIn, TxOut, Witness,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::str::FromStr;
use std::time::Duration;

use super::rpc::{BlockchainProvider, RpcError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitcoinUtxo {
    pub txid: String,
    pub vout: u32,
    pub amount: f64,
    pub confirmations: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitcoinFeeEstimate {
    pub feerate: f64, // BTC per kvB from Bitcoin Core
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

    fn btc_per_kvb_to_sat_per_vbyte(feerate_btc_per_kvb: f64) -> f64 {
        feerate_btc_per_kvb * 100_000.0
    }
}

fn btc_to_sats(amount_btc: f64) -> Result<u64, String> {
    if !amount_btc.is_finite() || amount_btc < 0.0 {
        return Err("Amount must be a finite, non-negative BTC value".to_string());
    }

    Ok((amount_btc * 100_000_000.0).round() as u64)
}

fn estimated_input_vbytes(address: &str) -> u64 {
    let lower = address.to_lowercase();

    if lower.starts_with("bc1p") {
        58
    } else if lower.starts_with("bc1q") {
        68
    } else if lower.starts_with('3') {
        91
    } else {
        148
    }
}

fn estimated_output_vbytes(address: &str) -> u64 {
    let lower = address.to_lowercase();

    if lower.starts_with("bc1p") {
        43
    } else if lower.starts_with("bc1q") {
        31
    } else if lower.starts_with('3') {
        32
    } else {
        34
    }
}

fn estimate_transaction_vbytes(
    input_count: usize,
    recipient_address: &str,
    include_change: bool,
    change_address: &str,
) -> u64 {
    let input_vbytes = estimated_input_vbytes(change_address) * input_count as u64;
    let mut output_vbytes = estimated_output_vbytes(recipient_address);

    if include_change {
        output_vbytes += estimated_output_vbytes(change_address);
    }

    10 + input_vbytes + output_vbytes
}

pub fn estimate_bitcoin_fee_sats(
    input_count: usize,
    recipient_address: &str,
    fee_rate_sat_per_vbyte: f64,
    include_change: bool,
    change_address: &str,
) -> Result<u64, String> {
    if !fee_rate_sat_per_vbyte.is_finite() || fee_rate_sat_per_vbyte <= 0.0 {
        return Err("Fee rate must be a finite, positive sat/vByte value".to_string());
    }

    let tx_vbytes = estimate_transaction_vbytes(
        input_count,
        recipient_address,
        include_change,
        change_address,
    );
    Ok((fee_rate_sat_per_vbyte * tx_vbytes as f64).ceil() as u64)
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
        let utxos = BitcoinProvider::get_utxos(self, address).await?;
        Ok(utxos.iter().map(|u| u.amount).sum())
    }

    async fn estimate_fee(&self, blocks: u32) -> Result<f64, RpcError> {
        let result: serde_json::Value = self.call_rpc("estimatesmartfee", json!([blocks])).await?;

        let feerate_btc_per_kvb = result
            .get("feerate")
            .and_then(|v| v.as_f64())
            .ok_or_else(|| RpcError::Parse("Invalid feerate in response".to_string()))?;

        Ok(Self::btc_per_kvb_to_sat_per_vbyte(feerate_btc_per_kvb))
    }

    async fn broadcast_transaction(&self, tx_hex: &str) -> Result<String, RpcError> {
        self.call_rpc("sendrawtransaction", json!([tx_hex])).await
    }
}

#[async_trait]
impl BlockchainProvider for BitcoinRpcClient {
    async fn get_balance(&self, address: &str) -> Result<f64, RpcError> {
        BitcoinProvider::get_balance(self, address).await
    }

    async fn get_utxos(&self, address: &str) -> Result<Vec<BitcoinUtxo>, RpcError> {
        BitcoinProvider::get_utxos(self, address).await
    }

    async fn estimate_fee(&self, blocks: u32) -> Result<f64, RpcError> {
        BitcoinProvider::estimate_fee(self, blocks).await
    }

    async fn send_raw_transaction(&self, signed_hex: &str) -> Result<String, RpcError> {
        BitcoinProvider::broadcast_transaction(self, signed_hex).await
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
    let amount_sats = btc_to_sats(amount)?;
    build_bitcoin_transaction_sats(utxos, to_address, amount_sats, fee_rate, change_address)
}

pub fn build_bitcoin_transaction_sats(
    utxos: Vec<BitcoinUtxo>,
    to_address: &str,
    amount_sats: u64,
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

    if !fee_rate.is_finite() || fee_rate <= 0.0 {
        return Err("Fee rate must be a finite, positive sat/vByte value".to_string());
    }

    // Select UTXOs
    let mut selected_utxos = Vec::new();
    let mut total_input = 0u64;

    for utxo in utxos {
        selected_utxos.push(utxo.clone());
        total_input += btc_to_sats(utxo.amount)?;

        let estimated_fee = estimate_bitcoin_fee_sats(
            selected_utxos.len(),
            to_address,
            fee_rate,
            true,
            change_address,
        )?;

        if total_input >= amount_sats + estimated_fee {
            break;
        }
    }

    let fee_without_change = estimate_bitcoin_fee_sats(
        selected_utxos.len(),
        to_address,
        fee_rate,
        false,
        change_address,
    )?;
    let change_without_change = total_input.saturating_sub(amount_sats + fee_without_change);

    let (fee, include_change_output) = if change_without_change > 546 {
        let fee_with_change = estimate_bitcoin_fee_sats(
            selected_utxos.len(),
            to_address,
            fee_rate,
            true,
            change_address,
        )?;
        let change_with_change = total_input.saturating_sub(amount_sats + fee_with_change);

        if change_with_change > 546 {
            (fee_with_change, true)
        } else {
            (fee_without_change, false)
        }
    } else {
        (fee_without_change, false)
    };

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
    if include_change_output && change > 546 {
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

#[cfg(test)]
mod tests {
    use super::{
        build_bitcoin_transaction_sats, estimate_bitcoin_fee_sats, estimate_transaction_vbytes,
        BitcoinRpcClient, BitcoinUtxo,
    };

    #[test]
    fn converts_bitcoin_core_fee_units_to_sat_per_vbyte() {
        let sat_per_vbyte = BitcoinRpcClient::btc_per_kvb_to_sat_per_vbyte(0.0001);
        assert!((sat_per_vbyte - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn estimates_native_segwit_outputs_more_efficiently_than_legacy() {
        let legacy = estimate_transaction_vbytes(
            1,
            "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
            false,
            "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2",
        );
        let native_segwit = estimate_transaction_vbytes(
            1,
            "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
            false,
            "bc1q3q75vwv6s6hq0m2sqqqqqqqqqqqqqqqqf3h8x6",
        );

        assert!(native_segwit < legacy);
    }

    #[test]
    fn sweep_transaction_uses_exact_fee_instead_of_percent_haircut() {
        let utxos = vec![BitcoinUtxo {
            txid: "a".repeat(64),
            vout: 0,
            amount: 0.1,
            confirmations: 6,
        }];
        let recipient = "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh";
        let change = "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2";
        let fee_rate = 10.0;
        let total_input_sats = 10_000_000u64;
        let fee_sats = estimate_bitcoin_fee_sats(1, recipient, fee_rate, false, change).unwrap();
        let amount_sats = total_input_sats - fee_sats;

        let tx = build_bitcoin_transaction_sats(utxos, recipient, amount_sats, fee_rate, change)
            .unwrap();

        assert_eq!(tx.output.len(), 1);
        assert_eq!(tx.output[0].value.to_sat(), amount_sats);
    }
}
