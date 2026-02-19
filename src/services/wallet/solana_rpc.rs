use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use solana_sdk::{
    hash::Hash,
    message::Message,
    pubkey::Pubkey,
    transaction::Transaction,
};
use std::str::FromStr;
use std::time::Duration;

use super::rpc::RpcError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaBalance {
    pub lamports: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaRecentBlockhash {
    pub blockhash: String,
    pub last_valid_block_height: u64,
}

#[async_trait]
pub trait SolanaProvider: Send + Sync {
    async fn get_balance(&self, address: &str) -> Result<f64, RpcError>;
    async fn get_recent_blockhash(&self) -> Result<String, RpcError>;
    async fn send_transaction(&self, tx_base64: &str) -> Result<String, RpcError>;
    async fn get_minimum_balance_for_rent_exemption(&self) -> Result<u64, RpcError>;
}

pub struct SolanaRpcClient {
    client: reqwest::Client,
    url: String,
}

impl SolanaRpcClient {
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

#[derive(Deserialize)]
struct BalanceResult {
    value: u64,
}

#[derive(Deserialize)]
struct BlockhashResult {
    value: BlockhashValue,
}

#[derive(Deserialize)]
struct BlockhashValue {
    blockhash: String,
    #[allow(dead_code)]
    #[serde(rename = "lastValidBlockHeight")]
    last_valid_block_height: u64,
}

#[async_trait]
impl SolanaProvider for SolanaRpcClient {
    async fn get_balance(&self, address: &str) -> Result<f64, RpcError> {
        let result: BalanceResult = self
            .call_rpc("getBalance", json!([address, {"commitment": "confirmed"}]))
            .await?;
        
        // Convert lamports to SOL (1 SOL = 1e9 lamports)
        Ok(result.value as f64 / 1_000_000_000.0)
    }

    async fn get_recent_blockhash(&self) -> Result<String, RpcError> {
        let result: BlockhashResult = self
            .call_rpc("getLatestBlockhash", json!([{"commitment": "finalized"}]))
            .await?;
        
        Ok(result.value.blockhash)
    }

    async fn send_transaction(&self, tx_base64: &str) -> Result<String, RpcError> {
        let result: String = self
            .call_rpc(
                "sendTransaction",
                json!([tx_base64, {"encoding": "base64", "skipPreflight": false}]),
            )
            .await?;
        
        Ok(result)
    }

    async fn get_minimum_balance_for_rent_exemption(&self) -> Result<u64, RpcError> {
        let result: u64 = self
            .call_rpc("getMinimumBalanceForRentExemption", json!([0]))
            .await?;
        
        Ok(result)
    }
}

/// Build a Solana transfer transaction
pub fn build_solana_transaction(
    from_pubkey: &str,
    to_pubkey: &str,
    amount_sol: f64,
    recent_blockhash: &str,
) -> Result<Transaction, String> {
    let from = Pubkey::from_str(from_pubkey)
        .map_err(|e| format!("Invalid from pubkey: {}", e))?;
    
    let to = Pubkey::from_str(to_pubkey)
        .map_err(|e| format!("Invalid to pubkey: {}", e))?;
    
    let _blockhash = Hash::from_str(recent_blockhash)
        .map_err(|e| format!("Invalid blockhash: {}", e))?;

    // Convert SOL to lamports
    let lamports = (amount_sol * 1_000_000_000.0) as u64;

    // Create transfer instruction using solana_sdk directly
    let instruction = solana_sdk::system_instruction::transfer(&from, &to, lamports);

    // Create message
    let message = Message::new(&[instruction], Some(&from));

    // Create unsigned transaction
    Ok(Transaction::new_unsigned(message))
}

/// Sign a Solana transaction with a keypair
pub fn sign_solana_transaction(
    transaction: &mut Transaction,
    keypair_bytes: &[u8],
) -> Result<(), String> {
    let keypair = solana_sdk::signature::Keypair::try_from(keypair_bytes)
        .map_err(|e| format!("Invalid keypair: {}", e))?;
    
    transaction.sign(&[&keypair], transaction.message.recent_blockhash);
    
    Ok(())
}
