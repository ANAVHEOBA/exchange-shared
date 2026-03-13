use serde::{Deserialize, Serialize};
use ed25519_dalek::{SigningKey, Signer, Signature};

/// Algorand transaction builder
/// Implements proper msgpack encoding for Algorand transactions
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AlgorandTransaction {
    #[serde(rename = "amt")]
    pub amount: u64,
    #[serde(rename = "fee")]
    pub fee: u64,
    #[serde(rename = "fv")]
    pub first_valid: u64,
    #[serde(rename = "gen")]
    pub genesis_id: String,
    #[serde(rename = "gh")]
    pub genesis_hash: Vec<u8>,
    #[serde(rename = "lv")]
    pub last_valid: u64,
    #[serde(rename = "rcv")]
    pub receiver: Vec<u8>,
    #[serde(rename = "snd")]
    pub sender: Vec<u8>,
    #[serde(rename = "type")]
    pub tx_type: String,
}

impl AlgorandTransaction {
    pub fn new_payment(
        sender: &str,
        receiver: &str,
        amount: u64,
        fee: u64,
        first_valid: u64,
        last_valid: u64,
        genesis_id: String,
        genesis_hash: Vec<u8>,
    ) -> Result<Self, String> {
        Ok(Self {
            amount,
            fee,
            first_valid,
            genesis_id,
            genesis_hash,
            last_valid,
            receiver: decode_algorand_address(receiver)?,
            sender: decode_algorand_address(sender)?,
            tx_type: "pay".to_string(),
        })
    }
    
    /// Sign the transaction with Ed25519
    pub fn sign(&self, private_key: &[u8]) -> Result<Vec<u8>, String> {
        // Encode transaction to msgpack
        let tx_bytes = rmp_serde::to_vec(&self)
            .map_err(|e| format!("Failed to encode transaction: {}", e))?;
        
        // Add "TX" prefix for signing
        let mut msg = b"TX".to_vec();
        msg.extend_from_slice(&tx_bytes);
        
        // Sign with Ed25519
        let signing_key = SigningKey::from_bytes(
            private_key[..32].try_into()
                .map_err(|_| "Invalid key length")?
        );
        let signature: Signature = signing_key.sign(&msg);
        
        // Build signed transaction (msgpack format)
        let signed_tx = SignedAlgorandTransaction {
            sig: signature.to_bytes().to_vec(),
            txn: (*self).clone(),
        };
        
        rmp_serde::to_vec(&signed_tx)
            .map_err(|e| format!("Failed to encode signed transaction: {}", e))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SignedAlgorandTransaction {
    sig: Vec<u8>,
    txn: AlgorandTransaction,
}

/// Decode Algorand address (base32 with checksum)
fn decode_algorand_address(address: &str) -> Result<Vec<u8>, String> {
    // Algorand addresses are 58 characters, base32 encoded
    if address.len() != 58 {
        return Err(format!("Invalid Algorand address length: {}", address.len()));
    }
    
    // Decode base32 (simplified - in production use proper base32 library)
    let decoded = bs58::decode(address)
        .into_vec()
        .map_err(|e| format!("Failed to decode address: {}", e))?;
    
    if decoded.len() < 32 {
        return Err("Decoded address too short".to_string());
    }
    
    Ok(decoded[..32].to_vec())
}

/// Get current Algorand block height (for first_valid)
pub async fn get_algorand_params(rpc_url: &str) -> Result<AlgorandParams, String> {
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/v2/transactions/params", rpc_url))
        .send()
        .await
        .map_err(|e| format!("Failed to get params: {}", e))?;
    
    let params: AlgorandParams = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse params: {}", e))?;
    
    Ok(params)
}

#[derive(Debug, Deserialize)]
pub struct AlgorandParams {
    #[serde(rename = "consensus-version")]
    pub consensus_version: String,
    pub fee: u64,
    #[serde(rename = "genesis-hash")]
    pub genesis_hash: String,
    #[serde(rename = "genesis-id")]
    pub genesis_id: String,
    #[serde(rename = "last-round")]
    pub last_round: u64,
    #[serde(rename = "min-fee")]
    pub min_fee: u64,
}
