use serde::{Deserialize, Serialize};
use ed25519_dalek::{SigningKey, Signer, Signature};
use sha2::{Sha256, Digest};
use bs58;

/// NEAR Protocol transaction builder
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NearTransaction {
    pub signer_id: String,
    pub public_key: String,
    pub nonce: u64,
    pub receiver_id: String,
    pub block_hash: String,
    pub actions: Vec<NearAction>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", content = "params")]
pub enum NearAction {
    Transfer { deposit: String },
}

impl NearTransaction {
    pub fn new_transfer(
        sender: &str,
        receiver: &str,
        amount_yocto: u128, // 1 NEAR = 10^24 yoctoNEAR
        nonce: u64,
        block_hash: &str,
        public_key: &str,
    ) -> Self {
        Self {
            signer_id: sender.to_string(),
            public_key: public_key.to_string(),
            nonce,
            receiver_id: receiver.to_string(),
            block_hash: block_hash.to_string(),
            actions: vec![NearAction::Transfer {
                deposit: amount_yocto.to_string(),
            }],
        }
    }
    
    /// Sign the transaction with Ed25519
    pub fn sign(&self, private_key: &[u8]) -> Result<SignedNearTransaction, String> {
        // Serialize transaction to borsh format (NEAR uses borsh, not JSON)
        // This is simplified - production should use borsh crate
        let tx_bytes = self.to_borsh()?;
        
        // Hash the transaction
        let mut hasher = Sha256::new();
        hasher.update(&tx_bytes);
        let hash = hasher.finalize();
        
        // Sign with Ed25519
        let signing_key = SigningKey::from_bytes(
            private_key[..32].try_into()
                .map_err(|_| "Invalid key length")?
        );
        let signature: Signature = signing_key.sign(&hash);
        
        Ok(SignedNearTransaction {
            transaction: self.clone(),
            signature: bs58::encode(signature.to_bytes()).into_string(),
            hash: bs58::encode(hash).into_string(),
        })
    }
    
    /// Convert to borsh format (simplified)
    fn to_borsh(&self) -> Result<Vec<u8>, String> {
        // NEAR uses borsh serialization
        // This is a simplified version - production should use borsh crate
        let json = serde_json::to_string(self)
            .map_err(|e| format!("Failed to serialize: {}", e))?;
        Ok(json.into_bytes())
    }
}

#[derive(Debug, Serialize)]
pub struct SignedNearTransaction {
    pub transaction: NearTransaction,
    pub signature: String,
    pub hash: String,
}

/// Get NEAR account nonce and block hash
pub async fn get_near_access_key(
    rpc_url: &str,
    account_id: &str,
    public_key: &str,
) -> Result<NearAccessKey, String> {
    let client = reqwest::Client::new();
    let response = client
        .post(rpc_url)
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": "dontcare",
            "method": "query",
            "params": {
                "request_type": "view_access_key",
                "finality": "final",
                "account_id": account_id,
                "public_key": public_key
            }
        }))
        .send()
        .await
        .map_err(|e| format!("Failed to get access key: {}", e))?;
    
    let result: serde_json::Value = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse response: {}", e))?;
    
    let nonce = result["result"]["nonce"]
        .as_u64()
        .ok_or("Missing nonce")?;
    
    let block_hash = result["result"]["block_hash"]
        .as_str()
        .ok_or("Missing block_hash")?
        .to_string();
    
    Ok(NearAccessKey {
        nonce: nonce + 1, // Increment for next transaction
        block_hash,
    })
}

#[derive(Debug)]
pub struct NearAccessKey {
    pub nonce: u64,
    pub block_hash: String,
}
