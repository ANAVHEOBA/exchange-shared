use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use secp256k1::{Secp256k1, Message, SecretKey};

/// Tron transaction builder
/// Implements proper protobuf-like format for Tron transactions
#[derive(Debug, Serialize, Deserialize)]
pub struct TronTransaction {
    #[serde(rename = "txID")]
    pub tx_id: String,
    pub raw_data: TronRawData,
    pub signature: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TronRawData {
    pub contract: Vec<TronContract>,
    pub ref_block_bytes: String,
    pub ref_block_hash: String,
    pub expiration: u64,
    pub timestamp: u64,
    pub fee_limit: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TronContract {
    pub parameter: TronParameter,
    #[serde(rename = "type")]
    pub contract_type: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TronParameter {
    pub value: TronTransferValue,
    pub type_url: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TronTransferValue {
    pub amount: u64,
    pub owner_address: String,
    pub to_address: String,
}

impl TronTransaction {
    pub fn new_transfer(
        from: &str,
        to: &str,
        amount_sun: u64, // 1 TRX = 1,000,000 SUN
        ref_block_bytes: &str,
        ref_block_hash: &str,
        expiration: u64,
    ) -> Self {
        let raw_data = TronRawData {
            contract: vec![TronContract {
                parameter: TronParameter {
                    value: TronTransferValue {
                        amount: amount_sun,
                        owner_address: from.to_string(),
                        to_address: to.to_string(),
                    },
                    type_url: "type.googleapis.com/protocol.TransferContract".to_string(),
                },
                contract_type: "TransferContract".to_string(),
            }],
            ref_block_bytes: ref_block_bytes.to_string(),
            ref_block_hash: ref_block_hash.to_string(),
            expiration,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            fee_limit: 10_000_000, // 10 TRX max fee
        };
        
        // Calculate transaction ID (SHA256 of raw_data)
        let raw_json = serde_json::to_string(&raw_data).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(raw_json.as_bytes());
        let tx_id = hex::encode(hasher.finalize());
        
        Self {
            tx_id,
            raw_data,
            signature: vec![],
        }
    }
    
    /// Sign the transaction with Secp256k1
    pub fn sign(&mut self, private_key_hex: &str) -> Result<(), String> {
        let secp = Secp256k1::new();
        
        // Parse private key
        let secret_key = SecretKey::from_slice(
            &hex::decode(private_key_hex.trim_start_matches("0x"))
                .map_err(|e| format!("Invalid private key: {}", e))?
        ).map_err(|e| format!("Invalid secret key: {}", e))?;
        
        // Hash transaction ID
        let tx_id_bytes = hex::decode(&self.tx_id)
            .map_err(|e| format!("Invalid tx_id: {}", e))?;
        
        let message = Message::from_digest_slice(&tx_id_bytes)
            .map_err(|e| format!("Invalid message: {}", e))?;
        
        // Sign
        let signature = secp.sign_ecdsa(&message, &secret_key);
        
        // Add signature
        self.signature.push(hex::encode(signature.serialize_compact()));
        
        Ok(())
    }
    
    /// Convert to JSON for broadcast
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

/// Get latest block info for ref_block
pub async fn get_tron_block_info(rpc_url: &str) -> Result<TronBlockInfo, String> {
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/wallet/getnowblock", rpc_url))
        .send()
        .await
        .map_err(|e| format!("Failed to get block: {}", e))?;
    
    let block: serde_json::Value = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse block: {}", e))?;
    
    let block_header = block["block_header"]["raw_data"]
        .as_object()
        .ok_or("Missing block header")?;
    
    let number = block_header["number"]
        .as_u64()
        .ok_or("Missing block number")?;
    
    let timestamp = block_header["timestamp"]
        .as_u64()
        .ok_or("Missing timestamp")?;
    
    // Calculate ref_block_bytes (last 2 bytes of block number)
    let ref_block_bytes = format!("{:04x}", number & 0xFFFF);
    
    // Get block hash for ref_block_hash
    let block_id = block["blockID"]
        .as_str()
        .ok_or("Missing blockID")?;
    let ref_block_hash = block_id[16..32].to_string(); // Bytes 8-16 of block hash
    
    Ok(TronBlockInfo {
        ref_block_bytes,
        ref_block_hash,
        expiration: timestamp + 60000, // 60 seconds from now
    })
}

#[derive(Debug)]
pub struct TronBlockInfo {
    pub ref_block_bytes: String,
    pub ref_block_hash: String,
    pub expiration: u64,
}
