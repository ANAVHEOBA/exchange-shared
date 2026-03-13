use serde::{Deserialize, Serialize};
use ed25519_dalek::{SigningKey, Signer};
use blake2::{Blake2b512, Digest};

/// Waves transaction builder
#[derive(Debug, Serialize, Deserialize)]
pub struct WavesTransaction {
    #[serde(rename = "type")]
    pub tx_type: u8,
    pub version: u8,
    #[serde(rename = "senderPublicKey")]
    pub sender_public_key: String,
    pub recipient: String,
    pub amount: u64, // In wavelets (1 WAVES = 10^8 wavelets)
    pub fee: u64,
    pub timestamp: u64,
}

impl WavesTransaction {
    pub fn new_transfer(
        sender_pubkey: &str,
        recipient: &str,
        amount_wavelets: u64,
        fee_wavelets: u64,
    ) -> Self {
        Self {
            tx_type: 4, // Transfer transaction
            version: 2,
            sender_public_key: sender_pubkey.to_string(),
            recipient: recipient.to_string(),
            amount: amount_wavelets,
            fee: fee_wavelets,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }
    
    /// Sign with Curve25519 (Waves uses Curve25519, not Ed25519)
    pub fn sign(&self, private_key: &[u8]) -> Result<String, String> {
        // Serialize transaction
        let tx_bytes = self.to_bytes()?;
        
        // Hash with Blake2b-256
        let mut hasher = Blake2b512::new();
        hasher.update(&tx_bytes);
        let hash = hasher.finalize();
        let hash_256 = &hash[..32];
        
        // Sign (Waves uses Curve25519)
        let signing_key = SigningKey::from_bytes(
            private_key[..32].try_into()
                .map_err(|_| "Invalid key length")?
        );
        let signature = signing_key.sign(hash_256);
        
        Ok(bs58::encode(signature.to_bytes()).into_string())
    }
    
    /// Convert to bytes for signing
    fn to_bytes(&self) -> Result<Vec<u8>, String> {
        let mut bytes = Vec::new();
        bytes.push(self.tx_type);
        bytes.push(self.version);
        // Add other fields in Waves binary format
        // This is simplified - production should use proper binary encoding
        let json = serde_json::to_string(self)
            .map_err(|e| format!("Failed to serialize: {}", e))?;
        bytes.extend_from_slice(json.as_bytes());
        Ok(bytes)
    }
}
