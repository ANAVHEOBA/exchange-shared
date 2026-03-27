use ed25519_dalek::{Signer, SigningKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// TON (The Open Network) transaction builder
#[derive(Debug, Serialize, Deserialize)]
pub struct TonTransaction {
    pub from: String,
    pub to: String,
    pub amount: String, // In nanotons (1 TON = 10^9 nanotons)
    pub seqno: u32,
    pub timeout: u64,
    pub bounce: bool,
}

impl TonTransaction {
    pub fn new_transfer(from: &str, to: &str, amount_nanotons: u64, seqno: u32) -> Self {
        Self {
            from: from.to_string(),
            to: to.to_string(),
            amount: amount_nanotons.to_string(),
            seqno,
            timeout: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 60,
            bounce: true,
        }
    }

    /// Sign with Ed25519
    pub fn sign(&self, private_key: &[u8]) -> Result<String, String> {
        // Serialize to BOC (Bag of Cells) format
        // This is simplified - production should use ton-labs-types
        let tx_bytes = self.to_boc()?;

        // Hash
        let mut hasher = Sha256::new();
        hasher.update(&tx_bytes);
        let hash = hasher.finalize();

        // Sign
        let signing_key = SigningKey::from_bytes(
            private_key[..32]
                .try_into()
                .map_err(|_| "Invalid key length")?,
        );
        let signature = signing_key.sign(&hash);

        Ok(hex::encode(signature.to_bytes()))
    }

    /// Convert to BOC format (simplified)
    fn to_boc(&self) -> Result<Vec<u8>, String> {
        // TON uses BOC (Bag of Cells) format
        // This is simplified - production should use proper BOC encoding
        let json =
            serde_json::to_string(self).map_err(|e| format!("Failed to serialize: {}", e))?;
        Ok(json.into_bytes())
    }
}
