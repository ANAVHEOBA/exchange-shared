use serde::{Deserialize, Serialize};
use ed25519_dalek::{SigningKey, Signer};
use blake2::{Blake2b512, Digest};

/// Tezos transaction builder
#[derive(Debug, Serialize, Deserialize)]
pub struct TezosTransaction {
    pub branch: String,
    pub contents: Vec<TezosOperation>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TezosOperation {
    pub kind: String,
    pub source: String,
    pub fee: String,
    pub counter: String,
    pub gas_limit: String,
    pub storage_limit: String,
    pub amount: String,
    pub destination: String,
}

impl TezosTransaction {
    pub fn new_transfer(
        source: &str,
        destination: &str,
        amount_mutez: u64, // 1 XTZ = 1,000,000 mutez
        counter: u64,
        branch: &str,
    ) -> Self {
        Self {
            branch: branch.to_string(),
            contents: vec![TezosOperation {
                kind: "transaction".to_string(),
                source: source.to_string(),
                fee: "1420".to_string(), // Standard fee
                counter: counter.to_string(),
                gas_limit: "10600".to_string(),
                storage_limit: "300".to_string(),
                amount: amount_mutez.to_string(),
                destination: destination.to_string(),
            }],
        }
    }
    
    /// Sign with Ed25519
    pub fn sign(&self, private_key: &[u8]) -> Result<String, String> {
        // Forge operation (convert to binary)
        let forged = self.forge()?;
        
        // Hash with Blake2b-256
        let mut hasher = Blake2b512::new();
        hasher.update(&forged);
        let hash = hasher.finalize();
        let hash_256 = &hash[..32];
        
        // Sign
        let signing_key = SigningKey::from_bytes(
            private_key[..32].try_into()
                .map_err(|_| "Invalid key length")?
        );
        let signature = signing_key.sign(hash_256);
        
        Ok(hex::encode(signature.to_bytes()))
    }
    
    /// Forge operation to binary (simplified)
    fn forge(&self) -> Result<Vec<u8>, String> {
        // Tezos uses Micheline encoding
        // This is simplified - production should use proper forging
        let json = serde_json::to_string(self)
            .map_err(|e| format!("Failed to serialize: {}", e))?;
        Ok(json.into_bytes())
    }
}
