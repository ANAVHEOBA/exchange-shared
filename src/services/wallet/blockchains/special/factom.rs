use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct FactomDerivation;

impl BlockchainDerivation for FactomDerivation {
    fn coin_type(&self) -> u32 {
        131 // Factom coin type
    }
    
    fn name(&self) -> &'static str {
        "Factom"
    }
    
    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        // Factom uses Ed25519
        // Address format: FA-prefixed
        
        use bip39::{Mnemonic, Language};
        use ed25519_dalek::{SigningKey, VerifyingKey};
        use sha2::{Sha256, Digest};

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");
        
        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        let derived = hasher.finalize();
        
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(&derived[0..32]);
        
        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key: VerifyingKey = signing_key.verifying_key();
        let public_key_bytes = verifying_key.to_bytes();
        
        // Hash public key
        let mut hasher = Sha256::new();
        hasher.update(&public_key_bytes);
        let hash = hasher.finalize();
        
        // Factom addresses start with FA
        Ok(format!("FA{}", hex::encode(&hash[0..20])))
    }
}
