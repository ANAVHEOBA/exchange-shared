use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct PocketDerivation;

impl BlockchainDerivation for PocketDerivation {
    fn coin_type(&self) -> u32 {
        635
    }
    
    fn name(&self) -> &'static str {
        "Pocket Network"
    }
    
    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        // Pocket Network uses Ed25519
        // Address format: hex-encoded public key
        // BIP44 path: m/44'/635'/0'/0/{index}
        
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
        
        Ok(hex::encode(&public_key_bytes))
    }
}
