use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct StellarDerivation;

impl BlockchainDerivation for StellarDerivation {
    fn coin_type(&self) -> u32 {
        148
    }
    
    fn name(&self) -> &'static str {
        "Stellar"
    }
    
    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
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
        
        // Add version byte (0x30 for public key)
        let mut payload = vec![0x30u8];
        payload.extend_from_slice(&public_key_bytes);
        
        // Calculate checksum
        let mut hasher = Sha256::new();
        hasher.update(&payload);
        let hash1 = hasher.finalize();
        
        let mut hasher = Sha256::new();
        hasher.update(&hash1);
        let hash2 = hasher.finalize();
        
        payload.extend_from_slice(&hash2[0..2]);
        
        Ok(bs58::encode(&payload).into_string())
    }
}
