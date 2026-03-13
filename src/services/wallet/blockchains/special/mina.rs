use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct MinaDerivation;

impl BlockchainDerivation for MinaDerivation {
    fn coin_type(&self) -> u32 {
        12586
    }
    
    fn name(&self) -> &'static str {
        "Mina"
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
        
        // Mina uses base58check with version byte
        let mut payload = vec![0xCBu8]; // Version byte for mainnet
        payload.extend_from_slice(&public_key_bytes);
        
        // Calculate checksum
        let mut hasher = Sha256::new();
        hasher.update(&payload);
        let hash1 = hasher.finalize();
        
        let mut hasher = Sha256::new();
        hasher.update(&hash1);
        let hash2 = hasher.finalize();
        
        payload.extend_from_slice(&hash2[0..4]);
        
        Ok(format!("B62{}", bs58::encode(&payload).into_string()))
    }
}
