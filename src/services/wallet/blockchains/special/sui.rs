use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct SuiDerivation;

impl BlockchainDerivation for SuiDerivation {
    fn coin_type(&self) -> u32 {
        784
    }
    
    fn name(&self) -> &'static str {
        "Sui"
    }
    
    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Mnemonic, Language};
        use ed25519_dalek::{SigningKey, VerifyingKey};
        use sha2::{Sha256, Digest};
        use blake2::Blake2b512;
        
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
        
        // Sui address = Blake2b(0x00 || public_key)
        let mut hasher = Blake2b512::new();
        hasher.update(&[0x00]); // Ed25519 flag
        hasher.update(&public_key_bytes);
        let hash = hasher.finalize();
        
        Ok(format!("0x{}", hex::encode(&hash[0..32])))
    }
}
