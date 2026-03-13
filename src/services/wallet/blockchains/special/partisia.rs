use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct PartisiaDerivation;

impl BlockchainDerivation for PartisiaDerivation {
    fn coin_type(&self) -> u32 {
        0 // Not in BIP44 registry
    }
    
    fn name(&self) -> &'static str {
        "Partisia Blockchain"
    }
    
    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        // Partisia Blockchain uses Ed25519
        // Address format: 00-prefixed hex
        
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
        
        Ok(format!("00{}", hex::encode(&public_key_bytes)))
    }
}
