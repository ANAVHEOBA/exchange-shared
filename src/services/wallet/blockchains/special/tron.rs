use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct TronDerivation;

impl BlockchainDerivation for TronDerivation {
    fn coin_type(&self) -> u32 {
        195
    }
    
    fn name(&self) -> &'static str {
        "TRON"
    }
    
    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Mnemonic, Language};
        use secp256k1::{Secp256k1, SecretKey, PublicKey};
        use sha3::{Keccak256, Digest};
        use sha2::Sha256;
        
        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");
        
        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        let derived = hasher.finalize();
        
        let secret_key = SecretKey::from_slice(&derived)
            .map_err(|e| format!("Invalid secret key: {}", e))?;
        
        let secp = Secp256k1::new();
        let public_key = PublicKey::from_secret_key(&secp, &secret_key);
        let pub_bytes = public_key.serialize_uncompressed();
        
        // Hash public key with Keccak256
        let mut hasher = Keccak256::new();
        hasher.update(&pub_bytes[1..]); // Skip 0x04 prefix
        let hash = hasher.finalize();
        
        // Take last 20 bytes and add 0x41 prefix
        let mut payload = vec![0x41u8];
        payload.extend_from_slice(&hash[12..]);
        
        // Calculate checksum
        let mut hasher = Sha256::new();
        hasher.update(&payload);
        let hash1 = hasher.finalize();
        
        let mut hasher = Sha256::new();
        hasher.update(&hash1);
        let hash2 = hasher.finalize();
        
        payload.extend_from_slice(&hash2[0..4]);
        
        Ok(bs58::encode(&payload).into_string())
    }
}
