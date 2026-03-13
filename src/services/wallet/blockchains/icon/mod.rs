use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct IconDerivation;

impl BlockchainDerivation for IconDerivation {
    fn coin_type(&self) -> u32 {
        74
    }
    
    fn name(&self) -> &'static str {
        "ICON"
    }
    
    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Mnemonic, Language};
        use secp256k1::{Secp256k1, SecretKey, PublicKey};
        use sha2::{Sha256, Digest};
        use sha3::Sha3_256;
        
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
        
        // Hash public key with SHA3-256
        let mut hasher = Sha3_256::new();
        hasher.update(&pub_bytes[1..]); // Skip 0x04 prefix
        let hash = hasher.finalize();
        
        // Take last 20 bytes
        let address_bytes = &hash[12..];
        
        Ok(format!("hx{}", hex::encode(address_bytes)))
    }
}
