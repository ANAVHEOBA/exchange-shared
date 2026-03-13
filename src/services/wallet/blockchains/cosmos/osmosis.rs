use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct OsmosisDerivation;

impl BlockchainDerivation for OsmosisDerivation {
    fn coin_type(&self) -> u32 {
        118
    }
    
    fn name(&self) -> &'static str {
        "Osmosis"
    }
    
    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Mnemonic, Language};
        use secp256k1::{Secp256k1, SecretKey, PublicKey};
        use sha2::{Sha256, Digest};
        use ripemd::Ripemd160;
        
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
        let pub_bytes = public_key.serialize();
        
        // Hash public key
        let mut hasher = Sha256::new();
        hasher.update(&pub_bytes);
        let sha_hash = hasher.finalize();
        
        let mut hasher = Ripemd160::new();
        hasher.update(&sha_hash);
        let account_id = hasher.finalize();
        
        // Simplified bech32 encoding with osmo prefix
        Ok(format!("osmo1{}", hex::encode(&account_id)))
    }
    
    fn derive_private_key(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Mnemonic, Language};
        use sha2::{Sha256, Digest};
        
        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");
        
        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        let derived = hasher.finalize();
        
        Ok(hex::encode(derived))
    }
}
