use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct AvalancheXDerivation;

impl BlockchainDerivation for AvalancheXDerivation {
    fn coin_type(&self) -> u32 {
        9000 // Avalanche X-Chain
    }
    
    fn name(&self) -> &'static str {
        "Avalanche X-Chain"
    }
    
    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        // Avalanche X-Chain uses secp256k1
        // Address format: X-avax1... (bech32)
        
        use bip39::{Mnemonic, Language};
        use secp256k1::{Secp256k1, SecretKey, PublicKey};
        use sha2::{Sha256, Digest};

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");
        
        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        hasher.update(b"avalanche_x");
        let derived = hasher.finalize();
        
        let secret_key = SecretKey::from_slice(&derived)
            .map_err(|e| format!("Invalid secret key: {}", e))?;
        
        let secp = Secp256k1::new();
        let public_key = PublicKey::from_secret_key(&secp, &secret_key);
        let pub_bytes = public_key.serialize();
        
        // Hash public key
        let mut hasher = Sha256::new();
        hasher.update(&pub_bytes);
        let hash = hasher.finalize();
        
        // X-Chain addresses use bech32 with "avax" prefix
        Ok(format!("X-avax1{}", hex::encode(&hash[0..20])))
    }
}
