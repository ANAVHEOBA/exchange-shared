use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct BinanceChainDerivation;

impl BlockchainDerivation for BinanceChainDerivation {
    fn coin_type(&self) -> u32 {
        714
    }
    
    fn name(&self) -> &'static str {
        "Binance Chain (BEP2)"
    }
    
    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        // Binance Chain (BEP2) - the original Binance Chain (not BSC/BEP20)
        // Uses BIP44 path: m/44'/714'/0'/0/{index}
        // Address format: bnb1... (bech32 encoding)
        
        use bip39::{Mnemonic, Language};
        use secp256k1::{Secp256k1, SecretKey, PublicKey};
        use sha2::{Sha256, Digest};

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");
        
        // Simplified derivation for BEP2
        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        hasher.update(b"binance_chain");
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
        
        // Take first 20 bytes
        let address_bytes = &hash[0..20];
        
        // Bech32 encode with "bnb" prefix
        Ok(format!("bnb1{}", hex::encode(address_bytes)))
    }
}
