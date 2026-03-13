use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct StarknetDerivation;

impl BlockchainDerivation for StarknetDerivation {
    fn coin_type(&self) -> u32 {
        9004
    }
    
    fn name(&self) -> &'static str {
        "Starknet"
    }
    
    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Mnemonic, Language};
        use sha2::{Sha256, Digest};
        
        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");
        
        // Simplified Starknet derivation (actual uses Stark curve)
        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        hasher.update(b"starknet");
        let derived = hasher.finalize();
        
        Ok(format!("0x{}", hex::encode(&derived)))
    }
}
