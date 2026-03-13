use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct DockDerivation;

impl BlockchainDerivation for DockDerivation {
    fn coin_type(&self) -> u32 {
        0 // Dock uses Substrate
    }
    
    fn name(&self) -> &'static str {
        "Dock"
    }
    
    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        // Dock is a Substrate-based blockchain
        // Uses SR25519 or Ed25519
        
        use bip39::{Mnemonic, Language};
        use sha2::{Sha256, Digest};
        use blake2::Blake2b512;

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");
        
        // Simplified derivation
        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        hasher.update(b"dock");
        let derived = hasher.finalize();
        
        // SS58 encoding with Dock network ID (22)
        let mut payload = vec![0x16u8]; // Network ID for Dock
        payload.extend_from_slice(&derived);
        
        // Calculate checksum
        let mut hasher = Blake2b512::new();
        hasher.update(b"SS58PRE");
        hasher.update(&payload);
        let checksum_hash = hasher.finalize();
        payload.extend_from_slice(&checksum_hash[0..2]);
        
        Ok(bs58::encode(&payload).into_string())
    }
}
