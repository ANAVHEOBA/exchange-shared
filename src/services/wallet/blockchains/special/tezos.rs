use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct TezosDerivation;

impl BlockchainDerivation for TezosDerivation {
    fn coin_type(&self) -> u32 {
        1729
    }
    
    fn name(&self) -> &'static str {
        "Tezos"
    }
    
    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Mnemonic, Language};
        use ed25519_dalek::{SigningKey, VerifyingKey};
        use blake2::{Blake2b512, Digest};
        
        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");
        
        // Derive key using simple hash-based derivation
        let mut hasher = Blake2b512::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        let derived = hasher.finalize();
        
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(&derived[0..32]);
        
        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key: VerifyingKey = signing_key.verifying_key();
        let public_key_bytes = verifying_key.to_bytes();
        
        // Hash public key with Blake2b (20 bytes)
        let mut hasher = Blake2b512::new();
        hasher.update(&public_key_bytes);
        let hash = hasher.finalize();
        
        // Add tz1 prefix (Ed25519): [6, 161, 159]
        let mut payload = vec![6u8, 161, 159];
        payload.extend_from_slice(&hash[0..20]);
        
        // Calculate checksum
        let mut hasher = Blake2b512::new();
        hasher.update(&payload);
        let checksum_hash = hasher.finalize();
        payload.extend_from_slice(&checksum_hash[0..4]);
        
        Ok(bs58::encode(&payload).into_string())
    }
}
