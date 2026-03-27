use crate::services::wallet::blockchains::traits::{is_valid_seed_phrase, BlockchainDerivation};
use bip39::{Language, Mnemonic};
use ed25519_dalek::{SigningKey, VerifyingKey};
use sha2::{Digest, Sha256};

pub struct Near;

impl BlockchainDerivation for Near {
    fn coin_type(&self) -> u32 {
        397
    }

    fn name(&self) -> &'static str {
        "NEAR"
    }

    fn derive_address(&self, seed_phrase: &str, index: u32) -> Result<String, String> {
        if !is_valid_seed_phrase(seed_phrase) {
            return Err("Invalid seed phrase".to_string());
        }

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed_phrase)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        let derived_seed = hasher.finalize();

        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(&derived_seed[0..32]);

        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key: VerifyingKey = signing_key.verifying_key();
        let public_key_bytes = verifying_key.to_bytes();

        // NEAR uses hex-encoded public key
        Ok(hex::encode(public_key_bytes))
    }
}

/// Derive NEAR private key for signing
pub async fn derive_near_key(seed_phrase: &str, index: u32) -> Result<String, String> {
    if !is_valid_seed_phrase(seed_phrase) {
        return Err("Invalid seed phrase".to_string());
    }

    let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed_phrase)
        .map_err(|e| format!("Invalid mnemonic: {}", e))?;
    let seed = mnemonic.to_seed("");

    let mut hasher = Sha256::new();
    hasher.update(&seed);
    hasher.update(&index.to_le_bytes());
    let derived_seed = hasher.finalize();

    let mut key_bytes = [0u8; 32];
    key_bytes.copy_from_slice(&derived_seed[0..32]);

    Ok(hex::encode(key_bytes))
}
