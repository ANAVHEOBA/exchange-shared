use crate::services::wallet::blockchains::traits::{is_valid_seed_phrase, BlockchainDerivation};
use bip39::{Language, Mnemonic};
use bs58;
use ed25519_dalek::{SigningKey, VerifyingKey};
use sha2::{Digest, Sha256};

pub struct Solana;

impl BlockchainDerivation for Solana {
    fn coin_type(&self) -> u32 {
        501
    }

    fn name(&self) -> &'static str {
        "Solana"
    }

    fn derive_address(&self, seed_phrase: &str, index: u32) -> Result<String, String> {
        if !is_valid_seed_phrase(seed_phrase) {
            return Err("Invalid seed phrase".to_string());
        }

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed_phrase)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        // Solana uses a deterministic derivation from seed + index
        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        let derived_seed = hasher.finalize();

        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(&derived_seed[0..32]);

        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key: VerifyingKey = signing_key.verifying_key();
        let public_key_bytes = verifying_key.to_bytes();

        Ok(bs58::encode(&public_key_bytes).into_string())
    }

    fn derive_private_key(&self, seed_phrase: &str, index: u32) -> Result<String, String> {
        if !is_valid_seed_phrase(seed_phrase) {
            return Err("Invalid seed phrase".to_string());
        }

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed_phrase)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        // Solana uses a deterministic derivation from seed + index
        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        let derived_seed = hasher.finalize();

        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(&derived_seed[0..32]);

        Ok(hex::encode(key_bytes))
    }
}
