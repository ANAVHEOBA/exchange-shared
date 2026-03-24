use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct AptosDerivation;

impl BlockchainDerivation for AptosDerivation {
    fn coin_type(&self) -> u32 {
        637
    }

    fn name(&self) -> &'static str {
        "Aptos"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use ed25519_dalek::{SigningKey, VerifyingKey};
        use sha2::{Digest, Sha256};
        use sha3::Sha3_256;

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        let derived = hasher.finalize();

        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(&derived[0..32]);

        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key: VerifyingKey = signing_key.verifying_key();
        let public_key_bytes = verifying_key.to_bytes();

        // Aptos address = SHA3-256(public_key || 0x00)
        let mut hasher = Sha3_256::new();
        hasher.update(&public_key_bytes);
        hasher.update(&[0x00]); // Single-sig scheme
        let hash = hasher.finalize();

        Ok(format!("0x{}", hex::encode(&hash)))
    }
}
