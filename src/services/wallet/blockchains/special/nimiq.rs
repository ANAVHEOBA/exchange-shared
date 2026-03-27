use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct NimiqDerivation;

impl BlockchainDerivation for NimiqDerivation {
    fn coin_type(&self) -> u32 {
        242
    }

    fn name(&self) -> &'static str {
        "Nimiq"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        // Nimiq uses Ed25519
        // Address format: NQ-prefixed with spaces (user-friendly)
        // BIP44 path: m/44'/242'/0'/0/{index}

        use bip39::{Language, Mnemonic};
        use ed25519_dalek::{SigningKey, VerifyingKey};
        use sha2::{Digest, Sha256};

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

        // Nimiq address = first 20 bytes of public key hash
        let mut hasher = Sha256::new();
        hasher.update(&public_key_bytes);
        let hash = hasher.finalize();

        Ok(format!("NQ{}", hex::encode(&hash[0..20])))
    }
}
