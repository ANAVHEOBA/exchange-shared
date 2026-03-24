use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct MultiversxDerivation;

impl BlockchainDerivation for MultiversxDerivation {
    fn coin_type(&self) -> u32 {
        508
    }

    fn name(&self) -> &'static str {
        "MultiversX"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
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

        // MultiversX uses bech32 with "erd1" prefix (simplified)
        Ok(format!("erd1{}", hex::encode(&public_key_bytes)))
    }
}
