use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct HederaDerivation;

impl BlockchainDerivation for HederaDerivation {
    fn coin_type(&self) -> u32 {
        3030
    }

    fn name(&self) -> &'static str {
        "Hedera"
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

        // Hedera uses account ID format: shard.realm.num
        // For simplicity, derive num from public key hash
        let mut hasher = Sha256::new();
        hasher.update(&public_key_bytes);
        let hash = hasher.finalize();
        let account_num = u64::from_be_bytes([
            hash[0], hash[1], hash[2], hash[3], hash[4], hash[5], hash[6], hash[7],
        ]) % 1_000_000_000;

        Ok(format!("0.0.{}", account_num))
    }
}
