use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct WavesDerivation;

impl BlockchainDerivation for WavesDerivation {
    fn coin_type(&self) -> u32 {
        5741
    }

    fn name(&self) -> &'static str {
        "Waves"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use blake2::Blake2b512;
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

        // Hash public key with Blake2b256
        let mut hasher = Blake2b512::new();
        hasher.update(&public_key_bytes);
        let hash = hasher.finalize();

        // Add version (0x01) and chain ID (0x57 for mainnet)
        let mut payload = vec![0x01u8, 0x57];
        payload.extend_from_slice(&hash[0..20]);

        // Calculate checksum
        let mut hasher = Blake2b512::new();
        hasher.update(&payload);
        let checksum_hash = hasher.finalize();
        payload.extend_from_slice(&checksum_hash[0..4]);

        Ok(bs58::encode(&payload).into_string())
    }
}
